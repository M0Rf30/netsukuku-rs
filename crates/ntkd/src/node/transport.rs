//! Production-only transport composition: binds the real [`ntk_rpc::TcpServer`] and one
//! [`ntk_rpc::UdpBroadcaster`] per configured NIC, spawns [`ntk_neighborhood::Manager`] against
//! the real [`ntk_netlink::RealNetlink`] backend (the one call site allowed to name that
//! concrete type — see `crate::node::lifecycle`'s module doc on why `Manager<K>` can't be
//! spawned generically), and wires everything into [`crate::node::lifecycle::run`].

use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use ntk_neighborhood::{
    IcmpRttProbe, LocalNic, NeighborhoodConfig, NeighborhoodRpcHandler, NeighborhoodTiming, NodeId,
};
use ntk_netlink::RealNetlink;
use ntk_rpc::{RpcError, TcpServer, UdpBroadcaster};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::kernel::config::NtkdConfig;
use crate::node::ip_route::RealIpRouteManager;
use crate::node::lifecycle::{
    self, NodeInputs, StartedNode, TcpDialer, linklocal_allocator, synthetic_mac,
};
use crate::node::peers::PeerLinks;
use crate::node::registry::LinkRegistry;
use crate::node::stubs::NeighborhoodStubFactoryAdapter;

/// Turns a failed socket-bind syscall into a message naming the transport, the device (if any),
/// and the port — and, when the kernel refused the bind because the port is privileged, naming
/// the exact capability that fixes it. The one place the "was this EACCES/EPERM binding a port
/// below 1024" branch lives, so the UDP and TCP bind sites in [`start`] cannot drift apart.
///
/// Deliberately does not pre-check `CAP_NET_BIND_SERVICE` or `port < 1024` before calling
/// `bind`: the kernel, not this function, is the authority on whether an unprivileged process
/// may claim a given port right now. `net.ipv4.ip_unprivileged_port_start` can be lowered below
/// `port` (or the capability granted some other way, e.g. running as root), in which case
/// binding port 269 unprivileged legitimately succeeds — a pre-check keyed purely on
/// `port < 1024` would then refuse to start even though the kernel would have allowed it.
/// Interpreting the real errno of an actual failed bind can never be wrong that way.
fn describe_bind_failure(
    transport: &str,
    device: Option<&str>,
    port: u16,
    source: &io::Error,
) -> String {
    let target = match device {
        Some(dev) => format!("{transport} socket on {dev:?} port {port}"),
        None => format!("{transport} socket on port {port}"),
    };
    // `ErrorKind::PermissionDenied` covers EACCES; EPERM (raw os error 1) is included
    // explicitly since some kernels/LSMs report a privileged-port bind refusal that way and
    // std does not always classify it as `PermissionDenied`.
    let is_permission_denied = source.kind() == io::ErrorKind::PermissionDenied
        || matches!(source.raw_os_error(), Some(1) | Some(13));
    if is_permission_denied && port < 1024 {
        format!(
            "failed to bind {target}: {source} — ports below 1024 are privileged; grant \
             CAP_NET_BIND_SERVICE (AmbientCapabilities in the systemd unit) or set a port >= \
             1024 in the ntkd config"
        )
    } else {
        format!("failed to bind {target}: {source}")
    }
}

/// Per-identity cap on exported arcs. No config knob exists for this in the batch contract's
/// `NtkdConfig` (gsizes/nics/port only); a generous fixed default is used instead of inventing
/// an unrequested config surface.
const MAX_ARCS: usize = 64;

/// Runs the full production startup: binds transport, spawns neighborhood against the real
/// kernel, then delegates to [`lifecycle::run`] for the rest.
///
/// # Errors
/// Any I/O failure binding sockets, or an error from [`lifecycle::run`].
pub async fn start(
    config: NtkdConfig,
    nics: &[String],
    tasks: &mut JoinSet<()>,
    cancel: CancellationToken,
) -> anyhow::Result<StartedNode<RealNetlink>> {
    let registry = Arc::new(LinkRegistry::new());
    let links = Arc::new(PeerLinks::new());

    let netlink = RealNetlink::new()?;
    crate::kernel::preflight::check_nics(&netlink, nics).await?;
    crate::kernel::preflight::warn_address_space_conflicts(&netlink).await;

    let mut broadcasters = HashMap::new();
    for nic in nics {
        let broadcaster = match UdpBroadcaster::bind(Some(nic), config.port(), 1 << 16) {
            Ok(broadcaster) => broadcaster,
            Err(RpcError::Io(source)) => {
                return Err(anyhow::anyhow!(describe_bind_failure(
                    "UDP broadcast",
                    Some(nic),
                    config.port(),
                    &source
                )));
            }
            Err(other) => return Err(other.into()),
        };
        broadcasters.insert(nic.clone(), Arc::new(broadcaster));
    }

    let neighborhood_stub_factory = Arc::new(NeighborhoodStubFactoryAdapter {
        broadcasters: broadcasters.clone(),
        links: links.clone(),
        registry: registry.clone(),
    });
    let my_id = NodeId::generate();
    let signing_key = match config.node_key_path() {
        Some(path) => Some(crate::node::andna_key::load_or_generate(path)?),
        None => None,
    };
    let neighborhood_config = NeighborhoodConfig {
        my_id,
        max_arcs: MAX_ARCS,
        kernel: netlink,
        stub_factory: neighborhood_stub_factory,
        ip_route_manager: Arc::new(RealIpRouteManager {
            kernel: RealNetlink::new()?,
        }),
        rtt_probe: Arc::new(IcmpRttProbe),
        timing: NeighborhoodTiming::default(),
        new_linklocal_address: linklocal_allocator(my_id),
        signing_key,
        require_auth: config.require_auth(),
    };
    let (neighborhood, neighborhood_join) =
        ntk_neighborhood::Manager::spawn(neighborhood_config, cancel.child_token());
    tasks.spawn(async move {
        let _ = neighborhood_join.await;
    });

    for nic in nics {
        neighborhood
            .start_monitor(LocalNic {
                dev: nic.clone(),
                mac: synthetic_mac(nic, my_id),
            })
            .await?;
    }

    let routing_kernel = Arc::new(RealNetlink::new()?);
    let started = lifecycle::run(
        NodeInputs {
            config: config.clone(),
            neighborhood: neighborhood.clone(),
            registry,
            links,
            routing_kernel,
            dialer: Arc::new(TcpDialer::default()),
            initial_position: None,
            preformed: None,
            my_id,
        },
        tasks,
        cancel.clone(),
    )
    .await?;

    let server = match TcpServer::bind(format!("0.0.0.0:{}", config.port()).parse()?, 1 << 20).await
    {
        Ok(server) => server,
        Err(source) => {
            return Err(anyhow::anyhow!(describe_bind_failure(
                "TCP",
                None,
                config.port(),
                &source
            )));
        }
    };
    let dispatcher = started.dispatcher.clone();
    let server_cancel = cancel.child_token();
    tasks.spawn(async move {
        server.serve(dispatcher, server_cancel).await;
    });

    for (dev, broadcaster) in broadcasters {
        let handler = Arc::new(NeighborhoodRpcHandler::for_broadcast(
            neighborhood.clone(),
            dev,
        ));
        let broadcast_cancel = cancel.child_token();
        tasks.spawn(async move {
            ntk_neighborhood::serve_broadcast(broadcaster, handler, broadcast_cancel).await;
        });
    }

    Ok(started)
}

#[cfg(test)]
mod tests {
    use super::describe_bind_failure;
    use std::io;

    fn permission_denied() -> io::Error {
        io::Error::from_raw_os_error(13) // EACCES, what an unprivileged bind of port 269 returns
    }

    #[test]
    fn permission_denied_on_a_privileged_port_names_cap_net_bind_service() {
        let message = describe_bind_failure(
            "UDP broadcast",
            Some("enp0s31f6"),
            269,
            &permission_denied(),
        );
        assert!(
            message.contains("269"),
            "message must name the port: {message}"
        );
        assert!(
            message.contains("CAP_NET_BIND_SERVICE"),
            "message must name the capability: {message}"
        );
        assert!(
            message.contains(">= 1024"),
            "message must offer the non-privileged-port alternative: {message}"
        );
    }

    #[test]
    fn permission_denied_on_a_non_privileged_port_does_not_mention_the_capability() {
        let message = describe_bind_failure(
            "UDP broadcast",
            Some("enp0s31f6"),
            26900,
            &permission_denied(),
        );
        assert!(
            !message.contains("CAP_NET_BIND_SERVICE"),
            "a non-privileged port's EACCES is not the privileged-port trap, so the capability \
             hint would mislead: {message}"
        );
        assert!(
            message.contains("26900"),
            "message must still name the port: {message}"
        );
    }
}

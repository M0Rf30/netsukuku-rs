//! Production-only transport composition: binds the real [`ntk_rpc::TcpServer`] and one
//! [`ntk_rpc::UdpBroadcaster`] per configured NIC, spawns [`ntk_neighborhood::Manager`] against
//! the real [`ntk_netlink::RealNetlink`] backend (the one call site allowed to name that
//! concrete type — see `crate::node::lifecycle`'s module doc on why `Manager<K>` can't be
//! spawned generically), and wires everything into [`crate::node::lifecycle::run`].

use std::collections::HashMap;
use std::sync::Arc;

use ntk_neighborhood::{
    IcmpRttProbe, LocalNic, NeighborhoodConfig, NeighborhoodRpcHandler, NeighborhoodTiming, NodeId,
};
use ntk_netlink::RealNetlink;
use ntk_rpc::{TcpServer, UdpBroadcaster};
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

    let mut broadcasters = HashMap::new();
    for nic in nics {
        let broadcaster = Arc::new(UdpBroadcaster::bind(Some(nic), config.port(), 1 << 16)?);
        broadcasters.insert(nic.clone(), broadcaster);
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
        kernel: RealNetlink::new()?,
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

    let server = TcpServer::bind(format!("0.0.0.0:{}", config.port()).parse()?, 1 << 20).await?;
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

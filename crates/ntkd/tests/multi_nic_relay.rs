//! Real-kernel regression test for the multi-NIC outbound-dial defect: a relay node
//! monitoring 2+ NICs must be able to establish a neighborhood arc — and install a route — over
//! *every* NIC, not just the first.
//!
//! # Root cause
//! `RealIpRouteManager` installs each monitored NIC's linklocal address as a `169.254.0.0/16`
//! connected route (`crate::node::ip_route` — the full `/16` is required by RFC 3927 and stays
//! that way; see that module's doc). With 2+ NICs, the kernel FIB holds several routes to that
//! *same* prefix and picks exactly one for any unscoped outbound dial, independent of
//! destination (confirmed outside this codebase: `ip addr add 169.254.10.5/16 dev veth0; ip addr
//! add 169.254.20.7/16 dev veth1; ip route get 169.254.99.99` always resolves via `veth0`,
//! whichever NIC's connected route was installed first). Neighborhood discovery itself is
//! unaffected (UDP broadcast uses `SO_BINDTODEVICE`, bypassing the FIB entirely), but the
//! outbound TCP dial in `NeighborhoodStubFactory::unicast` — the `nop()` call that gates
//! `ArcAdded` — used to go through the ordinary, unscoped route table, so it always left via
//! the first-monitored NIC regardless of which neighbor it was meant to reach. Fixed by binding
//! that dial's socket to the arc's own known-correct NIC (`ntk_rpc::TcpRpcClient::connect_via`'s
//! `SO_BINDTODEVICE`, threaded through `crate::node::lifecycle::Dialer::dial_via` and
//! `crate::node::stubs::LazyLinkClient`).
//!
//! # Second, still-open defect this test also exposes: the reply path, not just the dial
//! Fixing the outbound *dial* (above) was not sufficient — this test still fails, 100%
//! reproducibly, leaf-b's arc to the relay permanently stuck `Discovered`/`cost: None`. Traced
//! to ground with per-attempt dial logging (`crate::node::stubs::LazyLinkClient::resolve`) plus
//! a direct read of `/proc/thread-self/net/route` inside the relay's own netns: the relay's
//! kernel routing table holds **two** routes to the identical `169.254.0.0/16` prefix (one via
//! `ntkd-mnr-ra`, one via `ntkd-mnr-rb`, both metric 0) — confirming the `RealIpRouteManager`
//! module doc's own warning verbatim, but on the *other* side of the connection than the
//! already-fixed case above. `SO_BINDTODEVICE` (fixed above) only constrains a socket *this
//! process actively dials out on*; it does nothing for a `TcpServer` accepted connection's own
//! *reply* traffic (a SYN-ACK, or any later packet on that same accepted socket) — that still
//! goes through the kernel's ordinary, unscoped destination-based route lookup, which is
//! ambiguous across the relay's two identical-prefix routes and deterministically prefers
//! whichever NIC's route was installed first (`ntkd-mnr-ra`, monitored before `ntkd-mnr-rb` in
//! `spawn_real_node`) for *every* destination in that prefix — including a peer that is only
//! actually reachable via the *other* NIC. So: leaf-b dials the relay (`ntkd-mnr-rb`'s address)
//! fine as a fresh outbound connection elsewhere in this same handshake, but the relay's own
//! reply traffic on `ntkd-mnr-rb`-side connections keeps leaving via `ntkd-mnr-ra` instead,
//! where it is undeliverable — a plain SYN timeout, 13 consecutive unanswered attempts logged
//! over the full 5s dial-retry budget, not a transient race (raising that budget changes
//! nothing, confirmed).
//!
//! **Not fixable inside `ntkd`/`ntk-neighborhood`/`ntk-hooking`.** The correct fix needs one of:
//! - an `ntk_netlink` on-link/no-gateway `RouteTarget` variant, so `RealIpRouteManager` can
//!   finally implement `add_neighbor` for real (currently a documented no-op — see
//!   `crate::node::ip_route`'s module doc) as a `/32` host route to each *specific* discovered
//!   neighbor via the correct device: longest-prefix-match then wins over the ambiguous `/16`
//!   regardless of insertion order; or
//! - an `ntk_netlink::RuleSelector::From(Ipv4Addr)` policy-routing selector, so each monitored
//!   NIC's own linklocal address can be routed through a dedicated table holding only that
//!   NIC's connected route; or
//! - `ntk_rpc::TcpServer` binding its listening socket(s) to a specific device
//!   (`SO_BINDTODEVICE`), which Linux propagates to every accepted child socket's own reply
//!   path, mirroring `TcpRpcClient::connect_via`'s existing dial-side fix.
//!
//! This almost certainly also explains `tests/mesh.rs`'s `chain_of_four_converges_to_exact_multi_hop_routes`
//! symptom A (node2, the relay adjacent to node1, is exactly this shape: a 2-NIC relay whose
//! second-monitored segment cannot receive replies) — see that investigation's own findings for
//! the full cross-check. Until one of the above lands, this test is expected to keep failing.
//!
//! # Topology
//! Three network namespaces, two veth pairs, no `ip`/subprocess calls anywhere (see
//! `tests/multi_node.rs`'s scenario-3 module doc for the full native-netns rationale this test
//! reuses verbatim: `unshare(CLONE_NEWNET)` per namespace, raw `rtnetlink` for veth
//! creation/link-up, `ntk_netlink::RealNetlink` for everything address/route-shaped):
//!
//! ```text
//! ns-leaf-a --[ntkd-mnr-a <-> ntkd-mnr-ra]-- ns-relay --[ntkd-mnr-rb <-> ntkd-mnr-b]-- ns-leaf-b
//! ```
//!
//! `ns-relay` monitors *both* `ntkd-mnr-ra` and `ntkd-mnr-rb` — the exact "any node monitoring
//! 2+ NICs" case from the bug report. All three share `tests/multi_node.rs`'s `[4, 2, 2, 2]`
//! topology at positions `[0,0,0,0]`/`[1,0,0,0]`/`[2,0,0,0]`, so the relay is each leaf's direct
//! topological neighbor.
//!
//! # Running
//! Same privilege model as `tests/multi_node.rs`'s real-kernel suite — the equivalent of
//! `CAP_NET_ADMIN` over a set of network namespaces this process creates and owns, which a
//! rootless user namespace grants without any host capability:
//!
//! ```text
//! unshare --net --map-root-user -- \
//!     cargo test -p ntkd --test multi_nic_relay -- --ignored
//! ```

use ntkd::kernel::addressing;
use ntkd::kernel::config::NtkdConfig;
use ntkd::node::ip_route::{NEIGHBOR_ROUTE_TABLE, RealIpRouteManager, cleanup_neighbor_routes};
use ntkd::node::lifecycle::{
    self, NodeInputs, StartedNode, TcpDialer, linklocal_allocator, synthetic_mac,
};
use ntkd::node::peers::PeerLinks;
use ntkd::node::registry::LinkRegistry;
use ntkd::node::stubs::NeighborhoodStubFactoryAdapter;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use ntk_common::{HCoord, Naddr, Topology};
use ntk_neighborhood::{
    Arc as NeighborArc, FixedRttProbe, LocalNic, NeighborhoodConfig, NeighborhoodRpcHandler,
    NeighborhoodTiming, NodeId,
};
use ntk_netlink::{AddressTable, Interface, Ipv4Net, RouteTable, RouteTarget, TopologyQuery};
use ntk_rpc::{TcpServer, UdpBroadcaster};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

fn topology() -> Topology {
    Topology::new([4, 2, 2, 2]).unwrap()
}

fn position(idx: u32) -> Vec<u32> {
    vec![idx, 0, 0, 0]
}

fn naddr(idx: u32) -> Naddr {
    Naddr::new(topology(), position(idx)).unwrap()
}

/// One namespace worker's findings, read back through an independent `ntk_netlink::RealNetlink`
/// connection — never trusted from the daemon's own in-process state alone (matching
/// `tests/multi_node.rs`'s `NamespaceReport` discipline).
#[derive(Debug)]
struct NodeReport {
    label: &'static str,
    arcs: Vec<NeighborArc>,
    /// The route CIDR `addressing::gnode_destination` predicts for a direct arc to each peer —
    /// one entry for a leaf, two for the relay.
    expected_destinations: Vec<Ipv4Net>,
    routes: Vec<ntk_netlink::RouteSpec>,
    /// [`NEIGHBOR_ROUTE_TABLE`]'s contents — the actual mechanism this test pins, not merely
    /// its outcome: one `/32` on-link route per arc, via that arc's own device.
    neighbor_routes: Vec<ntk_netlink::RouteSpec>,
    addresses: Vec<ntk_netlink::AddressEntry>,
    /// `dev name -> ifindex`, resolved once per namespace — needed to compare an arc's `my_dev`
    /// (a name) against a route's `Interface::Index` (what the kernel always reports).
    dev_indexes: HashMap<String, u32>,
    /// [`NEIGHBOR_ROUTE_TABLE`]'s contents after this namespace's own graceful-shutdown
    /// cleanup sweep ([`cleanup_neighbor_routes`]) — proving a killed daemon leaves nothing
    /// behind, not merely assuming it because the sweep didn't error.
    leftover_neighbor_routes: Vec<ntk_netlink::RouteSpec>,
}

impl NodeReport {
    fn all_routes_found(&self) -> bool {
        self.expected_destinations
            .iter()
            .all(|dest| self.routes.iter().any(|r| &r.destination == dest))
    }

    /// Whether every arc has exactly the `/32` on-link route [`RealIpRouteManager::add_neighbor`]
    /// should have installed for it, via the correct device — the fix under test, pinned
    /// directly rather than only through its emergent effect on `all_routes_found`.
    fn all_neighbor_routes_found(&self) -> bool {
        self.arcs.iter().all(|arc| {
            let Some(&dev_index) = self.dev_indexes.get(&arc.my_dev) else {
                return false;
            };
            let expected = ntk_netlink::RouteSpec {
                destination: Ipv4Net::host(
                    arc.neighbour_nic_addr
                        .parse()
                        .expect("neighbour_nic_addr is always a valid IPv4 address"),
                ),
                table: NEIGHBOR_ROUTE_TABLE,
                target: RouteTarget::OnLink {
                    dev: Interface::Index(dev_index),
                },
            };
            self.neighbor_routes.contains(&expected)
        })
    }
}

/// Everything [`run_namespace_worker`] needs for one namespace. A leaf has one `dev`/one
/// `peer_idxs` entry; the relay has two of each.
struct NamespaceSpec {
    label: &'static str,
    my_id: NodeId,
    my_idx: u32,
    peer_idxs: Vec<u32>,
    devs: Vec<&'static str>,
    port: u16,
    fd_tx: std::sync::mpsc::Sender<std::os::fd::RawFd>,
    /// Blocks until the coordinator has moved every one of this namespace's veth ends in — one
    /// `recv()` per `devs` entry.
    moved_rx: std::sync::mpsc::Receiver<()>,
    /// Sent once this namespace's own polling loop finishes — one entry per veth end this
    /// namespace holds (1 for a leaf, 2 for the relay), each paired with that end's specific
    /// partner namespace. A veth pair spans two namespaces and deleting either end deletes
    /// both (`tests/multi_node.rs`'s scenario-3 doc), so neither side of a pair may reclaim its
    /// namespace before the *other* side of that same pair has finished polling — the relay
    /// alone straddles two independent pairs, hence a `Vec` here instead of one flag.
    done_txs: Vec<tokio::sync::oneshot::Sender<()>>,
    /// Received from each veth-end partner before this namespace reclaims itself. Bounded by a
    /// timeout (`namespace_body`), not awaited unconditionally: if a partner errored out before
    /// ever sending, this namespace still exits and reclaims itself (and its now-orphaned veth
    /// end) rather than hanging forever — a data-collection nicety, not a correctness
    /// requirement, since every assertion is made on data already captured before this point.
    peer_done_rxs: Vec<tokio::sync::oneshot::Receiver<()>>,
    report_tx: tokio::sync::oneshot::Sender<anyhow::Result<NodeReport>>,
}

async fn link_index(handle: &rtnetlink::Handle, name: &str) -> anyhow::Result<u32> {
    use futures::TryStreamExt;
    handle
        .link()
        .get()
        .match_name(name.to_owned())
        .execute()
        .try_next()
        .await
        .with_context(|| format!("resolving link {name:?}"))?
        .map(|link| link.header.index)
        .ok_or_else(|| anyhow::anyhow!("link {name:?} not found"))
}

async fn bring_link_up(handle: &rtnetlink::Handle, name: &str) -> anyhow::Result<()> {
    let index = link_index(handle, name).await?;
    handle
        .link()
        .change(rtnetlink::LinkUnspec::new_with_index(index).up().build())
        .execute()
        .await
        .with_context(|| format!("bringing {name:?} up"))
}

/// Composes one real `ntkd` node against the real kernel over `devs` — the exact production
/// wiring (`RealNetlink`, real `UdpBroadcaster`s/a real `TcpServer`,
/// `NeighborhoodStubFactoryAdapter`, `RealIpRouteManager`), generalized from
/// `tests/multi_node.rs`'s single-NIC `spawn_real_node` to the `devs.len() >= 1` case this
/// scenario's relay needs.
async fn spawn_real_node(
    my_id: NodeId,
    position: Vec<u32>,
    devs: &[&'static str],
    port: u16,
    tasks: &mut JoinSet<()>,
    cancel: CancellationToken,
) -> anyhow::Result<StartedNode<ntk_netlink::RealNetlink>> {
    let nics_field = devs.join("\", \"");
    let config = NtkdConfig::from_str(&format!(
        "gsizes = [4, 2, 2, 2]\nnics = [\"{nics_field}\"]\nport = {port}\n"
    ))?;

    let registry = Arc::new(LinkRegistry::new());
    let links = Arc::new(PeerLinks::new());

    let mut broadcasters = HashMap::new();
    for &dev in devs {
        let broadcaster = Arc::new(UdpBroadcaster::bind(Some(dev), port, 1 << 16)?);
        broadcasters.insert(dev.to_owned(), broadcaster);
    }

    let neighborhood_config = NeighborhoodConfig {
        my_id,
        max_arcs: 64,
        kernel: ntk_netlink::RealNetlink::new()?,
        stub_factory: Arc::new(NeighborhoodStubFactoryAdapter {
            broadcasters: broadcasters.clone(),
            links: links.clone(),
            registry: registry.clone(),
        }),
        ip_route_manager: Arc::new(RealIpRouteManager {
            kernel: ntk_netlink::RealNetlink::new()?,
        }),
        rtt_probe: Arc::new(FixedRttProbe(Some(0))),
        timing: NeighborhoodTiming {
            radar_interval: Duration::from_millis(200),
            arc_monitor_interval: (Duration::from_millis(20), Duration::from_millis(40)),
        },
        new_linklocal_address: linklocal_allocator(my_id),
        signing_key: None,
        require_auth: false,
    };
    let (neighborhood, neighborhood_join) =
        ntk_neighborhood::Manager::spawn(neighborhood_config, cancel.child_token());
    tasks.spawn(async move {
        let _ = neighborhood_join.await;
    });

    for &dev in devs {
        neighborhood
            .start_monitor(LocalNic {
                dev: dev.to_owned(),
                mac: synthetic_mac(dev, my_id),
            })
            .await?;
    }

    let routing_kernel = Arc::new(ntk_netlink::RealNetlink::new()?);
    let started = lifecycle::run(
        NodeInputs {
            config,
            neighborhood: neighborhood.clone(),
            registry,
            links,
            routing_kernel,
            dialer: Arc::new(TcpDialer::default()),
            initial_position: Some(position),
            preformed: None,
            my_id,
        },
        tasks,
        cancel.clone(),
    )
    .await?;

    let server = TcpServer::bind(format!("0.0.0.0:{port}").parse()?, 1 << 20).await?;
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

/// Runs entirely inside the namespace worker thread's own `current_thread` runtime: brings `lo`
/// and every one of `spec.devs` up, composes the real node, then polls the *real kernel* until
/// every expected arc has a cost and every expected route has appeared, or the timeout elapses.
async fn namespace_body(spec: &mut NamespaceSpec) -> anyhow::Result<NodeReport> {
    let (connection, handle, _) = rtnetlink::new_connection()
        .with_context(|| format!("{}: rtnetlink connection", spec.label))?;
    tokio::spawn(connection);
    bring_link_up(&handle, "lo")
        .await
        .with_context(|| format!("{}: bring lo up", spec.label))?;
    for dev in &spec.devs {
        bring_link_up(&handle, dev)
            .await
            .with_context(|| format!("{}: bring {} up", spec.label, dev))?;
    }
    drop(handle);

    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let started = spawn_real_node(
        spec.my_id,
        position(spec.my_idx),
        &spec.devs,
        spec.port,
        &mut tasks,
        cancel.clone(),
    )
    .await
    .with_context(|| format!("{}: compose real node", spec.label))?;

    let observer = ntk_netlink::RealNetlink::new()
        .with_context(|| format!("{}: observer RealNetlink", spec.label))?;
    let expected_destinations = spec
        .peer_idxs
        .iter()
        .map(|&peer_idx| {
            addressing::gnode_destination(&naddr(spec.my_idx), HCoord::new(0, peer_idx))
                .with_context(|| {
                    format!("{}: expected destination for peer {peer_idx}", spec.label)
                })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let (routes, arcs) = loop {
        let arcs: Vec<NeighborArc> = started.running.neighborhood.snapshot().borrow().clone();
        let routes = observer
            .list_routes(Some(started.running.route_table))
            .await
            .with_context(|| format!("{}: list_routes", spec.label))?;
        let all_costed =
            arcs.len() == spec.peer_idxs.len() && arcs.iter().all(|a| a.cost.is_some());
        let all_routed = expected_destinations
            .iter()
            .all(|dest| routes.iter().any(|r| &r.destination == dest));
        if (all_costed && all_routed) || tokio::time::Instant::now() >= deadline {
            break (routes, arcs);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let addresses = observer
        .list_addresses(None)
        .await
        .with_context(|| format!("{}: list_addresses", spec.label))?;
    let neighbor_routes = observer
        .list_routes(Some(NEIGHBOR_ROUTE_TABLE))
        .await
        .with_context(|| format!("{}: list_routes (neighbor table)", spec.label))?;
    let dev_indexes: HashMap<String, u32> = observer
        .list_links()
        .await
        .with_context(|| format!("{}: list_links", spec.label))?
        .into_iter()
        .map(|link| (link.name, link.index))
        .collect();

    let mut report = NodeReport {
        label: spec.label,
        arcs,
        expected_destinations,
        routes,
        neighbor_routes,
        addresses,
        dev_indexes,
        leftover_neighbor_routes: Vec::new(),
    };

    // Signal every veth-end partner that this side's own polling is done, then wait (bounded —
    // see `NamespaceSpec::peer_done_rxs`'s doc) for the same from them, before reclaiming this
    // namespace (and, with it, every veth end living in it).
    for done_tx in spec.done_txs.drain(..) {
        let _ = done_tx.send(());
    }
    for peer_done_rx in spec.peer_done_rxs.drain(..) {
        // Must exceed every node's own polling deadline (30s above): the relay has two peers
        // to converge routes for and can legitimately still be inside its own polling loop
        // when a single-peer leaf has already finished its (much smaller) one-peer check. A
        // leaf giving up here before the relay can possibly have finished would reclaim its
        // namespace — destroying its veth end and, with it, the relay's paired end — out from
        // under a relay that is still using it (confirmed: an earlier 20s bound here raced
        // exactly this way and truncated a real, otherwise-successful relay arc mid-flight).
        if tokio::time::timeout(Duration::from_secs(45), peer_done_rx)
            .await
            .is_err()
        {
            tracing::warn!(
                "{}: timed out waiting for a veth-end partner to finish",
                spec.label
            );
        }
    }

    // Best-effort graceful teardown, mirroring `supervisor::run`'s own shutdown sequence —
    // bounded so a stuck task/RPC can never hang this test past the point its report data was
    // already captured above. The namespace itself is reclaimed once this thread exits
    // regardless (`tests/multi_node.rs`'s scenario-3 doc), so a timeout here only forgoes the
    // extra "cleanly torn down" proof, never this test's actual arc/route assertions.
    let cleanup = async {
        cancel.cancel();
        while tasks.join_next().await.is_some() {}
        if let Err(err) = started
            .running
            .route_installer
            .lock()
            .await
            .teardown()
            .await
        {
            tracing::warn!(%err, "{}: route teardown failed", spec.label);
        }
        if let Err(err) = cleanup_neighbor_routes(&observer).await {
            tracing::warn!(%err, "{}: neighbor on-link route cleanup failed", spec.label);
        }
    };
    if tokio::time::timeout(Duration::from_secs(15), cleanup)
        .await
        .is_err()
    {
        tracing::warn!("{}: graceful teardown timed out", spec.label);
    }
    report.leftover_neighbor_routes = observer
        .list_routes(Some(NEIGHBOR_ROUTE_TABLE))
        .await
        .with_context(|| format!("{}: list_routes (neighbor table, post-cleanup)", spec.label))?;

    Ok(report)
}

fn run_namespace_worker(mut spec: NamespaceSpec) {
    let outcome = (|| -> anyhow::Result<NodeReport> {
        use std::os::fd::AsRawFd;

        nix::sched::unshare(nix::sched::CloneFlags::CLONE_NEWNET)
            .map_err(|errno| anyhow::anyhow!("{}: unshare(CLONE_NEWNET): {errno}", spec.label))?;
        // Held until this closure returns — see `tests/multi_node.rs`'s `run_namespace_worker`
        // doc comment for why this fd must stay open for the coordinator's `setns_by_fd` call.
        let ns_file = std::fs::File::open("/proc/thread-self/ns/net")
            .with_context(|| format!("{}: open own netns fd", spec.label))?;
        spec.fd_tx
            .send(ns_file.as_raw_fd())
            .map_err(|_| anyhow::anyhow!("{}: coordinator dropped fd channel", spec.label))?;
        for _ in 0..spec.devs.len() {
            spec.moved_rx.recv().map_err(|_| {
                anyhow::anyhow!("{}: coordinator dropped veth-moved signal", spec.label)
            })?;
        }

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .with_context(|| format!("{}: build current-thread runtime", spec.label))?;
        let report = rt.block_on(namespace_body(&mut spec));
        drop(ns_file);
        report
    })();
    let _ = spec.report_tx.send(outcome);
}

/// Three real `ntkd` node compositions in three network namespaces, joined by two native veth
/// pairs: the middle one (`relay`) monitors both links — the exact "any node monitoring 2+
/// NICs" case from the bug report. Asserts the relay establishes a costed neighborhood arc *and*
/// a real kernel route to *both* leaves, not just the first-monitored one.
#[ignore = "requires the equivalent of CAP_NET_ADMIN over its own network namespaces"]
#[tokio::test]
async fn real_netns_relay_with_two_nics_establishes_arcs_on_both() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_thread_names(true)
        .with_test_writer()
        .try_init();

    const DEV_LEAF_A: &str = "ntkd-mnr-a";
    const DEV_RELAY_A: &str = "ntkd-mnr-ra";
    const DEV_RELAY_B: &str = "ntkd-mnr-rb";
    const DEV_LEAF_B: &str = "ntkd-mnr-b";
    const PORT: u16 = 27369;

    let (connection, root, _) = rtnetlink::new_connection().expect("rtnetlink connection");
    tokio::spawn(connection);
    root.link()
        .add(rtnetlink::LinkVeth::new(DEV_LEAF_A, DEV_RELAY_A).build())
        .execute()
        .await
        .expect("create veth pair a<->relay");
    root.link()
        .add(rtnetlink::LinkVeth::new(DEV_RELAY_B, DEV_LEAF_B).build())
        .execute()
        .await
        .expect("create veth pair relay<->b");

    let idx_leaf_a = link_index(&root, DEV_LEAF_A)
        .await
        .expect("resolve leaf-a end");
    let idx_relay_a = link_index(&root, DEV_RELAY_A)
        .await
        .expect("resolve relay a-side end");
    let idx_relay_b = link_index(&root, DEV_RELAY_B)
        .await
        .expect("resolve relay b-side end");
    let idx_leaf_b = link_index(&root, DEV_LEAF_B)
        .await
        .expect("resolve leaf-b end");

    // Per-veth-pair done rendezvous (`NamespaceSpec::done_txs`/`peer_done_rxs`'s doc): a leaf
    // sends its own done to the relay and awaits the relay's; the relay awaits both leaves'
    // done before sending its own to each — a DAG, not a cycle, so it can't deadlock even if
    // every wait were unbounded (they're bounded anyway, belt and braces).
    let (a_done_tx, a_done_rx) = tokio::sync::oneshot::channel();
    let (relay_done_a_tx, relay_done_a_rx) = tokio::sync::oneshot::channel();
    let (b_done_tx, b_done_rx) = tokio::sync::oneshot::channel();
    let (relay_done_b_tx, relay_done_b_rx) = tokio::sync::oneshot::channel();

    let (fd_tx_a, fd_rx_a) = std::sync::mpsc::channel();
    let (fd_tx_relay, fd_rx_relay) = std::sync::mpsc::channel();
    let (fd_tx_b, fd_rx_b) = std::sync::mpsc::channel();
    let (moved_tx_a, moved_rx_a) = std::sync::mpsc::channel();
    let (moved_tx_relay, moved_rx_relay) = std::sync::mpsc::channel();
    let (moved_tx_b, moved_rx_b) = std::sync::mpsc::channel();
    let (report_tx_a, report_rx_a) = tokio::sync::oneshot::channel();
    let (report_tx_relay, report_rx_relay) = tokio::sync::oneshot::channel();
    let (report_tx_b, report_rx_b) = tokio::sync::oneshot::channel();

    let thread_a = std::thread::Builder::new()
        .name("leaf-a".to_owned())
        .spawn(move || {
            run_namespace_worker(NamespaceSpec {
                label: "leaf-a",
                my_id: NodeId::from_raw(201).unwrap(),
                my_idx: 0,
                peer_idxs: vec![1],
                devs: vec![DEV_LEAF_A],
                port: PORT,
                fd_tx: fd_tx_a,
                moved_rx: moved_rx_a,
                done_txs: vec![a_done_tx],
                peer_done_rxs: vec![relay_done_a_rx],
                report_tx: report_tx_a,
            });
        })
        .expect("spawn leaf-a worker thread");
    let thread_relay = std::thread::Builder::new()
        .name("relay".to_owned())
        .spawn(move || {
            run_namespace_worker(NamespaceSpec {
                label: "relay",
                my_id: NodeId::from_raw(202).unwrap(),
                my_idx: 1,
                peer_idxs: vec![0, 2],
                devs: vec![DEV_RELAY_A, DEV_RELAY_B],
                port: PORT,
                fd_tx: fd_tx_relay,
                moved_rx: moved_rx_relay,
                done_txs: vec![relay_done_a_tx, relay_done_b_tx],
                peer_done_rxs: vec![a_done_rx, b_done_rx],
                report_tx: report_tx_relay,
            });
        })
        .expect("spawn relay worker thread");
    let thread_b = std::thread::Builder::new()
        .name("leaf-b".to_owned())
        .spawn(move || {
            run_namespace_worker(NamespaceSpec {
                label: "leaf-b",
                my_id: NodeId::from_raw(203).unwrap(),
                my_idx: 2,
                peer_idxs: vec![1],
                devs: vec![DEV_LEAF_B],
                port: PORT,
                fd_tx: fd_tx_b,
                moved_rx: moved_rx_b,
                done_txs: vec![b_done_tx],
                peer_done_rxs: vec![relay_done_b_rx],
                report_tx: report_tx_b,
            });
        })
        .expect("spawn leaf-b worker thread");

    // Brief, bounded blocking recvs (each worker sends within microseconds of starting) with
    // nothing else scheduled on this runtime at this point — not worth `spawn_blocking`.
    let fd_a = fd_rx_a.recv().expect("leaf-a netns fd");
    let fd_relay = fd_rx_relay.recv().expect("relay netns fd");
    let fd_b = fd_rx_b.recv().expect("leaf-b netns fd");

    root.link()
        .change(
            rtnetlink::LinkUnspec::new_with_index(idx_leaf_a)
                .setns_by_fd(fd_a)
                .build(),
        )
        .execute()
        .await
        .expect("move leaf-a's veth end into ns-leaf-a");
    root.link()
        .change(
            rtnetlink::LinkUnspec::new_with_index(idx_relay_a)
                .setns_by_fd(fd_relay)
                .build(),
        )
        .execute()
        .await
        .expect("move relay's a-side veth end into ns-relay");
    root.link()
        .change(
            rtnetlink::LinkUnspec::new_with_index(idx_relay_b)
                .setns_by_fd(fd_relay)
                .build(),
        )
        .execute()
        .await
        .expect("move relay's b-side veth end into ns-relay");
    root.link()
        .change(
            rtnetlink::LinkUnspec::new_with_index(idx_leaf_b)
                .setns_by_fd(fd_b)
                .build(),
        )
        .execute()
        .await
        .expect("move leaf-b's veth end into ns-leaf-b");

    moved_tx_a.send(()).expect("signal leaf-a");
    moved_tx_relay.send(()).expect("signal relay (a-side)");
    moved_tx_relay.send(()).expect("signal relay (b-side)");
    moved_tx_b.send(()).expect("signal leaf-b");

    let report_a = tokio::time::timeout(Duration::from_secs(120), report_rx_a)
        .await
        .expect("leaf-a namespace worker did not finish within 120s")
        .expect("leaf-a report channel");
    let report_relay = tokio::time::timeout(Duration::from_secs(120), report_rx_relay)
        .await
        .expect("relay namespace worker did not finish within 120s")
        .expect("relay report channel");
    let report_b = tokio::time::timeout(Duration::from_secs(120), report_rx_b)
        .await
        .expect("leaf-b namespace worker did not finish within 120s")
        .expect("leaf-b report channel");
    thread_a
        .join()
        .unwrap_or_else(|e| panic!("leaf-a worker thread panicked: {e:?}"));
    thread_relay
        .join()
        .unwrap_or_else(|e| panic!("relay worker thread panicked: {e:?}"));
    thread_b
        .join()
        .unwrap_or_else(|e| panic!("leaf-b worker thread panicked: {e:?}"));

    let report_a = report_a.expect("leaf-a namespace body");
    let report_relay = report_relay.expect("relay namespace body");
    let report_b = report_b.expect("leaf-b namespace body");

    eprintln!(
        "{}: arcs={:#?} routes={:#?}",
        report_a.label, report_a.arcs, report_a.routes
    );
    eprintln!(
        "{}: arcs={:#?} routes={:#?}",
        report_relay.label, report_relay.arcs, report_relay.routes
    );
    eprintln!(
        "{}: arcs={:#?} routes={:#?}",
        report_b.label, report_b.arcs, report_b.routes
    );

    // The exact failure reported: with the multi-NIC dial defect, the relay's neighborhood
    // discovery on *both* NICs physically arrives (UDP broadcast bypasses the FIB), but the
    // outbound `nop()` unicast dial that gates `ArcAdded` used to leave via whichever NIC's
    // connected `169.254.0.0/16` route the kernel happened to prefer — so at most one of these
    // two arcs ever gained a cost, and only one route ever got installed.
    assert_eq!(
        report_relay.arcs.len(),
        2,
        "relay should have discovered exactly 2 neighborhood arcs (one per monitored NIC): {:#?}",
        report_relay.arcs
    );
    assert!(
        report_relay.arcs.iter().all(|a| a.cost.is_some()),
        "relay failed to establish an arc (measure a cost) on at least one of its 2 monitored \
         NICs — this is the exact multi-NIC outbound-dial defect this test pins: {:#?}",
        report_relay.arcs
    );
    assert!(
        report_a.arcs.iter().any(|a| a.cost.is_some()),
        "leaf-a never measured a cost for its neighbor (the relay): {:#?}",
        report_a.arcs
    );
    assert!(
        report_b.arcs.iter().any(|a| a.cost.is_some()),
        "leaf-b never measured a cost for its neighbor (the relay): {:#?}",
        report_b.arcs
    );
    assert!(
        report_relay.all_routes_found(),
        "relay's real kernel routing table is missing a route to at least one leaf's g-node; \
         expected: {:#?}, routes: {:#?}, addresses: {:#?}",
        report_relay.expected_destinations,
        report_relay.routes,
        report_relay.addresses
    );
    assert!(
        report_a.all_routes_found(),
        "leaf-a's real kernel routing table never gained a route to the relay's g-node; \
         expected: {:#?}, routes: {:#?}, addresses: {:#?}",
        report_a.expected_destinations,
        report_a.routes,
        report_a.addresses
    );
    assert!(
        report_b.all_routes_found(),
        "leaf-b's real kernel routing table never gained a route to the relay's g-node; \
         expected: {:#?}, routes: {:#?}, addresses: {:#?}",
        report_b.expected_destinations,
        report_b.routes,
        report_b.addresses
    );
    // The mechanism, not just the outcome: a `/32` on-link route per arc, via that arc's own
    // device, in `NEIGHBOR_ROUTE_TABLE` — this is what makes accepted-connection reply traffic
    // resolve unambiguously despite the relay's two identical-prefix `169.254.0.0/16` connected
    // routes (one per monitored NIC).
    assert!(
        report_relay.all_neighbor_routes_found(),
        "relay is missing an on-link route for at least one neighbor arc; arcs: {:#?}, \
         neighbor_routes: {:#?}",
        report_relay.arcs,
        report_relay.neighbor_routes
    );
    assert!(
        report_a.all_neighbor_routes_found(),
        "leaf-a is missing an on-link route for its neighbor arc; arcs: {:#?}, \
         neighbor_routes: {:#?}",
        report_a.arcs,
        report_a.neighbor_routes
    );
    assert!(
        report_b.all_neighbor_routes_found(),
        "leaf-b is missing an on-link route for its neighbor arc; arcs: {:#?}, \
         neighbor_routes: {:#?}",
        report_b.arcs,
        report_b.neighbor_routes
    );

    // A killed daemon leaves nothing behind: `cleanup_neighbor_routes` (called by every
    // namespace's own graceful-shutdown path above) must have removed every on-link route it
    // installed, not merely returned without error.
    assert!(
        report_relay.leftover_neighbor_routes.is_empty(),
        "relay's neighbor route table was not fully cleaned up: {:#?}",
        report_relay.leftover_neighbor_routes
    );
    assert!(
        report_a.leftover_neighbor_routes.is_empty(),
        "leaf-a's neighbor route table was not fully cleaned up: {:#?}",
        report_a.leftover_neighbor_routes
    );
    assert!(
        report_b.leftover_neighbor_routes.is_empty(),
        "leaf-b's neighbor route table was not fully cleaned up: {:#?}",
        report_b.leftover_neighbor_routes
    );
}

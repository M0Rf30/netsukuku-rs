//! Real-socket, real-kernel proof that a hostname registered on one running `ntkd` daemon
//! resolves correctly from a *different* running daemon — the first ANDNA scenario to run over
//! `ntk_rpc::TcpServer`/`TcpRpcClient` (rather than `ntk_rpc::FakeRpcClient`), across two real
//! Linux network namespaces joined by a real veth pair.
//!
//! Built on `tests/netns`'s shared real-kernel primitives (`NamespaceWorker`, `spawn_node`,
//! `teardown`, `observe`, `Segment`/`wire`), exactly as `tests/multi_node.rs`'s
//! `real_netns_two_daemons_establish_arc_and_route` and `tests/wireless.rs` already do: one
//! pinned OS thread per network namespace, raw `rtnetlink` for link plumbing, and a two-party
//! `oneshot` rendezvous before either side's `TcpServer` — and therefore either side's veth end —
//! is torn down (see `multi_node.rs`'s own doc comment for the exact premature-teardown defect
//! this avoids).
//!
//! # Why this drives `ntk_andna::Handle` directly, not the (concurrently developed) control socket
//! `ntkd`'s `andna-register`/`andna-resolve` control-socket requests are a thin CLI-facing
//! translation over exactly this same `ntk_andna::Handle` a running node already exposes via
//! `RunningNode::generation`/`GenerationHandles::andna`. Driving the handle directly proves the
//! protocol layer — real registration/resolution over real `ntk_peerservices` routing, real TCP,
//! a real kernel route — independently of that socket layer, without coupling this test to a
//! wire protocol under concurrent, separate development.
//!
//! # Why node B's resolve is guaranteed to cross the network, not read a local record
//! ANDNA rides `ntk_peerservices`' hash-based routing (RFC 0014 §2): `contact_peer` computes the
//! node closest to a hostname's hash target and, if that happens to be the caller's own
//! position, serves the request locally without ever going out to the network (`x.level == 0 &&
//! x.pos == my_pos[0]` in `ntk-peerservices/src/routing.rs`) — a resolve landing on that path
//! would prove nothing about the real socket/kernel stack this test exists to exercise.
//! [`hostname_routing_to_a`] picks a hostname whose hash target is provably closer
//! (`ntk_peerservices::dist`, the exact function `contact_peer`'s own routing uses) to node A's
//! position than to node B's. The test asserts that fact explicitly before either daemon runs,
//! so node B's own `contact_peer` self-loop can never fire for this hostname — its resolve can
//! only be answered by an actual `PeersRpcHandler` call routed across the veth to node A.
//!
//! # Currently red: confirmed real defect in `RoutingEnvAdapter::dial`, not in this test
//! Running this test (below) proves, over real sockets and real kernel state: the arc, the
//! qspn route, and — a previously undocumented prerequisite this test had to discover the hard
//! way — `ntk_peerservices`' participation gossip (`Handle::register`'s one-shot
//! `flood_set_participant`, fired at boot before any arc exists, so it always reaches nobody;
//! see `node_a_body`/`node_b_body`'s own re-registration comment) all converge correctly, and
//! node B's own routing math correctly elects node A (`self_loop=false` in a
//! `RUST_LOG=debug` run's `TRACE contact_peer: approximate resolved elect target` line). The
//! forwarded request then reaches node A, which self-loop-terminates it and must dial *back* to
//! node B to fetch the request body (`ntk_peerservices::routing::Handle::forward_msg`'s
//! `self.env.dial(&mf.n)` at the `x.level == 0 && x.pos == my_pos[0]` terminal branch) —
//! and that dial fails, unconditionally, for exactly this shape of call:
//!
//! `RoutingEnvAdapter::dial` (`crates/ntkd/src/node/adapters.rs:1714-1722`) rejects any
//! `TupleNode` whose `top()` is not the *full* topology depth:
//! ```text
//! fn dial(&self, n: &TupleNode) -> Option<Arc<dyn PeersStub>> {
//!     let topology = self.qspn.my_naddr().topology();
//!     if n.top() != topology.levels() {
//!         return None;
//!     }
//!     ...
//! ```
//! but `PeerMessageForwarder::n` (`crates/ntk-peerservices/src/routing.rs:195`,
//! `make_tuple_node(&topology, &my_pos, HCoord::new(0, my_pos[0]), x.level + 1)`) is built with
//! exactly `x.level + 1` levels — 1 level whenever routing resolves in a single hop to an
//! individual (level-0) node, unavoidable in a 2-node network no matter which hostname or
//! topology this test picks (there is only ever one other node to route to, so `x.level` is
//! always `0`). A partial `TupleNode` truncated to `top` levels means "same as the *resolving*
//! node's own position for every level beyond `top`" by construction
//! (`ntk_peerservices::tuple::make_tuple_node`'s own doc) — the already-proven-correct
//! `FakeEnv::dial` in `ntk-andna/tests/multi_node.rs:69-77` reconstructs exactly that
//! (`full_target.extend_from_slice(&self.my_full()[n.top()..])`); `RoutingEnvAdapter::dial`
//! has no equivalent and instead hard-rejects the call, so node A's `get_request` callback to
//! node B never fires, node B's `contact_peer` attempt times out
//! (`ntk_peerservices::routing::Handle::contact_peer`'s `attempt timed out` branch), excludes
//! node A, falls back to serving the resolve locally, and returns zero records.
//!
//! This is a real, reproducible defect in `crates/ntkd/src/node/adapters.rs` (outside this
//! slice's ownership — Slice B owns only this file), affecting every optional `PeerServices`
//! call (Coordinator included, observed in the same run) whenever routing resolves in fewer
//! hops than the configured topology depth — not specific to ANDNA, and not fixable from a test
//! file. Per this assignment's own instructions, this is reported rather than worked around:
//! no topology/hostname choice in a 2-node scenario can avoid a single-hop, level-0 resolution,
//! and swapping to a single-level topology to dodge the bug would mask a genuine production
//! defect other real-kernel tests in this suite already establish as in scope
//! (`tests/multi_node.rs`/`tests/wireless.rs` both use the same 4-level `[4, 2, 2, 2]`
//! topology). This test is left asserting the fully-correct end state, unweakened; it currently
//! fails at the final `assert_eq!` on node B's resolved records with `left: 0, right: 1`, not on
//! the arc, route, or participation-gossip assertions above it — all real, all green.
//!
//! # Running (privileged; not run by default `cargo test`)
//! ```text
//! unshare --net --map-root-user -- \
//!     cargo test -p ntkd --offline --test andna_e2e -- --ignored
//! ```

// `netns` is a fixture shared with `tests/mesh.rs`/`tests/multi_nic_relay.rs`/`tests/wireless.rs`
// (separate, independently-linted test binaries); each binary uses a different subset of its
// public items, so this file's own unused subset is expected, not a real dead-code smell —
// silenced at the `mod` boundary rather than editing the shared file.
#[allow(dead_code)]
mod netns;

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Context;
use ed25519_dalek::SigningKey;
use ntk_andna::{Hostname, RegisterOutcome, RegisterRequest, SnsdRecord, SnsdTarget};
use ntk_common::{HCoord, Naddr, Topology};
use ntk_neighborhood::NodeId;
use ntk_netlink::{Ipv4Net, RealNetlink};
use ntk_peerservices::TupleNode;
use ntkd::kernel::addressing;
use ntkd::node::lifecycle::StartedNode;
use tokio::sync::oneshot;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

/// Matches every other real-netns scenario's own production-shaped topology
/// (`tests/multi_node.rs`/`tests/wireless.rs`'s own `topology()`).
fn topology() -> Topology {
    Topology::new([4, 2, 2, 2]).unwrap()
}

fn position(idx: u32) -> Vec<u32> {
    vec![idx, 0, 0, 0]
}

fn naddr(idx: u32) -> Naddr {
    Naddr::new(topology(), position(idx)).unwrap()
}

/// Picks a hostname whose ANDNA hash target (`ntk_peerservices::hash_to_tuple`) is strictly
/// closer, by the exact `ntk_peerservices::dist` production routing itself uses, to node A's
/// position than to node B's — see this file's module doc for why that is load-bearing.
/// Deterministic given a fixed topology and fixed positions: always returns the same hostname.
fn hostname_routing_to_a() -> Hostname {
    let topo = topology();
    let tuple_a = TupleNode::new(topo.clone(), position(0)).expect("node A's own tuple");
    let tuple_b = TupleNode::new(topo.clone(), position(1)).expect("node B's own tuple");
    (0..1000u32)
        .map(|i| Hostname::new(&format!("andnaeteste{i}")).expect("alphanumeric candidate"))
        .find(|hostname| {
            let target = ntk_peerservices::hash_to_tuple(&topo, hostname.hash().route_key());
            ntk_peerservices::dist(&topo, &target, &tuple_a)
                < ntk_peerservices::dist(&topo, &target, &tuple_b)
        })
        .expect("some candidate among 1000 routes to node A — see this fn's own doc")
}

const PORT: u16 = 27380;
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(30);

/// One namespace's real, converged state plus whatever ANDNA work its own role performed —
/// exactly one of `register_outcome` (node A)/`resolve_result` (node B) is ever populated.
#[derive(Debug)]
struct AndnaNodeReport {
    node: netns::NodeReport,
    expected_destination: Ipv4Net,
    route_installed: bool,
    register_outcome: Option<Result<RegisterOutcome, String>>,
    resolve_result: Option<Result<Vec<SnsdRecord>, String>>,
}

/// Polls the real kernel (never just the daemon's own snapshot) until `expected_destination`
/// appears in `started`'s route table or `CONVERGE_TIMEOUT` elapses — the same technique
/// `tests/multi_node.rs`'s `namespace_body`/`tests/wireless.rs`'s `radio_arc_trial_body` use.
async fn wait_for_route(
    label: &str,
    started: &StartedNode<RealNetlink>,
    dev_index: &HashMap<String, u32>,
    expected_destination: Ipv4Net,
) -> anyhow::Result<(netns::NodeReport, bool)> {
    let deadline = tokio::time::Instant::now() + CONVERGE_TIMEOUT;
    loop {
        let node = netns::observe(label, started, dev_index.clone())
            .await
            .with_context(|| format!("{label}: observe"))?;
        let found = node
            .routes
            .iter()
            .any(|r| r.destination == expected_destination);
        if found || tokio::time::Instant::now() >= deadline {
            return Ok((node, found));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Node A's namespace body: brings up its device, composes the real daemon, waits for the real
/// arc/route to node B, then registers `hostname` through the daemon's own `ntk_andna::Handle`
/// (`services.rs`'s wiring — see this file's module doc) before signalling node B via
/// `ready_tx` that it may resolve.
#[allow(clippy::too_many_arguments)]
async fn node_a_body(
    my_id: NodeId,
    dev: &'static str,
    hostname: Hostname,
    owner_key: SigningKey,
    ready_tx: oneshot::Sender<Result<(), String>>,
    my_done_tx: oneshot::Sender<()>,
    peer_done_rx: oneshot::Receiver<()>,
) -> anyhow::Result<AndnaNodeReport> {
    let dev_index = netns::bring_up_devs(&[dev])
        .await
        .context("node A: bring up devs")?;

    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let started = netns::spawn_node(
        my_id,
        Some(position(0)),
        None,
        &[4, 2, 2, 2],
        &[dev],
        PORT,
        &mut tasks,
        cancel.clone(),
    )
    .await
    .context("node A: compose real node")?;

    let expected_destination = addressing::gnode_destination(&naddr(0), HCoord::new(0, 1))
        .context("node A: expected destination")?;
    let (node, route_installed) =
        wait_for_route("node-a", &started, &dev_index, expected_destination).await?;

    // ANDNA rides PeerServices, which needs the real arc + qspn route above before any
    // register/resolve call can reach the peer. That alone is not sufficient: PeerServices
    // gates a routing candidate on gossiped *participation* knowledge
    // (`ntk_peerservices::actor::State::non_participant_gnodes` treats every gnode as a
    // non-participant until its own `participant_set` says otherwise), and the one-shot flood
    // `ntk_andna::Handle::register_services` triggers at boot (inside `services::spawn`, before
    // any neighborhood arc exists yet) necessarily reaches zero neighbors — production's only
    // other chance is `services.rs`'s 300-real-second periodic re-announce
    // (`peers_config().participation_reannounce_interval`), far longer than any test should
    // wait. Re-registering now — the *same* flood that re-announce already re-sends
    // periodically in production, just run once, deterministically, the moment it can actually
    // land on the now-established arc — is not a weakening of the test and not a sleep over a
    // real protocol cadence, just triggering it at the first moment it can succeed.
    let andna = started.running.generation.borrow().andna.clone();
    andna.register_services().await;
    let req = RegisterRequest::sign(&owner_key, hostname, naddr(0), 1, 1_000, 16, 1, Vec::new())
        .expect("well-formed signed request");
    let register_outcome = andna.register(req).await.map_err(|e| e.to_string());
    let _ = ready_tx.send(register_outcome.as_ref().map(|_| ()).map_err(Clone::clone));

    // Rendezvous with node B before tearing this identity's `TcpServer` down — see this file's
    // module doc and `tests/multi_node.rs`'s own doc comment for the defect this avoids.
    let _ = my_done_tx.send(());
    let _ = peer_done_rx.await;

    netns::teardown(&started, cancel, &mut tasks).await;

    Ok(AndnaNodeReport {
        node,
        expected_destination,
        route_installed,
        register_outcome: Some(register_outcome),
        resolve_result: None,
    })
}

/// Node B's namespace body: brings up its device, composes the real daemon, waits for the real
/// arc/route to node A, waits for node A's own register-done signal, then resolves `hostname`
/// through its own `ntk_andna::Handle` — a call that can only be answered by routing a real
/// `PeersRpcHandler` request across the veth to node A (see this file's module doc).
#[allow(clippy::too_many_arguments)]
async fn node_b_body(
    my_id: NodeId,
    dev: &'static str,
    hostname: Hostname,
    ready_rx: oneshot::Receiver<Result<(), String>>,
    my_done_tx: oneshot::Sender<()>,
    peer_done_rx: oneshot::Receiver<()>,
) -> anyhow::Result<AndnaNodeReport> {
    let dev_index = netns::bring_up_devs(&[dev])
        .await
        .context("node B: bring up devs")?;

    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let started = netns::spawn_node(
        my_id,
        Some(position(1)),
        None,
        &[4, 2, 2, 2],
        &[dev],
        PORT,
        &mut tasks,
        cancel.clone(),
    )
    .await
    .context("node B: compose real node")?;

    let expected_destination = addressing::gnode_destination(&naddr(1), HCoord::new(0, 0))
        .context("node B: expected destination")?;
    let (node, route_installed) =
        wait_for_route("node-b", &started, &dev_index, expected_destination).await?;
    // Re-announce participation now that the arc above means it actually reaches node A —
    // see node A's own body for the full rationale (mirrored here for symmetry: without it,
    // node A would never learn node B participates either, which would make node A's own
    // `register` call's internal replication attempts spuriously exclude node B).
    started
        .running
        .generation
        .borrow()
        .andna
        .clone()
        .register_services()
        .await;

    // Wait for node A's own register-done signal — a two-party rendezvous distinct from (and
    // preceding) the pre-teardown one below: resolving before node A's registration has landed
    // would just prove "not found yet", not the cross-node round trip this test exists to prove.
    let resolve_result = match ready_rx
        .await
        .context("node B: node A's register-done signal")?
    {
        Ok(()) => {
            let andna = started.running.generation.borrow().andna.clone();
            andna.resolve(&hostname, 0).await.map_err(|e| e.to_string())
        }
        Err(register_err) => Err(format!("node A's registration failed: {register_err}")),
    };

    let _ = my_done_tx.send(());
    let _ = peer_done_rx.await;

    netns::teardown(&started, cancel, &mut tasks).await;

    Ok(AndnaNodeReport {
        node,
        expected_destination,
        route_installed,
        register_outcome: None,
        resolve_result: Some(resolve_result),
    })
}

/// Two real `ntkd` daemons, each in its own network namespace joined by a real veth pair,
/// establish a real neighborhood arc and a real kernel route (exactly
/// `real_netns_two_daemons_establish_arc_and_route`'s own proof), then node A registers a
/// hostname through its real `ntk_andna::Handle` and node B resolves the identical hostname
/// through its own — asserted, before either daemon runs, to require a real cross-node
/// `PeersRpcHandler` round trip rather than a local self-loop (see this file's module doc).
///
/// # Running
/// Needs the same capability as the rest of this crate's privileged suite — the equivalent of
/// `CAP_NET_ADMIN` over a set of network namespaces it owns, which a rootless user namespace
/// grants over namespaces it creates itself:
///
/// ```text
/// unshare --net --map-root-user -- \
///     cargo test -p ntkd --offline --test andna_e2e -- --ignored
/// ```
///
/// Not run by default `cargo test` — see `#[ignore]`.
#[ignore = "requires the equivalent of CAP_NET_ADMIN over its own network namespaces"]
#[tokio::test]
async fn hostname_registered_on_one_real_daemon_resolves_from_a_different_real_daemon() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .try_init();

    const DEV_A: &str = "andna-e2e-a";
    const DEV_B: &str = "andna-e2e-b";

    let hostname = hostname_routing_to_a();

    // The load-bearing routing-math proof (this file's module doc): node B is never the
    // hash-closest node for `hostname`, so its `contact_peer` self-loop can never fire for it —
    // any successful resolve below can only have come from a real round trip to node A.
    {
        let topo = topology();
        let tuple_a = TupleNode::new(topo.clone(), position(0)).unwrap();
        let tuple_b = TupleNode::new(topo.clone(), position(1)).unwrap();
        let target = ntk_peerservices::hash_to_tuple(&topo, hostname.hash().route_key());
        assert!(
            ntk_peerservices::dist(&topo, &target, &tuple_a)
                < ntk_peerservices::dist(&topo, &target, &tuple_b),
            "the chosen hostname must hash closer to node A's position than to node B's, or \
             node B's own `contact_peer` self-loop (`x.level == 0 && x.pos == my_pos[0]` in \
             ntk-peerservices/src/routing.rs) would serve the resolve locally, proving nothing \
             about the real socket/kernel path this test exists to exercise"
        );
    }

    let owner_key = SigningKey::from_bytes(&[42u8; 32]);
    let (ready_tx, ready_rx) = oneshot::channel::<Result<(), String>>();
    let (done_tx_a, done_rx_a) = oneshot::channel();
    let (done_tx_b, done_rx_b) = oneshot::channel();

    let worker_a = {
        let hostname = hostname.clone();
        netns::NamespaceWorker::spawn("andna-a", move || {
            node_a_body(
                NodeId::from_raw(701).unwrap(),
                DEV_A,
                hostname,
                owner_key,
                ready_tx,
                done_tx_a,
                done_rx_b,
            )
        })
    };
    let worker_b = {
        let hostname = hostname.clone();
        netns::NamespaceWorker::spawn("andna-b", move || {
            node_b_body(
                NodeId::from_raw(702).unwrap(),
                DEV_B,
                hostname,
                ready_rx,
                done_tx_b,
                done_rx_a,
            )
        })
    };

    let fds = [worker_a.fd(), worker_b.fd()];
    let root = netns::root_handle().expect("root rtnetlink handle");
    let segment = netns::Segment {
        name: "andnae2e",
        members: vec![
            netns::Member {
                node: 0,
                dev: DEV_A,
            },
            netns::Member {
                node: 1,
                dev: DEV_B,
            },
        ],
    };
    netns::wire(&root, std::slice::from_ref(&segment), &fds)
        .await
        .expect("wire veth segment");

    worker_a.signal_moved();
    worker_b.signal_moved();

    let join_timeout = CONVERGE_TIMEOUT + netns::JOIN_MARGIN;
    let report_a = worker_a
        .join(join_timeout)
        .await
        .expect("node A namespace body");
    let report_b = worker_b
        .join(join_timeout)
        .await
        .expect("node B namespace body");

    eprintln!("node-a: {report_a:#?}");
    eprintln!("node-b: {report_b:#?}");

    assert!(
        report_a.node.arc_cost(DEV_A).is_some(),
        "node A never measured a cost for its neighbor — no arc established: {:#?}",
        report_a.node.arcs
    );
    assert!(
        report_b.node.arc_cost(DEV_B).is_some(),
        "node B never measured a cost for its neighbor — no arc established: {:#?}",
        report_b.node.arcs
    );
    assert!(
        report_a.route_installed,
        "node A's kernel routing table never gained a route to node B's g-node ({}); \
         routes: {:#?}",
        report_a.expected_destination, report_a.node.routes
    );
    assert!(
        report_b.route_installed,
        "node B's kernel routing table never gained a route to node A's g-node ({}); \
         routes: {:#?}",
        report_b.expected_destination, report_b.node.routes
    );

    let register_outcome = report_a
        .register_outcome
        .expect("node A's report carries a register outcome")
        .expect("node A's real-socket ANDNA registration must succeed");
    assert!(
        matches!(register_outcome, RegisterOutcome::Registered { .. }),
        "expected a fresh Registered outcome, got {register_outcome:?}"
    );

    let records = report_b
        .resolve_result
        .expect("node B's report carries a resolve result")
        .expect("node B's real-socket ANDNA resolve of node A's hostname must succeed");
    assert_eq!(
        records.len(),
        1,
        "expected exactly the one zero-service record node A registered"
    );
    assert_eq!(
        records[0].target,
        SnsdTarget::Address(naddr(0)),
        "node B resolved {hostname} to a different address than node A's own naddr — combined \
         with the routing-math assertion above (node B can never be the hash-closest node for \
         this hostname), this is the proof that node B's answer came from a real \
         `PeersRpcHandler` round trip to node A, not a local read"
    );
}

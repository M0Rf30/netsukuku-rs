//! Multi-node integration tests for the `ntkd` daemon core, driving the real startup/steady-state
//! sequence ([`node::lifecycle::run`]) over an in-memory transport: [`ntk_netlink::FakeNetlink`]
//! for kernel state per simulated node, and a hand-rolled in-process [`Medium`] mirroring
//! upstream's `eth_domain` full-mesh relay plus its `FakeCommandDispatcher` golden-output harness
//! (`research/notes/02-vala-services-daemon.md` §6) for neighborhood broadcast/unicast and the
//! general per-arc RPC connection every other module's stub factory dials through.
//!
//! Every non-ignored test here uses real (not paused) `tokio::time`, but with
//! [`ntk_neighborhood::NeighborhoodTiming`] shortened to single-digit milliseconds — the crate's
//! own documented seam for "never sleep the real 28-30s/60s cadence" — so convergence is real
//! wall-clock work measured in milliseconds, not a simulated/paused clock.
//!
//! # Historical note: `chain_converges_then_arc_flap_reinstalls_only_the_affected_route`
//! This scenario was red under a confirmed `ntkd` composition defect, not a library bug — see
//! that test's own doc comment for the hypothesis, its refutation, and the real root cause.

use ntkd::kernel::addressing;
use ntkd::kernel::config::NtkdConfig;
use ntkd::node::adapters::QspnArcResolverAdapter;
use ntkd::node::dispatch::Dispatcher;
use ntkd::node::lifecycle::{self, Dialer, NodeInputs, StartedNode};
use ntkd::node::peers::PeerLinks;
use ntkd::node::registry::{LinkRegistry, encode_caller_id};
use ntkd::node::stubs::IdentityStubFactoryAdapter;

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use futures::future::BoxFuture;
use ntk_common::{Cost, HCoord, Naddr, Topology};
use ntk_neighborhood::{
    Arc as NeighborArc, FakeIpRouteManager, FixedRttProbe, LocalNic, NeighborhoodConfig,
    NeighborhoodRpcHandler, NeighborhoodStubFactory, NeighborhoodTiming, NodeId,
};
use ntk_netlink::{
    AddressTable, FakeNetlink, Interface, Ipv4Net, LinkInfo, Operation, RouteKey, RouteTable,
    RouteTarget, RuleSelector, RuleSpec, RuleTable,
};
use ntk_proto::v1::{CallerContext, MethodCall, RemoteError, ResponsePayload, TypedValue};
use ntk_rpc::{FakeRpcClient, RpcClient, RpcHandler};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------------------------
// Shared topology helpers
// ---------------------------------------------------------------------------------------------

fn topology() -> Topology {
    Topology::new([4, 2, 2, 2]).unwrap()
}

fn config() -> NtkdConfig {
    NtkdConfig::from_str("gsizes = [4, 2, 2, 2]\nnics = []\nport = 269\n").unwrap()
}

/// This test's convention: every node shares one topology and differs only at the innermost
/// level, so they all live in the same outer g-node.
fn position(idx: u32) -> Vec<u32> {
    vec![idx, 0, 0, 0]
}

fn naddr(idx: u32) -> Naddr {
    Naddr::new(topology(), position(idx)).unwrap()
}

/// The linklocal-style address node `idx` assigns its `n`-th monitored NIC (1-based, matching
/// call order into [`ntk_neighborhood::Handle::start_monitor`]) — see [`addr_allocator`].
fn nic_address(idx: u32, n: u8) -> Ipv4Addr {
    Ipv4Addr::new(10, 99, idx as u8, n)
}

fn nic_mac(idx: u32, domain_index: usize) -> String {
    format!("02:00:00:{idx:02x}:{domain_index:02x}:00")
}

// ---------------------------------------------------------------------------------------------
// Medium: in-memory transport mirroring upstream's eth_domain / system-ntkd harness
// ---------------------------------------------------------------------------------------------

/// The in-process stand-in for physical connectivity: named broadcast domains (one per shared
/// link, mirroring upstream's per-`dev` `eth_domain` relay process) for neighborhood discovery,
/// plus a general address->dispatcher directory for the per-arc [`Dialer`] every other module's
/// outbound stub factory (`identities`/`qspn`/`peers`/`coordinator`/`hooking`, via `PeerLinks`)
/// resolves through once an arc is established.
#[derive(Debug, Default)]
struct Medium {
    domains: Mutex<HashMap<String, Vec<(String, ntk_neighborhood::Handle)>>>,
    /// Keyed by the node-index octet [`nic_address`] embeds, not by the full address, so a
    /// re-`start_monitor` (an arc flap) picking a fresh counter value keeps resolving to the
    /// same node.
    dispatchers: Mutex<HashMap<u8, Arc<Dispatcher>>>,
}

impl Medium {
    fn join_domain(&self, domain: &str, node_id: &str, handle: ntk_neighborhood::Handle) {
        self.domains
            .lock()
            .unwrap()
            .entry(domain.to_owned())
            .or_default()
            .push((node_id.to_owned(), handle));
    }

    fn peers_on(&self, domain: &str, self_id: &str) -> Vec<ntk_neighborhood::Handle> {
        self.domains
            .lock()
            .unwrap()
            .get(domain)
            .into_iter()
            .flatten()
            .filter(|(id, _)| id != self_id)
            .map(|(_, h)| h.clone())
            .collect()
    }

    fn register_dispatcher(&self, idx: u32, dispatcher: Arc<Dispatcher>) {
        self.dispatchers
            .lock()
            .unwrap()
            .insert(idx as u8, dispatcher);
    }

    fn dispatcher_for_addr(&self, addr: &str) -> Option<Arc<Dispatcher>> {
        let ip: Ipv4Addr = addr.parse().ok()?;
        let idx = ip.octets()[2];
        self.dispatchers.lock().unwrap().get(&idx).cloned()
    }
}

/// Fans a broadcast `notify` out to every other member of a domain — the in-process equivalent
/// of upstream's `eth_domain` full-mesh relay (`research/notes/02-vala-services-daemon.md` §6).
/// Neighborhood only ever `.notify()`s broadcasts (never `.call()`s), so the reply this returns
/// is never observed.
#[derive(Debug)]
struct FanOutHandler(Vec<NeighborhoodRpcHandler>);

/// A real UDP broadcast's `notify()` returns the instant the packet is sent — it never waits for
/// a receiver to finish processing it. `FanOutHandler` must preserve that: spawning each target's
/// `handle()` as an independent task (rather than awaiting it inline) is load-bearing, not just
/// an optimization — awaiting inline would make a broadcast synchronously depend on every
/// receiver's *entire* reaction to it, including any call the receiver makes back to the
/// broadcaster (e.g. `handle_here_i_am`'s `request_arc` broadcast, answered by the receiver's own
/// `unicast().call(can_you_export)` back to us) — a receiver-calls-sender-while-sender-awaits-
/// receiver cycle that deadlocks both single-threaded actors.
impl RpcHandler for FanOutHandler {
    fn handle<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
        auth: Option<ntk_proto::v1::Auth>,
    ) -> BoxFuture<'a, Result<ResponsePayload, RemoteError>> {
        Box::pin(async move {
            for target in self.0.clone() {
                let caller = caller.clone();
                let unicast_id = unicast_id.clone();
                let call = call.clone();
                let auth = auth.clone();
                tokio::spawn(async move {
                    let _ = target.handle(caller, unicast_id, call, auth).await;
                });
            }
            Ok(ResponsePayload::default())
        })
    }
}

/// [`NeighborhoodStubFactory`] over [`Medium`]: `broadcast` fans out to every other domain
/// member; `unicast` resolves the single other member of a point-to-point domain directly, with
/// no `PeerLinks`/dial step involved — this is the negotiation-time seam neighborhood needs
/// *before* any general outbound connection exists (`can_you_export`/`nop`, sent while an arc is
/// still `Discovered`/`Requested`).
#[derive(Debug)]
struct TestNeighborhoodStubFactory {
    self_id: String,
    medium: Arc<Medium>,
}

impl NeighborhoodStubFactory for TestNeighborhoodStubFactory {
    fn broadcast(&self, dev: &str) -> Arc<dyn RpcClient> {
        let targets = self
            .medium
            .peers_on(dev, &self.self_id)
            .into_iter()
            .map(|h| NeighborhoodRpcHandler::for_broadcast(h, dev.to_owned()))
            .collect();
        Arc::new(FakeRpcClient::new(Arc::new(FanOutHandler(targets))))
    }

    fn unicast(&self, arc: &NeighborArc) -> Arc<dyn RpcClient> {
        let peer = self
            .medium
            .peers_on(&arc.my_dev, &self.self_id)
            .into_iter()
            .next()
            .unwrap_or_else(|| {
                panic!(
                    "test medium: no peer on domain {} for {}",
                    arc.my_dev, self.self_id
                )
            });
        Arc::new(FakeRpcClient::new(Arc::new(
            NeighborhoodRpcHandler::for_unicast(peer),
        )))
    }
}

/// The general [`Dialer`] every other module's stub factory resolves an outbound connection
/// through once `on_neighborhood_event`'s `ArcAdded` handler dials a newly-exported arc's
/// address. Resolves by the node-index octet [`Medium::dispatcher_for_addr`] extracts, so it
/// keeps working across an address reassigned by a re-`start_monitor`.
#[derive(Debug)]
struct TestDialer {
    medium: Arc<Medium>,
}

impl Dialer for TestDialer {
    fn dial(&self, addr: &str, _port: u16) -> BoxFuture<'_, Option<Arc<dyn RpcClient>>> {
        let target = self.medium.dispatcher_for_addr(addr);
        Box::pin(
            async move { target.map(|d| Arc::new(FakeRpcClient::new(d)) as Arc<dyn RpcClient>) },
        )
    }
}

/// Mints [`nic_address`] values in call order, matching [`ntk_neighborhood::Handle::start_monitor`]
/// call order in [`spawn_node`] (and any later re-`start_monitor` for a flap).
fn addr_allocator(idx: u32) -> Box<dyn FnMut() -> String + Send> {
    let mut n = 0u8;
    Box::new(move || {
        n += 1;
        nic_address(idx, n).to_string()
    })
}

// ---------------------------------------------------------------------------------------------
// Node harness
// ---------------------------------------------------------------------------------------------

struct SimNode {
    tasks: JoinSet<()>,
    cancel: CancellationToken,
    kernel: Arc<FakeNetlink>,
    neighborhood: ntk_neighborhood::Handle,
    started: StartedNode<FakeNetlink>,
}

impl SimNode {
    fn qspn(&self) -> ntk_qspn::QspnHandle {
        self.started.running.generation.borrow().qspn.clone()
    }

    fn route_table(&self) -> u32 {
        self.started.running.route_table
    }
}
/// Composes one simulated daemon instance over [`FakeNetlink`], joining `domains` (in order —
/// determines which [`nic_address`] each gets) on the shared [`Medium`].
async fn spawn_node(idx: u32, domains: &[&str], medium: &Arc<Medium>) -> SimNode {
    let mut links = vec![LinkInfo {
        index: 1,
        name: "lo".into(),
        is_up: true,
    }];
    for (i, dev) in domains.iter().enumerate() {
        links.push(LinkInfo {
            index: (i + 2) as u32,
            name: (*dev).to_owned(),
            is_up: true,
        });
    }
    let neighborhood_kernel = FakeNetlink::with_links(links.clone());
    let routing_kernel = Arc::new(FakeNetlink::with_links(links));

    let self_id = format!("node-{idx}");
    let stub_factory = Arc::new(TestNeighborhoodStubFactory {
        self_id: self_id.clone(),
        medium: medium.clone(),
    });

    let my_id = NodeId::from_raw((idx + 1) as i32).unwrap();
    let neighborhood_config = NeighborhoodConfig {
        my_id,
        max_arcs: 8,
        kernel: neighborhood_kernel,
        stub_factory,
        ip_route_manager: Arc::new(FakeIpRouteManager::new()),
        rtt_probe: Arc::new(FixedRttProbe(Some(10))),
        timing: NeighborhoodTiming {
            radar_interval: Duration::from_millis(15),
            arc_monitor_interval: (Duration::from_millis(1), Duration::from_millis(3)),
        },
        new_linklocal_address: addr_allocator(idx),
        signing_key: None,
        require_auth: false,
    };

    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let (neighborhood, neighborhood_join) =
        ntk_neighborhood::Manager::spawn(neighborhood_config, cancel.child_token());
    tasks.spawn(async move {
        let _ = neighborhood_join.await;
    });

    for dev in domains {
        medium.join_domain(dev, &self_id, neighborhood.clone());
    }

    let dialer = Arc::new(TestDialer {
        medium: medium.clone(),
    });
    let started = lifecycle::run(
        NodeInputs {
            config: config(),
            neighborhood: neighborhood.clone(),
            registry: Arc::new(LinkRegistry::new()),
            links: Arc::new(PeerLinks::new()),
            routing_kernel: routing_kernel.clone(),
            dialer,
            initial_position: Some(position(idx)),
            preformed: None,
            my_id,
        },
        &mut tasks,
        cancel.child_token(),
    )
    .await
    .expect("lifecycle::run");

    // `run()`'s last step spawns `run_steady_state`, which subscribes to
    // `neighborhood.subscribe()` as its very first statement. Spawning only *schedules* that
    // task; nothing guarantees it has actually run before `run()` returns. Monitoring (and
    // therefore the first possible `Event::ArcAdded`) must not start until that subscribe() has
    // definitely happened, or a fast-negotiating peer's very first event — broadcast-only,
    // `tokio::sync::broadcast` never replays history to a late subscriber — is silently lost
    // forever. A short yield gives the scheduler a guaranteed chance to poll it once.
    tokio::time::sleep(Duration::from_millis(5)).await;

    for (i, dev) in domains.iter().enumerate() {
        neighborhood
            .start_monitor(LocalNic {
                dev: (*dev).to_owned(),
                mac: nic_mac(idx, i),
            })
            .await
            .expect("start_monitor");
    }

    medium.register_dispatcher(idx, started.dispatcher.clone());

    SimNode {
        tasks,
        cancel,
        kernel: routing_kernel,
        neighborhood,
        started,
    }
}

/// Polls a synchronous predicate (no real sleep beyond small yields) until it is true or
/// `timeout` elapses.
async fn wait_until(mut check: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
}

fn cost_at(snapshot: &ntk_qspn::RouteSnapshot, pos: u32) -> Option<Cost> {
    snapshot
        .levels
        .first()?
        .iter()
        .find(|e| e.destination == HCoord::new(0, pos))?
        .paths
        .first()
        .map(|p| p.cost)
}

/// The destination CIDR an [`Operation`] mutates, or `None` for address/rule operations.
fn route_destination(op: &Operation) -> Option<&Ipv4Net> {
    match op {
        Operation::AddRoute(spec) | Operation::ChangeRoute(spec) => Some(&spec.destination),
        Operation::RemoveRoute(key) => Some(&key.destination),
        _ => None,
    }
}

/// The routing-table id `ntk_netlink::capability`'s multipath preflight probe uses
/// (`CAPABILITY_PROBE_TABLE`, not exported — vanishingly unlikely to collide with anything
/// real, hardcoded here to match). `preflight::check` issues a real add-then-remove route on
/// this table before `lifecycle::run` installs anything for the identity itself.
const CAPABILITY_PROBE_TABLE: u32 = 0xFFFF_FFF0;

fn is_capability_probe(op: &Operation) -> bool {
    match op {
        Operation::AddRoute(spec) | Operation::ChangeRoute(spec) => {
            spec.table == CAPABILITY_PROBE_TABLE
        }
        Operation::RemoveRoute(key) => key.table == CAPABILITY_PROBE_TABLE,
        _ => false,
    }
}

/// The exact route this daemon's own [`kernel::routes::RouteInstaller`] would install for
/// `my_idx`'s route to `dest_pos`, gatewayed through `via` on `dev` — computed with the crate's
/// own [`addressing`] functions so this is a prediction from first principles, not a copy of
/// whatever the daemon happens to produce.
fn expected_route(my_idx: u32, dest_pos: u32, via: Ipv4Addr, dev: &str) -> ntk_netlink::RouteSpec {
    ntk_netlink::RouteSpec {
        destination: addressing::gnode_destination(&naddr(my_idx), HCoord::new(0, dest_pos))
            .unwrap(),
        table: ntk_netlink::DEFAULT_MAIN_TABLE_ID,
        target: RouteTarget::Gateway {
            via,
            dev: Interface::name(dev),
            src: Some(addressing::host_address(&naddr(my_idx)).unwrap().address()),
        },
    }
}

const CONVERGE_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------------------------
// Scenario 0: LinkId-collision invariant (unit-level, no simulated network)
// ---------------------------------------------------------------------------------------------

/// The regression test that should have existed before `chain_converges_then_arc_flap_...` ever
/// went red: pins directly, at the daemon's arc-resolution seam, the invariant whose absence
/// caused that corruption — **no node may ever resolve an inbound call to an arc belonging to a
/// different peer**, even when two peers' independently-minted
/// [`ntkd::node::registry::LinkId`]s collide in raw value.
///
/// Peer A and peer B each discover exactly one arc, so each mints the identical first `LinkId`
/// from its own independent per-process counter — precisely the collision that misrouted
/// node1's inbound `get_full_etp` to its unrelated arc-to-node0 in the 3-node chain. The
/// receiver holds arcs to both and must disambiguate an inbound caller by the caller's own
/// stable `NodeId` (`crate::node::registry::encode_caller_id`), never by that colliding,
/// meaningless-across-processes `LinkId`.
#[test]
fn inbound_caller_never_resolves_to_a_different_peers_arc_on_link_id_collision() {
    let receiver_id = NodeId::from_raw(1).unwrap();
    let peer_a_id = NodeId::from_raw(101).unwrap();
    let peer_b_id = NodeId::from_raw(202).unwrap();

    // Each peer independently discovers its one and only arc (to the receiver), minting
    // LinkId(1) from its own fresh counter -- the exact collision this test exercises.
    let peer_a_registry = LinkRegistry::new();
    let peer_a_link = peer_a_registry.link_for_neighbour(receiver_id, "receiver-mac@a", "eth0");
    let peer_b_registry = LinkRegistry::new();
    let peer_b_link = peer_b_registry.link_for_neighbour(receiver_id, "receiver-mac@b", "eth0");
    assert_eq!(
        peer_a_link, peer_b_link,
        "peer A and peer B must independently mint the same first LinkId for this test to \
         exercise the real collision, not a hypothetical one"
    );

    // The receiver discovers both arcs, each keyed by the *peer's* own mac/NodeId. Its own
    // LinkIds for them are irrelevant to this test: the whole point is that resolution must
    // never depend on that node-local value at all.
    let receiver_registry = Arc::new(LinkRegistry::new());
    let receiver_link_to_a = receiver_registry.link_for_neighbour(peer_a_id, "a-mac", "eth0");
    let receiver_link_to_b = receiver_registry.link_for_neighbour(peer_b_id, "b-mac", "eth1");
    let qspn_arc_a = ntk_qspn::ArcId::from(11u32);
    let qspn_arc_b = ntk_qspn::ArcId::from(22u32);
    receiver_registry.set_qspn_arc(receiver_link_to_a, qspn_arc_a);
    receiver_registry.set_qspn_arc(receiver_link_to_b, qspn_arc_b);

    // Exactly what each peer's outbound stub embeds: its own stable NodeId, never the
    // (colliding) LinkId it minted locally for the arc.
    let caller_from_a = CallerContext {
        source_id: None,
        src_nic: Some(encode_caller_id(peer_a_id)),
    };
    let caller_from_b = CallerContext {
        source_id: None,
        src_nic: Some(encode_caller_id(peer_b_id)),
    };

    // Decode site 1: `QspnArcResolverAdapter::resolve`, the seam real qspn traffic goes through.
    let qspn_resolver = QspnArcResolverAdapter {
        registry: receiver_registry.clone(),
    };
    assert_eq!(
        ntk_qspn::ArcResolver::resolve(&qspn_resolver, &caller_from_a),
        Some(qspn_arc_a),
        "peer A's inbound call must resolve to the receiver's own arc-to-A"
    );
    assert_eq!(
        ntk_qspn::ArcResolver::resolve(&qspn_resolver, &caller_from_b),
        Some(qspn_arc_b),
        "peer B's inbound call must resolve to the receiver's own arc-to-B -- never arc-to-A --\
         despite peer A and peer B having minted the identical LinkId(1) locally"
    );

    // Decode site 2: `IdentityStubFactoryAdapter::arc_for_caller`, the same seam for identities.
    let identity_stub_factory = IdentityStubFactoryAdapter {
        links: Arc::new(PeerLinks::new()),
        registry: receiver_registry.clone(),
    };
    assert_eq!(
        ntk_identities::IdentityStubFactory::arc_for_caller(&identity_stub_factory, &caller_from_a),
        Some(receiver_link_to_a.identities()),
        "peer A's inbound call must resolve to the receiver's own identities arc-to-A"
    );
    assert_eq!(
        ntk_identities::IdentityStubFactory::arc_for_caller(&identity_stub_factory, &caller_from_b),
        Some(receiver_link_to_b.identities()),
        "peer B's inbound call must resolve to the receiver's own identities arc-to-B -- never \
         arc-to-A"
    );
}

// ---------------------------------------------------------------------------------------------
// Scenario 1: multi-node convergence + arc flap
// ---------------------------------------------------------------------------------------------

/// A 3-node chain (0 — 1 — 2) converges to the exact route set the topology implies, over the
/// real daemon core (`node::lifecycle::run`) driven by [`FakeNetlink`] and [`Medium`]. Then
/// flaps the middle node's arc to node 0 and asserts reconvergence reinstalls exactly the
/// affected route (removed, then re-added identically), leaving node 1's unrelated route to
/// node 2 completely untouched.
///
/// # Root-cause history: node-local `LinkId` conflated across the wire (not `ntk-qspn`)
/// This scenario was red: node1 (the only node with two arcs) lost its route-snapshot entry for
/// one of its two direct neighbors, even though node0 and node2 both converged fully correctly.
///
/// Initial hypothesis: an `ntk-qspn` implicit-withdrawal bug in `revise_etp`/`update_map`.
/// Refuted by instrumented trace: both the implicit-withdrawal filter
/// (`existing_paths_via_arc`) and the `peer_naddr_changed` identity-migration rule correctly
/// scope to `path.arcs[0] == arc` — the arc an ETP actually arrived on, matching
/// `research/impl/vala/qspn/qspn.vala:1185-1223` — confirmed by three `ntk-qspn` regression
/// tests pinning that scoping, all green on unmodified `ntk-qspn`.
///
/// Real cause: `crate::node::registry::LinkId` was minted *per-node-locally* from each node's
/// own counter (every node starts at 1), then shipped *raw* as `CallerContext.src_nic` and
/// decoded by the receiving node against its *own* registry
/// (`QspnArcResolverAdapter::resolve`/`IdentityStubFactory::arc_for_caller`) as if it were a
/// `LinkId` that node itself minted. In this chain, node2's `LinkId(1)` (its only link) collided
/// with node1's own `LinkId(1)` (its unrelated link to node0): node2's inbound `get_full_etp`
/// misresolved to node1's arc-to-node0, and `ntk-qspn`'s `peer_naddr_changed` rule then correctly
/// concluded that arc's peer had migrated and withdrew node0's old position — right behavior on
/// garbage input. Deterministic given each node's counter, not a race: staggering node2's join
/// only flipped *which* arc's entry survived. Fixed by resolving an inbound arc from the
/// receiving side's own view of the caller, never the caller's local counter — see
/// `registry.rs`/`adapters.rs`/`stubs.rs`. Left asserting the fully-correct converged state
/// throughout — a real bug never got a weakened assertion.
#[tokio::test]
async fn chain_converges_then_arc_flap_reinstalls_only_the_affected_route() {
    let medium = Arc::new(Medium::default());
    let node0 = spawn_node(0, &["link-a"], &medium).await;
    let node1 = spawn_node(1, &["link-a", "link-b"], &medium).await;
    let node2 = spawn_node(2, &["link-b"], &medium).await;

    // -- Convergence: every node learns the other two, cost accumulating hop-by-hop. --
    let converged = wait_until(
        || {
            cost_at(&node0.qspn().snapshot(), 1) == Some(Cost::Finite(10))
                && cost_at(&node0.qspn().snapshot(), 2) == Some(Cost::Finite(20))
                && cost_at(&node1.qspn().snapshot(), 0) == Some(Cost::Finite(10))
                && cost_at(&node1.qspn().snapshot(), 2) == Some(Cost::Finite(10))
                && cost_at(&node2.qspn().snapshot(), 1) == Some(Cost::Finite(10))
                && cost_at(&node2.qspn().snapshot(), 0) == Some(Cost::Finite(20))
        },
        CONVERGE_TIMEOUT,
    )
    .await;
    if !converged {
        eprintln!("node0 snapshot: {:?}", node0.qspn().snapshot());
        eprintln!("node1 snapshot: {:?}", node1.qspn().snapshot());
        eprintln!("node2 snapshot: {:?}", node2.qspn().snapshot());
        eprintln!(
            "node0 arcs: {:?}",
            node0.neighborhood.snapshot().borrow().clone()
        );
        eprintln!(
            "node1 arcs: {:?}",
            node1.neighborhood.snapshot().borrow().clone()
        );
        eprintln!(
            "node2 arcs: {:?}",
            node2.neighborhood.snapshot().borrow().clone()
        );
        eprintln!(
            "node1 current_arcs: {:?}",
            node1.qspn().current_arcs().await
        );
    }
    assert!(
        converged,
        "chain did not converge to the expected route set in time"
    );

    // -- The installed kernel routes match the converged snapshot exactly: real destinations,
    //    real gateway choices, not merely "something got installed". --
    let expected0_1 = expected_route(0, 1, nic_address(1, 1), "link-a");
    let expected0_2 = expected_route(0, 2, nic_address(1, 1), "link-a");
    let expected1_0 = expected_route(1, 0, nic_address(0, 1), "link-a");
    let expected1_2 = expected_route(1, 2, nic_address(2, 1), "link-b");
    let expected2_1 = expected_route(2, 1, nic_address(1, 2), "link-b");
    let expected2_0 = expected_route(2, 0, nic_address(1, 2), "link-b");

    let installed = wait_until(
        || {
            node0
                .kernel
                .operations()
                .iter()
                .any(|op| op == &Operation::AddRoute(expected0_1.clone()))
                && node0
                    .kernel
                    .operations()
                    .iter()
                    .any(|op| op == &Operation::AddRoute(expected0_2.clone()))
                && node1
                    .kernel
                    .operations()
                    .iter()
                    .any(|op| op == &Operation::AddRoute(expected1_0.clone()))
                && node1
                    .kernel
                    .operations()
                    .iter()
                    .any(|op| op == &Operation::AddRoute(expected1_2.clone()))
                && node2
                    .kernel
                    .operations()
                    .iter()
                    .any(|op| op == &Operation::AddRoute(expected2_1.clone()))
                && node2
                    .kernel
                    .operations()
                    .iter()
                    .any(|op| op == &Operation::AddRoute(expected2_0.clone()))
        },
        CONVERGE_TIMEOUT,
    )
    .await;
    assert!(
        installed,
        "converged routes were not installed into the kernel model"
    );

    let node1_routes = node1
        .kernel
        .list_routes(Some(node1.route_table()))
        .await
        .unwrap();
    assert_eq!(
        node1_routes.len(),
        2,
        "node1 must have exactly the two converged destinations installed, nothing stale"
    );

    // -- Arc flap: node1's link to node0 goes down and back up. --
    node1.kernel.clear_operations();
    node1.neighborhood.stop_monitor("link-a").await.unwrap();
    node1
        .neighborhood
        .start_monitor(LocalNic {
            dev: "link-a".to_owned(),
            mac: nic_mac(1, 0),
        })
        .await
        .unwrap();

    let reconverged = wait_until(
        || cost_at(&node1.qspn().snapshot(), 0) == Some(Cost::Finite(10)),
        CONVERGE_TIMEOUT,
    )
    .await;
    assert!(
        reconverged,
        "arc to node0 did not reconverge after the flap"
    );

    let reinstalled = wait_until(
        || {
            node1
                .kernel
                .operations()
                .iter()
                .any(|op| op == &Operation::AddRoute(expected1_0.clone()))
        },
        CONVERGE_TIMEOUT,
    )
    .await;
    assert!(
        reinstalled,
        "route to node0 was not reinstalled, identically, after the flap"
    );

    let ops_after_flap = node1.kernel.operations();
    assert!(
        ops_after_flap.iter().any(
            |op| matches!(op, Operation::RemoveRoute(k) if k.destination == expected1_0.destination)
        ),
        "flap must remove the stale route to node0 before reinstalling it: {ops_after_flap:?}"
    );
    assert!(
        !ops_after_flap
            .iter()
            .any(|op| route_destination(op) == Some(&expected1_2.destination)),
        "flap of the link to node0 must not touch node1's unrelated route to node2: {ops_after_flap:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// Scenario 2: shutdown/cleanup
// ---------------------------------------------------------------------------------------------

/// Cancelling the root [`CancellationToken`] reaps every task this node spawned (the `JoinSet`
/// drains within a bound), and graceful teardown removes exactly what this run installed, in the
/// exact reverse order — mirroring `supervisor::run`'s own shutdown sequence (cancel, drain,
/// then [`kernel::routes::RouteInstaller::teardown`]) — proving a killed daemon leaves no kernel
/// state behind.
#[tokio::test]
async fn cancellation_reaps_tasks_and_teardown_is_exact_inverse_of_install() {
    let medium = Arc::new(Medium::default());
    let mut node0 = spawn_node(0, &["link-a"], &medium).await;
    let _node1 = spawn_node(1, &["link-a"], &medium).await;

    let converged = wait_until(
        || cost_at(&node0.qspn().snapshot(), 1) == Some(Cost::Finite(10)),
        CONVERGE_TIMEOUT,
    )
    .await;
    assert!(converged, "arc did not establish in time");

    let expected_route0_1 = expected_route(0, 1, nic_address(1, 1), "link-a");
    let installed = wait_until(
        || {
            node0
                .kernel
                .operations()
                .iter()
                .any(|op| op == &Operation::AddRoute(expected_route0_1.clone()))
        },
        CONVERGE_TIMEOUT,
    )
    .await;
    assert!(installed, "route to node1 was not installed in time");

    // `lifecycle::run`'s preflight capability check probes multipath support with its own real
    // add-then-remove route against `CAPABILITY_PROBE_TABLE` (`ntk_netlink::capability`) before
    // installing anything for this identity — expected, unrelated to what this test asserts on.
    let raw_install_ops = node0.kernel.operations();
    let install_ops: Vec<Operation> = raw_install_ops
        .iter()
        .filter(|op| !is_capability_probe(op))
        .cloned()
        .collect();
    let expected_install = vec![
        Operation::AddAddress {
            interface: Interface::name("lo"),
            network: addressing::host_address(&naddr(0)).unwrap(),
        },
        Operation::AddRule(RuleSpec {
            table: ntk_netlink::DEFAULT_MAIN_TABLE_ID,
            priority: ntk_netlink::DEFAULT_MAIN_RULE_PRIORITY,
            selector: RuleSelector::Any,
        }),
        Operation::AddRoute(expected_route0_1.clone()),
    ];
    assert_eq!(
        install_ops, expected_install,
        "install log must be exactly [address, rule, route] with no churn"
    );

    // -- Cancellation reaps every spawned task. --
    node0.cancel.cancel();
    let tasks = &mut node0.tasks;
    let drained = tokio::time::timeout(Duration::from_secs(5), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    assert!(
        drained.is_ok(),
        "JoinSet did not drain within 5s after cancelling the root token — some task ignored it"
    );

    // -- Graceful teardown removes exactly what was installed, in exact reverse order. --
    node0
        .started
        .running
        .route_installer
        .lock()
        .await
        .teardown()
        .await
        .unwrap();
    let after_teardown = node0.kernel.operations();
    let teardown_ops = &after_teardown[raw_install_ops.len()..];
    let expected_teardown = vec![
        Operation::RemoveRoute(RouteKey {
            destination: expected_route0_1.destination,
            table: ntk_netlink::DEFAULT_MAIN_TABLE_ID,
        }),
        Operation::RemoveRule(RuleSpec {
            table: ntk_netlink::DEFAULT_MAIN_TABLE_ID,
            priority: ntk_netlink::DEFAULT_MAIN_RULE_PRIORITY,
            selector: RuleSelector::Any,
        }),
        Operation::RemoveAddress {
            interface: Interface::name("lo"),
            network: addressing::host_address(&naddr(0)).unwrap(),
        },
    ];
    assert_eq!(
        teardown_ops,
        expected_teardown.as_slice(),
        "teardown log must be the exact reverse of the install log"
    );

    // Independent confirmation via the kernel model's own queries, not just the op log.
    assert!(
        node0
            .kernel
            .list_routes(Some(node0.route_table()))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(node0.kernel.list_rules().await.unwrap().is_empty());
    assert!(node0.kernel.list_addresses(None).await.unwrap().is_empty());
}

// ---------------------------------------------------------------------------------------------
// Scenario 3: real-kernel netns smoke test (ignored by default)
// ---------------------------------------------------------------------------------------------
//
// Fixture design: no `ip`/subprocess calls anywhere below — this project replaced upstream's
// shelled-out `ip`/`iptables` with native netlink specifically because shelling out was a
// "concrete, fixable weakness" (`research/notes/06-rust-stack.md`, `research/notes/
// 02-vala-services-daemon.md` §5); a test fixture that shells out to `ip netns add`/`ip link`
// to build its own scenario would contradict that. Namespaces are created with
// `nix::sched::unshare(CloneFlags::CLONE_NEWNET)`; the veth pair and link-up state (the two
// primitives `ntk_netlink` deliberately has no API for — it only ever needed to manage
// addresses/routes/rules, never link creation/state) go through raw `rtnetlink`; everything
// address/route-shaped after that goes through `ntk_netlink::RealNetlink`, the code under test.
//
// # Threading: one dedicated OS thread per namespace, pinned for its entire life
// `setns`/`unshare(CLONE_NEWNET)` change the *calling thread's* namespace, not the process's —
// and a multi-thread tokio runtime will freely resume a suspended `.await` on any other worker
// thread, silently un-pinning it from the namespace it started in. Get this wrong and every
// symptom is confusing and nondeterministic (a route add that mysteriously lands in the wrong
// namespace, a socket that mysteriously can't reach its peer) rather than a clean failure.
// The fix here: each namespace gets its own plain `std::thread`, which calls `unshare` once,
// then builds and drives its *own* `tokio::runtime::Builder::new_current_thread()` runtime via
// `block_on` for the rest of its life. A current-thread runtime never migrates a task to another
// OS thread — there is no other thread in it — so every `.await` inside that `block_on` call
// provably keeps running on the one thread that is actually pinned to that namespace. Only the
// veth pair's creation and its move into each namespace happen on the *un-unshared* main test
// thread (the only one that can still see both the veth's original namespace and, by fd, each
// worker's new one).
//
// This also means no `setns()` call is needed anywhere: each worker thread enters its namespace
// exactly once (via `unshare`) and never needs to re-enter a different one afterward; moving the
// veth end is a link *attribute* (`IFLA_NET_NS_FD`) the kernel applies on the sender's behalf,
// not a namespace switch performed by the sender itself.
//
// # Namespace lifetime: no explicit cleanup needed
// An anonymous network namespace (one never bind-mounted under `/run/netns`, i.e. never touched
// by `ip netns add`) is destroyed automatically once nothing references it — no thread is
// pinned to it and no fd holds it open. Each worker thread holds its own namespace's `/proc/
// thread-self/ns/net` fd for its entire life and is the only thread ever pinned to it, so the
// namespace (and both veth ends, and everything the daemon installed into it) is reclaimed the
// instant the thread exits, on any exit path including a panic. No `Drop`-based guard required.

/// One namespace worker's final findings, read back through `ntk_netlink::RealNetlink` — an
/// observer connection independent of the one the daemon itself writes through — rather than
/// trusted from the daemon's own in-process state alone.
#[derive(Debug)]
struct NamespaceReport {
    label: &'static str,
    /// This node's own [`ntk_neighborhood::Handle::snapshot`] when polling stopped —
    /// diagnostic only: shows whether the *daemon* believes it has a neighbor, independent of
    /// whether the real kernel route below ever appeared.
    arcs: Vec<NeighborArc>,
    /// The route CIDR `ntkd::kernel::addressing::gnode_destination` predicts for a direct arc to
    /// the peer, computed from the two nodes' known topology positions from first principles —
    /// not copied from whatever the daemon happened to install.
    expected_destination: Ipv4Net,
    /// Whether `expected_destination` appeared in a real `RealNetlink::list_routes` call before
    /// the timeout.
    route_installed: bool,
    routes: Vec<ntk_netlink::RouteSpec>,
    addresses: Vec<ntk_netlink::AddressEntry>,
}

/// Everything [`run_namespace_worker`] needs for one side of the pair.
struct NamespaceSpec {
    label: &'static str,
    my_id: NodeId,
    my_idx: u32,
    peer_idx: u32,
    dev: &'static str,
    port: u16,
    /// Sends this thread's own netns fd (as a raw value — fd numbers are shared process-wide
    /// across threads, so the coordinator can use it directly) to the coordinator.
    fd_tx: std::sync::mpsc::Sender<std::os::fd::RawFd>,
    /// Blocks until the coordinator has moved this namespace's veth end in.
    moved_rx: std::sync::mpsc::Receiver<()>,
    /// Signalled by [`namespace_body`] once *this* side's own polling loop has finished (route
    /// found or deadline hit) — see that function's doc comment for why this side must not tear
    /// its own namespace down before the peer has finished too. `Option` only so
    /// [`namespace_body`] can [`Option::take`] it through a `&mut` borrow; always `Some` until
    /// taken exactly once.
    my_done_tx: Option<tokio::sync::oneshot::Sender<()>>,
    /// Awaited by [`namespace_body`] before tearing this side's own namespace down. Same
    /// always-`Some`-until-taken-once contract as `my_done_tx`.
    peer_done_rx: Option<tokio::sync::oneshot::Receiver<()>>,
    report_tx: tokio::sync::oneshot::Sender<anyhow::Result<NamespaceReport>>,
}

/// Resolves `name` to its current kernel `ifindex` via a raw link dump. Used only for the two
/// link-identity/state operations `ntk_netlink` has no API for (see this section's own doc);
/// every address/route lookup after this point goes through `ntk_netlink::RealNetlink` instead.
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

/// `ip link set <name> up`, natively: `LinkHandle::change` sends the same `RTM_NEWLINK`/
/// `NLM_F_REQUEST|NLM_F_ACK` request `ip link set` itself sends — `LinkHandle::set`'s
/// `RTM_SETLINK` is reserved for bridge/vlan port config, not ordinary interface changes.
async fn bring_link_up(handle: &rtnetlink::Handle, name: &str) -> anyhow::Result<()> {
    let index = link_index(handle, name).await?;
    handle
        .link()
        .change(rtnetlink::LinkUnspec::new_with_index(index).up().build())
        .execute()
        .await
        .with_context(|| format!("bringing {name:?} up"))
}

/// Composes one real `ntkd` node against the real kernel over `dev` — the exact production
/// wiring `ntkd::node::transport::start` uses (`RealNetlink`, a real `UdpBroadcaster`/
/// `TcpServer`, `NeighborhoodStubFactoryAdapter`, `RealIpRouteManager`), reproduced here rather
/// than calling `transport::start` directly because it hardcodes the two knobs this scenario
/// needs to vary:
///
/// - `my_id`: `transport::start` always calls [`NodeId::generate`]; this scenario wants a known,
///   deterministic id per side.
/// - `initial_position`: `transport::start` always passes `None` — a brand-new identity
///   bootstraps as its own network-of-one at the all-zero address (`lifecycle::run`'s module
///   doc). Two such freshly-booted identities meeting for the first time would then have to
///   merge via `ntk-hooking`'s network-merge protocol before qspn has any real position to route
///   to — and `lifecycle::run`'s own module doc names identity migration during a merge as
///   explicitly *not* fully implemented yet ("Scope boundary: identity migration during network
///   merge"). Exercising that unfinished path is not what this scenario is for. Instead both
///   sides get distinct, already-known positions *in the same topology* — precisely
///   [`NodeInputs::initial_position`]'s documented intended use for "a multi-node test harness
///   composing several `run` calls against one shared kernel/topology" — so the direct-neighbor
///   arc and its route are real without also exercising the separately-scoped merge path.
async fn spawn_real_node(
    my_id: NodeId,
    position: Vec<u32>,
    dev: &str,
    port: u16,
    tasks: &mut JoinSet<()>,
    cancel: CancellationToken,
) -> anyhow::Result<StartedNode<ntk_netlink::RealNetlink>> {
    use ntk_rpc::{TcpServer, UdpBroadcaster};
    use ntkd::node::ip_route::RealIpRouteManager;
    use ntkd::node::lifecycle::{TcpDialer, linklocal_allocator, synthetic_mac};
    use ntkd::node::peers::PeerLinks;
    use ntkd::node::registry::LinkRegistry;
    use ntkd::node::stubs::NeighborhoodStubFactoryAdapter;

    let config = NtkdConfig::from_str(&format!(
        "gsizes = [4, 2, 2, 2]\nnics = [\"{dev}\"]\nport = {port}\n"
    ))?;

    let registry = Arc::new(LinkRegistry::new());
    let links = Arc::new(PeerLinks::new());

    let broadcaster = Arc::new(UdpBroadcaster::bind(Some(dev), port, 1 << 16)?);
    let mut broadcasters = HashMap::new();
    broadcasters.insert(dev.to_owned(), broadcaster);

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

    neighborhood
        .start_monitor(LocalNic {
            dev: dev.to_owned(),
            mac: synthetic_mac(dev, my_id),
        })
        .await?;

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

/// Runs entirely inside the namespace worker thread's own `current_thread` runtime (see this
/// section's doc comment): brings `lo` and `dev` up (freshly created namespaces start with only
/// a down `lo`, and [`ntk_neighborhood::Handle::start_monitor`] refuses a down interface),
/// composes the real node, then polls the *real kernel* — never just the daemon's own snapshot —
/// until either the predicted route to the peer appears or the timeout elapses.
///
/// # Why this waits for the peer before tearing its own namespace down
/// `dev` (`ntkd-test-a`/`ntkd-test-b`) is one end of a veth *pair* — deleting either end deletes
/// both, regardless of which network namespace each end lives in. Once this function returns,
/// `run_namespace_worker` drops this thread's own netns fd and the thread exits, and per this
/// section's own doc comment that reclaims the namespace (and everything in it, including this
/// side's veth end) the instant nothing references it any more — which would delete the peer's
/// still-live veth end too, out from under a peer that has not finished yet. Rendezvousing here
/// (`my_done_tx`/`peer_done_rx`) ensures neither side's namespace — and therefore neither side's
/// veth end — is reclaimed until *both* sides have finished their own polling loop.
async fn namespace_body(spec: &mut NamespaceSpec) -> anyhow::Result<NamespaceReport> {
    let (connection, handle, _) = rtnetlink::new_connection()
        .with_context(|| format!("{}: rtnetlink connection", spec.label))?;
    tokio::spawn(connection);
    bring_link_up(&handle, "lo")
        .await
        .with_context(|| format!("{}: bring lo up", spec.label))?;
    bring_link_up(&handle, spec.dev)
        .await
        .with_context(|| format!("{}: bring {} up", spec.label, spec.dev))?;
    drop(handle);

    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let started = spawn_real_node(
        spec.my_id,
        position(spec.my_idx),
        spec.dev,
        spec.port,
        &mut tasks,
        cancel.clone(),
    )
    .await
    .with_context(|| format!("{}: compose real node", spec.label))?;

    let observer = ntk_netlink::RealNetlink::new()
        .with_context(|| format!("{}: observer RealNetlink", spec.label))?;
    let expected_destination =
        addressing::gnode_destination(&naddr(spec.my_idx), HCoord::new(0, spec.peer_idx))
            .with_context(|| format!("{}: expected destination", spec.label))?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let (route_installed, routes, arcs) = loop {
        let arcs: Vec<NeighborArc> = started.running.neighborhood.snapshot().borrow().clone();
        let routes = observer
            .list_routes(Some(started.running.route_table))
            .await
            .with_context(|| format!("{}: list_routes", spec.label))?;
        let found = routes.iter().any(|r| r.destination == expected_destination);
        if found || tokio::time::Instant::now() >= deadline {
            break (found, routes, arcs);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let addresses = observer
        .list_addresses(None)
        .await
        .with_context(|| format!("{}: list_addresses", spec.label))?;

    // Rendezvous with the peer (this function's doc comment) before either side's namespace —
    // and therefore either side's veth end — can be reclaimed.
    let my_done_tx = spec
        .my_done_tx
        .take()
        .expect("my_done_tx is taken exactly once, here");
    let peer_done_rx = spec
        .peer_done_rx
        .take()
        .expect("peer_done_rx is taken exactly once, here");
    let _ = my_done_tx.send(());
    let _ = peer_done_rx.await;

    // Graceful per-identity teardown, mirroring `supervisor::run`'s own shutdown sequence — the
    // namespace itself is reclaimed once this thread exits regardless (this section's doc
    // comment), but tearing down cleanly here proves this run leaves no kernel state behind, the
    // same property `cancellation_reaps_tasks_and_teardown_is_exact_inverse_of_install` already
    // proves against `FakeNetlink`.
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

    Ok(NamespaceReport {
        label: spec.label,
        arcs,
        expected_destination,
        route_installed,
        routes,
        addresses,
    })
}

/// One namespace's entire life, on its own dedicated OS thread (this section's doc comment):
/// `unshare` into a fresh network namespace, hand the coordinator this namespace's fd, wait for
/// it to move this side's veth end in, then drive [`namespace_body`] to completion on a
/// `current_thread` runtime built and run from this same thread.
fn run_namespace_worker(mut spec: NamespaceSpec) {
    let outcome = (|| -> anyhow::Result<NamespaceReport> {
        use std::os::fd::AsRawFd;

        nix::sched::unshare(nix::sched::CloneFlags::CLONE_NEWNET)
            .map_err(|errno| anyhow::anyhow!("{}: unshare(CLONE_NEWNET): {errno}", spec.label))?;
        // Held until this closure returns: its fd must stay valid for the coordinator's
        // `setns_by_fd` call below, and holding it keeps this namespace alive independently of
        // this thread's own pinning for as long as this function runs.
        let ns_file = std::fs::File::open("/proc/thread-self/ns/net")
            .with_context(|| format!("{}: open own netns fd", spec.label))?;
        spec.fd_tx
            .send(ns_file.as_raw_fd())
            .map_err(|_| anyhow::anyhow!("{}: coordinator dropped fd channel", spec.label))?;
        spec.moved_rx.recv().map_err(|_| {
            anyhow::anyhow!("{}: coordinator dropped veth-moved signal", spec.label)
        })?;

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

/// Two real `ntkd` node compositions (via [`lifecycle::run`], the real
/// [`ntk_netlink::RealNetlink`] backend, distinct [`NodeId`]s), each in its own network
/// namespace, joined by a veth pair created natively (this section's doc comment), attempt to
/// exchange enough over the veth to establish a neighborhood arc and install a real kernel route
/// to each other — verified by reading the route back through an independent `RealNetlink`
/// connection, not by trusting either daemon's own snapshot.
///
/// # History: two confirmed real-kernel-only bugs, since fixed
/// This was the first test to run `ntk_neighborhood`'s real transport (`RealIpRouteManager` +
/// `ntk_rpc::UdpBroadcaster`/`serve_broadcast`) against a genuine kernel — every other test in
/// this file drives the identical protocol logic over `FakeNetlink`/`Medium`, which never
/// enforces real IP semantics. Two independent, compounding bugs in that real transport meant no
/// `here_i_am` broadcast was ever received by either side, so no arc — and therefore no route —
/// was ever established:
///
/// 1. **`RealIpRouteManager::add_address` (`crates/ntkd/src/node/ip_route.rs`) installed the
///    neighborhood linklocal address as a `/32` host route (`Ipv4Net::host(addr)`).** RFC 3927
///    linklocal addressing requires the full `169.254.0.0/16` for a reason: a `/32` gives the
///    kernel no connected-subnet route on that interface, and — confirmed by a minimal
///    reproduction outside this codebase, two `unshare(CLONE_NEWNET)` namespaces joined by a
///    veth, one side `169.254.0.1/32`, the other `169.254.0.2/32` — a real Linux kernel then
///    silently fails to deliver `INADDR_BROADCAST` (255.255.255.255) datagrams sent from that
///    interface to the peer, even though `sendto()` itself reports success. Widening only the
///    prefix (both sides distinct `/16` addresses, same reproduction) restores delivery. Fixed:
///    `crate::node::ip_route::linklocal_net` now installs the full `/16`.
/// 2. **`crate::node::lifecycle::linklocal_allocator` started its per-process counter at a
///    fixed `1`.** Two freshly-started `ntkd` processes each monitoring their first (and, in the
///    common two-node case, only) NIC therefore *both* self-assigned the identical address
///    `169.254.0.1` — confirmed by the same minimal reproduction: two `/16` addresses that are
///    *equal* on both sides of the veth also fail to exchange a broadcast (a real kernel treats
///    an inbound packet claiming your own configured source address as martian and drops it),
///    while two *distinct* `/16` addresses succeed. Fixed: `linklocal_allocator` now salts
///    `derive_linklocal` with this identity's own `my_id`, hashed to a
///    per-node starting slot in a structurally-distinct-per-NIC address space.
///
/// Both lived in `crates/ntkd/src/node/{ip_route.rs,lifecycle.rs}`. This test intentionally kept
/// using the real, unmodified `RealIpRouteManager` and `linklocal_allocator` — the actual
/// production seams — throughout, so it was an honest red signal for exactly these defects and
/// turned green on its own once they were fixed (verified below).
///
/// # Fixed: an independent-teardown veth-cascade failure
/// While deliberately re-running this test to catch the warning below, it also intermittently
/// (~2 of ~24 runs) *failed* outright with `ns-b never measured a cost for its neighbor — no arc
/// established: []`, ns-b's own log flooded with `here_i_am broadcast failed error=i/o error: No
/// such device (os error 19) dev="ntkd-test-b"` — ns-b's own veth end had vanished from under it.
/// Root cause: `dev` here is one end of a veth *pair*, and deleting either end deletes both,
/// regardless of which network namespace each end lives in. Each `namespace_body` used to call
/// `cancel.cancel()` (and then return, letting its thread exit) the moment *its own* expected
/// route appeared, with no regard for whether the peer had finished — and per this function's own
/// doc comment, an anonymous network namespace (and everything in it, including its veth end) is
/// reclaimed the instant its pinning thread exits. So it was possible for ns-a to finish, exit,
/// and have its namespace (and `ntkd-test-a`) reclaimed *before* ns-b had finished — which deleted
/// `ntkd-test-b` right along with it, out from under a still-running ns-b. Fixed by adding a
/// two-party rendezvous (`NamespaceSpec::my_done_tx`/`peer_done_rx`, awaited in `namespace_body`
/// right after the polling loop and before either side tears anything down): neither side's
/// namespace can be reclaimed until *both* sides have finished polling.
///
/// # Investigated: a hooking merge-negotiation warning, not a bug
/// A run of this test can log, during network-merge negotiation (`ntk_hooking::arc`):
/// `WARN ... bad arc on authoritative retrieve_network_data ... connection closed`. Diagnosed
/// and confirmed **not** a defect in `ntkd` or `ntk-hooking`:
///
/// - Both identities here bootstrap as their own network-of-one (`n_nodes() == 1` on each side,
///   `lifecycle::run`'s `network_id = random_i64()`), so the arc always ties on
///   `merge_direction` and both sides' `ntk_hooking::arc::run_arc_handler` take the
///   `AskCoordinator` branch, issuing an *authoritative* `retrieve_network_data(true)` back to
///   each other (`research/impl/vala/hooking/arc_handler.vala:179-208`).
/// - `RpcError::ConnectionClosed` is produced by exactly two places in `ntk_rpc::client::run_actor`
///   (the read half seeing EOF/an error, or the pending-reply channel being dropped) — both mean
///   the *peer's* socket genuinely closed, not a local resolution bug. A standalone repro (a
///   `TcpServer`/`TcpRpcClient` pair, one in-flight call, the *server's* `CancellationToken`
///   fired mid-call) reproduces exactly this: the caller's `.call()` resolves to
///   `Err(RpcError::ConnectionClosed)`, matching `ntk_rpc::server::serve_connection`'s
///   `cancel.cancelled() => { inflight.abort_all(); break; }` arm, which drops the accepted
///   socket without waiting for in-flight replies.
/// - Each `namespace_body` calls `cancel.cancel()` (tearing that identity's whole `TcpServer`
///   down) the moment *their own* expected route appears — a condition satisfied by
///   `ntk_neighborhood`/`ntk_qspn` alone, independent of whether hooking's merge has finished. The
///   rendezvous added for the veth-cascade fix above now keeps both sides' teardown synchronized
///   to (roughly) the same instant rather than independently-timed, which narrows but does not
///   eliminate the window: if either side's authoritative call is still genuinely in flight at
///   that shared instant, this warning can still fire.
/// - `ntk_hooking::arc::run_arc_handler`'s response — `warn!` then mark the arc `Failed` — is
///   exactly upstream's own response to the equivalent `StubError` on the same authoritative call
///   (`arc_handler.vala:184-199`: `warning(...); signal_and_exit(ia);`), i.e. the correct,
///   upstream-matching reaction to a peer that has genuinely disconnected mid-negotiation. It
///   does not affect this test's arc/route assertions (route installation does not depend on
///   hooking's merge completing) and needs no severity change: `WARN` already matches upstream's
///   own `warning(...)` for this exact case.
///
/// # Running
/// Needs the same capability the rest of this crate's privileged suite needs — the equivalent of
/// `CAP_NET_ADMIN` over a set of network namespaces it owns — which a rootless user namespace
/// grants over namespaces it creates itself. Verified running, repeatedly green, exactly as:
///
/// ```text
/// unshare --net --map-root-user -- \
///     cargo test -p ntkd --test multi_node -- --ignored real_netns_two_daemons_establish_arc_and_route
/// ```
///
/// Real root (`sudo -E` in place of `unshare --net --map-root-user --`) works identically; the
/// unprivileged form is preferred since, like the rest of this crate's privileged suite, it needs
/// no host capability beyond a user namespace the invoking user already owns.
///
/// Not run by default `cargo test` — see `#[ignore]`.
#[ignore = "requires the equivalent of CAP_NET_ADMIN over its own network namespaces"]
#[tokio::test]
async fn real_netns_two_daemons_establish_arc_and_route() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .try_init();

    const VETH_A: &str = "ntkd-test-a";
    const VETH_B: &str = "ntkd-test-b";
    const PORT: u16 = 27269;

    let (connection, root, _) = rtnetlink::new_connection().expect("rtnetlink connection");
    tokio::spawn(connection);
    root.link()
        .add(rtnetlink::LinkVeth::new(VETH_A, VETH_B).build())
        .execute()
        .await
        .expect("create veth pair");
    let idx_a = link_index(&root, VETH_A)
        .await
        .expect("resolve ntkd-test-a");
    let idx_b = link_index(&root, VETH_B)
        .await
        .expect("resolve ntkd-test-b");

    let (fd_tx_a, fd_rx_a) = std::sync::mpsc::channel();
    let (fd_tx_b, fd_rx_b) = std::sync::mpsc::channel();
    let (moved_tx_a, moved_rx_a) = std::sync::mpsc::channel();
    let (moved_tx_b, moved_rx_b) = std::sync::mpsc::channel();
    let (report_tx_a, report_rx_a) = tokio::sync::oneshot::channel();
    let (report_tx_b, report_rx_b) = tokio::sync::oneshot::channel();
    let (done_tx_a, done_rx_a) = tokio::sync::oneshot::channel();
    let (done_tx_b, done_rx_b) = tokio::sync::oneshot::channel();

    let thread_a = std::thread::spawn(move || {
        run_namespace_worker(NamespaceSpec {
            label: "ns-a",
            my_id: NodeId::from_raw(101).unwrap(),
            my_idx: 0,
            peer_idx: 1,
            dev: VETH_A,
            port: PORT,
            fd_tx: fd_tx_a,
            moved_rx: moved_rx_a,
            my_done_tx: Some(done_tx_a),
            peer_done_rx: Some(done_rx_b),
            report_tx: report_tx_a,
        });
    });
    let thread_b = std::thread::spawn(move || {
        run_namespace_worker(NamespaceSpec {
            label: "ns-b",
            my_id: NodeId::from_raw(102).unwrap(),
            my_idx: 1,
            peer_idx: 0,
            dev: VETH_B,
            port: PORT,
            fd_tx: fd_tx_b,
            moved_rx: moved_rx_b,
            my_done_tx: Some(done_tx_b),
            peer_done_rx: Some(done_rx_a),
            report_tx: report_tx_b,
        });
    });

    // Brief, bounded blocking recvs (each worker sends within microseconds of starting) with
    // nothing else scheduled on this runtime at this point — not worth `spawn_blocking`.
    let fd_a = fd_rx_a.recv().expect("ns-a netns fd");
    let fd_b = fd_rx_b.recv().expect("ns-b netns fd");

    root.link()
        .change(
            rtnetlink::LinkUnspec::new_with_index(idx_a)
                .setns_by_fd(fd_a)
                .build(),
        )
        .execute()
        .await
        .expect("move ntkd-test-a into ns-a");
    root.link()
        .change(
            rtnetlink::LinkUnspec::new_with_index(idx_b)
                .setns_by_fd(fd_b)
                .build(),
        )
        .execute()
        .await
        .expect("move ntkd-test-b into ns-b");

    moved_tx_a.send(()).expect("signal ns-a");
    moved_tx_b.send(()).expect("signal ns-b");

    let report_a = report_rx_a.await.expect("ns-a report channel");
    let report_b = report_rx_b.await.expect("ns-b report channel");
    thread_a
        .join()
        .unwrap_or_else(|e| panic!("ns-a worker thread panicked: {e:?}"));
    thread_b
        .join()
        .unwrap_or_else(|e| panic!("ns-b worker thread panicked: {e:?}"));

    let report_a = report_a.expect("ns-a namespace body");
    let report_b = report_b.expect("ns-b namespace body");

    eprintln!(
        "{}: arcs={:#?} routes={:#?}",
        report_a.label, report_a.arcs, report_a.routes
    );
    eprintln!(
        "{}: arcs={:#?} routes={:#?}",
        report_b.label, report_b.arcs, report_b.routes
    );

    assert!(
        report_a.arcs.iter().any(|a| a.cost.is_some()),
        "ns-a never measured a cost for its neighbor — no arc established: {:#?}",
        report_a.arcs
    );
    assert!(
        report_b.arcs.iter().any(|a| a.cost.is_some()),
        "ns-b never measured a cost for its neighbor — no arc established: {:#?}",
        report_b.arcs
    );
    assert!(
        report_a.route_installed,
        "ns-a's real kernel routing table never gained a route to {} (its neighbor's g-node); \
         routes: {:#?}, addresses: {:#?}, arcs: {:#?}",
        report_a.expected_destination, report_a.routes, report_a.addresses, report_a.arcs
    );
    assert!(
        report_b.route_installed,
        "ns-b's real kernel routing table never gained a route to {} (its neighbor's g-node); \
         routes: {:#?}, addresses: {:#?}, arcs: {:#?}",
        report_b.expected_destination, report_b.routes, report_b.addresses, report_b.arcs
    );
}

// ---------------------------------------------------------------------------------------------
// Scenario 3 (privileged): two virgin daemons negotiate a shared network over a real veth pair
// ---------------------------------------------------------------------------------------------

/// This scenario's own single-level topology (distinct from [`topology`]'s `[4, 2, 2, 2]`,
/// which `real_netns_two_daemons_establish_arc_and_route` deliberately keeps untouched) —
/// matching `crate::node::negotiation_tests`' in-memory `gsizes = [8]` harness, whose fix this
/// test verifies survives the real kernel and real transport.
fn negotiation_topology() -> Topology {
    Topology::new([8]).unwrap()
}

/// One namespace worker's findings for the negotiation scenario, read back through
/// `ntk_netlink::RealNetlink` (an observer connection independent of the daemon's own) — mirrors
/// [`NamespaceReport`]'s "trust the kernel, not just in-process state" discipline, but for a
/// peer whose final position is unknown ahead of time: both sides here bootstrap
/// `initial_position: None`, so which one (if either) ends up adopting a negotiated position is
/// this test's own observation, not a precondition.
#[derive(Debug)]
struct NegotiationNamespaceReport {
    label: &'static str,
    initial_position: Vec<u32>,
    final_position: Vec<u32>,
    /// Mirrors `crate::node::lifecycle::GenerationHandles::rehooked` verbatim — see that
    /// field's own doc for why this is a dedicated daemon-reported flag, not a
    /// `final_position != initial_position` inference (unsound: the negotiated position can
    /// coincidentally equal the discarded one).
    rehooked: bool,
    route_count: usize,
    routes: Vec<ntk_netlink::RouteSpec>,
    addresses: Vec<ntk_netlink::AddressEntry>,
}

/// Everything [`run_negotiation_namespace_worker`] needs for one side of the pair — the
/// negotiation-scenario analogue of [`NamespaceSpec`], minus `my_idx`/`peer_idx` (meaningless
/// here: neither side's position is known ahead of time).
struct NegotiationNamespaceSpec {
    label: &'static str,
    my_id: NodeId,
    dev: &'static str,
    port: u16,
    fd_tx: std::sync::mpsc::Sender<std::os::fd::RawFd>,
    moved_rx: std::sync::mpsc::Receiver<()>,
    my_done_tx: Option<tokio::sync::oneshot::Sender<()>>,
    peer_done_rx: Option<tokio::sync::oneshot::Receiver<()>>,
    report_tx: tokio::sync::oneshot::Sender<anyhow::Result<NegotiationNamespaceReport>>,
}

/// Composes one real `ntkd` node against the real kernel over `dev`, identical to
/// [`spawn_real_node`] except `initial_position: None` (the production path this scenario
/// exists to exercise — see [`spawn_real_node`]'s own doc comment for why *that* test avoids
/// it) and [`negotiation_topology`]'s single-level `[8]` gsizes.
async fn spawn_real_negotiating_node(
    my_id: NodeId,
    dev: &str,
    port: u16,
    tasks: &mut JoinSet<()>,
    cancel: CancellationToken,
) -> anyhow::Result<StartedNode<ntk_netlink::RealNetlink>> {
    use ntk_rpc::{TcpServer, UdpBroadcaster};
    use ntkd::node::ip_route::RealIpRouteManager;
    use ntkd::node::lifecycle::{TcpDialer, linklocal_allocator, synthetic_mac};
    use ntkd::node::peers::PeerLinks;
    use ntkd::node::registry::LinkRegistry;
    use ntkd::node::stubs::NeighborhoodStubFactoryAdapter;

    let config = NtkdConfig::from_str(&format!(
        "gsizes = [8]\nnics = [\"{dev}\"]\nport = {port}\n"
    ))?;

    let registry = Arc::new(LinkRegistry::new());
    let links = Arc::new(PeerLinks::new());

    let broadcaster = Arc::new(UdpBroadcaster::bind(Some(dev), port, 1 << 16)?);
    let mut broadcasters = HashMap::new();
    broadcasters.insert(dev.to_owned(), broadcaster);

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

    neighborhood
        .start_monitor(LocalNic {
            dev: dev.to_owned(),
            mac: synthetic_mac(dev, my_id),
        })
        .await?;

    let routing_kernel = Arc::new(ntk_netlink::RealNetlink::new()?);
    let started = lifecycle::run(
        NodeInputs {
            config,
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

/// Runs entirely inside the namespace worker thread's own runtime (see [`namespace_body`]'s doc
/// comment for the shared rationale): brings `lo`/`dev` up, composes the real node at a
/// *negotiated* position, then polls until either this identity's own `Naddr` changes (it lost
/// the merge tiebreak and [`crate::node::lifecycle::rehook`] adopted the Coordinator-reserved
/// position) or a real kernel route appears (it won the tiebreak, stayed put, and the peer's
/// migration converged qspn/routes on this side) — whichever this side actually turns out to
/// be, never assumed ahead of time.
async fn negotiation_namespace_body(
    spec: &mut NegotiationNamespaceSpec,
) -> anyhow::Result<NegotiationNamespaceReport> {
    let (connection, handle, _) = rtnetlink::new_connection()
        .with_context(|| format!("{}: rtnetlink connection", spec.label))?;
    tokio::spawn(connection);
    bring_link_up(&handle, "lo")
        .await
        .with_context(|| format!("{}: bring lo up", spec.label))?;
    bring_link_up(&handle, spec.dev)
        .await
        .with_context(|| format!("{}: bring {} up", spec.label, spec.dev))?;
    drop(handle);

    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let started =
        spawn_real_negotiating_node(spec.my_id, spec.dev, spec.port, &mut tasks, cancel.clone())
            .await
            .with_context(|| format!("{}: compose real negotiating node", spec.label))?;

    let initial_position = started
        .running
        .generation
        .borrow()
        .qspn
        .my_naddr()
        .positions()
        .to_vec();

    let observer = ntk_netlink::RealNetlink::new()
        .with_context(|| format!("{}: observer RealNetlink", spec.label))?;

    // Unlike `namespace_body`'s fixed-position scenario, a real kernel route to the peer's
    // *linklocal* address appears almost immediately from ordinary qspn arc convergence — long
    // before hooking's merge negotiation even starts (`ntk_neighborhood`'s own discovery
    // cadence) — so route presence alone is never a valid "negotiation settled" signal here.
    // The only unambiguous local signal is `GenerationHandles::rehooked` itself (see its own
    // doc comment): a numeric `positions()` comparison looks equivalent but is unsound — the
    // Coordinator-reserved position can coincidentally equal this identity's own discarded
    // starting position, which a stress run of exactly this test once caught (the guest's own
    // log showed a complete, successful rehook, yet `positions()` before and after matched).
    // Once `rehooked` latches, `rehook` still needs a beat to tear down the old generation's
    // kernel state and re-attach the known arc to the new one
    // (`crate::node::lifecycle::reattach_known_arcs`), so this keeps polling until a route
    // reappears too. The winner has no rehook signal at all and simply waits out the deadline.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
    let (final_position, routes, rehooked) = loop {
        let generation = started.running.generation.borrow().clone();
        let position = generation.qspn.my_naddr().positions().to_vec();
        let routes = observer
            .list_routes(Some(started.running.route_table))
            .await
            .with_context(|| format!("{}: list_routes", spec.label))?;
        let rehooked_and_reattached = generation.rehooked && !routes.is_empty();
        if rehooked_and_reattached || tokio::time::Instant::now() >= deadline {
            break (position, routes, generation.rehooked);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let addresses = observer
        .list_addresses(None)
        .await
        .with_context(|| format!("{}: list_addresses", spec.label))?;

    // Rendezvous with the peer before either side's namespace (and veth end) is reclaimed —
    // identical rationale to `namespace_body`'s own doc comment.
    let my_done_tx = spec
        .my_done_tx
        .take()
        .expect("my_done_tx is taken exactly once, here");
    let peer_done_rx = spec
        .peer_done_rx
        .take()
        .expect("peer_done_rx is taken exactly once, here");
    let _ = my_done_tx.send(());
    let _ = peer_done_rx.await;

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

    Ok(NegotiationNamespaceReport {
        label: spec.label,
        initial_position,
        final_position,
        rehooked,
        route_count: routes.len(),
        routes,
        addresses,
    })
}

/// One namespace's entire life for the negotiation scenario, on its own dedicated OS thread —
/// identical structure to [`run_namespace_worker`], driving [`negotiation_namespace_body`]
/// instead.
fn run_negotiation_namespace_worker(mut spec: NegotiationNamespaceSpec) {
    let outcome = (|| -> anyhow::Result<NegotiationNamespaceReport> {
        use std::os::fd::AsRawFd;

        nix::sched::unshare(nix::sched::CloneFlags::CLONE_NEWNET)
            .map_err(|errno| anyhow::anyhow!("{}: unshare(CLONE_NEWNET): {errno}", spec.label))?;
        let ns_file = std::fs::File::open("/proc/thread-self/ns/net")
            .with_context(|| format!("{}: open own netns fd", spec.label))?;
        spec.fd_tx
            .send(ns_file.as_raw_fd())
            .map_err(|_| anyhow::anyhow!("{}: coordinator dropped fd channel", spec.label))?;
        spec.moved_rx.recv().map_err(|_| {
            anyhow::anyhow!("{}: coordinator dropped veth-moved signal", spec.label)
        })?;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .with_context(|| format!("{}: build current-thread runtime", spec.label))?;
        let report = rt.block_on(negotiation_namespace_body(&mut spec));
        drop(ns_file);
        report
    })();
    let _ = spec.report_tx.send(outcome);
}

/// Two real, virgin `ntkd` daemons — each bootstrapping `initial_position: None`, the
/// production path (`crate::node::lifecycle::derive_initial_position`) — meet over a real veth
/// pair and genuinely negotiate a shared network: exactly one adopts the other's
/// Coordinator-reserved position end to end (`crate::node::lifecycle::rehook`), verified against
/// the real kernel address table and routing table, not the daemon's own in-process state.
/// Complements `real_netns_two_daemons_establish_arc_and_route` (fixed, pre-known positions;
/// the merge path deliberately not exercised there — see its own doc comment) by exercising
/// exactly that path for real, over the identical real transport/kernel stack.
///
/// # Running
/// Needs the same capability as the rest of this crate's privileged suite. Verified running,
/// repeatedly green, exactly as:
///
/// ```text
/// unshare --net --map-root-user -- \
///     cargo test -p ntkd --test multi_node -- --ignored real_netns_two_daemons_negotiate_a_shared_network
/// ```
///
/// Not run by default `cargo test` — see `#[ignore]`.
#[ignore = "requires the equivalent of CAP_NET_ADMIN over its own network namespaces"]
#[tokio::test]
async fn real_netns_two_daemons_negotiate_a_shared_network() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .with_thread_names(true)
        .try_init();

    const VETH_A: &str = "ntkd-negot-a";
    const VETH_B: &str = "ntkd-negot-b";
    const PORT: u16 = 27271;

    let (connection, root, _) = rtnetlink::new_connection().expect("rtnetlink connection");
    tokio::spawn(connection);
    root.link()
        .add(rtnetlink::LinkVeth::new(VETH_A, VETH_B).build())
        .execute()
        .await
        .expect("create veth pair");
    let idx_a = link_index(&root, VETH_A)
        .await
        .expect("resolve ntkd-negot-a");
    let idx_b = link_index(&root, VETH_B)
        .await
        .expect("resolve ntkd-negot-b");

    let (fd_tx_a, fd_rx_a) = std::sync::mpsc::channel();
    let (fd_tx_b, fd_rx_b) = std::sync::mpsc::channel();
    let (moved_tx_a, moved_rx_a) = std::sync::mpsc::channel();
    let (moved_tx_b, moved_rx_b) = std::sync::mpsc::channel();
    let (report_tx_a, report_rx_a) = tokio::sync::oneshot::channel();
    let (report_tx_b, report_rx_b) = tokio::sync::oneshot::channel();
    let (done_tx_a, done_rx_a) = tokio::sync::oneshot::channel();
    let (done_tx_b, done_rx_b) = tokio::sync::oneshot::channel();

    let thread_a = std::thread::Builder::new()
        .name("ns-a".to_string())
        .spawn(move || {
            run_negotiation_namespace_worker(NegotiationNamespaceSpec {
                label: "ns-a",
                my_id: NodeId::from_raw(201).unwrap(),
                dev: VETH_A,
                port: PORT,
                fd_tx: fd_tx_a,
                moved_rx: moved_rx_a,
                my_done_tx: Some(done_tx_a),
                peer_done_rx: Some(done_rx_b),
                report_tx: report_tx_a,
            });
        })
        .expect("spawn ns-a worker thread");
    let thread_b = std::thread::Builder::new()
        .name("ns-b".to_string())
        .spawn(move || {
            run_negotiation_namespace_worker(NegotiationNamespaceSpec {
                label: "ns-b",
                my_id: NodeId::from_raw(202).unwrap(),
                dev: VETH_B,
                port: PORT,
                fd_tx: fd_tx_b,
                moved_rx: moved_rx_b,
                my_done_tx: Some(done_tx_b),
                peer_done_rx: Some(done_rx_a),
                report_tx: report_tx_b,
            });
        })
        .expect("spawn ns-b worker thread");

    let fd_a = fd_rx_a.recv().expect("ns-a netns fd");
    let fd_b = fd_rx_b.recv().expect("ns-b netns fd");

    root.link()
        .change(
            rtnetlink::LinkUnspec::new_with_index(idx_a)
                .setns_by_fd(fd_a)
                .build(),
        )
        .execute()
        .await
        .expect("move ntkd-negot-a into ns-a");
    root.link()
        .change(
            rtnetlink::LinkUnspec::new_with_index(idx_b)
                .setns_by_fd(fd_b)
                .build(),
        )
        .execute()
        .await
        .expect("move ntkd-negot-b into ns-b");

    moved_tx_a.send(()).expect("signal ns-a");
    moved_tx_b.send(()).expect("signal ns-b");

    let report_a = report_rx_a.await.expect("ns-a report channel");
    let report_b = report_rx_b.await.expect("ns-b report channel");
    thread_a
        .join()
        .unwrap_or_else(|e| panic!("ns-a worker thread panicked: {e:?}"));
    thread_b
        .join()
        .unwrap_or_else(|e| panic!("ns-b worker thread panicked: {e:?}"));

    let report_a = report_a.expect("ns-a namespace body");
    let report_b = report_b.expect("ns-b namespace body");

    eprintln!("{}: {report_a:#?}", report_a.label);
    eprintln!("{}: {report_b:#?}", report_b.label);

    assert_ne!(
        report_a.rehooked, report_b.rehooked,
        "exactly one side should adopt the negotiated position, never both or neither"
    );
    let (winner, loser) = if report_a.rehooked {
        (&report_b, &report_a)
    } else {
        (&report_a, &report_b)
    };

    assert_eq!(
        loser.final_position.len(),
        1,
        "single-level topology: a resolved position always has exactly one entry"
    );
    assert_ne!(
        loser.final_position, winner.initial_position,
        "the loser must resolve to a *free* slot in the winner's network, not the winner's own \
         already-occupied position"
    );

    let topology = negotiation_topology();
    let old_address = addressing::host_address(
        &Naddr::new(topology.clone(), loser.initial_position.clone()).unwrap(),
    )
    .expect("loser's own trivial address is always addressable");
    let new_address =
        addressing::host_address(&Naddr::new(topology, loser.final_position.clone()).unwrap())
            .expect("loser's negotiated address is always addressable");

    // The loser's negotiated position can coincidentally equal its own discarded trivial
    // position (both are independently derived — see `GenerationHandles::rehooked`'s own doc
    // comment for a captured case), making `old_address == new_address` a real, expected
    // outcome, not a leak. Asserting `old_address`'s bare absence is therefore unsound: it
    // would fail on the coincidence even though nothing leaked. Counting every Netsukuku-space
    // (`10.0.0.0/8`) address instead catches a genuine leak (`old_address` surviving alongside
    // a *different* `new_address` would show up as a second entry) without false-flagging the
    // coincidence (which naturally still has only one entry).
    let netsukuku_addresses: Vec<_> = loser
        .addresses
        .iter()
        .filter(|a| a.network.address().octets()[0] == 10)
        .collect();
    assert_eq!(
        netsukuku_addresses.len(),
        1,
        "the loser's real kernel address table should carry exactly one Netsukuku-space \
         address (its negotiated one, {new_address}) — a second entry would mean its \
         torn-down trivial-generation address {old_address} leaked alongside it: {:#?}",
        loser.addresses
    );
    assert_eq!(
        netsukuku_addresses[0].network, new_address,
        "the loser's sole Netsukuku-space address is not its negotiated one: {:#?}",
        loser.addresses
    );

    assert!(
        winner.route_count >= 1,
        "the winner's real kernel routing table never gained a route to its (migrated) peer: \
         {:#?}",
        winner.routes
    );
    assert!(
        loser.route_count >= 1,
        "the loser's real kernel routing table never gained a route to its (winner) peer after \
         rehook re-attached the arc: {:#?}",
        loser.routes
    );
}

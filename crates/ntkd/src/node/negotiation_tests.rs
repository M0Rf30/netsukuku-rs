//! Unit-level coverage for the negotiated re-address path (`crate::node::lifecycle`'s
//! "Negotiated re-address" module doc): alone stays create-net-equivalent, a discovered peer
//! resolves a real join and the daemon adopts it (rehook), an unresolvable arc (incompatible
//! topology) recovers to a well-defined steady state instead of wedging, and a rehook's
//! generation swap tears down the previous generation's kernel state exactly once.
//!
//! Same in-memory harness shape as `tests/multi_node.rs` (`FakeNetlink` per node, a hand-rolled
//! `Medium` standing in for `eth_domain`/broadcast), trimmed to a single shared domain and a
//! single-level topology — this module only exercises the daemon's own reaction to hooking's
//! resolved position, not qspn/route convergence details (already covered by `tests/
//! multi_node.rs` and `ntk-hooking`'s own arc-handler test suite).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use ntk_hooking::{ArcPhase, CoordinatorClient as HookingCoordinatorClient, EvaluateEnterRequest};
use ntk_neighborhood::{
    Arc as NeighborArc, FakeIpRouteManager, FixedRttProbe, LocalNic, NeighborhoodConfig,
    NeighborhoodRpcHandler, NeighborhoodStubFactory, NeighborhoodTiming, NodeId,
};
use ntk_netlink::{FakeNetlink, LinkInfo, Operation};
use ntk_proto::v1::{CallerContext, MethodCall, RemoteError, ResponsePayload, TypedValue};
use ntk_rpc::{FakeRpcClient, RpcClient, RpcHandler};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::kernel::config::NtkdConfig;
use crate::node::adapters::CoordinatorClientAdapter;
use crate::node::dispatch::Dispatcher;
use crate::node::lifecycle::{self, Dialer, NodeInputs, StartedNode};
use crate::node::peers::PeerLinks;
use crate::node::registry::LinkRegistry;

fn config(gsizes: &str) -> NtkdConfig {
    NtkdConfig::from_str(&format!("gsizes = {gsizes}\nnics = []\nport = 269\n")).unwrap()
}

/// Matches this daemon's own production wiring (`crate::node::services::coordinator_config`'s
/// `n_nodes_cache_ttl`) for tests that don't care about `decide_merge`'s freshness window
/// itself — long enough that a sleep-free sequence of `.await`s never crosses it.
const TEST_MERGE_DECISION_TTL: Duration = Duration::from_millis(200);

// -------------------------------------------------------------------------------------------
// Medium: in-memory transport (trimmed `tests/multi_node.rs` pattern — one shared domain)
// -------------------------------------------------------------------------------------------

#[derive(Debug, Default)]
struct Medium {
    domain: Mutex<Vec<(String, ntk_neighborhood::Handle)>>,
    dispatchers: Mutex<HashMap<u8, Arc<Dispatcher>>>,
}

impl Medium {
    fn join(&self, node_id: &str, handle: ntk_neighborhood::Handle) {
        self.domain
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((node_id.to_owned(), handle));
    }

    fn peers_of(&self, self_id: &str) -> Vec<ntk_neighborhood::Handle> {
        self.domain
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(id, _)| id != self_id)
            .map(|(_, h)| h.clone())
            .collect()
    }

    /// Resolves the specific peer a unicast RPC targets, by its stable [`NodeId`] — `"node-{idx}"`
    /// self-ids encode `idx`, from which the same `NodeId::from_raw((idx + 1))` every
    /// [`spawn_node`] assigns is recoverable. Unlike a bare "first other peer" pick (only ever
    /// correct for exactly two nodes on the domain), this is what makes 3+-node unicast RPCs
    /// (`nop`/liveness probes, arc-specific requests) reach the arc's actual peer rather than
    /// whichever node happened to join the domain first.
    fn peer_by_id(&self, self_id: &str, neighbour_id: NodeId) -> Option<ntk_neighborhood::Handle> {
        self.domain
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(id, _)| id != self_id)
            .find(|(id, _)| {
                id.strip_prefix("node-")
                    .and_then(|idx| idx.parse::<i32>().ok())
                    .and_then(|idx| NodeId::from_raw(idx + 1).ok())
                    == Some(neighbour_id)
            })
            .map(|(_, h)| h.clone())
    }

    fn register_dispatcher(&self, idx: u32, dispatcher: Arc<Dispatcher>) {
        self.dispatchers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(idx as u8, dispatcher);
    }

    fn dispatcher_for_addr(&self, addr: &str) -> Option<Arc<Dispatcher>> {
        let ip: std::net::Ipv4Addr = addr.parse().ok()?;
        self.dispatchers
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&ip.octets()[2])
            .cloned()
    }
}

#[derive(Debug)]
struct FanOutHandler(Vec<NeighborhoodRpcHandler>);

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

#[derive(Debug)]
struct TestNeighborhoodStubFactory {
    self_id: String,
    medium: Arc<Medium>,
}

impl NeighborhoodStubFactory for TestNeighborhoodStubFactory {
    fn broadcast(&self, dev: &str) -> Arc<dyn RpcClient> {
        let targets = self
            .medium
            .peers_of(&self.self_id)
            .into_iter()
            .map(|h| NeighborhoodRpcHandler::for_broadcast(h, dev.to_owned()))
            .collect();
        Arc::new(FakeRpcClient::new(Arc::new(FanOutHandler(targets))))
    }

    fn unicast(&self, arc: &NeighborArc) -> Arc<dyn RpcClient> {
        let peer = self
            .medium
            .peer_by_id(&self.self_id, arc.neighbour_id)
            .expect("test medium: no peer matching this arc's neighbour_id");
        Arc::new(FakeRpcClient::new(Arc::new(
            NeighborhoodRpcHandler::for_unicast(peer),
        )))
    }
}

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

fn addr_allocator(idx: u32) -> Box<dyn FnMut() -> String + Send> {
    let mut n = 0u8;
    Box::new(move || {
        n += 1;
        std::net::Ipv4Addr::new(10, 88, idx as u8, n).to_string()
    })
}

// -------------------------------------------------------------------------------------------
// Node harness
// -------------------------------------------------------------------------------------------

struct SimNode {
    _tasks: JoinSet<()>,
    _cancel: CancellationToken,
    kernel: Arc<FakeNetlink>,
    started: StartedNode<FakeNetlink>,
}

impl SimNode {
    fn qspn(&self) -> ntk_qspn::QspnHandle {
        self.started.running.generation.borrow().qspn.clone()
    }

    fn hooking(&self) -> &ntk_hooking::HookingHandle {
        &self.started.running.hooking
    }
}

/// Composes one simulated daemon instance over [`FakeNetlink`], joining the shared one-domain
/// [`Medium`]. `initial_position: None` exercises the real negotiated path end to end.
async fn spawn_node(
    idx: u32,
    gsizes: &str,
    initial_position: Option<Vec<u32>>,
    medium: &Arc<Medium>,
) -> SimNode {
    let links = vec![
        LinkInfo {
            index: 1,
            name: "lo".into(),
            is_up: true,
        },
        LinkInfo {
            index: 2,
            name: "eth0".into(),
            is_up: true,
        },
    ];
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

    medium.join(&self_id, neighborhood.clone());

    let dialer = Arc::new(TestDialer {
        medium: medium.clone(),
    });
    let started = lifecycle::run(
        NodeInputs {
            config: config(gsizes),
            neighborhood: neighborhood.clone(),
            registry: Arc::new(LinkRegistry::new()),
            links: Arc::new(PeerLinks::new()),
            routing_kernel: routing_kernel.clone(),
            dialer,
            initial_position,
            preformed: None,
            my_id,
        },
        &mut tasks,
        cancel.child_token(),
    )
    .await
    .expect("lifecycle::run");

    // See `tests/multi_node.rs::spawn_node`'s identical comment: `run_steady_state` must
    // actually have subscribed to `neighborhood.subscribe()` before monitoring starts, or a
    // fast peer's very first `ArcAdded` is silently lost (broadcast channels never replay).
    tokio::time::sleep(Duration::from_millis(5)).await;

    neighborhood
        .start_monitor(LocalNic {
            dev: "eth0".to_owned(),
            mac: format!("02:00:00:00:{idx:02x}:00"),
        })
        .await
        .expect("start_monitor");

    medium.register_dispatcher(idx, started.dispatcher.clone());

    SimNode {
        _tasks: tasks,
        _cancel: cancel,
        kernel: routing_kernel,
        started,
    }
}

async fn wait_until(mut check: impl FnMut() -> bool, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if check() {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return check();
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn address_and_rule_ops(kernel: &FakeNetlink) -> (usize, usize, usize, usize) {
    let ops = kernel.operations();
    let adds = ops
        .iter()
        .filter(|o| matches!(o, Operation::AddAddress { .. }))
        .count();
    let removes = ops
        .iter()
        .filter(|o| matches!(o, Operation::RemoveAddress { .. }))
        .count();
    let rule_adds = ops
        .iter()
        .filter(|o| matches!(o, Operation::AddRule(_)))
        .count();
    let rule_removes = ops
        .iter()
        .filter(|o| matches!(o, Operation::RemoveRule(_)))
        .count();
    (adds, removes, rule_adds, rule_removes)
}

// -------------------------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------------------------

/// Alone (no peer ever discovered), a negotiated identity is its own permanent, immediately-
/// hooked network-of-one — exactly `create_net`'s upstream semantics — and never moves.
#[tokio::test]
async fn lone_identity_stays_at_its_own_position_and_stays_hooked() {
    let medium = Arc::new(Medium::default());
    let node = spawn_node(0, "[2]", None, &medium).await;
    let initial = node.qspn().my_naddr().positions().to_vec();

    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(node.qspn().my_naddr().positions(), initial.as_slice());
    let snap = node.hooking().snapshot();
    assert!(snap.hooked, "create_net is hooked from construction");
    // Not asserted: `snap.chosen.naddr`'s exact value — `ntk_hooking::spawn`'s `CreateNet`
    // branch always records the hardcoded all-zero `entry_data.pos` there regardless of the
    // real `Naddr` this identity was actually given (`ntk_hooking`'s own internal informational
    // bookkeeping, never consulted by this daemon's production code — `rehook`'s real trigger is
    // `HookingEvent::DoFinishEnter`, not `chosen`; see the module doc).
}

/// Isolates `execute_search`'s `coord.reserve(min_host_lvl, ..)` call from the two-node
/// negotiation path: for a solitary, no-peer network-of-one, do `evaluate_enter`/`reserve`
/// resolve against *itself*, or correctly fail?
///
/// These two calls are not the same shape. `evaluate_enter` (`ntk_hooking::arc`'s own direct
/// caller) is always the *guest* asking a *candidate host*'s Coordinator for permission —
/// `CoordinatorClient::call_entering` (see its own doc) excludes *my own* g-node from that round
/// trip, since the servant it wants is by construction never myself. An earlier version of this
/// test asserted `evaluate_enter` against a solitary node "should resolve" — true under the
/// routing behavior in place at the time, but that behavior *was* the mechanism the real
/// `isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit` self-loop defect exploited: a
/// node whose own address coincidentally matched `target_for`'s elect-key answered its own
/// `evaluate_enter` call instead of ever reaching the network it was trying to enter. For a
/// solitary node with no peer at all, excluding my own g-node leaves nothing reachable at all —
/// correctly: there is genuinely no host to enter — so `evaluate_enter` must fail with
/// `NoParticipants`, never silently resolve against itself.
///
/// `reserve` (`ntk_hooking::search::execute_search`'s own caller, reached only via
/// `ntk_hooking::rpc`'s `search_migration_path` server handler and hop-forwarding, never
/// directly from `ntk_hooking::arc`) is different: it always runs as the *servant* granting a
/// slot from its own network to whichever guest asked, so — like `n_nodes` — it asks about
/// *this node's own* network and legitimately resolves against itself when nothing foreign is
/// in the way (`CoordinatorClient::call`'s own doc). For a solitary, freshly-bootstrapped
/// network-of-one, that predicts `reserve` resolves locally, granting a real, `my_pos`-distinct
/// position exactly as it always did.
#[tokio::test]
async fn single_node_coordinator_reserves_its_own_position_but_never_self_answers_entry() {
    let medium = Arc::new(Medium::default());
    let node = spawn_node(0, "[8]", None, &medium).await;
    let my_pos = node.qspn().my_naddr().positions().to_vec();

    let dht = ntk_coordinator::CoordinatorClient::new(
        node.started.running.generation.borrow().peers.clone(),
        ntk_coordinator::Config::default(),
    );
    let adapter = CoordinatorClientAdapter::new(
        dht.clone(),
        node.started.running.generation.borrow().coordinator.clone(),
        node.qspn(),
        node.started.running.net.clone(),
        TEST_MERGE_DECISION_TTL,
    );

    // `n_nodes` for contrast: a lone identity's Coordinator should at least see itself.
    let n_nodes = HookingCoordinatorClient::n_nodes(&adapter).await;
    assert_eq!(
        n_nodes, 1,
        "a network-of-one's own coordinator counts exactly itself"
    );

    // `evaluate_enter` always targets a candidate host — for a solitary node, nothing is left
    // to answer once my own g-node is excluded.
    let evaluate = HookingCoordinatorClient::evaluate_enter(
        &adapter,
        EvaluateEnterRequest {
            network_id: 42,
            neighbor_pos: my_pos.clone(),
            neighbor_min_lvl: 0,
            min_lvl: 0,
            evaluate_enter_id: 1,
        },
    )
    .await;
    assert!(
        evaluate.is_err(),
        "a solitary node has no host to enter — evaluate_enter must fail, never silently \
         answer itself: {evaluate:?}"
    );

    // The exact call `execute_search` makes: `coord.reserve(min_host_lvl, reserve_request_id)`.
    // Unlike `evaluate_enter`, this asks about *this node's own* network, so it still resolves.
    let reservation = HookingCoordinatorClient::reserve(&adapter, 1, 7)
        .await
        .expect("a virgin network-of-one's own coordinator resolves its own reservation");
    assert_ne!(
        reservation.pos, my_pos[0],
        "a fresh reservation must never collide with a position this identity already holds"
    );
}

/// Two negotiated identities, each deriving its own distinct starting position, discover each
/// other over one arc; `ntk-hooking`'s own merge protocol resolves exactly one side as the guest
/// that must enter the other's (trivial, one-node) network, and this daemon's `rehook` (wired to
/// `HookingEvent::DoFinishEnter`, module doc) adopts that resolved position end to end: qspn
/// moves, kernel routes are reinstalled for the new address, and the loser's previous
/// generation's kernel state (address + rule) is torn down exactly once — never a leak, never a
/// double-install.
///
/// # Root cause of the earlier permanent `NoMigrationPathFound` (now fixed)
/// Traced to `CoordinatorClientAdapter::reserve`/`delete_reserve` (`crate::node::adapters`)
/// adding a spurious `+ 1` to `host_lvl` before calling `ntk_coordinator::CoordinatorClient`,
/// whose `top` is *already* the same 1-indexed `CoordinatorKey` scale `execute_search` computes
/// (`hooking_helpers.vala:319-327` forwards `host_lvl` verbatim, no offset) — the bug shifted
/// every reservation one level too deep, so for this test's single-level topology the very
/// first attempt (`top = 2`) always exceeded `levels` and failed outright with
/// `ProxyError::InvalidTop`, which `execute_search` cannot distinguish from an ordinary
/// "no coordinator at this level" answer. `evaluate_enter`'s DHT target had a related bug
/// (`req.min_lvl + 1` instead of always `levels`, upstream's own "coordinator of the whole
/// network" — `api.vala:63`); both are fixed with doc comments on the adapter methods
/// themselves. A second, independent bug in `RunningNode` (now [`GenerationHandles`]) meant
/// even a successful rehook was unobservable from outside the steady-state loop: `qspn`/
/// `peers`/`coordinator`/`andna` were captured once from the first generation and never
/// updated, unlike `route_installer`'s own `Arc<Mutex<_>>`.
#[tokio::test]
async fn discovering_a_peer_joins_and_adopts_the_negotiated_position() {
    let medium = Arc::new(Medium::default());
    let node0 = spawn_node(0, "[8]", None, &medium).await;
    let node1 = spawn_node(1, "[8]", None, &medium).await;
    let pos0 = node0.qspn().my_naddr().positions().to_vec();
    let pos1 = node1.qspn().my_naddr().positions().to_vec();

    let moved = wait_until(
        || {
            node0.qspn().my_naddr().positions() != pos0
                || node1.qspn().my_naddr().positions() != pos1
        },
        Duration::from_secs(30),
    )
    .await;
    assert!(moved, "neither node ever adopted a negotiated position");

    let (winner, loser) = if node0.qspn().my_naddr().positions() == pos0 {
        (&node0, &node1)
    } else {
        (&node1, &node0)
    };

    let loser_snapshot = loser.hooking().snapshot();
    let chosen = loser_snapshot
        .chosen
        .expect("finish_enter resolved a chosen address");
    let negotiated = chosen
        .naddr
        .expect("single-level entry always resolves a full Naddr");
    assert_eq!(loser.qspn().my_naddr().positions(), negotiated.positions());
    assert_ne!(
        negotiated.positions(),
        winner.qspn().my_naddr().positions().to_owned().as_slice()
    );

    // `migrate` (`crate::node::lifecycle`) now suppresses kernel installation until the
    // negotiated generation's own qspn bootstrap confirms — real for a `spawn_entering` actor,
    // unlike the old `spawn`-based cold rebuild's instant `create_net` completion — so the
    // loser's own position (checked above) can visibly change before its kernel address/rule
    // are actually installed. Wait for that installation directly rather than assume it is
    // already done the instant the position changed.
    let installed = wait_until(
        || address_and_rule_ops(&loser.kernel).0 == 2,
        Duration::from_secs(10),
    )
    .await;
    assert!(
        installed,
        "loser never installed its negotiated generation's kernel address: {:?}",
        address_and_rule_ops(&loser.kernel)
    );
    let (adds, removes, rule_adds, rule_removes) = address_and_rule_ops(&loser.kernel);
    assert_eq!(
        adds, 2,
        "trivial generation + negotiated generation, each installs one address"
    );
    assert_eq!(
        removes, 1,
        "exactly one teardown of the trivial generation's address"
    );
    assert_eq!(rule_adds, 2);
    assert_eq!(
        rule_removes, 1,
        "exactly one teardown of the trivial generation's rule"
    );
}

/// An arc whose peer never resolves to the same or a mergeable network (incompatible
/// topology — the arc-handler's permanently-inert `IncompatibleTopology` phase) recovers to a
/// well-defined steady state instead of wedging: both identities keep running at their own
/// trivial position, neither ever claims a resolved entry, and — since `rehook` never fires —
/// no kernel state is ever torn down.
#[tokio::test]
async fn incompatible_topology_never_hooks_and_never_wedges() {
    let medium = Arc::new(Medium::default());
    let node0 = spawn_node(0, "[2]", None, &medium).await;
    let node1 = spawn_node(1, "[3]", None, &medium).await;
    let pos0 = node0.qspn().my_naddr().positions().to_vec();
    let pos1 = node1.qspn().my_naddr().positions().to_vec();

    let reached = wait_until(
        || {
            node0
                .hooking()
                .snapshot()
                .arcs
                .values()
                .any(|p| matches!(p, ArcPhase::IncompatibleTopology))
        },
        Duration::from_secs(5),
    )
    .await;
    assert!(reached, "the arc never reached IncompatibleTopology");

    for (node, initial) in [(&node0, &pos0), (&node1, &pos1)] {
        assert_eq!(
            node.qspn().my_naddr().positions(),
            initial.as_slice(),
            "no wedge: still routable at its own create_net position"
        );
        let snap = node.hooking().snapshot();
        assert!(
            snap.hooked,
            "create_net is hooked from construction, unaffected by the arc"
        );
        let (_, removes, _, rule_removes) = address_and_rule_ops(&node.kernel);
        assert_eq!(
            removes, 0,
            "no rehook ever fired, so nothing was ever torn down"
        );
        assert_eq!(rule_removes, 0);
    }
}

/// `decide_merge` (`crate::node::adapters::CoordinatorClientAdapter`) must be a fact of *my own
/// g-node* about a given target, not a per-asker verdict: within its freshness window, once
/// *any* asker for a given `neighbor_network_id` has computed and persisted a verdict against
/// this g-node's elected Coordinator, every later asker — even a totally fresh adapter
/// instance with a cold local cache, reporting a different local `neighbor_n_nodes` sample —
/// gets the identical verdict rather than recomputing its own. This is exactly the property
/// whose absence produced a real multi-member merge's `a_rehooked=2 b_rehooked=3` (members of
/// the same g-node reaching opposite conclusions), and, in a second real run, three members of
/// one single-level g-node reaching three different conclusions about the same neighbor network
/// because each asked at a slightly different moment.
#[tokio::test]
async fn decide_merge_agrees_across_members_within_the_freshness_window() {
    let medium = Arc::new(Medium::default());
    let node = spawn_node(0, "[8]", None, &medium).await;

    let dht = ntk_coordinator::CoordinatorClient::new(
        node.started.running.generation.borrow().peers.clone(),
        ntk_coordinator::Config::default(),
    );
    let coordinator = node.started.running.generation.borrow().coordinator.clone();

    // Three independently-constructed adapters — modeling three members of one g-node, none
    // sharing the others' local cache — asking about the *same* target with three different
    // local `neighbor_n_nodes` samples (as three members would if they asked at slightly
    // different moments, or via different arcs into the target).
    let members: Vec<CoordinatorClientAdapter> = (0..3)
        .map(|_| {
            CoordinatorClientAdapter::new(
                dht.clone(),
                coordinator.clone(),
                node.qspn(),
                node.started.running.net.clone(),
                TEST_MERGE_DECISION_TTL,
            )
        })
        .collect();
    let samples = [100u64, 0, 55];
    let mut verdicts = Vec::with_capacity(3);
    for (member, &neighbor_n_nodes) in members.iter().zip(&samples) {
        let req = ntk_hooking::MergeArbitrationRequest {
            my_network_id: 1,
            neighbor_network_id: 42,
            neighbor_n_nodes,
        };
        verdicts.push(HookingCoordinatorClient::decide_merge(member, req).await);
    }

    assert!(
        verdicts.iter().all(|&v| v == verdicts[0]),
        "every member of the same g-node must agree on the same target within the freshness \
         window, regardless of each one's own differing local sample: {verdicts:?}"
    );
}

/// The reverse-direction guard `crate::node::adapters::CoordinatorClientAdapter::decide_merge`'s
/// doc names: two *independent* g-nodes' Coordinators, each asking about the other, must reach
/// exactly one `true` (migrate) and one `false` (stay) — never both `true` (the earlier
/// `a_rehooked=3 b_rehooked=3` mutual-surrender symptom). Once decided, the losing side's own
/// verdict is memoized (same mechanism as
/// [`decide_merge_agrees_across_members_within_the_freshness_window`]) and, *within its
/// freshness window*, must not flip back to `true` on a later re-ask, even a contradicting one —
/// which is exactly what "the winning side declines to migrate for the rest of the window"
/// reduces to: a stale foreign `network_id` reappearing before the window elapses cannot
/// re-open an already-closed arbitration.
#[tokio::test]
async fn decide_merge_reaches_exactly_one_migrator_and_the_winner_never_flips_within_its_window() {
    let medium_a = Arc::new(Medium::default());
    let medium_b = Arc::new(Medium::default());
    let node_a = spawn_node(0, "[8]", None, &medium_a).await;
    let node_b = spawn_node(0, "[8]", None, &medium_b).await;

    let dht_a = ntk_coordinator::CoordinatorClient::new(
        node_a.started.running.generation.borrow().peers.clone(),
        ntk_coordinator::Config::default(),
    );
    let adapter_a = CoordinatorClientAdapter::new(
        dht_a,
        node_a
            .started
            .running
            .generation
            .borrow()
            .coordinator
            .clone(),
        node_a.qspn(),
        node_a.started.running.net.clone(),
        TEST_MERGE_DECISION_TTL,
    );
    let dht_b = ntk_coordinator::CoordinatorClient::new(
        node_b.started.running.generation.borrow().peers.clone(),
        ntk_coordinator::Config::default(),
    );
    let adapter_b = CoordinatorClientAdapter::new(
        dht_b,
        node_b
            .started
            .running
            .generation
            .borrow()
            .coordinator
            .clone(),
        node_b.qspn(),
        node_b.started.running.net.clone(),
        TEST_MERGE_DECISION_TTL,
    );

    // Both networks report the same (real, solo) size, so the arbitration ties on
    // `network_id`: the smaller id must be the one that migrates (`ntk_hooking::merge_tiebreak`
    // — larger id proceeds/stays).
    let (small_id, large_id) = (1_i64, 2_i64);
    let ask_a = ntk_hooking::MergeArbitrationRequest {
        my_network_id: small_id,
        neighbor_network_id: large_id,
        neighbor_n_nodes: 1,
    };
    let ask_b = ntk_hooking::MergeArbitrationRequest {
        my_network_id: large_id,
        neighbor_network_id: small_id,
        neighbor_n_nodes: 1,
    };
    let a_migrates = HookingCoordinatorClient::decide_merge(&adapter_a, ask_a).await;
    let b_migrates = HookingCoordinatorClient::decide_merge(&adapter_b, ask_b).await;
    assert!(
        a_migrates,
        "the smaller network_id must be the one that migrates"
    );
    assert!(
        !b_migrates,
        "the larger network_id must stay — never both sides migrating (mutual surrender)"
    );

    // A later, contradicting re-ask on the winner's own adapter, still inside the freshness
    // window — as if it had since seen a stale re-announcement of the (by-then-migrated)
    // foreign network reporting a much bigger size — must still read back its own
    // already-decided `false`, not re-open the arbitration.
    let stale_reask = ntk_hooking::MergeArbitrationRequest {
        my_network_id: large_id,
        neighbor_network_id: small_id,
        neighbor_n_nodes: 1_000,
    };
    let b_still_declines = HookingCoordinatorClient::decide_merge(&adapter_b, stale_reask).await;
    assert!(
        !b_still_declines,
        "a stale re-announcement of the foreign network, within the freshness window, must not \
         reopen a closed arbitration"
    );
}

/// Freshness (`CoordinatorClientAdapter::decide_merge`'s own doc, "Why a verdict cannot be
/// trusted forever"): once the freshness window elapses, a real change on either side's size
/// must be picked up on the next ask, not buried under a verdict computed from numbers that no
/// longer hold. Unlike [`decide_merge_reaches_exactly_one_migrator_and_the_winner_never_flips_within_its_window`]'s
/// re-ask (deliberately *inside* the window, where sticking is correct), this re-ask happens
/// *after* an explicit sleep past a short, test-local TTL.
#[tokio::test]
async fn decide_merge_re_evaluates_once_its_freshness_window_elapses() {
    let medium = Arc::new(Medium::default());
    let node = spawn_node(0, "[8]", None, &medium).await;
    let dht = ntk_coordinator::CoordinatorClient::new(
        node.started.running.generation.borrow().peers.clone(),
        ntk_coordinator::Config::default(),
    );
    let coordinator = node.started.running.generation.borrow().coordinator.clone();
    let short_ttl = Duration::from_millis(20);
    let adapter = CoordinatorClientAdapter::new(
        dht,
        coordinator,
        node.qspn(),
        node.started.running.net.clone(),
        short_ttl,
    );

    // My own network is a lone node (n_nodes == 1). A neighbor reporting 100 nodes must win.
    let grew = ntk_hooking::MergeArbitrationRequest {
        my_network_id: 1,
        neighbor_network_id: 99,
        neighbor_n_nodes: 100,
    };
    assert!(
        HookingCoordinatorClient::decide_merge(&adapter, grew).await,
        "a lone node must yield to a 100-node neighbor"
    );

    // Sleep well past the short TTL, then re-ask about the *same* target reporting that it has
    // genuinely shrunk to nothing (as if its own members had since migrated elsewhere for
    // real) — the freshness window having elapsed, this must recompute from the new numbers,
    // not replay the earlier "yield" verdict.
    tokio::time::sleep(short_ttl * 3).await;
    let shrank = ntk_hooking::MergeArbitrationRequest {
        my_network_id: 1,
        neighbor_network_id: 99,
        neighbor_n_nodes: 0,
    };
    let re_evaluated = HookingCoordinatorClient::decide_merge(&adapter, shrank).await;
    assert!(
        !re_evaluated,
        "once the freshness window elapses, a real size change on the neighbor's side must be \
         picked up, not buried under the earlier stale verdict"
    );
}

/// Cooperation with the abort-and-redo path `crate::arc`'s "target network changed during
/// entry" branch drives (`entry_data.network_id != network_data.network_id`, in
/// `ntk-hooking`'s own arc-handler state machine): a member that began entering and then
/// aborted must be able to re-decide the *same* `neighbor_network_id` from fresh data on its
/// next attempt, not replay whatever that key resolved to before the abort. Simulated here at
/// the adapter level — decide once, let the freshness window that would have elapsed during a
/// real `evaluate_enter`/`begin_enter`/`search_migration_path`/`abort_enter` round trip elapse,
/// then re-decide with the neighbor now reporting a genuinely different size.
#[tokio::test]
async fn decide_merge_after_an_aborted_entry_redecides_from_fresh_data() {
    let medium = Arc::new(Medium::default());
    let node = spawn_node(0, "[8]", None, &medium).await;
    let dht = ntk_coordinator::CoordinatorClient::new(
        node.started.running.generation.borrow().peers.clone(),
        ntk_coordinator::Config::default(),
    );
    let coordinator = node.started.running.generation.borrow().coordinator.clone();
    let short_ttl = Duration::from_millis(20);
    let adapter = CoordinatorClientAdapter::new(
        dht,
        coordinator,
        node.qspn(),
        node.started.running.net.clone(),
        short_ttl,
    );

    // Decide to enter a tied-but-larger-id neighbor (my own lone network loses the tie).
    let (my_id, neighbor_id) = (1_i64, 2_i64);
    let first_try = ntk_hooking::MergeArbitrationRequest {
        my_network_id: my_id,
        neighbor_network_id: neighbor_id,
        neighbor_n_nodes: 1,
    };
    assert!(
        HookingCoordinatorClient::decide_merge(&adapter, first_try).await,
        "a tie must be broken toward the larger network id"
    );

    // The arc's own entry attempt aborts (the real trigger: the target's `network_id` changed
    // mid-negotiation) and redoes from start once the freshness window has elapsed — by then
    // the same neighbor genuinely has 1000 real members, having absorbed others while this
    // negotiation was in flight.
    tokio::time::sleep(short_ttl * 3).await;
    let redo = ntk_hooking::MergeArbitrationRequest {
        my_network_id: my_id,
        neighbor_network_id: neighbor_id,
        neighbor_n_nodes: 1000,
    };
    assert!(
        HookingCoordinatorClient::decide_merge(&adapter, redo).await,
        "a redecide after abort must still say yes here — but from the fresh 1000-node reading, \
         not a frozen replay of the earlier tie-broken verdict"
    );
}

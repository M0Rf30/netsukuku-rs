//! Reserve protocol, eldership, propagation anti-replay, and multi-node election tests.
//!
//! Exercises the real public contract: [`ntk_peerservices::PeerService::exec`] directly for the
//! fixed-keys database's request surface (idempotency, virtual fallback, eldership, TTL
//! expiry), [`ntk_rpc::RpcHandler::handle`] for propagation anti-replay, and a genuine
//! multi-node PeerServices DHT (mirroring `ntk-peerservices`' own `tests/routing.rs` harness)
//! for the property the whole module exists for: two concurrent reservers converge on the same
//! elected servant and get distinct positions.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use futures::future::BoxFuture;
use ntk_common::{HCoord, Naddr, Topology};
use ntk_coordinator::v1 as cv1;
use ntk_coordinator::{
    AbortEnterHandler, BeginEnterHandler, CompletedEnterHandler, Config, CoordinatorClient,
    CoordinatorMap, CoordinatorRpcHandler, CoordinatorService, EnterHandlers, EvaluateEnterHandler,
    FakeCoordinatorStubFactory, Manager, PropagationHandler,
};
use ntk_peerservices::{
    Config as PeersConfig, ExecError, Handle as PeersHandle, Manager as PeersManager, PeerService,
    PeersRpcHandler, PeersStub, RoutingEnv, RpcPeersStub, TupleGNode, TupleNode,
};
use ntk_proto::domain::{from_typed_value, typed_value};
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::{CallerContext, CoordinatorExecuteArgs, MethodCall, TypedValue};
use ntk_rpc::{FakeRpcClient, RpcHandler};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct TestMap {
    can_reserve: bool,
    free: Vec<u32>,
    n_nodes: u64,
}

impl CoordinatorMap for TestMap {
    fn n_nodes(&self) -> u64 {
        self.n_nodes
    }
    fn free_positions(&self, _level: usize) -> Vec<u32> {
        self.free.clone()
    }
    fn can_reserve(&self, _level: usize) -> bool {
        self.can_reserve
    }
    fn my_pos(&self, _level: usize) -> u32 {
        0
    }
    fn fp_id(&self, _level: usize) -> i64 {
        0
    }
}

/// Echoes `data` back unchanged — evaluate/begin/completed/abort enter are never exercised by
/// these tests, only wired to satisfy `Manager::new`.
struct NoopHandlers;

impl EvaluateEnterHandler for NoopHandlers {
    fn evaluate_enter<'a>(
        &'a self,
        _top: usize,
        data: TypedValue,
        _client_tuple: &'a [u32],
    ) -> BoxFuture<'a, TypedValue> {
        Box::pin(async move { data })
    }
}
impl BeginEnterHandler for NoopHandlers {
    fn begin_enter<'a>(
        &'a self,
        _top: usize,
        data: TypedValue,
        _client_tuple: &'a [u32],
    ) -> BoxFuture<'a, TypedValue> {
        Box::pin(async move { data })
    }
}
impl CompletedEnterHandler for NoopHandlers {
    fn completed_enter<'a>(
        &'a self,
        _top: usize,
        data: TypedValue,
        _client_tuple: &'a [u32],
    ) -> BoxFuture<'a, TypedValue> {
        Box::pin(async move { data })
    }
}
impl AbortEnterHandler for NoopHandlers {
    fn abort_enter<'a>(
        &'a self,
        _top: usize,
        data: TypedValue,
        _client_tuple: &'a [u32],
    ) -> BoxFuture<'a, TypedValue> {
        Box::pin(async move { data })
    }
}

fn noop_enter_handlers() -> EnterHandlers {
    let h = Arc::new(NoopHandlers);
    EnterHandlers {
        evaluate_enter: h.clone(),
        begin_enter: h.clone(),
        completed_enter: h.clone(),
        abort_enter: h,
    }
}

struct NoopPropagationHandler;
impl PropagationHandler for NoopPropagationHandler {
    fn prepare_migration(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn finish_migration(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn prepare_enter(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn finish_enter(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn we_have_splitted(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// Records every `prepare_migration` invocation's level onto a channel — used to observe
/// whether a (deduplicated) propagation actually reached the local handler.
struct RecordingPropagationHandler(mpsc::UnboundedSender<usize>);
impl PropagationHandler for RecordingPropagationHandler {
    fn prepare_migration(&self, level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        let tx = self.0.clone();
        Box::pin(async move {
            let _ = tx.send(level);
        })
    }
    fn finish_migration(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn prepare_enter(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn finish_enter(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn we_have_splitted(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

// ---------------------------------------------------------------------------
// Minimal single-node PeerServices harness (only needed to satisfy
// `CoordinatorService::new`'s replication seam; these tests call `PeerService::exec` directly,
// never routing through it).
// ---------------------------------------------------------------------------

struct SingleNodeEnv;
impl RoutingEnv for SingleNodeEnv {
    fn gnode_exists(&self, hc: HCoord) -> bool {
        hc.level == 0 && hc.pos == 0
    }
    fn gateway(
        &self,
        _hc: HCoord,
        _failed: Option<&Arc<dyn PeersStub>>,
    ) -> Option<Arc<dyn PeersStub>> {
        None
    }
    fn dial(&self, _n: &TupleNode) -> Option<Arc<dyn PeersStub>> {
        None
    }
    fn nodes_in_my_group(&self, _level: usize) -> usize {
        1
    }
    fn neighbors(&self) -> Vec<Arc<dyn PeersStub>> {
        Vec::new()
    }
}

fn single_node_peers(topology: Topology) -> (PeersHandle, CancellationToken) {
    let my_addr = Naddr::new(topology.clone(), vec![0u32; topology.levels()]).unwrap();
    let (manager, handle) = PeersManager::new(
        topology.clone(),
        my_addr,
        Arc::new(SingleNodeEnv),
        PeersConfig::default(),
        topology.levels(),
    );
    let cancel = CancellationToken::new();
    tokio::spawn(manager.run(cancel.child_token()));
    (handle, cancel)
}

fn build_coordinator(
    peers: PeersHandle,
    map: Arc<dyn CoordinatorMap>,
    propagation: Arc<dyn PropagationHandler>,
) -> (
    ntk_coordinator::Handle,
    CoordinatorService,
    CancellationToken,
) {
    let (manager, handle) = Manager::new(
        peers.topology().clone(),
        map,
        Arc::new(FakeCoordinatorStubFactory::new(Vec::new())),
        propagation,
        noop_enter_handlers(),
        Config::default(),
        None,
    );
    let cancel = CancellationToken::new();
    tokio::spawn(manager.run(cancel.child_token()));
    let service = CoordinatorService::new(handle.clone(), peers);
    (handle, service, cancel)
}

// ---------------------------------------------------------------------------
// Wire helpers (white-box: same tags `crate::wire` uses internally) so tests can call
// `PeerService::exec` directly without a full DHT round trip.
// ---------------------------------------------------------------------------

fn pack_reserve(top: u32, reserve_request_id: i64) -> TypedValue {
    let req = cv1::CoordinatorRequest {
        body: Some(cv1::coordinator_request::Body::ReserveEnter(
            cv1::ReserveEnterRequest {
                top,
                reserve_request_id,
            },
        )),
    };
    typed_value("coordinator.CoordinatorRequest", &req)
}

fn unpack_reservation(tv: &TypedValue) -> Option<cv1::Reservation> {
    let resp: cv1::CoordinatorResponse =
        from_typed_value(tv, "coordinator.CoordinatorResponse").unwrap();
    match resp.body {
        Some(cv1::coordinator_response::Body::ReserveEnter(r)) => r.reservation,
        other => panic!("expected a ReserveEnter response, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Reserve protocol tests (fk_database.vala:502-573)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn reserve_replay_returns_the_same_reservation() {
    let topology = Topology::new([4]).unwrap();
    let (peers, _peers_cancel) = single_node_peers(topology);
    let map: Arc<dyn CoordinatorMap> = Arc::new(TestMap {
        can_reserve: true,
        free: vec![0, 1, 2, 3],
        n_nodes: 1,
    });
    let (_handle, service, _cancel) =
        build_coordinator(peers, map, Arc::new(NoopPropagationHandler));

    let first = unpack_reservation(&service.exec(pack_reserve(1, 42), &[]).await.unwrap())
        .expect("reserved");
    let replay = unpack_reservation(&service.exec(pack_reserve(1, 42), &[]).await.unwrap())
        .expect("replay still reserved");
    assert_eq!(
        first, replay,
        "replaying the same request_id must return the SAME reservation"
    );

    // A genuinely new request_id must never collide with the replayed one's position.
    let other = unpack_reservation(&service.exec(pack_reserve(1, 99), &[]).await.unwrap())
        .expect("reserved");
    assert_ne!(first.new_pos, other.new_pos);
}

#[tokio::test]
async fn reserve_falls_back_to_a_virtual_position_when_no_real_position_is_free() {
    let topology = Topology::new([4]).unwrap();
    let (peers, _peers_cancel) = single_node_peers(topology);
    let map: Arc<dyn CoordinatorMap> = Arc::new(TestMap {
        can_reserve: true,
        free: Vec::new(), // no real position available
        n_nodes: 1,
    });
    let (_handle, service, _cancel) =
        build_coordinator(peers, map, Arc::new(NoopPropagationHandler));

    let reservation = unpack_reservation(&service.exec(pack_reserve(1, 7), &[]).await.unwrap())
        .expect("reserved");
    // gsize is 4 (topology level 0); the first virtual position is gsize + 1, matching upstream's
    // pre-increment `++mem.max_virtual_pos` seeded at `gsizes[lvl-1]` exactly
    // (research/impl/vala/coordinator/fk_database.vala:556, peer_service.vala:79-88).
    assert_eq!(
        reservation.new_pos, 5,
        "virtual position must be >= gsize (here, gsize+1)"
    );
    assert!(
        reservation.new_pos >= 4,
        "a virtual position must never look like a real one"
    );
}

#[tokio::test]
async fn cannot_reserve_returns_a_normal_answer_not_a_routing_refusal() {
    let topology = Topology::new([4]).unwrap();
    let (peers, _peers_cancel) = single_node_peers(topology);
    let map: Arc<dyn CoordinatorMap> = Arc::new(TestMap {
        can_reserve: false,
        free: vec![0, 1, 2, 3],
        n_nodes: 1,
    });
    let (_handle, service, _cancel) =
        build_coordinator(peers, map, Arc::new(NoopPropagationHandler));

    let response = service
        .exec(pack_reserve(1, 1), &[])
        .await
        .expect("exec succeeds, it just can't reserve");
    assert!(unpack_reservation(&response).is_none());
}

proptest::proptest! {
    /// Eldership is a globally increasing counter per level, never reused, for every genuinely
    /// new reservation (`fk_database.vala:558`) — independent of position/request-id values.
    #[test]
    fn eldership_is_monotonically_increasing(n in 1usize..30) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async move {
            let topology = Topology::new([1000]).unwrap();
            let (peers, _peers_cancel) = single_node_peers(topology);
            let map: Arc<dyn CoordinatorMap> = Arc::new(TestMap { can_reserve: true, free: (0..1000).collect(), n_nodes: 1 });
            let (_handle, service, _cancel) = build_coordinator(peers, map, Arc::new(NoopPropagationHandler));

            let mut last_eldership = 0u64;
            for i in 0..n {
                let request_id = i64::try_from(i).unwrap();
                let reservation = unpack_reservation(&service.exec(pack_reserve(1, request_id), &[]).await.unwrap()).unwrap();
                proptest::prop_assert!(reservation.new_eldership > last_eldership, "eldership must strictly increase");
                proptest::prop_assert_eq!(reservation.new_eldership, u64::try_from(i).unwrap() + 1);
                last_eldership = reservation.new_eldership;
            }
            Ok(())
        })?;
    }
}

#[tokio::test(start_paused = true)]
async fn booking_expires_after_its_ttl_and_a_replay_is_treated_as_a_new_reservation() {
    let topology = Topology::new([4]).unwrap();
    let (peers, _peers_cancel) = single_node_peers(topology);
    let map: Arc<dyn CoordinatorMap> = Arc::new(TestMap {
        can_reserve: true,
        free: vec![0, 1, 2, 3],
        n_nodes: 1,
    });
    let (_handle, service, _cancel) =
        build_coordinator(peers, map, Arc::new(NoopPropagationHandler));

    let first = unpack_reservation(&service.exec(pack_reserve(1, 5), &[]).await.unwrap()).unwrap();

    // Still alive just before the 60s TTL: a replay must return the identical reservation.
    tokio::time::advance(Config::default().booking_ttl - Duration::from_millis(1)).await;
    let still_alive =
        unpack_reservation(&service.exec(pack_reserve(1, 5), &[]).await.unwrap()).unwrap();
    assert_eq!(first, still_alive);

    // Past the TTL from the *refreshed* deadline: advance well beyond it so the booking is
    // purged, then replay the same request_id — it must be treated as brand new (a fresh,
    // higher eldership), proving TTL expiry actually happened rather than idempotent replay.
    tokio::time::advance(Config::default().booking_ttl * 2).await;
    let after_expiry =
        unpack_reservation(&service.exec(pack_reserve(1, 5), &[]).await.unwrap()).unwrap();
    assert_ne!(
        first.new_eldership, after_expiry.new_eldership,
        "an expired booking must be purged, not idempotently replayed"
    );
}

// ---------------------------------------------------------------------------
// Coordinator hand-off protocol (coord.vala:142-146): a migrating identity's exported state
// seeds the replacement `Manager` it spawns next.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hand_off_carries_reservations_forward_to_the_next_identity() {
    let topology = Topology::new([4]).unwrap();
    let (peers, _peers_cancel) = single_node_peers(topology.clone());
    let map: Arc<dyn CoordinatorMap> = Arc::new(TestMap {
        can_reserve: true,
        free: vec![0, 1, 2, 3],
        n_nodes: 1,
    });
    let (old_handle, old_service, old_cancel) =
        build_coordinator(peers.clone(), map.clone(), Arc::new(NoopPropagationHandler));
    let original =
        unpack_reservation(&old_service.exec(pack_reserve(1, 9), &[]).await.unwrap()).unwrap();

    let handoff = old_handle.hand_off().await;
    old_cancel.cancel();

    let (new_manager, new_handle) = Manager::new(
        topology,
        map,
        Arc::new(FakeCoordinatorStubFactory::new(Vec::new())),
        Arc::new(NoopPropagationHandler),
        noop_enter_handlers(),
        Config::default(),
        Some(handoff),
    );
    let new_cancel = CancellationToken::new();
    tokio::spawn(new_manager.run(new_cancel.child_token()));
    let new_service = CoordinatorService::new(new_handle, peers);

    // Replaying the *old* identity's request_id on the *new* identity's Manager must return the
    // same reservation, never a fresh one — proving the fixed-keys record was actually handed
    // off, not rebuilt from scratch.
    let after_handoff =
        unpack_reservation(&new_service.exec(pack_reserve(1, 9), &[]).await.unwrap()).unwrap();
    assert_eq!(original, after_handoff);

    new_cancel.cancel();
}

// ---------------------------------------------------------------------------
// `n_nodes` cache invalidation: membership change beats the TTL (see this crate's parent task
// notes — a merge-direction decision made from a stale, pre-absorption count is irreversible).
// ---------------------------------------------------------------------------

/// Reports a caller-mutable `n_nodes`, so a test can simulate this g-node absorbing a member
/// mid-test without waiting out `Config::n_nodes_cache_ttl`.
struct MutableNNodesMap(AtomicU64);
impl CoordinatorMap for MutableNNodesMap {
    fn n_nodes(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn free_positions(&self, _level: usize) -> Vec<u32> {
        Vec::new()
    }
    fn can_reserve(&self, _level: usize) -> bool {
        false
    }
    fn my_pos(&self, _level: usize) -> u32 {
        0
    }
    fn fp_id(&self, _level: usize) -> i64 {
        0
    }
}

#[tokio::test(start_paused = true)]
async fn membership_change_invalidates_the_n_nodes_cache_before_the_ttl_expires() {
    let topology = Topology::new([4]).unwrap();
    let (peers, _peers_cancel) = single_node_peers(topology);
    let map = Arc::new(MutableNNodesMap(AtomicU64::new(1)));
    let (handle, service, _cancel) = build_coordinator(
        peers.clone(),
        map.clone() as Arc<dyn CoordinatorMap>,
        Arc::new(NoopPropagationHandler),
    );
    peers.register(Arc::new(service)).await;
    let client = CoordinatorClient::new(peers, Config::default());

    assert_eq!(
        client.get_n_nodes(&[]).await.unwrap(),
        1,
        "primes the cache at the pre-absorption size"
    );

    // This g-node just absorbed a second member.
    map.0.store(2, Ordering::SeqCst);
    handle
        .finish_enter(0, TypedValue::new("test.FinishEnter", Vec::new()))
        .await;

    // No time has passed at all — `Config::default()`'s 20s TTL has not come close to expiring.
    // Only invalidation on membership change, not the TTL, can make this see the new size.
    assert_eq!(
        client.get_n_nodes(&[]).await.unwrap(),
        2,
        "a completed finish_enter must invalidate the cached n_nodes immediately"
    );
}

// ---------------------------------------------------------------------------
// Propagation anti-replay (coord.vala:424-440, 200s retention window)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replaying_a_propagation_within_the_retention_window_is_rejected() {
    let topology = Topology::new([1]).unwrap();
    let (peers, _peers_cancel) = single_node_peers(topology);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let map: Arc<dyn CoordinatorMap> = Arc::new(TestMap {
        can_reserve: true,
        free: vec![0],
        n_nodes: 1,
    });
    let (handle, _service, _cancel) =
        build_coordinator(peers, map, Arc::new(RecordingPropagationHandler(tx)));
    let rpc_handler = CoordinatorRpcHandler::new(handle);

    let tuple = typed_value(
        "coordinator.PropagationTuple",
        &cv1::PropagationTuple { positions: vec![0] },
    );
    let args = CoordinatorExecuteArgs {
        tuple: Some(tuple),
        fp_id: 0,
        propagation_id: 4242,
        lvl: 0,
        data: Some(TypedValue::new(
            "test.PrepareMigration",
            b"payload".to_vec(),
        )),
    };
    let call = MethodCall {
        call: Some(Call::CoordinatorExecutePrepareMigration(args)),
    };
    let caller = CallerContext {
        source_id: None,
        src_nic: None,
    };

    rpc_handler
        .handle(
            caller.clone(),
            TypedValue::new(String::new(), Vec::new()),
            call.clone(),
            None,
        )
        .await
        .unwrap();
    let delivered = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("first propagation reaches the local handler")
        .unwrap();
    assert_eq!(delivered, 0);

    // Exact replay: same tuple/fp_id/propagation_id/lvl/data.
    rpc_handler
        .handle(
            caller,
            TypedValue::new(String::new(), Vec::new()),
            call,
            None,
        )
        .await
        .unwrap();
    let replay = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
    assert!(
        replay.is_err(),
        "a replayed propagation_id must never re-trigger the local propagation handler"
    );
}

// ---------------------------------------------------------------------------
// Multi-node election: the property the whole module exists for.
// ---------------------------------------------------------------------------

struct FullMeshEnv {
    n: usize,
    my_index: usize,
    stubs: Arc<OnceLock<Vec<Arc<dyn PeersStub>>>>,
}

impl RoutingEnv for FullMeshEnv {
    fn gnode_exists(&self, hc: HCoord) -> bool {
        hc.level == 0 && (hc.pos as usize) < self.n
    }
    fn gateway(
        &self,
        hc: HCoord,
        failed: Option<&Arc<dyn PeersStub>>,
    ) -> Option<Arc<dyn PeersStub>> {
        let target = hc.pos as usize;
        if target == self.my_index {
            return None;
        }
        let stub = self.stubs.get().expect("stubs installed")[target].clone();
        (!failed.is_some_and(|f| Arc::ptr_eq(f, &stub))).then_some(stub)
    }
    fn dial(&self, n: &TupleNode) -> Option<Arc<dyn PeersStub>> {
        let target = n.positions().first().copied()? as usize;
        Some(self.stubs.get().expect("stubs installed")[target].clone())
    }
    fn nodes_in_my_group(&self, _level: usize) -> usize {
        self.n
    }
    fn neighbors(&self) -> Vec<Arc<dyn PeersStub>> {
        self.stubs
            .get()
            .expect("stubs installed")
            .iter()
            .enumerate()
            .filter(|&(i, _)| i != self.my_index)
            .map(|(_, s)| s.clone())
            .collect()
    }
}

fn build_network(n: usize) -> (Vec<PeersHandle>, CancellationToken) {
    let topology = Topology::new([u32::try_from(n).unwrap()]).unwrap();
    let cancel = CancellationToken::new();
    let stub_cell: Arc<OnceLock<Vec<Arc<dyn PeersStub>>>> = Arc::new(OnceLock::new());

    let mut handles = Vec::new();
    for i in 0..n {
        let my_addr = Naddr::new(topology.clone(), vec![u32::try_from(i).unwrap()]).unwrap();
        let env = Arc::new(FullMeshEnv {
            n,
            my_index: i,
            stubs: stub_cell.clone(),
        });
        let (manager, handle) = PeersManager::new(
            topology.clone(),
            my_addr,
            env,
            PeersConfig::default(),
            topology.levels(),
        );
        tokio::spawn(manager.run(cancel.child_token()));
        handles.push(handle);
    }

    let stubs: Vec<Arc<dyn PeersStub>> = handles
        .iter()
        .map(|h| {
            let handler = Arc::new(PeersRpcHandler::new(h.clone()));
            let client = Arc::new(FakeRpcClient::new(handler));
            Arc::new(RpcPeersStub::new(client, topology.clone())) as Arc<dyn PeersStub>
        })
        .collect();
    assert!(
        stub_cell.set(stubs).is_ok(),
        "stub_cell set exactly once, before any routing call"
    );

    (handles, cancel)
}

#[tokio::test]
async fn concurrent_reservers_on_the_same_gnode_get_distinct_positions() {
    let (handles, cancel) = build_network(4);

    // Mandatory-service participation is model-wide, not gossiped
    // (`ntk_peerservices::PeerService::is_optional`'s own doc comment) — upstream registers
    // `CoordService` on every main identity (`coord.vala:136-146`), not only the one the DHT
    // will eventually elect; only the elected node's *state* (position 0's `TestMap`) actually
    // matters, but every node's local `services` registry must know the p_id is mandatory for
    // `non_participant_gnodes` to route to it without gossip.
    let mut coord_cancels = Vec::new();
    for handle in &handles {
        let map: Arc<dyn CoordinatorMap> = Arc::new(TestMap {
            can_reserve: true,
            free: vec![0, 1, 2, 3],
            n_nodes: 4,
        });
        let (_coord_handle, service, coord_cancel) =
            build_coordinator(handle.clone(), map, Arc::new(NoopPropagationHandler));
        handle.register(Arc::new(service)).await;
        coord_cancels.push(coord_cancel);
    }

    // Two other nodes concurrently try to reserve a position in the *same* g-node.
    let client_a = CoordinatorClient::new(handles[1].clone(), Config::default());
    let client_b = CoordinatorClient::new(handles[2].clone(), Config::default());

    let (res_a, res_b) = tokio::join!(client_a.reserve(1, 111, &[]), client_b.reserve(1, 222, &[]));
    let res_a = res_a
        .expect("routing succeeds")
        .expect("reservation granted");
    let res_b = res_b
        .expect("routing succeeds")
        .expect("reservation granted");

    assert_ne!(
        res_a.new_pos, res_b.new_pos,
        "two concurrent reservers on the same g-node must never collide on a position"
    );

    cancel.cancel();
    for c in coord_cancels {
        c.cancel();
    }
}

// ---------------------------------------------------------------------------
// Exclusion-aware routing: never self, never a foreign network, while a genuinely foreign host
// stays reachable when a call is *for* entering it (`CoordinatorClient::call_entering`'s own
// doc) — the two directions of one invariant.
// ---------------------------------------------------------------------------

/// Answers `evaluate_enter` with a fixed, node-distinguishable tag, ignoring the request
/// payload — unlike [`NoopHandlers`] (which echoes the request back, so the response can never
/// tell a test *which* node actually answered), this lets a test tell node 0's Coordinator
/// apart from node 1's.
struct TaggedEvaluateEnterHandler(u8);
impl EvaluateEnterHandler for TaggedEvaluateEnterHandler {
    fn evaluate_enter<'a>(
        &'a self,
        _top: usize,
        _data: TypedValue,
        _client_tuple: &'a [u32],
    ) -> BoxFuture<'a, TypedValue> {
        let tag = self.0;
        Box::pin(async move {
            TypedValue {
                type_tag: "test.answered_by".to_owned(),
                payload: vec![tag],
            }
        })
    }
}

fn tagged_enter_handlers(tag: u8) -> EnterHandlers {
    let noop = Arc::new(NoopHandlers);
    EnterHandlers {
        evaluate_enter: Arc::new(TaggedEvaluateEnterHandler(tag)),
        begin_enter: noop.clone(),
        completed_enter: noop.clone(),
        abort_enter: noop,
    }
}

fn build_coordinator_tagged(
    peers: PeersHandle,
    map: Arc<dyn CoordinatorMap>,
    tag: u8,
) -> (CoordinatorService, CancellationToken) {
    let (manager, handle) = Manager::new(
        peers.topology().clone(),
        map,
        Arc::new(FakeCoordinatorStubFactory::new(Vec::new())),
        Arc::new(NoopPropagationHandler),
        tagged_enter_handlers(tag),
        Config::default(),
        None,
    );
    let cancel = CancellationToken::new();
    tokio::spawn(manager.run(cancel.child_token()));
    let service = CoordinatorService::new(handle, peers);
    (service, cancel)
}

/// Pins the real-kernel `isolated_merge_migrates_a_preformed_losing_gnode_as_a_unit` defect and
/// its fix directly at the mechanism that caused it: `CoordinatorClient::target_for`'s elect-key
/// (`[0,0,...,0]`) is matched by raw position alone, with no notion of network identity, so a
/// node whose own address happens to be all zeros answered its own `evaluate_enter` calls
/// instead of ever reaching the network it was actually trying to enter
/// (`isolated_position(0) == [0, 0]`, this test's own `node 0` collapsed to a single level).
///
/// Two nodes, mutually reachable (mirroring a just-bridged, not-yet-merged pair of networks):
/// node 0 claims position 0 — `target_for(1)`'s own elect-key for this single-level topology,
/// an exact match to *node 0's own address* — and node 1 claims position 1, the network node 0
/// is actually trying to enter. Both run a real `CoordinatorService`, each tagging its own
/// `evaluate_enter` answer so the test can tell who actually answered.
///
/// A too-blunt fix (excluding every currently-known-foreign g-node, tried and reverted) closes
/// the self-loop but also excludes node 1 — the very host this call exists to reach — leaving no
/// candidate at all. `CoordinatorClient::evaluate_enter` excludes only *node 0's own* g-node
/// (`call_entering`), so the self-loop closes without blocking legitimate contact: this same
/// call now resolves to node 1's Coordinator, never node 0's own.
#[tokio::test]
async fn evaluate_enter_excludes_only_my_own_gnode_never_the_host_it_is_entering() {
    let (handles, cancel) = build_network(2);

    let map0: Arc<dyn CoordinatorMap> = Arc::new(TestMap {
        can_reserve: true,
        free: vec![],
        n_nodes: 1,
    });
    let map1: Arc<dyn CoordinatorMap> = Arc::new(TestMap {
        can_reserve: true,
        free: vec![],
        n_nodes: 1,
    });
    let (service0, cancel0) = build_coordinator_tagged(handles[0].clone(), map0, 0);
    let (service1, cancel1) = build_coordinator_tagged(handles[1].clone(), map1, 1);
    handles[0].register(Arc::new(service0)).await;
    handles[1].register(Arc::new(service1)).await;

    // Called against node 0's own peer-services handle: node 0's own address ([0]) is an exact
    // match for `target_for(1)`'s elect-key, the same coincidence that caused the real defect.
    let client = CoordinatorClient::new(handles[0].clone(), Config::default());

    let answer = client
        .evaluate_enter(1, TypedValue::default())
        .await
        .expect("routing succeeds");
    assert_eq!(
        answer.payload,
        vec![1u8],
        "must reach node 1's Coordinator (the host this call is entering) — never self-loop \
         onto node 0's own, and never fail to route at all"
    );

    cancel.cancel();
    cancel0.cancel();
    cancel1.cancel();
}

/// The other direction: a call that is genuinely *about my own network* — `get_n_nodes`, unlike
/// `reserve` — must never cross into a foreign, merely-position-colliding node either. Pins
/// `CoordinatorClientAdapter::foreign_exclusions` (`NetworkInfo::foreign_positions`, built from
/// `ntk_hooking::QspnView::note_foreign`/`note_same_network`) still doing its original job for
/// these calls, unlike `reserve`/`evaluate_enter`/etc. (see
/// `reserve_excludes_only_my_own_gnode_never_the_host_it_is_entering`'s own doc for why those
/// must not use this same mechanism).
#[tokio::test]
async fn get_n_nodes_excludes_a_foreign_gnode_that_collides_with_the_elect_key() {
    let (handles, cancel) = build_network(2);

    let map0: Arc<dyn CoordinatorMap> = Arc::new(TestMap {
        can_reserve: true,
        free: vec![],
        n_nodes: 40,
    });
    let map1: Arc<dyn CoordinatorMap> = Arc::new(TestMap {
        can_reserve: true,
        free: vec![],
        n_nodes: 4,
    });
    let (_h0, service0, cancel0) =
        build_coordinator(handles[0].clone(), map0, Arc::new(NoopPropagationHandler));
    let (_h1, service1, cancel1) =
        build_coordinator(handles[1].clone(), map1, Arc::new(NoopPropagationHandler));
    handles[0].register(Arc::new(service0)).await;
    handles[1].register(Arc::new(service1)).await;

    // Called against node 1's own peer-services handle, wanting node 1's own network size.
    let client = CoordinatorClient::new(handles[1].clone(), Config::default());

    let unexcluded = client.get_n_nodes(&[]).await.unwrap();
    assert_eq!(
        unexcluded, 40,
        "without exclusion, the position-colliding foreign node answers instead of my own — \
         reproducing the real defect"
    );

    let topology = Topology::new([2]).unwrap();
    let foreign_gnode = TupleGNode::new(topology, 1, vec![0]).unwrap();
    let excluded = client.get_n_nodes(&[foreign_gnode]).await.unwrap();
    assert_eq!(
        excluded, 4,
        "excluding the foreign gnode must route to my own network's coordinator instead"
    );

    cancel.cancel();
    cancel0.cancel();
    cancel1.cancel();
}

// ---------------------------------------------------------------------------
// Pure selection-logic invariant: `reserve_enter` itself never grants an occupied slot.
// ---------------------------------------------------------------------------

proptest::proptest! {
    /// The reserve protocol's whole purpose (`fk_database.vala:502-573`): a granted *real*
    /// position (`new_pos < gsize`) must never be one `CoordinatorMap::free_positions` already
    /// excluded as occupied, for any occupied-slot set and any number of requesters. This pure
    /// selection logic was never the routing-level bug this module's own
    /// `reserve_excludes_a_foreign_gnode_that_collides_with_the_elect_key` test pins — it always
    /// correctly avoided whatever `free_positions` told it was occupied — but it is the other
    /// half of the invariant a whole-g-node entry depends on end to end, so it is pinned here
    /// too.
    #[test]
    fn reserve_never_grants_an_occupied_position(
        gsize in 1u32..16,
        occupied in proptest::collection::hash_set(0u32..16, 0..8),
        n_requests in 1usize..8,
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async move {
            let occupied: std::collections::HashSet<u32> =
                occupied.into_iter().filter(|p| *p < gsize).collect();
            let free: Vec<u32> = (0..gsize).filter(|p| !occupied.contains(p)).collect();
            let topology = Topology::new([gsize]).unwrap();
            let (peers, _peers_cancel) = single_node_peers(topology);
            let map: Arc<dyn CoordinatorMap> = Arc::new(TestMap {
                can_reserve: !free.is_empty(),
                free: free.clone(),
                n_nodes: 1,
            });
            let (_handle, service, _cancel) =
                build_coordinator(peers, map, Arc::new(NoopPropagationHandler));

            for i in 0..n_requests {
                let request_id = i64::try_from(i).unwrap();
                if let Some(reservation) =
                    unpack_reservation(&service.exec(pack_reserve(1, request_id), &[]).await.unwrap())
                    && reservation.new_pos < gsize
                {
                    proptest::prop_assert!(
                        !occupied.contains(&reservation.new_pos),
                        "granted an occupied real position: {} (occupied={:?})",
                        reservation.new_pos,
                        occupied
                    );
                }
            }
            Ok(())
        })?;
    }
}

// ---------------------------------------------------------------------------
// `SetHookingMemoryRequest`'s removed `client_tuple` check (`service.rs`'s own doc on the fix):
// upstream's identical-looking guard is enforceable only under a calling convention (the
// caller has already verified `am_i_servant_for(k)`) this port's own callers
// (`decide_merge`/`EnterArbiter`) don't have — a genuine multi-hop own-network write must
// succeed, while whatever this handler still legitimately refuses (a request this crate's own
// decoder cannot make sense of) must keep failing.
// ---------------------------------------------------------------------------

fn pack_set_hooking_memory(top: u32, data: Option<TypedValue>) -> TypedValue {
    let req = cv1::CoordinatorRequest {
        body: Some(cv1::coordinator_request::Body::SetHookingMemory(
            cv1::SetHookingMemoryRequest { top, data },
        )),
    };
    typed_value("coordinator.CoordinatorRequest", &req)
}

fn pack_get_hooking_memory(top: u32) -> TypedValue {
    let req = cv1::CoordinatorRequest {
        body: Some(cv1::coordinator_request::Body::GetHookingMemory(
            cv1::GetHookingMemoryRequest { top },
        )),
    };
    typed_value("coordinator.CoordinatorRequest", &req)
}

fn unpack_hooking_memory(tv: &TypedValue) -> Option<TypedValue> {
    let resp: cv1::CoordinatorResponse =
        from_typed_value(tv, "coordinator.CoordinatorResponse").unwrap();
    match resp.body {
        Some(cv1::coordinator_response::Body::GetHookingMemory(r)) => r.data,
        other => panic!("expected a GetHookingMemory response, got {other:?}"),
    }
}

/// Direction 1: a `client_tuple` that names a real forwarding chain (never this node's own
/// `contact_peer` self-loop) must still be honored — this is the ordinary shape of any other
/// member of the same network asking its elected Coordinator to persist shared state.
#[tokio::test]
async fn set_hooking_memory_from_a_forwarded_non_empty_client_tuple_succeeds() {
    let topology = Topology::new([4]).unwrap();
    let (peers, _peers_cancel) = single_node_peers(topology);
    let map: Arc<dyn CoordinatorMap> = Arc::new(TestMap {
        can_reserve: true,
        free: vec![0, 1, 2, 3],
        n_nodes: 1,
    });
    let (_handle, service, _cancel) =
        build_coordinator(peers, map, Arc::new(NoopPropagationHandler));

    let payload = typed_value("test.Payload", &cv1::NumberOfNodesRequest {});
    // A non-empty `client_tuple` simulates a write that arrived via `forward_msg` (a genuine
    // multi-hop own-network round trip), not `contact_peer`'s self-loop.
    let forwarded_client_tuple = [3u32, 1u32];
    service
        .exec(
            pack_set_hooking_memory(1, Some(payload.clone())),
            &forwarded_client_tuple,
        )
        .await
        .expect("a multi-hop own-network write must succeed, not be refused");

    let stored =
        unpack_hooking_memory(&service.exec(pack_get_hooking_memory(1), &[]).await.unwrap());
    assert_eq!(
        stored,
        Some(payload),
        "the forwarded write must actually persist"
    );
}

/// Direction 2: what this handler still legitimately refuses — a request this crate's own
/// decoder cannot make sense of — keeps failing after the `client_tuple` check is gone,
/// regardless of `client_tuple`'s own shape.
#[tokio::test]
async fn a_malformed_request_is_still_refused_regardless_of_client_tuple() {
    let topology = Topology::new([4]).unwrap();
    let (peers, _peers_cancel) = single_node_peers(topology);
    let map: Arc<dyn CoordinatorMap> = Arc::new(TestMap {
        can_reserve: true,
        free: vec![0, 1, 2, 3],
        n_nodes: 1,
    });
    let (_handle, service, _cancel) =
        build_coordinator(peers, map, Arc::new(NoopPropagationHandler));

    // Not a `coordinator.CoordinatorRequest` at all: `unpack_request` can never decode it.
    let garbage = typed_value("not.a.coordinator.request", &cv1::NumberOfNodesRequest {});
    for client_tuple in [&[][..], &[3u32, 1u32][..]] {
        let err = service
            .exec(garbage.clone(), client_tuple)
            .await
            .expect_err("a request this crate cannot decode must still be refused");
        assert!(matches!(err, ExecError::Refuse(_)));
    }
}

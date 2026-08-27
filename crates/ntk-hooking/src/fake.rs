//! In-memory test doubles for [`QspnView`], [`CoordinatorClient`], and
//! [`HookingStubFactory`] — the fake half of each dependency-inverted seam
//! (`research/notes/06-rust-stack.md` §"Where Rust traits replace...",
//! mirroring `ntk_rpc::FakeRpcClient`'s role for the transport layer).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use ntk_common::Topology;
use ntk_rpc::RpcError;

use crate::arc::ArcId;
use crate::coordinator::{
    CoordinatorClient, CoordinatorError, MergeArbitrationRequest, Reservation,
};
use crate::domain::{
    DeleteReservationRequest, EntryData, EvaluateEnterRequest, ExploreGNodeRequest,
    ExploreGNodeResponse, FinishEnterData, FinishMigrationData, NetworkData, RequestPacket,
    ResponsePacket, SearchMigrationPathErrorPkt, SearchMigrationPathRequest,
    SearchMigrationPathResponse,
};
use crate::rpc::HookingRpcHandler;
use crate::stub::{HookingStub, HookingStubFactory};
use crate::view::{AdjacentGNode, QspnView};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// FakeQspnView
// ---------------------------------------------------------------------------

/// A hand-configurable [`QspnView`]. Every field is `pub` — tests build one
/// with [`FakeQspnView::new`] then mutate directly before wrapping it in an
/// `Arc` (all fields are read afterward through `&dyn QspnView`, so no
/// interior mutability is needed for a fixed test scenario).
#[derive(Debug)]
pub struct FakeQspnView {
    pub topology: Topology,
    pub my_pos: Vec<u32>,
    pub my_eldership: Vec<i32>,
    pub network_id: i64,
    pub n_nodes: u64,
    pub subnetlevel: usize,
    pub epsilon: usize,
    pub is_bootstrapped: bool,
    pub adjacency: HashMap<(usize, usize), Vec<AdjacentGNode>>,
    pub eldership_by_hc: HashMap<(usize, u32), i32>,
}

impl FakeQspnView {
    #[must_use]
    pub fn new(topology: Topology, my_pos: Vec<u32>) -> Self {
        let levels = topology.levels();
        Self {
            topology,
            my_pos,
            my_eldership: vec![0; levels],
            network_id: 1,
            n_nodes: 1,
            subnetlevel: 0,
            epsilon: 0,
            is_bootstrapped: true,
            adjacency: HashMap::new(),
            eldership_by_hc: HashMap::new(),
        }
    }
}

impl QspnView for FakeQspnView {
    fn topology(&self) -> &Topology {
        &self.topology
    }
    fn network_id(&self) -> i64 {
        self.network_id
    }
    fn n_nodes(&self) -> u64 {
        self.n_nodes
    }
    fn my_pos(&self, level: usize) -> u32 {
        self.my_pos[level]
    }
    fn my_eldership(&self, level: usize) -> i32 {
        self.my_eldership[level]
    }
    fn subnetlevel(&self) -> usize {
        self.subnetlevel
    }
    fn epsilon(&self, _level: usize) -> usize {
        self.epsilon
    }
    fn eldership(&self, level: usize, pos: u32) -> i32 {
        *self.eldership_by_hc.get(&(level, pos)).unwrap_or(&0)
    }
    fn adjacent_to_my_gnode(
        &self,
        level_adjacent_gnodes: usize,
        level_my_gnode: usize,
    ) -> Vec<AdjacentGNode> {
        self.adjacency
            .get(&(level_adjacent_gnodes, level_my_gnode))
            .cloned()
            .unwrap_or_default()
    }
    fn is_bootstrapped(&self) -> bool {
        self.is_bootstrapped
    }
}

// ---------------------------------------------------------------------------
// FakeCoordinatorClient
// ---------------------------------------------------------------------------

/// A functional in-memory Coordinator: `reserve` hands out monotonically
/// increasing positions per host level (idempotent by
/// `(host_lvl, reserve_request_id)`, matching upstream's replay-safety),
/// `evaluate_enter`/`begin_enter` return a configurable, poppable sequence
/// of outcomes (default: succeed immediately), `decide_merge` memoizes its
/// verdict per `neighbor_network_id` for [`Self::set_merge_decision_ttl`]'s
/// duration (defaulted long enough that an ordinary, sleep-free test never
/// observes an expiry) — the real collective-decision behavior, see
/// [`CoordinatorClient::decide_merge`]'s own doc for why that memoization must
/// still expire rather than last forever — and every call is logged in
/// [`FakeCoordinatorClient::calls`] for assertions. Sharing one
/// `Arc<FakeCoordinatorClient>` across several simulated
/// [`HookingHandle`](crate::HookingHandle)s models "my own g-node's
/// elected Coordinator": every member's arc handler that calls
/// `decide_merge` against the same shared instance gets the same answer
/// while that answer is still fresh.
#[derive(Default, Debug)]
pub struct FakeCoordinatorClient {
    n_nodes: AtomicU64,
    next_pos: Mutex<HashMap<usize, u32>>,
    next_eldership: AtomicI64,
    reservations: Mutex<HashMap<(usize, i32), (u32, i32)>>,
    fail_reserve_at: Mutex<HashSet<usize>>,
    evaluate_enter_outcomes: Mutex<VecDeque<Result<usize, CoordinatorError>>>,
    begin_enter_outcomes: Mutex<VecDeque<Result<(), CoordinatorError>>>,
    merge_decisions: Mutex<HashMap<i64, (bool, tokio::time::Instant)>>,
    merge_decision_ttl: Mutex<Duration>,
    calls: Mutex<Vec<String>>,
}

impl FakeCoordinatorClient {
    #[must_use]
    pub fn new(n_nodes: u64) -> Self {
        Self {
            n_nodes: AtomicU64::new(n_nodes),
            merge_decision_ttl: Mutex::new(Duration::from_secs(3600)),
            ..Default::default()
        }
    }

    pub fn set_n_nodes(&self, n: u64) {
        self.n_nodes.store(n, Ordering::Relaxed);
    }

    /// Shortens [`Self::decide_merge`]'s memoization window — lets a test observe a verdict
    /// actually expire and get recomputed (via `tokio::time::advance` under
    /// `#[tokio::test(start_paused = true)]`) without waiting on the generous default.
    pub fn set_merge_decision_ttl(&self, ttl: Duration) {
        *lock(&self.merge_decision_ttl) = ttl;
    }

    /// Every subsequent `reserve` at `host_lvl` fails with
    /// [`CoordinatorError::NoCoordinatorForLevel`].
    pub fn fail_reserve_at(&self, host_lvl: usize) {
        lock(&self.fail_reserve_at).insert(host_lvl);
    }

    /// The next position `reserve(host_lvl, _)` hands out (for a
    /// not-yet-reserved id) — lets a test pre-seed "this level is already
    /// full" scenarios.
    pub fn set_next_pos(&self, host_lvl: usize, pos: u32) {
        lock(&self.next_pos).insert(host_lvl, pos);
    }

    /// Queues the next `evaluate_enter` outcome (FIFO); once the queue is
    /// drained, `evaluate_enter` succeeds immediately at
    /// `req.min_lvl`.
    pub fn queue_evaluate_enter(&self, outcome: Result<usize, CoordinatorError>) {
        lock(&self.evaluate_enter_outcomes).push_back(outcome);
    }

    /// Queues the next `begin_enter` outcome (FIFO); once drained,
    /// `begin_enter` always succeeds.
    pub fn queue_begin_enter(&self, outcome: Result<(), CoordinatorError>) {
        lock(&self.begin_enter_outcomes).push_back(outcome);
    }

    #[must_use]
    pub fn calls(&self) -> Vec<String> {
        lock(&self.calls).clone()
    }

    fn record(&self, call: impl Into<String>) {
        lock(&self.calls).push(call.into());
    }
}

impl CoordinatorClient for FakeCoordinatorClient {
    fn n_nodes(&self) -> BoxFuture<'_, u64> {
        Box::pin(async move { self.n_nodes.load(Ordering::Relaxed) })
    }

    fn decide_merge(&self, req: MergeArbitrationRequest) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            self.record(format!("decide_merge({})", req.neighbor_network_id));
            let now = tokio::time::Instant::now();
            let ttl = *lock(&self.merge_decision_ttl);
            if let Some(&(cached, decided_at)) =
                lock(&self.merge_decisions).get(&req.neighbor_network_id)
                && now.saturating_duration_since(decided_at) < ttl
            {
                return cached;
            }
            let my_n_nodes = self.n_nodes.load(Ordering::Relaxed);
            let decision = crate::merge::merge_tiebreak(
                my_n_nodes,
                req.neighbor_n_nodes,
                req.my_network_id,
                req.neighbor_network_id,
            );
            lock(&self.merge_decisions).insert(req.neighbor_network_id, (decision, now));
            decision
        })
    }

    fn evaluate_enter(
        &self,
        req: EvaluateEnterRequest,
    ) -> BoxFuture<'_, Result<usize, CoordinatorError>> {
        Box::pin(async move {
            self.record("evaluate_enter");
            lock(&self.evaluate_enter_outcomes)
                .pop_front()
                .unwrap_or(Ok(req.min_lvl))
        })
    }

    fn begin_enter(&self, lvl: usize) -> BoxFuture<'_, Result<(), CoordinatorError>> {
        Box::pin(async move {
            self.record(format!("begin_enter({lvl})"));
            lock(&self.begin_enter_outcomes)
                .pop_front()
                .unwrap_or(Ok(()))
        })
    }

    fn completed_enter(&self, lvl: usize) -> BoxFuture<'_, Result<(), CoordinatorError>> {
        Box::pin(async move {
            self.record(format!("completed_enter({lvl})"));
            Ok(())
        })
    }

    fn abort_enter(&self, lvl: usize) -> BoxFuture<'_, Result<(), CoordinatorError>> {
        Box::pin(async move {
            self.record(format!("abort_enter({lvl})"));
            Ok(())
        })
    }

    fn reserve(
        &self,
        host_lvl: usize,
        reserve_request_id: i32,
    ) -> BoxFuture<'_, Result<Reservation, CoordinatorError>> {
        Box::pin(async move {
            self.record(format!("reserve({host_lvl},{reserve_request_id})"));
            let key = (host_lvl, reserve_request_id);
            if let Some(&(pos, eldership)) = lock(&self.reservations).get(&key) {
                return Ok(Reservation { pos, eldership });
            }
            if lock(&self.fail_reserve_at).contains(&host_lvl) {
                return Err(CoordinatorError::NoCoordinatorForLevel);
            }
            let mut next_pos = lock(&self.next_pos);
            let pos = *next_pos.get(&host_lvl).unwrap_or(&0);
            next_pos.insert(host_lvl, pos + 1);
            drop(next_pos);
            let eldership = i32::try_from(self.next_eldership.fetch_add(1, Ordering::Relaxed))
                .unwrap_or(i32::MAX);
            lock(&self.reservations).insert(key, (pos, eldership));
            Ok(Reservation { pos, eldership })
        })
    }

    fn delete_reserve(&self, host_lvl: usize, reserve_request_id: i32) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.record(format!("delete_reserve({host_lvl},{reserve_request_id})"));
            lock(&self.reservations).remove(&(host_lvl, reserve_request_id));
        })
    }

    fn prepare_migration(&self, lvl: usize, migration_id: i32) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.record(format!("prepare_migration({lvl},{migration_id})"));
        })
    }

    fn finish_migration(&self, lvl: usize, _data: FinishMigrationData) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.record(format!("finish_migration({lvl})"));
        })
    }

    fn prepare_enter(&self, lvl: usize, enter_id: i32) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.record(format!("prepare_enter({lvl},{enter_id})"));
        })
    }

    fn finish_enter(&self, lvl: usize, _data: FinishEnterData) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.record(format!("finish_enter({lvl})"));
        })
    }
}

// ---------------------------------------------------------------------------
// ScriptedHookingStub — canned per-call responses, for testing an arc
// handler's reaction to a specific remote outcome without a full peer node.
// ---------------------------------------------------------------------------

type NetworkDataFn = Box<dyn Fn(bool) -> Result<NetworkData, RpcError> + Send + Sync>;
type SearchPathFn = Box<dyn Fn(usize) -> Result<EntryData, RpcError> + Send + Sync>;

/// A [`HookingStub`] whose `retrieve_network_data`/`search_migration_path`
/// answers are supplied as plain closures — for tests that need to script a
/// specific remote outcome (refusal, timeout, a fixed `NetworkData`)
/// without standing up a full peer [`HookingRpcHandler`]. The 8 `route_*`
/// methods are no-ops (`Ok(())`): nothing in the `ArcHandler` state machine
/// this double is meant to exercise ever calls them.
pub struct ScriptedHookingStub {
    retrieve_network_data: NetworkDataFn,
    search_migration_path: SearchPathFn,
}

impl std::fmt::Debug for ScriptedHookingStub {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScriptedHookingStub")
            .finish_non_exhaustive()
    }
}

impl ScriptedHookingStub {
    #[must_use]
    pub fn new(
        retrieve_network_data: impl Fn(bool) -> Result<NetworkData, RpcError> + Send + Sync + 'static,
        search_migration_path: impl Fn(usize) -> Result<EntryData, RpcError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            retrieve_network_data: Box::new(retrieve_network_data),
            search_migration_path: Box::new(search_migration_path),
        }
    }

    /// Always fails `retrieve_network_data` with [`RpcError::Timeout`] — the
    /// "far side times out mid-sequence" scenario.
    #[must_use]
    pub fn always_times_out() -> Self {
        Self::new(|_| Err(RpcError::Timeout), |_| Err(RpcError::Timeout))
    }
}

impl HookingStub for ScriptedHookingStub {
    fn retrieve_network_data(
        &self,
        ask_coord: bool,
    ) -> BoxFuture<'_, Result<NetworkData, RpcError>> {
        Box::pin(async move { (self.retrieve_network_data)(ask_coord) })
    }
    fn search_migration_path(&self, lvl: usize) -> BoxFuture<'_, Result<EntryData, RpcError>> {
        Box::pin(async move { (self.search_migration_path)(lvl) })
    }
    fn route_search_request(
        &self,
        _req: SearchMigrationPathRequest,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async { Ok(()) })
    }
    fn route_search_error(
        &self,
        _pkt: SearchMigrationPathErrorPkt,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async { Ok(()) })
    }
    fn route_search_response(
        &self,
        _resp: SearchMigrationPathResponse,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async { Ok(()) })
    }
    fn route_explore_request(
        &self,
        _req: ExploreGNodeRequest,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async { Ok(()) })
    }
    fn route_explore_response(
        &self,
        _resp: ExploreGNodeResponse,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async { Ok(()) })
    }
    fn route_delete_reserve_request(
        &self,
        _req: DeleteReservationRequest,
    ) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async { Ok(()) })
    }
    fn route_mig_request(&self, _req: RequestPacket) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async { Ok(()) })
    }
    fn route_mig_response(&self, _resp: ResponsePacket) -> BoxFuture<'_, Result<(), RpcError>> {
        Box::pin(async { Ok(()) })
    }
}

// ---------------------------------------------------------------------------
// FakeHookingStubFactory
// ---------------------------------------------------------------------------

/// In-memory [`HookingStubFactory`]: `arc_stub`/`gateway_stub` return
/// whatever was registered, either a [`ScriptedHookingStub`] or a
/// `crate::rpc::LocalHookingStub` wrapping a real peer
/// [`HookingRpcHandler`] (for a full in-memory multi-node simulation, the
/// same role `ntk_qspn::FakeQspnStubFactory` plays for QSPN).
#[derive(Default)]
pub struct FakeHookingStubFactory {
    arcs: Mutex<HashMap<ArcId, Arc<dyn HookingStub>>>,
    gateways: Mutex<HashMap<ntk_common::HCoord, Arc<dyn HookingStub>>>,
}

impl std::fmt::Debug for FakeHookingStubFactory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeHookingStubFactory")
            .finish_non_exhaustive()
    }
}

impl FakeHookingStubFactory {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_arc(&self, arc: ArcId, stub: Arc<dyn HookingStub>) {
        lock(&self.arcs).insert(arc, stub);
    }

    /// Registers `handler` as a real peer, reachable both as `arc` (for
    /// `ArcHandler`'s own client calls) and as the gateway toward `hc` (for
    /// migration-path routing).
    pub fn register_peer(
        &self,
        arc: ArcId,
        hc: ntk_common::HCoord,
        handler: Arc<HookingRpcHandler>,
    ) {
        let stub: Arc<dyn HookingStub> = Arc::new(crate::rpc::LocalHookingStub { handler });
        lock(&self.arcs).insert(arc, stub.clone());
        lock(&self.gateways).insert(hc, stub);
    }

    pub fn register_gateway(&self, hc: ntk_common::HCoord, stub: Arc<dyn HookingStub>) {
        lock(&self.gateways).insert(hc, stub);
    }
}

impl HookingStubFactory for FakeHookingStubFactory {
    fn arc_stub(&self, arc: ArcId) -> Arc<dyn HookingStub> {
        lock(&self.arcs)
            .get(&arc)
            .cloned()
            .expect("FakeHookingStubFactory: arc must be registered before use")
    }

    fn gateway_stub(&self, hc: ntk_common::HCoord) -> Option<Arc<dyn HookingStub>> {
        lock(&self.gateways).get(&hc).cloned()
    }
}

//! Collective-merge propagation robustness.
//!
//! The five propagation methods (`prepare_migration`/`finish_migration`/`prepare_enter`/
//! `finish_enter`/`we_have_splitted`) and the anti-replay dedup window already exist and are
//! covered by `tests/reserve.rs`'s own `replaying_a_propagation_within_the_retention_window_is_
//! rejected`. These tests instead prove the two robustness properties the collective merge
//! decision (`ntk-hooking`'s `CoordinatorClient::decide_merge`) leans on:
//!
//! - a member added to the g-node *after* an earlier flood attempt is still reached by a later
//!   one ("joins late" — `Handle::prepare_enter`'s own gossip re-flood at every hop,
//!   `coord.vala:322-342`, is what makes this possible: each hop floods to *its own* current
//!   neighbor list, not a fixed snapshot taken once at the very first send);
//! - one neighbor's delivery failure never blocks the others or wedges the caller ("misses the
//!   propagation, converges rather than wedging" — upstream's own `catch (StubError e) { //
//!   nop. }`, `coord.vala:267-278`, already treats a failed neighbor as ordinary, non-fatal).
//!   That failed member's own eventual convergence is Hooking's independent rediscovery (out of
//!   this crate's scope, see `ntk-hooking`'s `decide_merge` docs: it is a pull, so a member that
//!   missed one push simply asks again later and gets the same answer) — this crate's own
//!   obligation, proven here, is that the flood itself stays bounded and reaches everyone else.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use futures::future::BoxFuture;
use ntk_common::Topology;
use ntk_coordinator::v1 as cv1;
use ntk_coordinator::{
    AbortEnterHandler, BeginEnterHandler, CompletedEnterHandler, Config, CoordinatorMap,
    CoordinatorRpcHandler, CoordinatorStub, CoordinatorStubFactory, EnterHandlers,
    EvaluateEnterHandler, FakeCoordinatorStubFactory, Handle, Manager, PropagationArgs,
    PropagationHandler, direct_stub,
};
use ntk_peerservices::StubCallError;
use ntk_proto::domain::typed_value;
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::{CallerContext, CoordinatorExecuteArgs, MethodCall, TypedValue};
use ntk_rpc::RpcHandler;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

/// Every simulated node reports the same trivial (single-level, position 0) map — they are all
/// members of the same g-node, which `check_propagation` (`coord.vala:424-440`) requires.
#[derive(Debug, Clone)]
struct TestMap;
impl CoordinatorMap for TestMap {
    fn n_nodes(&self) -> u64 {
        1
    }
    fn free_positions(&self, _level: usize) -> Vec<u32> {
        vec![0]
    }
    fn can_reserve(&self, _level: usize) -> bool {
        true
    }
    fn my_pos(&self, _level: usize) -> u32 {
        0
    }
    fn fp_id(&self, _level: usize) -> i64 {
        0
    }
}

/// Echoes `data` back unchanged — evaluate/begin/completed/abort enter are never exercised by
/// these tests, only wired to satisfy `Manager::new` (mirrors `tests/reserve.rs`'s own
/// `NoopHandlers`).
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

/// Records every `prepare_enter` invocation onto a channel, tagged with this node's own
/// `label` — used to observe which simulated members actually received a propagation.
struct RecordingPropagationHandler {
    label: &'static str,
    tx: mpsc::UnboundedSender<&'static str>,
}
impl PropagationHandler for RecordingPropagationHandler {
    fn prepare_migration(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn finish_migration(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
    fn prepare_enter(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        let tx = self.tx.clone();
        let label = self.label;
        Box::pin(async move {
            let _ = tx.send(label);
        })
    }
    fn finish_enter(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        let tx = self.tx.clone();
        let label = self.label;
        Box::pin(async move {
            let _ = tx.send(label);
        })
    }
    fn we_have_splitted(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// A [`CoordinatorStub`] that always fails, simulating a neighbor whose delivery never arrives —
/// deliberately not backed by any [`Handle`], since it must never actually be reached.
struct AlwaysFailingStub;
impl CoordinatorStub for AlwaysFailingStub {
    fn execute_prepare_migration(
        &self,
        _args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Err(StubCallError("simulated delivery failure".to_owned())) })
    }
    fn execute_finish_migration(
        &self,
        _args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Err(StubCallError("simulated delivery failure".to_owned())) })
    }
    fn execute_prepare_enter(
        &self,
        _args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Err(StubCallError("simulated delivery failure".to_owned())) })
    }
    fn execute_finish_enter(
        &self,
        _args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Err(StubCallError("simulated delivery failure".to_owned())) })
    }
    fn execute_we_have_splitted(
        &self,
        _args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Err(StubCallError("simulated delivery failure".to_owned())) })
    }
}

/// Delivers to every currently-registered stub at once, ignoring individual failures — the test
/// double's own `stub_for_all_neighbors` group (not exercised by these tests, which only use
/// `prepare_enter`'s per-neighbor fanout, but required to satisfy [`CoordinatorStubFactory`]).
struct BroadcastAll(Vec<Arc<dyn CoordinatorStub>>);
impl CoordinatorStub for BroadcastAll {
    fn execute_prepare_migration(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let stubs = self.0.clone();
        Box::pin(async move {
            for s in stubs {
                let _ = s.execute_prepare_migration(args.clone()).await;
            }
            Ok(())
        })
    }
    fn execute_finish_migration(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let stubs = self.0.clone();
        Box::pin(async move {
            for s in stubs {
                let _ = s.execute_finish_migration(args.clone()).await;
            }
            Ok(())
        })
    }
    fn execute_prepare_enter(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let stubs = self.0.clone();
        Box::pin(async move {
            for s in stubs {
                let _ = s.execute_prepare_enter(args.clone()).await;
            }
            Ok(())
        })
    }
    fn execute_finish_enter(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let stubs = self.0.clone();
        Box::pin(async move {
            for s in stubs {
                let _ = s.execute_finish_enter(args.clone()).await;
            }
            Ok(())
        })
    }
    fn execute_we_have_splitted(
        &self,
        args: PropagationArgs,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let stubs = self.0.clone();
        Box::pin(async move {
            for s in stubs {
                let _ = s.execute_we_have_splitted(args.clone()).await;
            }
            Ok(())
        })
    }
}

/// A [`CoordinatorStubFactory`] whose "each neighbor" list can grow after construction (plain
/// `std::sync::Mutex`, since the trait's own accessors are synchronous) — models a member whose
/// arc/link comes up after an earlier flood already went out.
#[derive(Default)]
struct GrowableStubFactory {
    each: Mutex<Vec<Arc<dyn CoordinatorStub>>>,
}
impl GrowableStubFactory {
    fn add(&self, stub: Arc<dyn CoordinatorStub>) {
        lock(&self.each).push(stub);
    }
}
impl CoordinatorStubFactory for GrowableStubFactory {
    fn stub_for_each_neighbor(&self) -> Vec<Arc<dyn CoordinatorStub>> {
        lock(&self.each).clone()
    }
    fn stub_for_all_neighbors(&self) -> Arc<dyn CoordinatorStub> {
        Arc::new(BroadcastAll(lock(&self.each).clone()))
    }
}

fn spawn_node(
    label: &'static str,
) -> (
    Handle,
    Arc<GrowableStubFactory>,
    mpsc::UnboundedReceiver<&'static str>,
) {
    let (tx, rx) = mpsc::unbounded_channel();
    let factory = Arc::new(GrowableStubFactory::default());
    let (manager, handle) = Manager::new(
        Topology::new([1]).unwrap(),
        Arc::new(TestMap),
        factory.clone() as Arc<dyn CoordinatorStubFactory>,
        Arc::new(RecordingPropagationHandler { label, tx }),
        noop_enter_handlers(),
        Config::default(),
        None,
    );
    tokio::spawn(manager.run(CancellationToken::new()));
    (handle, factory, rx)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// A member added to the g-node after an earlier flood already went out is reached by a
/// subsequent one — "joins late" (see module docs for why this is the property that matters:
/// `Handle::prepare_enter` re-floods to each hop's *current* neighbor list, not a frozen one).
#[tokio::test]
async fn a_member_that_joins_after_an_earlier_flood_is_reached_by_a_later_one() {
    let (root, root_factory, _root_rx) = spawn_node("root");
    let (mid, mid_factory, mut mid_rx) = spawn_node("mid");
    let (late, _late_factory, mut late_rx) = spawn_node("late");

    // root -> mid only; mid starts with no relay target of its own.
    root_factory.add(direct_stub(mid.clone()));

    root.prepare_enter(0, TypedValue::new("test.PrepareEnter", b"first".to_vec()))
        .await;
    assert_eq!(
        mid_rx.recv().await,
        Some("mid"),
        "the direct neighbor must receive the first flood"
    );
    assert!(
        late_rx.try_recv().is_err(),
        "late is not yet a neighbor of anyone: it must not receive the first flood"
    );

    // `late` joins mid's neighbor list — its arc/link has just come up.
    mid_factory.add(direct_stub(late.clone()));

    root.prepare_enter(0, TypedValue::new("test.PrepareEnter", b"second".to_vec()))
        .await;
    assert_eq!(
        mid_rx.recv().await,
        Some("mid"),
        "the direct neighbor receives the second flood too"
    );
    assert_eq!(
        late_rx.recv().await,
        Some("late"),
        "the late-joined member is reached by the later flood, via mid's own re-flood"
    );
}

/// One neighbor's delivery failure never blocks the others or wedges the caller.
#[tokio::test]
async fn one_failing_neighbor_never_blocks_the_others_or_wedges_the_caller() {
    let (root, root_factory, _root_rx) = spawn_node("root");
    let (good, _good_factory, mut good_rx) = spawn_node("good");

    root_factory.add(direct_stub(good.clone()));
    root_factory.add(Arc::new(AlwaysFailingStub));

    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        root.prepare_enter(0, TypedValue::new("test.PrepareEnter", b"payload".to_vec())),
    )
    .await;
    assert!(
        outcome.is_ok(),
        "a failing neighbor must never hang the flood — it must complete within budget"
    );
    assert_eq!(
        good_rx.recv().await,
        Some("good"),
        "the other, working neighbor must still receive the propagation"
    );
}

// ---------------------------------------------------------------------------
// ask_lvl >= 1: members genuinely share a g-node, and a differing level-0 position must not
// block the propagation that tells them it moved (see this crate's parent task notes: a
// level-0 g-node has exactly one member, so `ask_lvl == 0` correctly has nobody left to notify
// — that is not the property under test here).
// ---------------------------------------------------------------------------

/// A `CoordinatorMap` with independently configurable per-level position/fingerprint, for
/// modeling "a different member of the same higher g-node" (unlike this file's own `TestMap`,
/// which reports the same trivial position at every level).
#[derive(Debug, Clone)]
struct LevelMap {
    pos: Vec<u32>,
    fp: Vec<i64>,
}
impl CoordinatorMap for LevelMap {
    fn n_nodes(&self) -> u64 {
        1
    }
    fn free_positions(&self, _level: usize) -> Vec<u32> {
        vec![0]
    }
    fn can_reserve(&self, _level: usize) -> bool {
        true
    }
    fn my_pos(&self, level: usize) -> u32 {
        self.pos[level]
    }
    fn fp_id(&self, level: usize) -> i64 {
        self.fp[level]
    }
}

/// `check_propagation` (`coord.vala:424-440`) builds its comparison tuple from `positions[level
/// + i] .. levels` only (`fk_database.vala:229-237`'s `prepare_propagation` never includes
/// anything below the propagation's own level) — so at `ask_lvl == 1` a sibling's own level-0
/// position never enters the check at all. This proves a `finish_enter` at `ask_lvl == 1` is
/// accepted by a sibling whose level-0 position (2) differs from the sender's own, as long as
/// level 1 (position + `fp_id`) — the level the propagation is actually about — agrees.
#[tokio::test]
async fn a_finish_enter_at_ask_lvl_1_is_accepted_despite_a_differing_level0_position() {
    let topology = Topology::new([4, 4]).unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let map: Arc<dyn CoordinatorMap> = Arc::new(LevelMap {
        pos: vec![2, 0],
        fp: vec![0, 99],
    });
    let (manager, handle) = Manager::new(
        topology,
        map,
        Arc::new(FakeCoordinatorStubFactory::new(Vec::new())),
        Arc::new(RecordingPropagationHandler {
            label: "sibling",
            tx,
        }),
        noop_enter_handlers(),
        Config::default(),
        None,
    );
    tokio::spawn(manager.run(CancellationToken::new()));
    let rpc_handler = CoordinatorRpcHandler::new(handle);

    // The sender's own tuple names only level 1 upward (`positions.len() == levels - ask_lvl ==
    // 1`); level 0 — where sender and sibling disagree — is never part of it.
    let tuple = typed_value(
        "coordinator.PropagationTuple",
        &cv1::PropagationTuple { positions: vec![0] },
    );
    let args = CoordinatorExecuteArgs {
        tuple: Some(tuple),
        fp_id: 99,
        propagation_id: 1,
        lvl: 1,
        data: Some(TypedValue::new("test.FinishEnter", b"payload".to_vec())),
    };
    let call = MethodCall {
        call: Some(Call::CoordinatorExecuteFinishEnter(args)),
    };
    let caller = CallerContext {
        source_id: None,
        src_nic: None,
    };

    rpc_handler
        .handle(
            caller,
            TypedValue::new(String::new(), Vec::new()),
            call,
            None,
        )
        .await
        .unwrap();

    let delivered = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("a finish_enter naming a level >= 1 the sibling genuinely shares must be accepted")
        .unwrap();
    assert_eq!(delivered, "sibling");
}

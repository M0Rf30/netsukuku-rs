//! `Config::participation_reannounce_interval`: `None` (the default) performs zero automatic
//! re-announcements, exactly matching this crate's behavior before the field existed; `Some(d)`
//! repeats `Handle::register`'s own flood every `d`, wire-identically, until the governing
//! `CancellationToken` is cancelled. `Manager::run` joins its own background re-announce task
//! before returning, so this test joins the whole `Manager` task rather than merely dropping it
//! — a swallowed panic in the periodic task would fail the test instead of vanishing.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use ntk_common::{HCoord, Naddr, Topology};
use ntk_peerservices::{
    Config, ExecError, GetRequestError, Handle, Manager, ParticipantSet, PeerMessageForwarder,
    PeerService, PeersStub, Refusal, RoutingEnv, ServiceId, StubCallError, TupleGNode, TupleNode,
};
use ntk_proto::v1::TypedValue;
use tokio_util::sync::CancellationToken;

/// Yields to the executor repeatedly so tasks woken by a `tokio::time::advance` (purely
/// in-memory, no further timers on this path) get to run — time-independent, so it plays
/// correctly with `start_paused = true` tests too.
async fn settle() {
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }
}

/// Records every `set_participant(p_id, gn)` call; every other method is unused by this test.
#[derive(Default)]
struct RecordingStub {
    calls: Mutex<Vec<(ServiceId, TupleGNode)>>,
}

impl RecordingStub {
    fn calls(&self) -> Vec<(ServiceId, TupleGNode)> {
        self.calls.lock().unwrap().clone()
    }
}

impl PeersStub for RecordingStub {
    fn forward_peer_message(
        &self,
        _msg: PeerMessageForwarder,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        unreachable!("not exercised by this test")
    }
    fn get_request(
        &self,
        _msg_id: i32,
        _respondant: TupleNode,
    ) -> BoxFuture<'_, Result<TypedValue, GetRequestError>> {
        unreachable!("not exercised by this test")
    }
    fn set_response(
        &self,
        _msg_id: i32,
        _response: TypedValue,
        _respondant: TupleNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        unreachable!("not exercised by this test")
    }
    fn set_refuse_message(
        &self,
        _msg_id: i32,
        _refusal: Refusal,
        _respondant: TupleNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        unreachable!("not exercised by this test")
    }
    fn set_redo_from_start(
        &self,
        _msg_id: i32,
        _respondant: TupleNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        unreachable!("not exercised by this test")
    }
    fn set_next_destination(
        &self,
        _msg_id: i32,
        _tuple: TupleGNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        unreachable!("not exercised by this test")
    }
    fn set_failure(
        &self,
        _msg_id: i32,
        _tuple: TupleGNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        unreachable!("not exercised by this test")
    }
    fn set_non_participant(
        &self,
        _msg_id: i32,
        _tuple: TupleGNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        unreachable!("not exercised by this test")
    }
    fn set_missing_optional_maps(&self, _msg_id: i32) -> BoxFuture<'_, Result<(), StubCallError>> {
        unreachable!("not exercised by this test")
    }
    fn set_participant(
        &self,
        p_id: ServiceId,
        tuple: TupleGNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        self.calls.lock().unwrap().push((p_id, tuple));
        Box::pin(async { Ok(()) })
    }
    fn give_participant_maps(
        &self,
        _maps: ParticipantSet,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        unreachable!("not exercised by this test")
    }
    fn ask_participant_maps(&self) -> BoxFuture<'_, Result<ParticipantSet, StubCallError>> {
        unreachable!("not exercised by this test")
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// A single node whose only neighbor is `stub`; every other `RoutingEnv` capability is unused by
/// this test (no routing/gateway lookups happen here, only registration and gossip).
struct FakeEnv {
    stub: Arc<dyn PeersStub>,
}

impl RoutingEnv for FakeEnv {
    fn gnode_exists(&self, _hc: HCoord) -> bool {
        unreachable!("not exercised by this test")
    }
    fn gateway(
        &self,
        _hc: HCoord,
        _failed: Option<&Arc<dyn PeersStub>>,
    ) -> Option<Arc<dyn PeersStub>> {
        unreachable!("not exercised by this test")
    }
    fn dial(&self, _n: &TupleNode) -> Option<Arc<dyn PeersStub>> {
        unreachable!("not exercised by this test")
    }
    fn nodes_in_my_group(&self, _level: usize) -> usize {
        unreachable!("not exercised by this test")
    }
    fn neighbors(&self) -> Vec<Arc<dyn PeersStub>> {
        vec![self.stub.clone()]
    }
}

/// An optional service; `exec` is never invoked by this test.
struct OptionalService(ServiceId);

impl PeerService for OptionalService {
    fn service_id(&self) -> ServiceId {
        self.0
    }
    fn is_optional(&self) -> bool {
        true
    }
    fn exec<'a>(
        &'a self,
        request: TypedValue,
        _client_tuple: &'a [u32],
    ) -> BoxFuture<'a, Result<TypedValue, ExecError>> {
        Box::pin(async move { Ok(request) })
    }
}

/// Spawns a single-node `Manager` whose one neighbor is a fresh [`RecordingStub`], returning the
/// `Handle`, the stub (to inspect recorded calls), the `CancellationToken` governing the actor,
/// and its `JoinHandle` (to be joined, never merely dropped, so a panic surfaces).
fn spawn_node(
    config: Config,
) -> (
    Handle,
    Arc<RecordingStub>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let topology = Topology::new([4]).unwrap();
    let my_pos = Naddr::new(topology.clone(), vec![0]).unwrap();
    let stub: Arc<RecordingStub> = Arc::new(RecordingStub::default());
    let env: Arc<dyn RoutingEnv> = Arc::new(FakeEnv { stub: stub.clone() });
    let (manager, handle) = Manager::new(topology.clone(), my_pos, env, config, topology.levels());
    let cancel = CancellationToken::new();
    let join = tokio::spawn(manager.run(cancel.clone()));
    (handle, stub, cancel, join)
}

#[tokio::test(start_paused = true)]
async fn disabled_by_default_never_reannounces() {
    let (handle, stub, cancel, join) = spawn_node(Config::default());
    settle().await;

    handle
        .register(Arc::new(OptionalService(ServiceId::new(7))))
        .await;
    assert_eq!(stub.calls().len(), 1, "the initial register() flood only");

    tokio::time::advance(Duration::from_secs(365 * 24 * 3600)).await;
    settle().await;
    assert_eq!(
        stub.calls().len(),
        1,
        "Config::default() must never schedule a periodic reannounce"
    );

    cancel.cancel();
    join.await.expect("Manager::run must not panic");
}

#[tokio::test(start_paused = true)]
async fn configured_interval_repeats_the_reactive_flood_until_cancelled() {
    let interval = Duration::from_secs(60);
    let config = Config {
        participation_reannounce_interval: Some(interval),
        ..Config::default()
    };
    let (handle, stub, cancel, join) = spawn_node(config);
    // Let the Manager's background re-announce task start (and read the still-paused clock for
    // its first deadline) before any time advances, so the cadence below is exact.
    settle().await;

    let sid = ServiceId::new(11);
    handle.register(Arc::new(OptionalService(sid))).await;
    assert_eq!(stub.calls().len(), 1, "the initial reactive flood");

    for expected_count in 2..=4 {
        tokio::time::advance(interval).await;
        settle().await;
        assert_eq!(
            stub.calls().len(),
            expected_count,
            "exactly one periodic reannounce per elapsed interval"
        );
    }

    let calls = stub.calls();
    let (reactive_p_id, reactive_gn) = &calls[0];
    assert_eq!(*reactive_p_id, sid);
    for (p_id, gn) in &calls[1..] {
        assert_eq!(
            p_id, reactive_p_id,
            "reannounce must repeat the same service"
        );
        assert_eq!(
            gn, reactive_gn,
            "periodic reannounce content must match the reactive one"
        );
    }

    cancel.cancel();
    join.await
        .expect("Manager::run must not panic on cancellation");

    let after_cancel = stub.calls().len();
    tokio::time::advance(interval * 5).await;
    settle().await;
    assert_eq!(
        stub.calls().len(),
        after_cancel,
        "cancellation must stop the periodic task promptly, with no further reannounces"
    );
}

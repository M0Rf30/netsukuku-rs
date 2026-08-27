//! Pins the actor-shutdown defect: cancelling a [`Manager`] while a `contact_peer` call has
//! genuine in-flight state registered with it (a `WaitingAnswer` for a real routing search) must
//! never panic in the caller's task. Before the fix, `Handle::call`/`Handle::cast` treated a
//! closed `cmd`/reply channel — the ordinary result of the actor's `run()` returning on
//! cancellation — as a broken invariant and `.expect()`-panicked; because such panics happen in
//! tasks nothing joins (exactly `tokio::spawn`'d here, mirroring how real callers use `Handle`),
//! they were silently swallowed by the runtime instead of failing any test.

use std::sync::Arc;
use std::time::Duration;

use ntk_common::{Naddr, Topology};
use ntk_peerservices::{
    Config, ContactPeerError, ExecError, Manager, PeerMessageForwarder, PeerService, PeersStub,
    RoutingEnv, ServiceId, StubCallError, TupleNode,
};
use ntk_proto::v1::TypedValue;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

/// A gateway stub that always accepts a forwarded message (so `contact_peer` registers a real
/// `WaitingAnswer` and starts waiting for a reply) but never actually answers it — nothing else
/// in this test is exercised.
struct SilentStub;

impl PeersStub for SilentStub {
    fn forward_peer_message(
        &self,
        _msg: PeerMessageForwarder,
    ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Ok(()) })
    }
    fn get_request(
        &self,
        _msg_id: i32,
        _respondant: TupleNode,
    ) -> futures::future::BoxFuture<'_, Result<TypedValue, ntk_peerservices::GetRequestError>> {
        Box::pin(async { Err(StubCallError("unused in this test".to_owned()).into()) })
    }
    fn set_response(
        &self,
        _msg_id: i32,
        _response: TypedValue,
        _respondant: TupleNode,
    ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Err(StubCallError("unused in this test".to_owned())) })
    }
    fn set_refuse_message(
        &self,
        _msg_id: i32,
        _refusal: ntk_peerservices::Refusal,
        _respondant: TupleNode,
    ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Err(StubCallError("unused in this test".to_owned())) })
    }
    fn set_redo_from_start(
        &self,
        _msg_id: i32,
        _respondant: TupleNode,
    ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Err(StubCallError("unused in this test".to_owned())) })
    }
    fn set_next_destination(
        &self,
        _msg_id: i32,
        _tuple: ntk_peerservices::TupleGNode,
    ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Err(StubCallError("unused in this test".to_owned())) })
    }
    fn set_failure(
        &self,
        _msg_id: i32,
        _tuple: ntk_peerservices::TupleGNode,
    ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Err(StubCallError("unused in this test".to_owned())) })
    }
    fn set_non_participant(
        &self,
        _msg_id: i32,
        _tuple: ntk_peerservices::TupleGNode,
    ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Err(StubCallError("unused in this test".to_owned())) })
    }
    fn set_missing_optional_maps(
        &self,
        _msg_id: i32,
    ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Err(StubCallError("unused in this test".to_owned())) })
    }
    fn set_participant(
        &self,
        _p_id: ServiceId,
        _tuple: ntk_peerservices::TupleGNode,
    ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Err(StubCallError("unused in this test".to_owned())) })
    }
    fn give_participant_maps(
        &self,
        _maps: ntk_peerservices::ParticipantSet,
    ) -> futures::future::BoxFuture<'_, Result<(), StubCallError>> {
        Box::pin(async { Err(StubCallError("unused in this test".to_owned())) })
    }
    fn ask_participant_maps(
        &self,
    ) -> futures::future::BoxFuture<'_, Result<ntk_peerservices::ParticipantSet, StubCallError>>
    {
        Box::pin(async { Err(StubCallError("unused in this test".to_owned())) })
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// A single-node environment where the only other g-node always exists and is always reachable
/// via [`SilentStub`]. `gateway` signals `ready` once `contact_peer` has reached the point of
/// dispatching to it, which only happens after the `Manager` has already registered a real
/// `WaitingAnswer` for the search — i.e. genuine in-flight actor state.
struct FakeEnv {
    stub: Arc<dyn PeersStub>,
    ready: Arc<Notify>,
}

impl RoutingEnv for FakeEnv {
    fn gnode_exists(&self, _hc: ntk_common::HCoord) -> bool {
        true
    }
    fn gateway(
        &self,
        _hc: ntk_common::HCoord,
        _failed: Option<&Arc<dyn PeersStub>>,
    ) -> Option<Arc<dyn PeersStub>> {
        self.ready.notify_one();
        Some(self.stub.clone())
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

/// A mandatory (non-optional) service, registered only so `non_participant_gnodes` doesn't
/// pre-exclude the routing target as "no known participants" before any actor round trip
/// happens — its `exec` is never actually invoked in this test (routing targets a different
/// position than `my_pos`).
struct MandatoryService(ServiceId);

impl PeerService for MandatoryService {
    fn service_id(&self) -> ServiceId {
        self.0
    }
    fn is_optional(&self) -> bool {
        false
    }
    fn exec<'a>(
        &'a self,
        _request: TypedValue,
        _client_tuple: &'a [u32],
    ) -> futures::future::BoxFuture<'a, Result<TypedValue, ExecError>> {
        Box::pin(async { unreachable!("routing targets a different node in this test") })
    }
}

/// Reproduces the defect directly: cancels the `Manager` while a `contact_peer` call it started
/// has a live `WaitingAnswer` registered, then asserts neither task panics.
#[tokio::test]
async fn cancellation_during_inflight_contact_peer_never_panics() {
    let topology = Topology::new([2]).unwrap();
    let my_addr = Naddr::new(topology.clone(), vec![0]).unwrap();
    let target = TupleNode::new(topology.clone(), vec![1]).unwrap();
    let ready = Arc::new(Notify::new());
    let env = Arc::new(FakeEnv {
        stub: Arc::new(SilentStub),
        ready: ready.clone(),
    });
    let (manager, handle) = Manager::new(
        topology.clone(),
        my_addr,
        env,
        Config::default(),
        topology.levels(),
    );

    let cancel = CancellationToken::new();
    let manager_task = tokio::spawn(manager.run(cancel.child_token()));
    let sid = ServiceId::new(1);
    handle.register(Arc::new(MandatoryService(sid))).await;
    let request = TypedValue::new("test.echo", b"hi".to_vec());
    let contact_task = tokio::spawn(async move {
        handle
            .contact_peer(
                sid,
                Some(target),
                request,
                Duration::from_secs(5),
                None,
                Vec::new(),
            )
            .await
    });

    // Block until `contact_peer` has registered its `WaitingAnswer` and dispatched to the
    // gateway — real in-flight actor state, exactly the scenario the ntkd integration test hit.
    ready.notified().await;
    cancel.cancel();

    manager_task
        .await
        .expect("the Manager's own task must not panic on cancellation");

    match contact_task.await {
        Err(join_err) => panic!(
            "contact_peer task panicked racing actor shutdown (the bug this test pins): {join_err}"
        ),
        Ok(Ok(_)) => panic!(
            "contact_peer unexpectedly succeeded — SilentStub never answers, so a real reply \
             would mean the test setup is broken"
        ),
        Ok(Err(ContactPeerError::NoParticipants | ContactPeerError::Database(_))) => {
            // Expected: a cancelled actor resolves the in-flight search to a clean, ordinary
            // routing failure instead of panicking.
        }
        Ok(Err(ContactPeerError::TooManyHops { .. })) => panic!(
            "contact_peer hit the hop bound in this single-attempt cancellation scenario — the \
             bound is far too tight, not the cancellation-shutdown path this test pins"
        ),
    }
}

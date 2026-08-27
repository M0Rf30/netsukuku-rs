//! Pins the `finish_enter` teardown-race defect: `Handle::finish_enter` spawns a detached task
//! that calls into the injected `PropagationHandler` (which, in `ntkd`, triggers `rehook()` and
//! tears down the very Coordinator generation the task belongs to) and then calls
//! `handle.schedule_propagation_cleanup(...)` back on that same, now-dead actor. Before the fix,
//! `Handle::call`/`Handle::cast` treated the resulting closed channel as a broken invariant and
//! `.expect()`-panicked; because such panics happen in tasks nothing joins (exactly
//! `tokio::spawn`'d here, mirroring how `Handle::finish_enter` really spawns), they were silently
//! swallowed by the runtime instead of failing any test. This test installs a panic hook to catch
//! that swallowed panic directly.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use ntk_common::Topology;
use ntk_coordinator::{
    AbortEnterHandler, BeginEnterHandler, CompletedEnterHandler, Config, CoordinatorMap,
    EnterHandlers, EvaluateEnterHandler, FakeCoordinatorStubFactory, Manager, PropagationHandler,
};
use ntk_proto::v1::TypedValue;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

struct NoopMap;
impl CoordinatorMap for NoopMap {
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

/// A [`PropagationHandler`] whose `finish_enter` notifies `ready` once it starts, then blocks on
/// `release` — exactly the window the real `rehook()` callback occupies in production, letting
/// the test cancel the actor's `CancellationToken` *while* the detached task is inside this call,
/// before it reaches `handle.schedule_propagation_cleanup`.
struct GatedPropagationHandler {
    ready: Arc<Notify>,
    release: Arc<Notify>,
}
impl PropagationHandler for GatedPropagationHandler {
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
        Box::pin(async move {
            self.ready.notify_one();
            self.release.notified().await;
        })
    }
    fn we_have_splitted(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }
}

/// Reproduces the defect directly: `finish_enter`'s detached task is parked inside the injected
/// `PropagationHandler` when the actor is cancelled out from under it (mirroring `rehook()`
/// tearing down its own generation), then released to run
/// `handle.schedule_propagation_cleanup(...)` — the exact call-back-into-a-dead-actor the bug
/// report describes. Asserts neither the actor task nor the detached task ever panics.
#[tokio::test]
async fn finish_enter_racing_its_own_generation_teardown_never_panics() {
    // Capture panics that would otherwise be silently swallowed by the unjoined `tokio::spawn`
    // tasks `Handle::finish_enter`/`schedule_propagation_cleanup` create.
    let panicked = Arc::new(AtomicBool::new(false));
    let panicked_hook = panicked.clone();
    let prev_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        eprintln!("captured panic: {info}");
        panicked_hook.store(true, Ordering::SeqCst);
    }));

    let ready = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let topology = Topology::new([2]).unwrap();
    let (manager, handle) = Manager::new(
        topology,
        Arc::new(NoopMap),
        Arc::new(FakeCoordinatorStubFactory::new(Vec::new())),
        Arc::new(GatedPropagationHandler {
            ready: ready.clone(),
            release: release.clone(),
        }),
        noop_enter_handlers(),
        // A short retention makes `schedule_propagation_cleanup`'s deferred `ExpirePropagation`
        // cast fire quickly once the detached task reaches it, instead of the 200s production
        // default.
        Config {
            propagation_retention: Duration::from_millis(20),
            ..Config::default()
        },
        None,
    );

    let cancel = CancellationToken::new();
    let manager_task = tokio::spawn(manager.run(cancel.child_token()));

    let finish_enter_task = tokio::spawn({
        let handle = handle.clone();
        async move {
            handle
                .finish_enter(1, TypedValue::new("test", Vec::new()))
                .await;
        }
    });

    // Wait until the detached task is parked inside `PropagationHandler::finish_enter` (real
    // in-flight actor-adjacent state), then tear the actor down out from under it.
    ready.notified().await;
    cancel.cancel();
    manager_task
        .await
        .expect("the Manager's own task must not panic on cancellation");

    // Now let the detached task resume: it calls `handle.schedule_propagation_cleanup(...)`,
    // which — after `propagation_retention` — casts `Cmd::ExpirePropagation` onto the now-closed
    // channel. This must be an ordinary no-op, not a panic.
    release.notify_one();
    finish_enter_task
        .await
        .expect("finish_enter's own call must not panic");

    // Give the deferred cleanup task (spawned inside `schedule_propagation_cleanup`) time to run
    // its `cast` against the closed channel.
    tokio::time::sleep(Duration::from_millis(200)).await;

    std::panic::set_hook(prev_hook);
    assert!(
        !panicked.load(Ordering::SeqCst),
        "a task panicked racing the actor's own generation teardown (the bug this test pins)"
    );
}

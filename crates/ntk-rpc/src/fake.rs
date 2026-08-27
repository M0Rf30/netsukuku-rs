//! In-memory [`RpcClient`] for tests/simulation: routes calls directly to a
//! registered [`RpcHandler`] with no socket involved, and supports
//! configurable latency and failure injection — the fake half of the
//! `RpcClient` substitutability seam
//! (research/notes/06-rust-stack.md §"Where Rust traits replace...").

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use futures::future::BoxFuture;
use ntk_proto::v1::{Auth, CallerContext, MethodCall, ResponsePayload, TypedValue};

use crate::client::RpcClient;
use crate::error::RpcError;
use crate::server::RpcHandler;

/// A closure producing the [`RpcError`] to inject instead of dispatching to
/// the handler.
pub type FailureFactory = Arc<dyn Fn() -> RpcError + Send + Sync>;

/// A scriptable fault schedule for a [`FakeRpcClient`]: which of its calls fail, and with what
/// error. `call`/`notify` share one 1-indexed counter per client (shared across every `Clone`,
/// since a clone is the same simulated link, not a new one) — deterministic by construction: it
/// counts calls, never wall-clock time, RNG, or task-scheduling order.
#[derive(Clone)]
enum Fault {
    /// Every call fails ([`FakeRpcClient::with_failure`]).
    Always(FailureFactory),
    /// Exactly the `at`-th call fails; every other call reaches the handler
    /// ([`FakeRpcClient::with_failure_at`]).
    Nth { at: u64, factory: FailureFactory },
    /// The first `remaining` calls fail; every call after that reaches the handler
    /// ([`FakeRpcClient::with_failures_for`]).
    NextN {
        remaining: Arc<AtomicU64>,
        factory: FailureFactory,
    },
}

impl Fault {
    /// Consults (and, for [`Fault::NextN`], advances) this schedule for the call numbered
    /// `call_number` (1-indexed); returns the error to inject, if any.
    fn outcome(&self, call_number: u64) -> Option<RpcError> {
        match self {
            Fault::Always(factory) => Some(factory()),
            Fault::Nth { at, factory } => (call_number == *at).then(|| factory()),
            Fault::NextN { remaining, factory } => remaining
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
                .is_ok()
                .then(|| factory()),
        }
    }
}

impl std::fmt::Debug for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Fault::Always(_) => f.write_str("Always"),
            Fault::Nth { at, .. } => f
                .debug_struct("Nth")
                .field("at", at)
                .finish_non_exhaustive(),
            Fault::NextN { remaining, .. } => f
                .debug_struct("NextN")
                .field("remaining", &remaining.load(Ordering::Relaxed))
                .finish_non_exhaustive(),
        }
    }
}

/// In-memory [`RpcClient`] that calls a registered [`RpcHandler`] directly.
#[derive(Clone)]
pub struct FakeRpcClient {
    handler: Arc<dyn RpcHandler>,
    latency: Duration,
    fault: Option<Fault>,
    /// Count of `call`/`notify` dispatches so far, shared across `Clone`s — the counter
    /// [`Fault::Nth`]/[`Fault::NextN`] scripts against.
    calls: Arc<AtomicU64>,
}

impl std::fmt::Debug for FakeRpcClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeRpcClient")
            .field("latency", &self.latency)
            .field("fault", &self.fault)
            .finish_non_exhaustive()
    }
}

impl FakeRpcClient {
    /// Routes every call straight to `handler`, with no latency or forced
    /// failure.
    #[must_use]
    pub fn new(handler: Arc<dyn RpcHandler>) -> Self {
        Self {
            handler,
            latency: Duration::ZERO,
            fault: None,
            calls: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Sleeps `latency` before dispatching (or failing) each call.
    #[must_use]
    pub fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = latency;
        self
    }

    /// Makes every subsequent `call`/`notify` fail with `factory()` instead
    /// of reaching the handler.
    #[must_use]
    pub fn with_failure(mut self, factory: impl Fn() -> RpcError + Send + Sync + 'static) -> Self {
        self.fault = Some(Fault::Always(Arc::new(factory)));
        self
    }

    /// Makes exactly the `at`-th `call`/`notify` (1-indexed, shared between the two) fail with
    /// `factory()`; every other call reaches the handler normally. `Clone`s of `self` share the
    /// same counter, so the schedule applies across the whole simulated link, not just one
    /// instance.
    #[must_use]
    pub fn with_failure_at(
        mut self,
        at: u64,
        factory: impl Fn() -> RpcError + Send + Sync + 'static,
    ) -> Self {
        self.fault = Some(Fault::Nth {
            at,
            factory: Arc::new(factory),
        });
        self
    }

    /// Makes the first `count` `call`/`notify` dispatches fail with `factory()`; every call
    /// after that reaches the handler normally. Like [`Self::with_failure_at`], the budget is
    /// shared across every `Clone` of `self`.
    #[must_use]
    pub fn with_failures_for(
        mut self,
        count: u64,
        factory: impl Fn() -> RpcError + Send + Sync + 'static,
    ) -> Self {
        self.fault = Some(Fault::NextN {
            remaining: Arc::new(AtomicU64::new(count)),
            factory: Arc::new(factory),
        });
        self
    }
}

impl RpcClient for FakeRpcClient {
    fn call<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
    ) -> BoxFuture<'a, Result<ResponsePayload, RpcError>> {
        self.call_authenticated(caller, unicast_id, call, None)
    }

    fn notify<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
    ) -> BoxFuture<'a, Result<(), RpcError>> {
        self.notify_authenticated(caller, unicast_id, call, None)
    }

    fn call_authenticated<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
        auth: Option<Auth>,
    ) -> BoxFuture<'a, Result<ResponsePayload, RpcError>> {
        Box::pin(async move {
            if !self.latency.is_zero() {
                tokio::time::sleep(self.latency).await;
            }
            if let Some(err) = self.next_fault() {
                return Err(err);
            }
            self.handler
                .handle(caller, unicast_id, call, auth)
                .await
                .map_err(RpcError::Remote)
        })
    }

    fn notify_authenticated<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
        auth: Option<Auth>,
    ) -> BoxFuture<'a, Result<(), RpcError>> {
        Box::pin(async move {
            if !self.latency.is_zero() {
                tokio::time::sleep(self.latency).await;
            }
            if let Some(err) = self.next_fault() {
                return Err(err);
            }
            let _ = self.handler.handle(caller, unicast_id, call, auth).await;
            Ok(())
        })
    }
}

impl FakeRpcClient {
    /// Advances this client's call counter and consults its fault schedule (if any) for the
    /// call just numbered — the one seam `call`/`notify` share.
    fn next_fault(&self) -> Option<RpcError> {
        let fault = self.fault.as_ref()?;
        let call_number = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
        fault.outcome(call_number)
    }
}

#[cfg(test)]
mod tests {
    use ntk_proto::v1::RemoteError;

    use super::*;
    use crate::server::FnHandler;

    fn ok_handler() -> Arc<dyn RpcHandler> {
        Arc::new(FnHandler(
            |_caller: CallerContext, _uid: TypedValue, _call: MethodCall, _auth: Option<Auth>| async move {
                Ok::<_, RemoteError>(ResponsePayload::default())
            },
        ))
    }

    fn call_it(client: &FakeRpcClient) -> Result<ResponsePayload, RpcError> {
        futures::executor::block_on(client.call(
            CallerContext::default(),
            TypedValue::default(),
            MethodCall::default(),
        ))
    }

    #[test]
    fn with_failure_at_fails_only_the_scripted_call() {
        let client =
            FakeRpcClient::new(ok_handler()).with_failure_at(2, || RpcError::ConnectionClosed);
        assert!(call_it(&client).is_ok(), "call 1 must pass through");
        assert!(
            matches!(call_it(&client), Err(RpcError::ConnectionClosed)),
            "call 2 must be the scripted failure"
        );
        assert!(call_it(&client).is_ok(), "call 3 must pass through again");
    }

    #[test]
    fn with_failures_for_fails_exactly_that_many_calls_then_recovers() {
        let client =
            FakeRpcClient::new(ok_handler()).with_failures_for(2, || RpcError::ConnectionClosed);
        assert!(matches!(call_it(&client), Err(RpcError::ConnectionClosed)));
        assert!(matches!(call_it(&client), Err(RpcError::ConnectionClosed)));
        assert!(call_it(&client).is_ok(), "the third call must recover");
        assert!(call_it(&client).is_ok(), "recovery must stay permanent");
    }

    #[test]
    fn fault_schedule_is_shared_across_clones() {
        // `unicast()`-style stub factories often hand out a fresh handle per call, backed by
        // the same underlying client — the schedule must still be scoped to the whole link, not
        // reset by cloning.
        let client =
            FakeRpcClient::new(ok_handler()).with_failure_at(2, || RpcError::ConnectionClosed);
        assert!(call_it(&client.clone()).is_ok());
        assert!(matches!(
            call_it(&client.clone()),
            Err(RpcError::ConnectionClosed)
        ));
        assert!(call_it(&client.clone()).is_ok());
    }

    #[test]
    fn with_failure_still_fails_every_call() {
        let client = FakeRpcClient::new(ok_handler()).with_failure(|| RpcError::ConnectionClosed);
        for _ in 0..5 {
            assert!(matches!(call_it(&client), Err(RpcError::ConnectionClosed)));
        }
    }
}

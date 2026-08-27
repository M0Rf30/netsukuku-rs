//! [`FakeRpcClient`] behavior: direct dispatch, latency injection, and
//! failure injection.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::response_payload::Value;
use ntk_proto::v1::{
    Auth, CallerContext, Empty, MethodCall, RemoteError, ResponsePayload, TypedValue,
};
use ntk_rpc::{FakeRpcClient, FnHandler, RpcClient, RpcError};

fn caller() -> CallerContext {
    CallerContext {
        source_id: Some(TypedValue::new("t", Vec::new())),
        src_nic: Some(TypedValue::new("t", Vec::new())),
    }
}

fn call() -> MethodCall {
    MethodCall {
        call: Some(Call::QspnGotDestroy(Empty::VALUE)),
    }
}

#[tokio::test]
async fn routes_directly_to_the_registered_handler() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let counted = invocations.clone();
    let handler = Arc::new(FnHandler(
        move |_c: CallerContext, _u: TypedValue, _call: MethodCall, _auth: Option<Auth>| {
            counted.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(ResponsePayload {
                    value: Some(Value::Empty(Empty::VALUE)),
                })
            }
        },
    ));
    let client = FakeRpcClient::new(handler);

    let response = client
        .call(caller(), TypedValue::new("t", Vec::new()), call())
        .await
        .expect("fake call succeeds");
    assert_eq!(response.value, Some(Value::Empty(Empty::VALUE)));
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn notify_invokes_the_handler_but_discards_its_outcome() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let counted = invocations.clone();
    let handler = Arc::new(FnHandler(
        move |_c: CallerContext, _u: TypedValue, _call: MethodCall, _auth: Option<Auth>| {
            counted.fetch_add(1, Ordering::SeqCst);
            async move {
                Err(RemoteError {
                    domain: 0,
                    message: "ignored by notify".to_owned(),
                })
            }
        },
    ));
    let client = FakeRpcClient::new(handler);

    client
        .notify(caller(), TypedValue::new("t", Vec::new()), call())
        .await
        .expect("notify never surfaces the handler's error");
    assert_eq!(invocations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn injects_configured_latency_before_dispatching() {
    let handler = Arc::new(FnHandler(
        |_c: CallerContext, _u: TypedValue, _call: MethodCall, _auth: Option<Auth>| async move {
            Ok(ResponsePayload {
                value: Some(Value::Empty(Empty::VALUE)),
            })
        },
    ));
    let client = FakeRpcClient::new(handler).with_latency(Duration::from_millis(40));

    let start = Instant::now();
    client
        .call(caller(), TypedValue::new("t", Vec::new()), call())
        .await
        .expect("call succeeds after the delay");
    assert!(start.elapsed() >= Duration::from_millis(40));
}

#[tokio::test]
async fn injects_a_failure_without_ever_reaching_the_handler() {
    let invocations = Arc::new(AtomicUsize::new(0));
    let counted = invocations.clone();
    let handler = Arc::new(FnHandler(
        move |_c: CallerContext, _u: TypedValue, _call: MethodCall, _auth: Option<Auth>| {
            counted.fetch_add(1, Ordering::SeqCst);
            async move {
                Ok(ResponsePayload {
                    value: Some(Value::Empty(Empty::VALUE)),
                })
            }
        },
    ));
    let client = FakeRpcClient::new(handler).with_failure(|| RpcError::Timeout);

    let error = client
        .call(caller(), TypedValue::new("t", Vec::new()), call())
        .await
        .expect_err("injected failure must surface");
    assert!(matches!(error, RpcError::Timeout));
    assert_eq!(
        invocations.load(Ordering::SeqCst),
        0,
        "handler must not run when a failure is injected"
    );
}

//! Real loopback TCP coverage: request/response round trip and call
//! timeout behavior.

use std::sync::Arc;
use std::time::Duration;

use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::response_payload::Value;
use ntk_proto::v1::{Auth, CallerContext, Empty, MethodCall, ResponsePayload, TypedValue};
use ntk_rpc::{FnHandler, RpcClient, RpcError, TcpRpcClient, TcpServer};
use tokio_util::sync::CancellationToken;

fn caller() -> CallerContext {
    CallerContext {
        source_id: Some(TypedValue::new("t", Vec::new())),
        src_nic: Some(TypedValue::new("t", Vec::new())),
    }
}

#[tokio::test]
async fn tcp_request_response_round_trip() {
    let server = TcpServer::bind("127.0.0.1:0".parse().unwrap(), 1 << 20)
        .await
        .expect("bind server");
    let addr = server.local_addr().expect("server local_addr");
    let cancel = CancellationToken::new();

    let handler = Arc::new(FnHandler(
        |_caller: CallerContext, _unicast_id: TypedValue, call: MethodCall, _auth: Option<Auth>| async move {
            let value = match call.call {
                Some(Call::NeighborhoodCanYouExport(requested)) => Value::Boolean(!requested),
                _ => Value::Empty(Empty::VALUE),
            };
            Ok(ResponsePayload { value: Some(value) })
        },
    ));
    let server_task = tokio::spawn(server.serve(handler, cancel.clone()));

    let client = TcpRpcClient::connect(addr, 1 << 20, Duration::from_secs(5))
        .await
        .expect("connect");
    let response = client
        .call(
            caller(),
            TypedValue::new("t", Vec::new()),
            MethodCall {
                call: Some(Call::NeighborhoodCanYouExport(true)),
            },
        )
        .await
        .expect("call succeeds");
    assert_eq!(response.value, Some(Value::Boolean(false)));

    // A second, concurrent call on the same connection proves multiplexing
    // by `correlation_id` actually works, not just a single request/reply.
    let response2 = client
        .call(
            caller(),
            TypedValue::new("t", Vec::new()),
            MethodCall {
                call: Some(Call::NeighborhoodCanYouExport(false)),
            },
        )
        .await
        .expect("second call succeeds");
    assert_eq!(response2.value, Some(Value::Boolean(true)));

    cancel.cancel();
    server_task.await.expect("server task joins");
}

#[tokio::test]
async fn notify_gets_no_response_and_completes_locally() {
    let server = TcpServer::bind("127.0.0.1:0".parse().unwrap(), 1 << 20)
        .await
        .expect("bind server");
    let addr = server.local_addr().expect("server local_addr");
    let cancel = CancellationToken::new();
    let handler = Arc::new(FnHandler(
        |_c: CallerContext, _u: TypedValue, _call: MethodCall, _auth: Option<Auth>| async move {
            Ok(ResponsePayload {
                value: Some(Value::Empty(Empty::VALUE)),
            })
        },
    ));
    let server_task = tokio::spawn(server.serve(handler, cancel.clone()));

    let client = TcpRpcClient::connect(addr, 1 << 20, Duration::from_secs(5))
        .await
        .expect("connect");
    client
        .notify(
            caller(),
            TypedValue::new("t", Vec::new()),
            MethodCall {
                call: Some(Call::QspnGotDestroy(Empty::VALUE)),
            },
        )
        .await
        .expect("notify completes locally without waiting for a reply");

    cancel.cancel();
    server_task.await.expect("server task joins");
}

#[tokio::test]
async fn call_times_out_when_the_handler_is_slower_than_the_deadline() {
    let server = TcpServer::bind("127.0.0.1:0".parse().unwrap(), 1 << 20)
        .await
        .expect("bind server");
    let addr = server.local_addr().expect("server local_addr");
    let cancel = CancellationToken::new();
    let handler = Arc::new(FnHandler(
        |_c: CallerContext, _u: TypedValue, _call: MethodCall, _auth: Option<Auth>| async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok(ResponsePayload {
                value: Some(Value::Empty(Empty::VALUE)),
            })
        },
    ));
    let server_task = tokio::spawn(server.serve(handler, cancel.clone()));

    let client = TcpRpcClient::connect(addr, 1 << 20, Duration::from_millis(50))
        .await
        .expect("connect");
    let error = client
        .call(
            caller(),
            TypedValue::new("t", Vec::new()),
            MethodCall {
                call: Some(Call::QspnGotDestroy(Empty::VALUE)),
            },
        )
        .await
        .expect_err("a 300ms handler must not answer within a 50ms deadline");
    assert!(
        matches!(error, RpcError::Timeout),
        "expected Timeout, got {error:?}"
    );

    cancel.cancel();
    server_task.await.expect("server task joins");
}

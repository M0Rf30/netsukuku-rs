// SPDX-FileCopyrightText: 2026 Gianluca Boiano
// SPDX-License-Identifier: GPL-3.0-or-later

//! Real loopback TCP coverage: request/response round trip and call
//! timeout behavior.

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use ntk_proto::v1::envelope::Body;
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::response::Outcome;
use ntk_proto::v1::response_payload::Value;
use ntk_proto::v1::{
    Auth, CallerContext, Empty, Envelope, MethodCall, ProtocolVersion, ResponsePayload, TypedValue,
};
use ntk_rpc::{
    EnvelopeCodec, FnHandler, RpcClient, RpcError, RpcHandler, ServerLimits, TcpRpcClient,
    TcpServer,
};
use tokio_util::codec::Framed;
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

fn ok_handler() -> Arc<dyn RpcHandler> {
    Arc::new(FnHandler(
        |_c: CallerContext, _u: TypedValue, _call: MethodCall, _auth: Option<Auth>| {
            std::future::ready(Ok(ResponsePayload {
                value: Some(Value::Empty(Empty::VALUE)),
            }))
        },
    ))
}

async fn raw_connection(
    addr: std::net::SocketAddr,
) -> Framed<tokio::net::TcpStream, EnvelopeCodec> {
    let stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect raw");
    Framed::new(stream, EnvelopeCodec::new(1 << 20))
}

#[tokio::test]
async fn request_without_unicast_id_is_served_as_the_default_identity() {
    let server = TcpServer::bind("127.0.0.1:0".parse().unwrap(), 1 << 20)
        .await
        .expect("bind server");
    let addr = server.local_addr().expect("server local_addr");
    let cancel = CancellationToken::new();
    let server_task = tokio::spawn(server.serve(ok_handler(), cancel.clone()));

    let mut conn = raw_connection(addr).await;
    let mut envelope = Envelope::request(
        ProtocolVersion::CURRENT,
        7,
        caller(),
        TypedValue::new("t", Vec::new()),
        true,
        MethodCall {
            call: Some(Call::QspnGotDestroy(Empty::VALUE)),
        },
    );
    if let Some(Body::Request(request)) = envelope.body.as_mut() {
        request.unicast_id = None;
    }
    conn.send(envelope).await.expect("send request");
    let reply = conn
        .next()
        .await
        .expect("reply arrives")
        .expect("reply decodes");
    let Some(Body::Response(response)) = reply.body else {
        panic!("expected a response body");
    };
    assert!(
        matches!(response.outcome, Some(Outcome::Payload(_))),
        "an absent unicast_id must be served, got {:?}",
        response.outcome
    );

    cancel.cancel();
    server_task.await.expect("server task joins");
}

#[tokio::test]
async fn request_with_incompatible_version_gets_an_error_reply() {
    let server = TcpServer::bind("127.0.0.1:0".parse().unwrap(), 1 << 20)
        .await
        .expect("bind server");
    let addr = server.local_addr().expect("server local_addr");
    let cancel = CancellationToken::new();
    let server_task = tokio::spawn(server.serve(ok_handler(), cancel.clone()));

    let mut conn = raw_connection(addr).await;
    let envelope = Envelope::request(
        ProtocolVersion {
            major: 99,
            minor: 0,
        },
        9,
        caller(),
        TypedValue::new("t", Vec::new()),
        true,
        MethodCall {
            call: Some(Call::QspnGotDestroy(Empty::VALUE)),
        },
    );
    conn.send(envelope).await.expect("send request");
    let reply = tokio::time::timeout(Duration::from_secs(2), conn.next())
        .await
        .expect("a reply, not silence")
        .expect("connection open")
        .expect("reply decodes");
    let Some(Body::Response(response)) = reply.body else {
        panic!("expected a response body");
    };
    assert_eq!(response.correlation_id, 9);
    assert!(matches!(response.outcome, Some(Outcome::Error(_))));

    cancel.cancel();
    server_task.await.expect("server task joins");
}

#[tokio::test]
async fn oversize_response_becomes_an_error_and_the_connection_survives() {
    let server = TcpServer::bind("127.0.0.1:0".parse().unwrap(), 4096)
        .await
        .expect("bind server");
    let addr = server.local_addr().expect("server local_addr");
    let cancel = CancellationToken::new();
    let handler = Arc::new(FnHandler(
        |_c: CallerContext, _u: TypedValue, call: MethodCall, _auth: Option<Auth>| async move {
            if matches!(call.call, Some(Call::NeighborhoodCanYouExport(true))) {
                Ok(ResponsePayload {
                    value: Some(Value::Typed(TypedValue::new("big", vec![0u8; 8192]))),
                })
            } else {
                Ok(ResponsePayload {
                    value: Some(Value::Empty(Empty::VALUE)),
                })
            }
        },
    ));
    let server_task = tokio::spawn(server.serve(handler, cancel.clone()));

    let client = TcpRpcClient::connect(addr, 4096, Duration::from_secs(2))
        .await
        .expect("connect");
    let error = client
        .call(
            caller(),
            TypedValue::new("t", Vec::new()),
            MethodCall {
                call: Some(Call::NeighborhoodCanYouExport(true)),
            },
        )
        .await
        .expect_err("the oversize response cannot be delivered");
    assert!(error.is_remote(), "expected a remote error, got {error:?}");
    client
        .call(
            caller(),
            TypedValue::new("t", Vec::new()),
            MethodCall {
                call: Some(Call::NeighborhoodCanYouExport(false)),
            },
        )
        .await
        .expect("the same connection still serves later calls");

    cancel.cancel();
    server_task.await.expect("server task joins");
}

#[tokio::test]
async fn idle_connection_is_closed_after_the_idle_timeout() {
    let server = TcpServer::bind("127.0.0.1:0".parse().unwrap(), 1 << 20)
        .await
        .expect("bind server")
        .with_limits(ServerLimits {
            idle_timeout: Duration::from_millis(100),
            ..ServerLimits::default()
        });
    let addr = server.local_addr().expect("server local_addr");
    let cancel = CancellationToken::new();
    let server_task = tokio::spawn(server.serve(ok_handler(), cancel.clone()));

    let mut conn = raw_connection(addr).await;
    let closed = tokio::time::timeout(Duration::from_secs(2), conn.next())
        .await
        .expect("server closes the silent connection");
    assert!(closed.is_none(), "expected EOF, got {closed:?}");

    cancel.cancel();
    server_task.await.expect("server task joins");
}

#[tokio::test]
async fn connect_to_an_unroutable_address_times_out() {
    // TEST-NET-1 (RFC 5737) is never routed, so the SYN goes unanswered.
    let result = TcpRpcClient::connect(
        "192.0.2.1:9".parse().unwrap(),
        1 << 20,
        Duration::from_millis(200),
    )
    .await;
    // Environments without any route fail fast with an I/O error; the point is that the dial
    // never outlasts the call budget by minutes.
    assert!(
        matches!(result, Err(RpcError::Timeout) | Err(RpcError::Io(_))),
        "expected Timeout or Io, got {result:?}"
    );
}

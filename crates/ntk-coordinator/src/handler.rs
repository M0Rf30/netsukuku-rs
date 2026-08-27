//! [`CoordinatorRpcHandler`]: the inbound [`ntk_rpc::RpcHandler`] dispatching the 5
//! `MethodCall::coordinator_execute_*` arms (`ntk-proto/proto/ntk.proto`) to a [`Handle`].
//!
//! Each arm's real work (propagation fanout, then the local Hooking callback) can itself cascade
//! into further outbound calls, so it is spawned rather than awaited inline — matching
//! `ntk_peerservices::PeersRpcHandler`'s own handling of `forward_peer_message`.

use std::fmt;

use futures::future::BoxFuture;
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::{CallerContext, Empty, MethodCall, RemoteError, ResponsePayload, TypedValue};
use ntk_rpc::RpcHandler;

use crate::actor::Handle;
use crate::wire::unpack_propagation_args;

fn malformed(message: impl Into<String>) -> RemoteError {
    RemoteError {
        domain: ntk_proto::v1::ErrorDomain::Deserialize.into(),
        message: message.into(),
    }
}

fn empty_ok() -> ResponsePayload {
    ResponsePayload {
        value: Some(ntk_proto::v1::response_payload::Value::Empty(Empty::VALUE)),
    }
}

/// Dispatches the Coordinator propagation method surface onto a [`Handle`]. One instance,
/// shared via `Arc`, serves every connection an `ntk_rpc::TcpServer` (or `FakeRpcClient`)
/// accepts — matches `ntk_rpc::RpcHandler`'s own contract.
pub struct CoordinatorRpcHandler {
    handle: Handle,
}

impl fmt::Debug for CoordinatorRpcHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoordinatorRpcHandler")
            .finish_non_exhaustive()
    }
}

impl CoordinatorRpcHandler {
    #[must_use]
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }
}

impl RpcHandler for CoordinatorRpcHandler {
    fn handle<'a>(
        &'a self,
        _caller: CallerContext,
        _unicast_id: TypedValue,
        call: MethodCall,
        // Hop-auth (`ntk-rpc`/`ntk-neighborhood`'s own concern) is orthogonal to
        // `ntk-peerservices`' origin-auth, which covers this crate's DHT-routed surface
        // (`PeerService::exec`) instead of these 5 point-to-point propagation methods —
        // not consulted here.
        _auth: Option<ntk_proto::v1::Auth>,
    ) -> BoxFuture<'a, Result<ResponsePayload, RemoteError>> {
        Box::pin(async move {
            let arm = call
                .call
                .ok_or_else(|| malformed("MethodCall.call unset"))?;
            match arm {
                Call::CoordinatorExecutePrepareMigration(wire_args) => {
                    let args = unpack_propagation_args(&wire_args)
                        .map_err(|e| malformed(e.to_string()))?;
                    let handle = self.handle.clone();
                    tokio::spawn(
                        async move { handle.handle_execute_prepare_migration(args).await },
                    );
                    Ok(empty_ok())
                }
                Call::CoordinatorExecuteFinishMigration(wire_args) => {
                    let args = unpack_propagation_args(&wire_args)
                        .map_err(|e| malformed(e.to_string()))?;
                    let handle = self.handle.clone();
                    tokio::spawn(async move { handle.handle_execute_finish_migration(args).await });
                    Ok(empty_ok())
                }
                Call::CoordinatorExecutePrepareEnter(wire_args) => {
                    let args = unpack_propagation_args(&wire_args)
                        .map_err(|e| malformed(e.to_string()))?;
                    let handle = self.handle.clone();
                    tokio::spawn(async move { handle.handle_execute_prepare_enter(args).await });
                    Ok(empty_ok())
                }
                Call::CoordinatorExecuteFinishEnter(wire_args) => {
                    let args = unpack_propagation_args(&wire_args)
                        .map_err(|e| malformed(e.to_string()))?;
                    let handle = self.handle.clone();
                    tokio::spawn(async move { handle.handle_execute_finish_enter(args).await });
                    Ok(empty_ok())
                }
                Call::CoordinatorExecuteWeHaveSplitted(wire_args) => {
                    let args = unpack_propagation_args(&wire_args)
                        .map_err(|e| malformed(e.to_string()))?;
                    let handle = self.handle.clone();
                    tokio::spawn(async move { handle.handle_execute_we_have_splitted(args).await });
                    Ok(empty_ok())
                }
                _ => Err(malformed("not a Coordinator method")),
            }
        })
    }
}

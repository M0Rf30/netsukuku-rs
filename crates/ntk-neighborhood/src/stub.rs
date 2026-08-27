//! Outbound-call seam (`INeighborhoodStubFactory`,
//! `research/impl/vala/neighborhood/api.vala:50-60`): [`NeighborhoodStubFactory`]
//! plus [`BroadcastRpcClient`], the real broadcast-transport adapter over
//! `ntk_rpc::UdpBroadcaster`, and [`serve_broadcast`], its receive-side
//! counterpart.
//!
//! Every neighborhood broadcast method (`here_i_am`/`request_arc`/
//! `remove_arc`) is void and fire-and-forget on the wire — there is no
//! application-level reply channel over UDP broadcast, only the orthogonal,
//! best-effort `BroadcastAck` (`crates/ntk-proto/proto/ntk.proto`'s
//! `BroadcastRequest.send_ack`/`BroadcastAck`, itself unimplemented here:
//! `research/notes/02-vala-services-daemon.md` §1 already documents it as
//! "no request retransmission" / "best-effort, no delivery guarantee", so
//! skipping it changes no observable protocol guarantee). That shape is
//! exactly [`ntk_rpc::RpcClient::notify`]'s shape, so [`NeighborhoodStubFactory::broadcast`]
//! reuses the existing `RpcClient`/`RpcHandler` seam wholesale instead of
//! inventing a parallel one: [`BroadcastRpcClient::notify`] sends a real UDP
//! broadcast, and in tests a plain [`ntk_rpc::FakeRpcClient`] wired to a
//! peer's [`crate::NeighborhoodRpcHandler`] stands in for the whole
//! broadcast medium with no socket involved.

use std::sync::Arc as StdArc;
use std::sync::atomic::{AtomicU64, Ordering};

use futures::future::BoxFuture;
use ntk_proto::v1::{CallerContext, MethodCall, ResponsePayload, TypedValue};
use ntk_rpc::{RpcClient, RpcError, RpcHandler, UdpBroadcaster};
use tokio_util::sync::CancellationToken;

use crate::arc::Arc as NeighborArc;

/// Outbound-call seam mirroring `INeighborhoodStubFactory`
/// (`api.vala:50-60`): where to send radar broadcasts on a given NIC
/// (`get_broadcast_for_radar`) and where to send unicast `can_you_export`/
/// `nop` calls to a given arc's peer (`get_unicast`).
pub trait NeighborhoodStubFactory: Send + Sync + std::fmt::Debug {
    /// The outbound channel for broadcast radar calls on `dev`. Every
    /// neighborhood broadcast method is void, so callers only ever use
    /// [`RpcClient::notify`] on the result.
    fn broadcast(&self, dev: &str) -> StdArc<dyn RpcClient>;

    /// The outbound unicast channel to `arc`'s peer, used for the
    /// `can_you_export` call and periodic `nop` liveness probes — both
    /// [`RpcClient::call`] (`Manager::handle_nop`'s doc explains why `nop`
    /// needs a reply, unlike every broadcast method above).
    fn unicast(&self, arc: &NeighborArc) -> StdArc<dyn RpcClient>;
}

/// Adapts an [`UdpBroadcaster`] to [`RpcClient`] so
/// [`NeighborhoodStubFactory::broadcast`]'s real implementation can reuse
/// the same call-shape tests exercise via `FakeRpcClient`. [`Self::call`]
/// is unsupported (broadcast never carries a reply); every neighborhood
/// broadcast method is void, so this is never exercised in practice.
#[derive(Debug)]
pub struct BroadcastRpcClient {
    broadcaster: StdArc<UdpBroadcaster>,
    next_packet_id: AtomicU64,
}

impl BroadcastRpcClient {
    #[must_use]
    pub fn new(broadcaster: StdArc<UdpBroadcaster>) -> Self {
        Self {
            broadcaster,
            next_packet_id: AtomicU64::new(0),
        }
    }
}

impl RpcClient for BroadcastRpcClient {
    fn call<'a>(
        &'a self,
        _caller: CallerContext,
        _unicast_id: TypedValue,
        _call: MethodCall,
    ) -> BoxFuture<'a, Result<ResponsePayload, RpcError>> {
        Box::pin(async move {
            Err(RpcError::Malformed(
                "neighborhood broadcast calls are void/fire-and-forget; use notify(), not call()"
                    .to_owned(),
            ))
        })
    }

    fn notify<'a>(
        &'a self,
        caller: CallerContext,
        broadcast_id: TypedValue,
        call: MethodCall,
    ) -> BoxFuture<'a, Result<(), RpcError>> {
        self.notify_authenticated(caller, broadcast_id, call, None)
    }

    fn notify_authenticated<'a>(
        &'a self,
        caller: CallerContext,
        broadcast_id: TypedValue,
        call: MethodCall,
        auth: Option<ntk_proto::v1::Auth>,
    ) -> BoxFuture<'a, Result<(), RpcError>> {
        Box::pin(async move {
            let packet_id = self.next_packet_id.fetch_add(1, Ordering::Relaxed);
            self.broadcaster
                .send_broadcast_request(packet_id, caller, broadcast_id, false, call, auth)
                .await
        })
    }
}

/// Receives broadcast radar traffic on `broadcaster` and dispatches each
/// decoded `BroadcastRequest` to `handler` — the receive-side counterpart
/// of [`BroadcastRpcClient`], mirroring how `ntk_rpc::TcpServer::serve`
/// dispatches unicast connections. `handler` should be a
/// `crate::NeighborhoodRpcHandler` built via
/// `NeighborhoodRpcHandler::for_broadcast` for the same `dev` this
/// broadcaster is bound to. Runs until `cancel` fires.
pub async fn serve_broadcast(
    broadcaster: StdArc<UdpBroadcaster>,
    handler: StdArc<dyn RpcHandler>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            received = broadcaster.recv() => {
                match received {
                    Ok((envelope, from)) => {
                        if let Err(error) = envelope.check_version() {
                            tracing::warn!(%error, %from, "ntk-neighborhood: dropping broadcast with incompatible version");
                            continue;
                        }
                        let Some(request) = envelope.as_broadcast_request() else {
                            continue;
                        };
                        let (Some(caller), Some(call)) = (request.caller.clone(), request.call.clone()) else {
                            continue;
                        };
                        let broadcast_id = request.broadcast_id.clone().unwrap_or_default();
                        let auth = envelope.auth.clone();
                        if let Err(error) = handler.handle(caller, broadcast_id, call, auth).await {
                            tracing::debug!(?error, %from, "ntk-neighborhood: broadcast request rejected");
                        }
                    }
                    Err(error) => tracing::warn!(%error, "ntk-neighborhood: broadcast recv failed"),
                }
            }
        }
    }
}

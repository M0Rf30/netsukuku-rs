//! Inbound RPC dispatch: [`ntk_rpc::RpcHandler`] for the 4 `qspn_*` arms of
//! `ntk_proto::v1::MethodCall` (`qspn_get_full_etp`, `qspn_send_etp`,
//! `qspn_got_prepare_destroy`, `qspn_got_destroy`), plus [`ArcResolver`] —
//! the seam that maps an inbound `CallerContext` to the arc it physically
//! arrived on: upstream's `IQspnArc::i_qspn_comes_from`
//! (`research/impl/vala/qspn/api.vala:118`) inverted, since this crate holds
//! no arc objects. Resolution is delegated to whichever crate (Neighborhood,
//! out of this crate's scope) owns the physical/NIC mapping.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use ntk_proto::v1::{
    CallerContext, Empty, ErrorDomain, MethodCall, RemoteError, ResponsePayload, TypedValue,
    method_call, response_payload,
};
use ntk_rpc::RpcHandler;
use tokio::time::Instant;

use crate::arc::ArcId;
use crate::error::QspnError;
use crate::manager::QspnHandle;
use crate::wire::{decode_etp_message, decode_naddr, encode_etp_message};

/// Resolves an inbound [`CallerContext`] to the arc it arrived on. Mirrors
/// `IQspnArc::i_qspn_comes_from` (`api.vala:118`), but inverted: instead of
/// asking every known arc "did you receive this?", the composition layer
/// (which owns the physical/NIC mapping) answers directly.
pub trait ArcResolver: Send + Sync {
    fn resolve(&self, caller: &CallerContext) -> Option<ArcId>;
}

/// Polls `resolver` against `handle`'s live arc set until `caller` resolves
/// to a *currently known* arc, or `timeout` elapses — the Rust equivalent of
/// the polling loop upstream's RPC skeletons run against `my_arcs`
/// (`research/impl/vala/qspn/qspn.vala:2551-2565,2614-2628,2772-2786`), at
/// `poll_interval` (upstream: a flat 10ms, `qspn.vala:2564`,
/// [`crate::QspnConfig::caller_arc_poll_interval`]).
async fn resolve_arc(
    handle: &QspnHandle,
    resolver: &dyn ArcResolver,
    caller: &CallerContext,
    timeout: Duration,
    poll_interval: Duration,
) -> Option<ArcId> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(arc) = resolver.resolve(caller)
            && let Ok(arcs) = handle.current_arcs().await
            && arcs.contains(&arc)
        {
            return Some(arc);
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(poll_interval).await;
    }
}

fn remote_error(domain: ErrorDomain, message: impl Into<String>) -> RemoteError {
    RemoteError {
        domain: domain as i32,
        message: message.into(),
    }
}

pub(crate) fn qspn_error_to_remote(e: QspnError) -> RemoteError {
    match e {
        QspnError::NotAnArc => remote_error(ErrorDomain::QspnNotAccepted, e.to_string()),
        QspnError::BootstrapInProgress => {
            remote_error(ErrorDomain::QspnBootstrapInProgress, e.to_string())
        }
        other => remote_error(ErrorDomain::Deserialize, other.to_string()),
    }
}

fn empty_ok() -> Result<ResponsePayload, RemoteError> {
    Ok(ResponsePayload {
        value: Some(response_payload::Value::Empty(Empty::VALUE)),
    })
}

/// [`RpcHandler`] for the 4 `qspn_*` [`MethodCall`] arms, wired to a single
/// identity's [`QspnHandle`]. Any other `MethodCall` arm is a routing bug in
/// whoever composed the dispatcher — reported as [`ErrorDomain::Deserialize`]
/// (notes/02 §1: an unrecognized/misrouted call is always `DeserializeError`
/// on the wire), never silently ignored.
pub struct QspnRpcHandler {
    handle: QspnHandle,
    resolver: Arc<dyn ArcResolver>,
    arc_timeout: Duration,
    poll_interval: Duration,
}

impl QspnRpcHandler {
    #[must_use]
    pub fn new(
        handle: QspnHandle,
        resolver: Arc<dyn ArcResolver>,
        arc_timeout: Duration,
        poll_interval: Duration,
    ) -> Self {
        Self {
            handle,
            resolver,
            arc_timeout,
            poll_interval,
        }
    }

    async fn resolve_or_reject(&self, caller: &CallerContext) -> Result<ArcId, RemoteError> {
        resolve_arc(
            &self.handle,
            self.resolver.as_ref(),
            caller,
            self.arc_timeout,
            self.poll_interval,
        )
        .await
        .ok_or_else(|| {
            remote_error(
                ErrorDomain::QspnNotAccepted,
                "caller did not resolve to a known arc",
            )
        })
    }
}

impl std::fmt::Debug for QspnRpcHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QspnRpcHandler").finish_non_exhaustive()
    }
}

impl RpcHandler for QspnRpcHandler {
    fn handle<'a>(
        &'a self,
        caller: CallerContext,
        _unicast_id: TypedValue,
        call: MethodCall,
        _auth: Option<ntk_proto::v1::Auth>,
    ) -> BoxFuture<'a, Result<ResponsePayload, RemoteError>> {
        Box::pin(async move {
            match call.call {
                Some(method_call::Call::QspnGetFullEtp(tv)) => {
                    let arc = self.resolve_or_reject(&caller).await?;
                    let requesting = decode_naddr(&tv).map_err(qspn_error_to_remote)?;
                    let etp = self
                        .handle
                        .handle_get_full_etp(arc, requesting)
                        .await
                        .map_err(qspn_error_to_remote)?;
                    Ok(ResponsePayload {
                        value: Some(response_payload::Value::Typed(encode_etp_message(&etp))),
                    })
                }
                Some(method_call::Call::QspnSendEtp(args)) => {
                    let arc = self.resolve_or_reject(&caller).await?;
                    let etp_tv = args.etp.ok_or_else(|| {
                        remote_error(ErrorDomain::Deserialize, "QspnSendEtpArgs.etp missing")
                    })?;
                    let etp = decode_etp_message(&etp_tv).map_err(qspn_error_to_remote)?;
                    self.handle
                        .handle_send_etp(arc, etp, args.is_full)
                        .await
                        .map_err(qspn_error_to_remote)?;
                    empty_ok()
                }
                Some(method_call::Call::QspnGotPrepareDestroy(_)) => {
                    self.handle
                        .handle_got_prepare_destroy()
                        .await
                        .map_err(qspn_error_to_remote)?;
                    empty_ok()
                }
                Some(method_call::Call::QspnGotDestroy(_)) => {
                    let arc = self.resolve_or_reject(&caller).await?;
                    self.handle
                        .handle_got_destroy(arc)
                        .await
                        .map_err(qspn_error_to_remote)?;
                    empty_ok()
                }
                _ => Err(remote_error(ErrorDomain::Deserialize, "not a qspn method")),
            }
        })
    }
}

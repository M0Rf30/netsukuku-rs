//! Inbound dispatch: adapts a running identity-manager [`Handle`] into an
//! [`ntk_rpc::RpcHandler`] serving the three `IdentityManager` arms of
//! [`ntk_proto::v1::MethodCall`] (`match_duplication`, `get_peer_main_id`,
//! `notify_identity_arc_removed` — `interfaces.rpcidl:9-11`).

use std::sync::Arc;

use futures::future::BoxFuture;
use ntk_proto::v1::{
    Auth, CallerContext, ErrorDomain, IdentityMatchDuplicationArgs,
    IdentityNotifyIdentityArcRemovedArgs, MethodCall, RemoteError, ResponsePayload, TypedValue,
    method_call, response_payload,
};
use ntk_rpc::RpcHandler;

use crate::actor::Handle;
use crate::identity::IdentityId;
use crate::stub::IdentityStubFactory;
use crate::wire::{
    duplication_data_to_typed_value, identity_id_from_typed_value, identity_id_to_typed_value,
};

/// Adapts a running identity-manager actor into an [`RpcHandler`].
#[derive(Clone)]
pub struct IdentityRpcHandler {
    handle: Handle,
    stub_factory: Arc<dyn IdentityStubFactory>,
}

impl std::fmt::Debug for IdentityRpcHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityRpcHandler").finish_non_exhaustive()
    }
}

impl IdentityRpcHandler {
    #[must_use]
    pub fn new(handle: Handle, stub_factory: Arc<dyn IdentityStubFactory>) -> Self {
        Self {
            handle,
            stub_factory,
        }
    }
}

impl RpcHandler for IdentityRpcHandler {
    fn handle<'a>(
        &'a self,
        caller: CallerContext,
        _unicast_id: TypedValue,
        call: MethodCall,
        _auth: Option<Auth>,
    ) -> BoxFuture<'a, Result<ResponsePayload, RemoteError>> {
        Box::pin(async move {
            match call.call {
                Some(method_call::Call::IdentityGetPeerMainId(_)) => {
                    // `get_peer_main_id` ignores its caller entirely
                    // upstream (`identities.vala:820-825`); the current
                    // main id is a live snapshot read, no actor round trip.
                    let main_id = self.handle.snapshot().main_id;
                    Ok(present(identity_id_to_typed_value(main_id)))
                }
                Some(method_call::Call::IdentityMatchDuplication(args)) => {
                    self.match_duplication(&caller, args).await
                }
                Some(method_call::Call::IdentityNotifyIdentityArcRemoved(args)) => {
                    self.notify_identity_arc_removed(&caller, args).await
                }
                _ => Err(remote_error(
                    ErrorDomain::Deserialize,
                    "not an identity-manager method",
                )),
            }
        })
    }
}

impl IdentityRpcHandler {
    /// `match_duplication` skeleton (`identities.vala:827-876`). Peer-side
    /// readiness wait is bounded (`Handle::lookup_pending_migration`'s doc
    /// explains the busy-wait substitution) rather than the upstream
    /// unbounded `while (!ready) tasklet.ms_wait(50)`.
    async fn match_duplication(
        &self,
        caller: &CallerContext,
        args: IdentityMatchDuplicationArgs,
    ) -> Result<ResponsePayload, RemoteError> {
        let Some(arc) = self.stub_factory.arc_for_caller(caller) else {
            return Err(remote_error(
                ErrorDomain::Deserialize,
                "match_duplication: unresolved caller arc",
            ));
        };
        let my_old_id = decode_id(args.peer_id.as_ref(), "peer_id")?;
        let peer_old_id = decode_id(args.old_id.as_ref(), "old_id")?;
        let peer_new_id = decode_id(args.new_id.as_ref(), "new_id")?;
        let migration_id = crate::migration::MigrationId(args.migration_id);

        let Some(lookup) = self
            .handle
            .lookup_pending_migration(migration_id, my_old_id)
            .await
        else {
            return self
                .unmatched(my_old_id, peer_old_id, peer_new_id, args, arc)
                .await;
        };

        let mut ready = lookup.ready;
        let ready_in_time = matches!(
            tokio::time::timeout_at(lookup.deadline, ready.wait_for(|r| *r)).await,
            Ok(Ok(_))
        );
        if !ready_in_time {
            return self
                .unmatched(my_old_id, peer_old_id, peer_new_id, args, arc)
                .await;
        }

        match self
            .handle
            .fetch_duplication_data(migration_id, my_old_id, arc)
            .await
        {
            Ok(data) => Ok(present(duplication_data_to_typed_value(&data))),
            Err(_) => {
                self.unmatched(my_old_id, peer_old_id, peer_new_id, args, arc)
                    .await
            }
        }
    }

    /// The "peer is not also migrating this identity" path
    /// (`identities.vala:862-875`): answer `null`, asynchronously add the
    /// new identity-arc.
    async fn unmatched(
        &self,
        my_old_id: IdentityId,
        peer_old_id: IdentityId,
        peer_new_id: IdentityId,
        args: IdentityMatchDuplicationArgs,
        arc: crate::arc::ArcId,
    ) -> Result<ResponsePayload, RemoteError> {
        self.handle
            .neighbour_migrated(
                my_old_id,
                peer_old_id,
                peer_new_id,
                args.old_id_new_mac,
                args.old_id_new_linklocal,
                arc,
            )
            .await;
        Ok(absent())
    }

    /// `notify_identity_arc_removed` skeleton (`identities.vala:780-796`).
    async fn notify_identity_arc_removed(
        &self,
        caller: &CallerContext,
        args: IdentityNotifyIdentityArcRemovedArgs,
    ) -> Result<ResponsePayload, RemoteError> {
        let Some(arc) = self.stub_factory.arc_for_caller(caller) else {
            return Err(remote_error(
                ErrorDomain::Deserialize,
                "notify_identity_arc_removed: unresolved caller arc",
            ));
        };
        let my_id = decode_id(args.my_id.as_ref(), "my_id")?;
        let peer_id = decode_id(args.peer_id.as_ref(), "peer_id")?;
        self.handle
            .notify_identity_arc_removed_inbound(my_id, peer_id, arc)
            .await;
        Ok(void())
    }
}

fn decode_id(tv: Option<&TypedValue>, field: &'static str) -> Result<IdentityId, RemoteError> {
    let tv =
        tv.ok_or_else(|| remote_error(ErrorDomain::Deserialize, format!("missing field {field}")))?;
    identity_id_from_typed_value(tv)
        .map_err(|err| remote_error(ErrorDomain::Deserialize, err.to_string()))
}

fn remote_error(domain: ErrorDomain, message: impl Into<String>) -> RemoteError {
    RemoteError {
        domain: domain as i32,
        message: message.into(),
    }
}

fn present(tv: TypedValue) -> ResponsePayload {
    ResponsePayload {
        value: Some(response_payload::Value::Typed(tv)),
    }
}

fn absent() -> ResponsePayload {
    ResponsePayload {
        value: Some(response_payload::Value::Empty(ntk_proto::v1::Empty {})),
    }
}

fn void() -> ResponsePayload {
    absent()
}

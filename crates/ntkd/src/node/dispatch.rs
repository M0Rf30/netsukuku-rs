//! The one inbound dispatcher: routes every [`MethodCall`] arm to the module `RpcHandler` that
//! owns it. Each per-module handler (`NeighborhoodRpcHandler`, `IdentityRpcHandler`, ...)
//! already returns `ErrorDomain::Deserialize` for any call outside its own arms ("a routing bug
//! in whoever composed the dispatcher", per `ntk_qspn::rpc`'s own doc comment) — so this
//! dispatcher's job is exact routing, never fallback/retry across handlers.

use std::sync::Arc;

use futures::future::BoxFuture;
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::{Auth, CallerContext, MethodCall, RemoteError, ResponsePayload, TypedValue};
use ntk_rpc::RpcHandler;
use tokio::sync::RwLock;

/// The four RPC handlers that get torn down and rebuilt together whenever this identity
/// re-addresses itself after a negotiated hooking entry — see `crate::node::lifecycle`'s
/// "Negotiated re-address" module doc section. `neighborhood`/`identity` are stable for the
/// daemon's whole life (same section) and are deliberately not part of this bundle.
#[derive(Debug)]
pub struct IdentityStack {
    pub qspn: ntk_qspn::QspnRpcHandler,
    pub peers: ntk_peerservices::PeersRpcHandler,
    pub coordinator: ntk_coordinator::CoordinatorRpcHandler,
    pub hooking: ntk_hooking::HookingRpcHandler,
}

/// One shared instance per bound listener (TCP or a NIC's UDP broadcast), dispatching to
/// whichever of the six per-module handlers a call's oneof arm names. `identity_stack` is an
/// `RwLock` rather than a plain field so [`Dispatcher::replace_identity_stack`] can swap all
/// four generation-scoped handlers atomically without rebinding the listener that owns this
/// `Dispatcher` — see [`IdentityStack`]'s doc.
pub struct Dispatcher {
    neighborhood: ntk_neighborhood::NeighborhoodRpcHandler,
    identity: ntk_identities::IdentityRpcHandler,
    identity_stack: RwLock<Arc<IdentityStack>>,
}

impl Dispatcher {
    #[must_use]
    pub fn new(
        neighborhood: ntk_neighborhood::NeighborhoodRpcHandler,
        identity: ntk_identities::IdentityRpcHandler,
        stack: IdentityStack,
    ) -> Self {
        Self {
            neighborhood,
            identity,
            identity_stack: RwLock::new(Arc::new(stack)),
        }
    }

    /// Atomically replaces the four generation-scoped RPC handlers. A call already dispatched
    /// against the old stack holds its own `Arc<IdentityStack>` clone and runs to completion
    /// unaffected; every call whose dispatch starts after this returns is routed to `stack`.
    pub async fn replace_identity_stack(&self, stack: IdentityStack) {
        *self.identity_stack.write().await = Arc::new(stack);
    }
}

impl std::fmt::Debug for Dispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dispatcher").finish_non_exhaustive()
    }
}

fn misrouted() -> RemoteError {
    RemoteError {
        domain: ntk_proto::v1::ErrorDomain::Deserialize as i32,
        message: "MethodCall with no oneof arm set".to_owned(),
    }
}

impl RpcHandler for Dispatcher {
    fn handle<'a>(
        &'a self,
        caller: CallerContext,
        unicast_id: TypedValue,
        call: MethodCall,
        auth: Option<Auth>,
    ) -> BoxFuture<'a, Result<ResponsePayload, RemoteError>> {
        Box::pin(async move {
            let Some(arm) = &call.call else {
                return Err(misrouted());
            };
            match arm {
                Call::NeighborhoodHereIAm(_)
                | Call::NeighborhoodRequestArc(_)
                | Call::NeighborhoodCanYouExport(_)
                | Call::NeighborhoodRemoveArc(_)
                | Call::NeighborhoodNop(_) => {
                    self.neighborhood
                        .handle(caller, unicast_id, call, auth)
                        .await
                }

                Call::IdentityMatchDuplication(_)
                | Call::IdentityGetPeerMainId(_)
                | Call::IdentityNotifyIdentityArcRemoved(_) => {
                    self.identity.handle(caller, unicast_id, call, auth).await
                }

                Call::QspnGetFullEtp(_)
                | Call::QspnSendEtp(_)
                | Call::QspnGotPrepareDestroy(_)
                | Call::QspnGotDestroy(_) => {
                    let stack = self.identity_stack.read().await.clone();
                    stack.qspn.handle(caller, unicast_id, call, auth).await
                }

                Call::PeersForwardPeerMessage(_)
                | Call::PeersGetRequest(_)
                | Call::PeersSetResponse(_)
                | Call::PeersSetRefuseMessage(_)
                | Call::PeersSetRedoFromStart(_)
                | Call::PeersSetNextDestination(_)
                | Call::PeersSetFailure(_)
                | Call::PeersSetNonParticipant(_)
                | Call::PeersSetMissingOptionalMaps(_)
                | Call::PeersSetParticipant(_)
                | Call::PeersGiveParticipantMaps(_)
                | Call::PeersAskParticipantMaps(_) => {
                    let stack = self.identity_stack.read().await.clone();
                    stack.peers.handle(caller, unicast_id, call, auth).await
                }

                Call::CoordinatorExecutePrepareMigration(_)
                | Call::CoordinatorExecuteFinishMigration(_)
                | Call::CoordinatorExecutePrepareEnter(_)
                | Call::CoordinatorExecuteFinishEnter(_)
                | Call::CoordinatorExecuteWeHaveSplitted(_) => {
                    let stack = self.identity_stack.read().await.clone();
                    stack
                        .coordinator
                        .handle(caller, unicast_id, call, auth)
                        .await
                }

                Call::HookingRetrieveNetworkData(_)
                | Call::HookingSearchMigrationPath(_)
                | Call::HookingRouteSearchRequest(_)
                | Call::HookingRouteSearchError(_)
                | Call::HookingRouteSearchResponse(_)
                | Call::HookingRouteExploreRequest(_)
                | Call::HookingRouteExploreResponse(_)
                | Call::HookingRouteDeleteReserveRequest(_)
                | Call::HookingRouteMigRequest(_)
                | Call::HookingRouteMigResponse(_) => {
                    let stack = self.identity_stack.read().await.clone();
                    stack.hooking.handle(caller, unicast_id, call, auth).await
                }
            }
        })
    }
}

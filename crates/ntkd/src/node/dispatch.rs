//! The one inbound dispatcher: routes every [`MethodCall`] arm to the module `RpcHandler` that
//! owns it. Each per-module handler (`NeighborhoodRpcHandler`, `IdentityRpcHandler`, ...)
//! already returns `ErrorDomain::Deserialize` for any call outside its own arms ("a routing bug
//! in whoever composed the dispatcher", per `ntk_qspn::rpc`'s own doc comment) — so this
//! dispatcher's job is exact routing, never fallback/retry across handlers.
//!
//! Which qspn/peers/coordinator/hooking generation a call reaches is itself now a routing
//! decision, resolved from `Request.unicast_id` by `Dispatcher::resolve_stack` (private) rather than
//! hardcoded to a single [`IdentityStack`] — see that method's doc for the
//! `ntk_proto::domain::UnicastId` cases and [`Dispatcher::register_identity`] for how a second
//! identity (e.g. mig-01's connectivity fork, `research/impl/vala/qspn/qspn.vala:2226-2505`)
//! joins the routing table. `neighborhood`/`identity` stay exactly as before: node-level,
//! unconditional, never consulting `unicast_id` at all.
//!
//! # Before mig-01 lands: `secondary` is keyed on the wrong id
//!
//! [`ntk_neighborhood::NodeId`] identifies the *node*, not an identity: it is
//! `NeighborhoodConfig::my_id`, fixed for the process's whole life. A connectivity fork and the
//! successor it bridges for would therefore share it and collide in `secondary`. Upstream keys
//! the equivalent lookup on an identities-level id — `IdentityAwareUnicastID(NodeID)` carries the
//! identity's own id, and `get_identity_skeleton` matches it against each entry of
//! `local_identities` (`research/impl/vala/ntkd/rpc/skeleton_factory.vala:284-291`).
//!
//! This is not a live defect: `secondary` is always empty today, and the main identity resolves
//! via `main_id` before `secondary` is consulted, so every path in use is correct and tested. It
//! simply does not extend. Whoever builds the fork must first re-key on
//! [`ntk_identities::IdentityId`] — which `ntk_identities::Handle::migrate` already returns for
//! the successor, and which identity arcs already carry for peers (`my_peer_old_id`/
//! `my_peer_new_id`), so a peer can name it. The wire needs no change: `identity_aware`'s payload
//! is an opaque `TypedValue`.

use std::collections::HashMap;
use std::sync::Arc;

use futures::future::BoxFuture;
use ntk_proto::domain::UnicastId;
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::{
    Auth, CallerContext, ErrorDomain, MethodCall, RemoteError, ResponsePayload, TypedValue,
};
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
/// whichever of the six per-module handlers a call's oneof arm names, and — for the four
/// generation-scoped ones — to whichever locally-hosted identity `Request.unicast_id` names
/// (see `Self::resolve_stack` (private)). `identity_stack` stays an `RwLock` exactly as before so
/// [`Dispatcher::replace_identity_stack`] can still swap the *main* identity's four handlers
/// atomically without rebinding the listener that owns this `Dispatcher` — see
/// [`IdentityStack`]'s doc; `secondary` is the same shape for every other identity this node
/// hosts.
pub struct Dispatcher {
    neighborhood: ntk_neighborhood::NeighborhoodRpcHandler,
    identity: ntk_identities::IdentityRpcHandler,
    /// The identity registry, consulted for *which* [`ntk_identities::IdentityId`] is currently
    /// main. Read live rather than cached, because unlike this node's
    /// [`ntk_neighborhood::NodeId`] the main identity id genuinely changes: every `migrate`
    /// retires one identity and promotes its successor, so a copy taken at construction would
    /// name a retired identity from the first migration onward. `Handle::main_id` is a
    /// `watch`-snapshot read, so this costs no round trip.
    identities_handle: ntk_identities::Handle,
    identity_stack: RwLock<Arc<IdentityStack>>,
    /// Every locally-hosted identity that is *not* the current main (e.g. mig-01's connectivity
    /// fork, `research/impl/qspn/qspn.vala:2226-2505`), keyed by its own
    /// [`ntk_identities::IdentityId`] — the id that distinguishes two identities inside one
    /// process, which this node's Neighborhood id cannot (see
    /// `crate::node::registry::encode_identity_id`'s doc). Empty until
    /// [`Self::register_identity`] is called.
    secondary: RwLock<HashMap<ntk_identities::IdentityId, Arc<IdentityStack>>>,
}

impl Dispatcher {
    #[must_use]
    pub fn new(
        neighborhood: ntk_neighborhood::NeighborhoodRpcHandler,
        identity: ntk_identities::IdentityRpcHandler,
        identities_handle: ntk_identities::Handle,
        stack: IdentityStack,
    ) -> Self {
        Self {
            neighborhood,
            identity,
            identities_handle,
            identity_stack: RwLock::new(Arc::new(stack)),
            secondary: RwLock::new(HashMap::new()),
        }
    }

    /// Atomically replaces the main identity's four generation-scoped RPC handlers. A call
    /// already dispatched against the old stack holds its own `Arc<IdentityStack>` clone and
    /// runs to completion unaffected; every call whose dispatch starts after this returns is
    /// routed to `stack`. Unaffected by `secondary`: `migrate`'s re-address flow only ever
    /// touches the main identity, never a second one.
    pub async fn replace_identity_stack(&self, stack: IdentityStack) {
        *self.identity_stack.write().await = Arc::new(stack);
    }

    /// Registers `id` as an additional locally-hosted identity: from now on, an inbound call
    /// whose `unicast_id` is `UnicastId::IdentityAware` naming `id` reaches `stack` instead of
    /// being rejected as unknown (see `Self::resolve_stack` (private)). Replaces any stack already
    /// registered under `id`, same swap-in-place semantics as
    /// [`Self::replace_identity_stack`].
    ///
    /// `id` should not be the current main identity — `Self::resolve_stack` (private) checks that first,
    /// so registering it here would simply never be consulted.
    pub async fn register_identity(&self, id: ntk_identities::IdentityId, stack: IdentityStack) {
        self.secondary.write().await.insert(id, Arc::new(stack));
    }

    /// Unregisters `id`, e.g. once mig-01's connectivity fork has finished
    /// `check_connectivity` -> `prepare_destroy` -> `destroy` and is no longer reachable. A call
    /// already dispatched to it runs to completion unaffected (holds its own `Arc` clone, as
    /// [`Self::replace_identity_stack`]'s doc describes); every call whose dispatch starts after
    /// this returns is rejected as unknown. A no-op if `id` was never registered.
    pub async fn unregister_identity(&self, id: ntk_identities::IdentityId) {
        self.secondary.write().await.remove(&id);
    }

    /// Resolves an inbound call's `unicast_id` to the [`IdentityStack`] that should handle it —
    /// used by every qspn/peers/coordinator/hooking arm in [`RpcHandler::handle`] below. Never
    /// consulted for the `neighborhood`/`identity` arms, which stay node-level and dispatch
    /// unconditionally (this module's own doc).
    ///
    /// - [`UnicastId::MainIdentity`], or an absent/empty `unicast_id` (an unmodified v0.1.5
    ///   peer, or any peer that has never heard of `UnicastId`) -> the main identity's stack —
    ///   the compatibility path, behaviour-identical to before this method existed.
    /// - [`UnicastId::IdentityAware`] naming whichever identity is currently main -> also the
    ///   main identity's stack: a peer addressing this node's identity by its actual id.
    /// - [`UnicastId::IdentityAware`] naming anything else -> that id's `secondary` entry, or a
    ///   [`RemoteError`] if this node hosts no such identity. Deliberately never falls back to
    ///   main here: that would route a second identity's traffic into the wrong generation's
    ///   maps — exactly the class of bug this work exists to prevent.
    /// - [`UnicastId::WholeNode`] -> a [`RemoteError`]: that variant addresses the node-level
    ///   skeleton, which never calls this method in the first place.
    ///
    /// # Errors
    /// See above; every error is [`ErrorDomain::Deserialize`], matching [`misrouted`]'s own
    /// reasoning for a malformed/misaddressed call.
    async fn resolve_stack(
        &self,
        unicast_id: &TypedValue,
    ) -> Result<Arc<IdentityStack>, RemoteError> {
        match select_identity(unicast_id, self.identities_handle.main_id())? {
            Selection::Main => Ok(self.identity_stack.read().await.clone()),
            Selection::Secondary(id) => {
                self.secondary
                    .read()
                    .await
                    .get(&id)
                    .cloned()
                    .ok_or_else(|| {
                        protocol_error(format!(
                            "Request.unicast_id: no locally-hosted identity with id {}",
                            id.into_raw()
                        ))
                    })
            }
        }
    }
}

/// Which locally-hosted identity `unicast_id` selects — the pure decision
/// [`Dispatcher::resolve_stack`] executes, factored out so it is testable without needing a real
/// [`IdentityStack`] (see this module's `tests`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Selection {
    Main,
    Secondary(ntk_identities::IdentityId),
}

/// - [`UnicastId::MainIdentity`], or an absent/empty `unicast_id` (an unmodified v0.1.5 peer, or
///   any peer that has never heard of `UnicastId`) -> [`Selection::Main`] — the compatibility
///   path, behaviour-identical to before `unicast_id` was ever consulted.
/// - [`UnicastId::IdentityAware`] naming the current main identity -> also [`Selection::Main`]: a
///   peer addressing this node's identity by its actual id.
/// - [`UnicastId::IdentityAware`] naming anything else -> [`Selection::Secondary`] with that id.
///   [`Dispatcher::resolve_stack`] rejects it if this node hosts no such identity — deliberately
///   never falling back to main, which would route a second identity's traffic into the wrong
///   generation's maps, exactly the class of bug this work exists to prevent.
/// - [`UnicastId::WholeNode`] -> a [`RemoteError`]: that variant addresses the node-level
///   skeleton, which never reaches this function in the first place.
///
/// # Errors
/// See above; every error is [`ErrorDomain::Deserialize`], matching [`misrouted`]'s own
/// reasoning for a malformed/misaddressed call.
fn select_identity(
    unicast_id: &TypedValue,
    main_id: ntk_identities::IdentityId,
) -> Result<Selection, RemoteError> {
    match UnicastId::from_typed_value(unicast_id)
        .map_err(|err| protocol_error(format!("Request.unicast_id: {err}")))?
    {
        UnicastId::MainIdentity => Ok(Selection::Main),
        UnicastId::WholeNode(_) => Err(protocol_error(
            "Request.unicast_id: WholeNode is not valid for an identity-scoped call",
        )),
        UnicastId::IdentityAware(id_tv) => {
            let id = crate::node::registry::decode_identity_id(&id_tv).ok_or_else(|| {
                protocol_error(
                    "Request.unicast_id: IdentityAware payload is not a valid identity id",
                )
            })?;
            Ok(if id == main_id {
                Selection::Main
            } else {
                Selection::Secondary(id)
            })
        }
    }
}

impl std::fmt::Debug for Dispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dispatcher").finish_non_exhaustive()
    }
}

fn protocol_error(message: impl Into<String>) -> RemoteError {
    RemoteError {
        domain: ErrorDomain::Deserialize as i32,
        message: message.into(),
    }
}

fn misrouted() -> RemoteError {
    protocol_error("MethodCall with no oneof arm set")
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
                    let stack = self.resolve_stack(&unicast_id).await?;
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
                    let stack = self.resolve_stack(&unicast_id).await?;
                    stack.peers.handle(caller, unicast_id, call, auth).await
                }

                Call::CoordinatorExecutePrepareMigration(_)
                | Call::CoordinatorExecuteFinishMigration(_)
                | Call::CoordinatorExecutePrepareEnter(_)
                | Call::CoordinatorExecuteFinishEnter(_)
                | Call::CoordinatorExecuteWeHaveSplitted(_) => {
                    let stack = self.resolve_stack(&unicast_id).await?;
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
                    let stack = self.resolve_stack(&unicast_id).await?;
                    stack.hooking.handle(caller, unicast_id, call, auth).await
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use futures::future::BoxFuture;
    use ntk_proto::v1::{
        Auth, CallerContext, MethodCall, RemoteError, ResponsePayload, TypedValue,
    };
    use ntk_rpc::{FakeRpcClient, RpcClient, RpcHandler};
    use tokio_util::sync::CancellationToken;

    use super::{Dispatcher, IdentityStack, Selection, protocol_error, select_identity};

    fn node_id(raw: i32) -> ntk_neighborhood::NodeId {
        ntk_neighborhood::NodeId::from_raw(raw).unwrap()
    }

    fn identity_id(raw: u64) -> ntk_identities::IdentityId {
        ntk_identities::IdentityId::from_raw(raw)
    }

    // -----------------------------------------------------------------------------------------
    // select_identity: the pure decision, no `IdentityStack` needed at all.
    // -----------------------------------------------------------------------------------------

    /// The single most important case: an old v0.1.5 peer (or one that has never heard of
    /// `UnicastId`) never sets `unicast_id`, and proto3's zero value for that unset field is
    /// this exact empty `TypedValue`. It must still resolve to the main identity.
    #[test]
    fn an_empty_unicast_id_selects_main() {
        let main = identity_id(1);
        assert_eq!(
            select_identity(&TypedValue::default(), main).unwrap(),
            Selection::Main
        );
    }

    #[test]
    fn an_identity_aware_unicast_id_naming_main_selects_main() {
        let main = identity_id(1);
        let tv = ntk_proto::domain::UnicastId::IdentityAware(
            crate::node::registry::encode_identity_id(main),
        )
        .to_typed_value();
        assert_eq!(select_identity(&tv, main).unwrap(), Selection::Main);
    }

    #[test]
    fn an_identity_aware_unicast_id_naming_another_identity_selects_that_identity_not_main() {
        let main = identity_id(1);
        let other = identity_id(2);
        let tv = ntk_proto::domain::UnicastId::IdentityAware(
            crate::node::registry::encode_identity_id(other),
        )
        .to_typed_value();
        assert_eq!(
            select_identity(&tv, main).unwrap(),
            Selection::Secondary(other)
        );
    }

    #[test]
    fn a_whole_node_unicast_id_is_rejected_on_an_identity_scoped_call() {
        let main = identity_id(1);
        let tv = ntk_proto::domain::UnicastId::WholeNode(
            crate::node::registry::encode_identity_id(main),
        )
        .to_typed_value();
        assert!(select_identity(&tv, main).is_err());
    }

    // -----------------------------------------------------------------------------------------
    // Dispatcher::resolve_stack end to end: every field a genuine actor-backed handler, wired
    // over trivial fixtures (mirroring each crate's own `Noop*`/`Fake*` test-double convention)
    // rather than a real network — proving `register_identity`/`unregister_identity` actually
    // wire into routing, not just that `select_identity` picks the right branch.
    // -----------------------------------------------------------------------------------------

    /// An [`RpcHandler`] for fixture [`RpcClient`]s this test never actually dials.
    struct NeverCalledHandler;
    impl RpcHandler for NeverCalledHandler {
        fn handle<'a>(
            &'a self,
            _caller: CallerContext,
            _unicast_id: TypedValue,
            _call: MethodCall,
            _auth: Option<Auth>,
        ) -> BoxFuture<'a, Result<ResponsePayload, RemoteError>> {
            Box::pin(async { Err(protocol_error("test fixture: never actually called")) })
        }
    }

    fn never_called_client() -> Arc<dyn RpcClient> {
        Arc::new(FakeRpcClient::new(Arc::new(NeverCalledHandler)))
    }

    /// Satisfies both the neighborhood and identities outbound-call seams — this test never
    /// discovers a real arc, so both are only ever asked to name a client, never dial one.
    #[derive(Debug)]
    struct NoRealArcs;
    impl ntk_neighborhood::NeighborhoodStubFactory for NoRealArcs {
        fn broadcast(&self, _dev: &str) -> Arc<dyn RpcClient> {
            never_called_client()
        }
        fn unicast(&self, _arc: &ntk_neighborhood::Arc) -> Arc<dyn RpcClient> {
            never_called_client()
        }
    }
    impl ntk_identities::IdentityStubFactory for NoRealArcs {
        fn stub(&self, _arc: ntk_identities::ArcId) -> Arc<dyn RpcClient> {
            never_called_client()
        }
        fn arc_for_caller(&self, _caller: &CallerContext) -> Option<ntk_identities::ArcId> {
            None
        }
    }

    struct NoArcResolver;
    impl ntk_qspn::ArcResolver for NoArcResolver {
        fn resolve(&self, _caller: &CallerContext) -> Option<ntk_qspn::ArcId> {
            None
        }
    }

    struct NoopRoutingEnv;
    impl ntk_peerservices::RoutingEnv for NoopRoutingEnv {
        fn gnode_exists(&self, _hc: ntk_common::HCoord) -> bool {
            false
        }
        fn gateway(
            &self,
            _hc: ntk_common::HCoord,
            _failed: Option<&Arc<dyn ntk_peerservices::PeersStub>>,
        ) -> Option<Arc<dyn ntk_peerservices::PeersStub>> {
            None
        }
        fn dial(
            &self,
            _n: &ntk_peerservices::TupleNode,
        ) -> Option<Arc<dyn ntk_peerservices::PeersStub>> {
            None
        }
        fn nodes_in_my_group(&self, _level: usize) -> usize {
            0
        }
        fn neighbors(&self) -> Vec<Arc<dyn ntk_peerservices::PeersStub>> {
            Vec::new()
        }
    }

    struct NoopCoordinatorMap;
    impl ntk_coordinator::CoordinatorMap for NoopCoordinatorMap {
        fn n_nodes(&self) -> u64 {
            1
        }
        fn free_positions(&self, _level: usize) -> Vec<u32> {
            Vec::new()
        }
        fn can_reserve(&self, _level: usize) -> bool {
            false
        }
        fn my_pos(&self, _level: usize) -> u32 {
            0
        }
        fn fp_id(&self, _level: usize) -> i64 {
            0
        }
    }

    struct NoopPropagationHandler;
    impl ntk_coordinator::PropagationHandler for NoopPropagationHandler {
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
            Box::pin(async {})
        }
        fn we_have_splitted(&self, _level: usize, _data: TypedValue) -> BoxFuture<'_, ()> {
            Box::pin(async {})
        }
    }

    struct NoopEnterHandler;
    impl ntk_coordinator::EvaluateEnterHandler for NoopEnterHandler {
        fn evaluate_enter<'a>(
            &'a self,
            _top: usize,
            _data: TypedValue,
            _client_tuple: &'a [u32],
        ) -> BoxFuture<'a, TypedValue> {
            Box::pin(async { TypedValue::default() })
        }
    }
    impl ntk_coordinator::BeginEnterHandler for NoopEnterHandler {
        fn begin_enter<'a>(
            &'a self,
            _top: usize,
            _data: TypedValue,
            _client_tuple: &'a [u32],
        ) -> BoxFuture<'a, TypedValue> {
            Box::pin(async { TypedValue::default() })
        }
    }
    impl ntk_coordinator::CompletedEnterHandler for NoopEnterHandler {
        fn completed_enter<'a>(
            &'a self,
            _top: usize,
            _data: TypedValue,
            _client_tuple: &'a [u32],
        ) -> BoxFuture<'a, TypedValue> {
            Box::pin(async { TypedValue::default() })
        }
    }
    impl ntk_coordinator::AbortEnterHandler for NoopEnterHandler {
        fn abort_enter<'a>(
            &'a self,
            _top: usize,
            _data: TypedValue,
            _client_tuple: &'a [u32],
        ) -> BoxFuture<'a, TypedValue> {
            Box::pin(async { TypedValue::default() })
        }
    }

    /// Builds one fully real (if inert) [`IdentityStack`] — every field a genuine actor-backed
    /// handler. Enough to prove [`Dispatcher::resolve_stack`]'s *routing* — which
    /// `Arc<IdentityStack>` a call reaches — without any of these handlers ever actually
    /// processing one.
    fn spawn_identity_stack(pos: u32) -> IdentityStack {
        let topology = ntk_common::Topology::new([4u32]).unwrap();
        let my_naddr = ntk_common::Naddr::new(topology.clone(), vec![pos]).unwrap();
        let fingerprint = ntk_common::Fingerprint::new(vec![1u8], 0, vec![0]);

        let (qspn_handle, _qspn_join) = ntk_qspn::spawn(
            my_naddr.clone(),
            fingerprint,
            ntk_qspn::QspnConfig::default(),
            Arc::new(ntk_qspn::FakeQspnStubFactory::new()),
            Arc::new(ntk_qspn::FixedThreshold::default()),
            Arc::new(ntk_qspn::DefaultArcIdSource::default()),
            CancellationToken::new(),
        );
        let qspn = ntk_qspn::QspnRpcHandler::new(
            qspn_handle,
            Arc::new(NoArcResolver),
            Duration::from_secs(1),
            Duration::from_millis(1),
        );

        let (peers_manager, peers_handle) = ntk_peerservices::Manager::new(
            topology.clone(),
            my_naddr.clone(),
            Arc::new(NoopRoutingEnv),
            ntk_peerservices::Config::default(),
            topology.levels(),
        );
        tokio::spawn(peers_manager.run(CancellationToken::new()));
        let peers = ntk_peerservices::PeersRpcHandler::new(peers_handle);

        let enter = Arc::new(NoopEnterHandler);
        let (coordinator_manager, coordinator_handle) = ntk_coordinator::Manager::new(
            topology.clone(),
            Arc::new(NoopCoordinatorMap),
            Arc::new(ntk_coordinator::FakeCoordinatorStubFactory::new(Vec::new())),
            Arc::new(NoopPropagationHandler),
            ntk_coordinator::EnterHandlers {
                evaluate_enter: enter.clone(),
                begin_enter: enter.clone(),
                completed_enter: enter.clone(),
                abort_enter: enter,
            },
            ntk_coordinator::Config::default(),
            None,
        );
        tokio::spawn(coordinator_manager.run(CancellationToken::new()));
        let coordinator = ntk_coordinator::CoordinatorRpcHandler::new(coordinator_handle);

        let view = Arc::new(ntk_hooking::FakeQspnView::new(topology, vec![pos]));
        let coord_client = Arc::new(ntk_hooking::FakeCoordinatorClient::default());
        let router = Arc::new(ntk_hooking::MessageRouting::new(
            view.clone(),
            coord_client.clone(),
            Arc::new(ntk_hooking::FakeHookingStubFactory::default()),
            Duration::from_secs(1),
        ));
        let hooking = ntk_hooking::HookingRpcHandler::new(view, coord_client, router);

        IdentityStack {
            qspn,
            peers,
            coordinator,
            hooking,
        }
    }

    /// A dormant (no NICs configured) [`ntk_neighborhood::NeighborhoodRpcHandler`] — never
    /// consulted by [`Dispatcher::resolve_stack`], but [`Dispatcher::new`] still needs a real
    /// one.
    fn spawn_neighborhood_rpc(
        my_id: ntk_neighborhood::NodeId,
    ) -> ntk_neighborhood::NeighborhoodRpcHandler {
        let (handle, _join) = ntk_neighborhood::Manager::spawn(
            ntk_neighborhood::NeighborhoodConfig {
                my_id,
                max_arcs: 8,
                kernel: ntk_netlink::FakeNetlink::new(),
                stub_factory: Arc::new(NoRealArcs),
                ip_route_manager: Arc::new(ntk_neighborhood::FakeIpRouteManager::new()),
                rtt_probe: Arc::new(ntk_neighborhood::FixedRttProbe(None)),
                timing: ntk_neighborhood::NeighborhoodTiming::default(),
                new_linklocal_address: Box::new(|| "10.0.0.1".to_owned()),
                signing_key: None,
                require_auth: false,
            },
            CancellationToken::new(),
        );
        ntk_neighborhood::NeighborhoodRpcHandler::for_unicast(handle)
    }

    /// Likewise dormant (no local identities beyond the one [`Dispatcher`] itself tracks) —
    /// never consulted by [`Dispatcher::resolve_stack`] either.
    /// Returns the dispatcher together with the `IdentityId` its registry actually reports as
    /// main. The id is read back rather than chosen: `Dispatcher` resolves it live from the
    /// handle (see its field doc), so a test that invented one would be asserting against a
    /// value the dispatcher never sees.
    fn dispatcher(node_id: ntk_neighborhood::NodeId) -> (Dispatcher, ntk_identities::IdentityId) {
        let (handle, _join) =
            ntk_identities::Handle::spawn(None, Arc::new(NoRealArcs), CancellationToken::new());
        let main_id = handle.main_id();
        let dispatcher = Dispatcher::new(
            spawn_neighborhood_rpc(node_id),
            ntk_identities::IdentityRpcHandler::new(handle.clone(), Arc::new(NoRealArcs)),
            handle,
            spawn_identity_stack(0),
        );
        (dispatcher, main_id)
    }

    #[tokio::test]
    async fn an_empty_unicast_id_resolves_the_main_stack() {
        let (d, _main_id) = dispatcher(node_id(1));
        let main_stack = d.identity_stack.read().await.clone();
        let resolved = d.resolve_stack(&TypedValue::default()).await.unwrap();
        assert!(Arc::ptr_eq(&resolved, &main_stack));
    }

    #[tokio::test]
    async fn a_registered_second_identity_resolves_to_its_own_stack_not_main() {
        let (d, main_id) = dispatcher(node_id(1));
        let other_id = identity_id(main_id.into_raw().wrapping_add(1));
        let main_stack = d.identity_stack.read().await.clone();
        d.register_identity(other_id, spawn_identity_stack(1)).await;
        let secondary_stack = d.secondary.read().await.get(&other_id).cloned().unwrap();

        let tv = ntk_proto::domain::UnicastId::IdentityAware(
            crate::node::registry::encode_identity_id(other_id),
        )
        .to_typed_value();
        let resolved = d.resolve_stack(&tv).await.unwrap();
        assert!(Arc::ptr_eq(&resolved, &secondary_stack));
        assert!(!Arc::ptr_eq(&resolved, &main_stack));
    }

    #[tokio::test]
    async fn an_identity_aware_unicast_id_naming_an_unregistered_identity_is_rejected() {
        let (d, main_id) = dispatcher(node_id(1));
        let unknown_id = identity_id(main_id.into_raw().wrapping_add(99));

        let tv = ntk_proto::domain::UnicastId::IdentityAware(
            crate::node::registry::encode_identity_id(unknown_id),
        )
        .to_typed_value();
        assert!(d.resolve_stack(&tv).await.is_err());
    }

    #[tokio::test]
    async fn unregistering_a_second_identity_makes_it_unknown_again() {
        let (d, main_id) = dispatcher(node_id(1));
        let other_id = identity_id(main_id.into_raw().wrapping_add(1));
        d.register_identity(other_id, spawn_identity_stack(1)).await;
        d.unregister_identity(other_id).await;

        let tv = ntk_proto::domain::UnicastId::IdentityAware(
            crate::node::registry::encode_identity_id(other_id),
        )
        .to_typed_value();
        assert!(d.resolve_stack(&tv).await.is_err());
    }
}

//! The outbound RPC seam: `IHookingManagerStub`/`IIdentityArc.get_stub()`/
//! `IHookingMapPaths.gateway()` (`research/impl/vala/hooking/api.vala:47-49,86-89`)
//! — the exact 10-method surface `ntk_proto::v1::MethodCall`'s `hooking_*`
//! arms carry on the wire. Implemented once for the real transport (by
//! whichever crate composes Hooking with Neighborhood/QSPN/RPC — out of
//! this crate's scope) and once for [`crate::fake::FakeHookingStubFactory`].

use std::sync::Arc;

use futures::future::BoxFuture;
use ntk_common::HCoord;
use ntk_rpc::RpcError;

use crate::arc::ArcId;
use crate::domain::{
    DeleteReservationRequest, EntryData, ExploreGNodeRequest, ExploreGNodeResponse, NetworkData,
    RequestPacket, ResponsePacket, SearchMigrationPathErrorPkt, SearchMigrationPathRequest,
    SearchMigrationPathResponse,
};

/// The 10 outbound calls one stub can make — `IHookingManagerStub`
/// (`interfaces.rpcidl:36-46`, `ntk-proto`'s `proto/ntk.proto:248-258`).
///
/// Errors are [`RpcError`] directly, matching `ntk-qspn::QspnStub`'s
/// convention: [`RpcError::Remote`] already carries the wire-typed
/// `ErrorDomain` (`HOOKING_NOT_PRINCIPAL`/`NOT_BOOTSTRAPPED`/
/// `NO_MIGRATION_PATH_FOUND`/`MIGRATION_PATH_EXECUTE_FAILURE`), the same
/// split upstream's `StubError` vs. domain `errordomain`s draws.
pub trait HookingStub: Send + Sync {
    /// `retrieve_network_data` (`hooking.vala:495-514`).
    fn retrieve_network_data(
        &self,
        ask_coord: bool,
    ) -> BoxFuture<'_, Result<NetworkData, RpcError>>;

    /// `search_migration_path` (`hooking.vala:516-579`).
    fn search_migration_path(&self, lvl: usize) -> BoxFuture<'_, Result<EntryData, RpcError>>;

    /// `route_search_request` (`message_routing.vala:166-349`).
    fn route_search_request(
        &self,
        req: SearchMigrationPathRequest,
    ) -> BoxFuture<'_, Result<(), RpcError>>;

    /// `route_search_error` (`message_routing.vala:379-417`).
    fn route_search_error(
        &self,
        pkt: SearchMigrationPathErrorPkt,
    ) -> BoxFuture<'_, Result<(), RpcError>>;

    /// `route_search_response` (`message_routing.vala:447-485`).
    fn route_search_response(
        &self,
        resp: SearchMigrationPathResponse,
    ) -> BoxFuture<'_, Result<(), RpcError>>;

    /// `route_explore_request` (`message_routing.vala:547-669`).
    fn route_explore_request(
        &self,
        req: ExploreGNodeRequest,
    ) -> BoxFuture<'_, Result<(), RpcError>>;

    /// `route_explore_response` (`message_routing.vala:699-737`).
    fn route_explore_response(
        &self,
        resp: ExploreGNodeResponse,
    ) -> BoxFuture<'_, Result<(), RpcError>>;

    /// `route_delete_reserve_request` (`message_routing.vala:790-817`).
    fn route_delete_reserve_request(
        &self,
        req: DeleteReservationRequest,
    ) -> BoxFuture<'_, Result<(), RpcError>>;

    /// `route_mig_request` (`message_routing.vala:859-901`).
    fn route_mig_request(&self, req: RequestPacket) -> BoxFuture<'_, Result<(), RpcError>>;

    /// `route_mig_response` (`message_routing.vala:931-968`).
    fn route_mig_response(&self, resp: ResponsePacket) -> BoxFuture<'_, Result<(), RpcError>>;
}

/// Builds outbound [`HookingStub`]s — `IIdentityArc.get_stub()`
/// (`api.vala:86-89`) plus `IHookingMapPaths.gateway()` (`api.vala:47-49`)
/// folded into one factory, since both ultimately hand back "a stub to talk
/// to some other Hooking instance", just keyed differently (a specific
/// identity-arc vs. a routing destination resolved via QSPN's route table).
///
/// **Deliberate simplification**: upstream's `gateway(level, pos,
/// received_from, failed)` lets a caller retry with the next-best gateway
/// after a `StubError` on the previous one (`message_routing.vala:118-138`
/// and every `route_*` call site). This trait drops the `received_from`/
/// `failed` retry-avoidance parameters — a single resolution attempt per
/// destination — trading upstream's best-effort failover for a much
/// simpler seam; a caller that needs the retry can just call
/// [`HookingStubFactory::gateway_stub`] again after treating the first
/// stub's `RpcError` as terminal for that hop.
pub trait HookingStubFactory: Send + Sync {
    /// The stub for a specific identity-arc (`IIdentityArc.get_stub()`).
    fn arc_stub(&self, arc: ArcId) -> Arc<dyn HookingStub>;

    /// A stub to reach the gateway bordering the g-node at `hc`, resolved
    /// via QSPN's routing table (`IHookingMapPaths.gateway`,
    /// `api.vala:47-49`). `None` if `hc` is currently unreachable.
    fn gateway_stub(&self, hc: HCoord) -> Option<Arc<dyn HookingStub>>;
}

//! Inbound RPC dispatch: [`ntk_rpc::RpcHandler`] for the 10 `hooking_*` arms
//! of `ntk_proto::v1::MethodCall` — `retrieve_network_data`
//! (`hooking.vala:495-514`), `search_migration_path` (`:516-579`), and the
//! 8 `route_*` routing envelopes (`message_routing.vala`, dispatched
//! straight to [`crate::routing::MessageRouting`]).

use std::sync::Arc;

use futures::future::BoxFuture;
use ntk_proto::v1::{
    CallerContext, Empty, ErrorDomain, MethodCall, RemoteError, ResponsePayload, TypedValue,
    method_call, response_payload,
};
use ntk_rpc::RpcHandler;
use thiserror::Error;

use crate::coordinator::CoordinatorClient;
use crate::domain::{EntryData, NetworkData, make_tuple_from_level, tuple_has_virtual_pos};
use crate::idgen;
use crate::routing::MessageRouting;
use crate::search::{SearchRouter, execute_shortest_mig, find_shortest_mig};
use crate::view::QspnView;
use crate::wire::{self, WireError};

/// The 4 upstream `errordomain`s `retrieve_network_data`/
/// `search_migration_path` can throw (`hooking.vala:498,519`), plus one
/// this port adds: [`Self::LevelOutOfRange`]. Upstream's `search_migration_path`
/// (`hooking.vala:516-579`) never validates its `lvl` argument at all — Vala's
/// `for (i = l; i < levels; i++)` in `make_tuple_from_level`
/// (`structs.vala:76-87`) simply no-ops when `l >= levels`, so an
/// out-of-range level silently produces an empty tuple instead of failing.
/// This port's equivalent ([`crate::domain::make_tuple_from_level`]) sizes a
/// `Vec` from `levels - l` up front, which has no such free no-op: an
/// out-of-range `lvl` reaching it underflows. Rather than reproduce that new
/// failure mode, this port rejects the request at the boundary that knows
/// the topology, matching the `HCoord.pos` precedent in
/// `ntk-qspn/src/validate.rs::check_hop_list`.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
enum HookingServerError {
    #[error("not bootstrapped")]
    NotBootstrapped,
    #[error("not the principal member of my g-node")]
    HookingNotPrincipal,
    #[error("no migration path found")]
    NoMigrationPathFound,
    #[error("migration path execution failed")]
    MigrationPathExecuteFailure,
    #[error("requested level exceeds the known topology")]
    LevelOutOfRange,
}

fn remote_error(domain: ErrorDomain, message: impl Into<String>) -> RemoteError {
    RemoteError {
        domain: domain as i32,
        message: message.into(),
    }
}

fn hooking_error_to_remote(e: HookingServerError) -> RemoteError {
    let domain = match e {
        HookingServerError::NotBootstrapped => ErrorDomain::NotBootstrapped,
        HookingServerError::HookingNotPrincipal => ErrorDomain::HookingNotPrincipal,
        HookingServerError::NoMigrationPathFound => ErrorDomain::NoMigrationPathFound,
        HookingServerError::MigrationPathExecuteFailure => ErrorDomain::MigrationPathExecuteFailure,
        // No dedicated upstream errordomain exists for this (upstream never
        // validates `lvl` at all, see this enum's own doc) — `Deserialize`
        // is the same domain the sibling "negative level" rejection below
        // already uses for a malformed `lvl` on the wire.
        HookingServerError::LevelOutOfRange => ErrorDomain::Deserialize,
    };
    remote_error(domain, e.to_string())
}

fn wire_error_to_remote(e: WireError) -> RemoteError {
    remote_error(ErrorDomain::Deserialize, e.to_string())
}

fn empty_ok() -> Result<ResponsePayload, RemoteError> {
    Ok(ResponsePayload {
        value: Some(response_payload::Value::Empty(Empty::default())),
    })
}

fn typed_ok(tv: TypedValue) -> Result<ResponsePayload, RemoteError> {
    Ok(ResponsePayload {
        value: Some(response_payload::Value::Typed(tv)),
    })
}

/// [`RpcHandler`] for the 10 `hooking_*` [`MethodCall`] arms, wired to a
/// single identity's [`QspnView`]/[`CoordinatorClient`]/[`MessageRouting`].
/// Any other `MethodCall` arm is a routing bug in whoever composed the
/// dispatcher — reported as [`ErrorDomain::Deserialize`] (notes/02 §1: an
/// unrecognized/misrouted call is always `DeserializeError` on the wire).
pub struct HookingRpcHandler {
    view: Arc<dyn QspnView>,
    coord: Arc<dyn CoordinatorClient>,
    router: Arc<MessageRouting>,
}

impl std::fmt::Debug for HookingRpcHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HookingRpcHandler").finish_non_exhaustive()
    }
}

impl HookingRpcHandler {
    #[must_use]
    pub fn new(
        view: Arc<dyn QspnView>,
        coord: Arc<dyn CoordinatorClient>,
        router: Arc<MessageRouting>,
    ) -> Self {
        Self {
            view,
            coord,
            router,
        }
    }

    /// `retrieve_network_data` (`hooking.vala:495-514`).
    async fn retrieve_network_data(
        &self,
        ask_coord: bool,
    ) -> Result<NetworkData, HookingServerError> {
        if !self.view.is_bootstrapped() {
            return Err(HookingServerError::NotBootstrapped);
        }
        let me = make_tuple_from_level(0, self.view.as_ref());
        if tuple_has_virtual_pos(&me, self.view.as_ref()) {
            return Err(HookingServerError::HookingNotPrincipal);
        }
        let levels = self.view.topology().levels();
        let neighbor_pos = (0..levels).map(|i| self.view.my_pos(i)).collect();
        let gsizes = (0..levels)
            .map(|i| self.view.topology().gsize(i).unwrap_or(0))
            .collect();
        let neighbor_n_nodes = if ask_coord {
            self.coord.n_nodes().await
        } else {
            self.view.n_nodes()
        };
        Ok(NetworkData {
            network_id: self.view.network_id(),
            neighbor_n_nodes,
            neighbor_min_level: self.view.subnetlevel(),
            gsizes,
            neighbor_pos,
        })
    }

    /// `search_migration_path` (`hooking.vala:516-579`).
    async fn search_migration_path(&self, lvl: usize) -> Result<EntryData, HookingServerError> {
        if !self.view.is_bootstrapped() {
            return Err(HookingServerError::NotBootstrapped);
        }
        let levels = self.view.topology().levels();
        // `first_host_lvl = lvl + 1` below is fed straight into
        // `find_shortest_mig` -> `make_tuple_from_level`, which sizes a `Vec`
        // from `levels - first_host_lvl`; an out-of-range `lvl` (a peer is
        // free to send any non-negative `i32`, see `HookingSearchMigrationPath`'s
        // dispatch in [`crate::rpc`]) must be rejected here, before that
        // arithmetic, not clamped or left to `find_shortest_mig`'s own `.max`
        // calls (those only enforce a *lower* bound). `lvl == levels - 1` is
        // the deepest legal request (`first_host_lvl == levels`, an empty
        // host tuple); `lvl >= levels` names no real level.
        if lvl >= levels {
            return Err(HookingServerError::LevelOutOfRange);
        }
        let epsilon = self.view.epsilon(lvl);
        let first_host_lvl = lvl + 1;
        let ok_host_lvl = lvl + epsilon;
        let reserve_request_id = idgen::next_i32();
        let my_pos_1 = (levels > 1).then(|| self.view.my_pos(1));
        tracing::info!(
            lvl,
            first_host_lvl,
            ok_host_lvl,
            reserve_request_id,
            network_id = self.view.network_id(),
            my_pos_0 = self.view.my_pos(0),
            ?my_pos_1,
            "hooking: search_migration_path minted reserve_request_id (this node is the search servant)"
        );

        let mut solutions = find_shortest_mig(
            self.view.as_ref(),
            self.router.as_ref(),
            reserve_request_id,
            first_host_lvl,
            ok_host_lvl,
        )
        .await;
        if solutions.is_empty() {
            return Err(HookingServerError::NoMigrationPathFound);
        }
        // The best (shallowest) solution is always last, see
        // find_shortest_mig's docs.
        let sol = solutions.pop().expect("just checked non-empty");
        for rejected in &solutions {
            self.router
                .send_delete_reserve_request(rejected.cleanup_target(levels), reserve_request_id);
        }

        if sol.distance() > 0 {
            execute_shortest_mig(self.view.as_ref(), self.router.as_ref(), &sol)
                .await
                .map_err(|_| HookingServerError::MigrationPathExecuteFailure)?;
        }
        Ok(sol.resolve_entry_data(self.view.as_ref()))
    }
}

impl RpcHandler for HookingRpcHandler {
    fn handle<'a>(
        &'a self,
        _caller: CallerContext,
        _unicast_id: TypedValue,
        call: MethodCall,
        _auth: Option<ntk_proto::v1::Auth>,
    ) -> BoxFuture<'a, Result<ResponsePayload, RemoteError>> {
        Box::pin(async move {
            match call.call {
                Some(method_call::Call::HookingRetrieveNetworkData(ask_coord)) => {
                    let nd = self
                        .retrieve_network_data(ask_coord)
                        .await
                        .map_err(hooking_error_to_remote)?;
                    typed_ok(wire::encode_network_data(&nd))
                }
                Some(method_call::Call::HookingSearchMigrationPath(lvl)) => {
                    let lvl = usize::try_from(lvl)
                        .map_err(|_| remote_error(ErrorDomain::Deserialize, "negative level"))?;
                    let entry = self
                        .search_migration_path(lvl)
                        .await
                        .map_err(hooking_error_to_remote)?;
                    typed_ok(wire::encode_entry_data(&entry))
                }
                Some(method_call::Call::HookingRouteSearchRequest(tv)) => {
                    let req = wire::decode_search_request(&tv).map_err(wire_error_to_remote)?;
                    self.router.route_search_request(req).await;
                    empty_ok()
                }
                Some(method_call::Call::HookingRouteSearchError(tv)) => {
                    let pkt = wire::decode_search_error(&tv).map_err(wire_error_to_remote)?;
                    self.router.route_search_error(pkt).await;
                    empty_ok()
                }
                Some(method_call::Call::HookingRouteSearchResponse(tv)) => {
                    let resp = wire::decode_search_response(&tv).map_err(wire_error_to_remote)?;
                    self.router.route_search_response(resp).await;
                    empty_ok()
                }
                Some(method_call::Call::HookingRouteExploreRequest(tv)) => {
                    let req = wire::decode_explore_request(&tv).map_err(wire_error_to_remote)?;
                    self.router.route_explore_request(req).await;
                    empty_ok()
                }
                Some(method_call::Call::HookingRouteExploreResponse(tv)) => {
                    let resp = wire::decode_explore_response(&tv).map_err(wire_error_to_remote)?;
                    self.router.route_explore_response(resp).await;
                    empty_ok()
                }
                Some(method_call::Call::HookingRouteDeleteReserveRequest(tv)) => {
                    let req =
                        wire::decode_delete_reserve_request(&tv).map_err(wire_error_to_remote)?;
                    self.router.route_delete_reserve_request(req).await;
                    empty_ok()
                }
                Some(method_call::Call::HookingRouteMigRequest(tv)) => {
                    let req = wire::decode_mig_request(&tv).map_err(wire_error_to_remote)?;
                    self.router.route_mig_request(req).await;
                    empty_ok()
                }
                Some(method_call::Call::HookingRouteMigResponse(tv)) => {
                    let resp = wire::decode_mig_response(&tv).map_err(wire_error_to_remote)?;
                    self.router.route_mig_response(resp).await;
                    empty_ok()
                }
                _ => Err(remote_error(
                    ErrorDomain::Deserialize,
                    "not a hooking method",
                )),
            }
        })
    }
}

/// Also implements [`crate::stub::HookingStub`] over this same handler, for
/// the in-memory [`crate::fake::FakeHookingStubFactory`]: routes every call
/// straight into the local handler methods, no wire encode/decode, no RPC
/// transport — mirrors `ntk_qspn::FakeQspnStubFactory`'s "routes calls
/// directly to a registered peer" design.
pub(crate) struct LocalHookingStub {
    pub handler: Arc<HookingRpcHandler>,
}

impl crate::stub::HookingStub for LocalHookingStub {
    fn retrieve_network_data(
        &self,
        ask_coord: bool,
    ) -> BoxFuture<'_, Result<NetworkData, ntk_rpc::RpcError>> {
        Box::pin(async move {
            self.handler
                .retrieve_network_data(ask_coord)
                .await
                .map_err(|e| ntk_rpc::RpcError::Remote(hooking_error_to_remote(e)))
        })
    }

    fn search_migration_path(
        &self,
        lvl: usize,
    ) -> BoxFuture<'_, Result<EntryData, ntk_rpc::RpcError>> {
        Box::pin(async move {
            self.handler
                .search_migration_path(lvl)
                .await
                .map_err(|e| ntk_rpc::RpcError::Remote(hooking_error_to_remote(e)))
        })
    }

    fn route_search_request(
        &self,
        req: crate::domain::SearchMigrationPathRequest,
    ) -> BoxFuture<'_, Result<(), ntk_rpc::RpcError>> {
        Box::pin(async move {
            self.handler.router.route_search_request(req).await;
            Ok(())
        })
    }

    fn route_search_error(
        &self,
        pkt: crate::domain::SearchMigrationPathErrorPkt,
    ) -> BoxFuture<'_, Result<(), ntk_rpc::RpcError>> {
        Box::pin(async move {
            self.handler.router.route_search_error(pkt).await;
            Ok(())
        })
    }

    fn route_search_response(
        &self,
        resp: crate::domain::SearchMigrationPathResponse,
    ) -> BoxFuture<'_, Result<(), ntk_rpc::RpcError>> {
        Box::pin(async move {
            self.handler.router.route_search_response(resp).await;
            Ok(())
        })
    }

    fn route_explore_request(
        &self,
        req: crate::domain::ExploreGNodeRequest,
    ) -> BoxFuture<'_, Result<(), ntk_rpc::RpcError>> {
        Box::pin(async move {
            self.handler.router.route_explore_request(req).await;
            Ok(())
        })
    }

    fn route_explore_response(
        &self,
        resp: crate::domain::ExploreGNodeResponse,
    ) -> BoxFuture<'_, Result<(), ntk_rpc::RpcError>> {
        Box::pin(async move {
            self.handler.router.route_explore_response(resp).await;
            Ok(())
        })
    }

    fn route_delete_reserve_request(
        &self,
        req: crate::domain::DeleteReservationRequest,
    ) -> BoxFuture<'_, Result<(), ntk_rpc::RpcError>> {
        Box::pin(async move {
            self.handler.router.route_delete_reserve_request(req).await;
            Ok(())
        })
    }

    fn route_mig_request(
        &self,
        req: crate::domain::RequestPacket,
    ) -> BoxFuture<'_, Result<(), ntk_rpc::RpcError>> {
        Box::pin(async move {
            self.handler.router.route_mig_request(req).await;
            Ok(())
        })
    }

    fn route_mig_response(
        &self,
        resp: crate::domain::ResponsePacket,
    ) -> BoxFuture<'_, Result<(), ntk_rpc::RpcError>> {
        Box::pin(async move {
            self.handler.router.route_mig_response(resp).await;
            Ok(())
        })
    }
}

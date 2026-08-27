//! `MessageRouting` (`research/impl/vala/hooking/message_routing.vala`): the
//! real [`SearchRouter`] implementation, plus the inbound handlers for the
//! 8 `route_*` wire methods, wired to [`crate::rpc::HookingRpcHandler`].
//!
//! **Deliberate simplification** (documented on
//! [`crate::stub::HookingStubFactory`] too): upstream's `route_*` skeletons
//! carry the *entire* `path_hops` chain and re-verify hop-by-hop adjacency
//! before forwarding one level at a time toward the destination
//! (`message_routing.vala:166-349`), and fall back to an explicit
//! `SearchMigrationPathErrorPkt` "unreachable" reply that itself gets
//! routed back through the chain. This port instead resolves the
//! destination's g-node directly via [`HookingStubFactory::gateway_stub`]
//! and lets each hop's own `route_*` handler recurse the same way — the
//! observable outcome (deliver, forward, or eventually time out) is the
//! same, but there is no per-hop adjacency re-verification and an
//! unreachable hop is a silent drop (the caller's own response-timeout is
//! what surfaces it as [`RoutingError`]), not an explicit error packet.
//! Rationale: `HookingStubFactory` already abstracts away neighbor/arc
//! topology entirely (out of this crate's scope), so re-deriving adjacency
//! verification without a real routing table would not be meaningfully
//! testable here.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use tokio::sync::{Mutex, oneshot};

use crate::coordinator::CoordinatorClient;
use crate::domain::{
    DeleteReservationRequest, ExploreGNodeRequest, ExploreGNodeResponse, RequestPacket,
    ResponsePacket, SearchMigrationPathErrorPkt, SearchMigrationPathRequest,
    SearchMigrationPathResponse, i_am_inside, make_tuple_from_level, tuple_to_hc,
};
use crate::search::{
    RoutingError, SearchRouter, SearchStepResult, execute_delete_reserve, execute_mig,
    execute_search,
};
use crate::stub::HookingStubFactory;
use crate::view::QspnView;

type SearchReply = oneshot::Sender<Result<SearchMigrationPathResponse, ()>>;
type ExploreReply = oneshot::Sender<crate::domain::TupleGNode>;
type MigReply = oneshot::Sender<()>;

/// The real message-routing layer: implements [`SearchRouter`] (outbound
/// BFS steps) and the inbound `route_*` handlers
/// [`crate::rpc::HookingRpcHandler`] dispatches to.
pub struct MessageRouting {
    view: Arc<dyn QspnView>,
    coord: Arc<dyn CoordinatorClient>,
    stubs: Arc<dyn HookingStubFactory>,
    response_timeout: Duration,
    pending_search: Mutex<HashMap<i32, SearchReply>>,
    pending_explore: Mutex<HashMap<i32, ExploreReply>>,
    pending_mig: Mutex<HashMap<i32, MigReply>>,
}

impl std::fmt::Debug for MessageRouting {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageRouting").finish_non_exhaustive()
    }
}

impl MessageRouting {
    #[must_use]
    pub fn new(
        view: Arc<dyn QspnView>,
        coord: Arc<dyn CoordinatorClient>,
        stubs: Arc<dyn HookingStubFactory>,
        response_timeout: Duration,
    ) -> Self {
        Self {
            view,
            coord,
            stubs,
            response_timeout,
            pending_search: Mutex::new(HashMap::new()),
            pending_explore: Mutex::new(HashMap::new()),
            pending_mig: Mutex::new(HashMap::new()),
        }
    }

    fn my_tuple(&self) -> crate::domain::TupleGNode {
        make_tuple_from_level(0, self.view.as_ref())
    }

    // -- Inbound: route_search_request / _error / _response --

    /// `route_search_request` (`message_routing.vala:166-349`).
    pub async fn route_search_request(&self, req: SearchMigrationPathRequest) {
        let Some(hop) = req.path_hops.last() else {
            return;
        };
        let target = hop.visiting_gnode.clone();
        if i_am_inside(&target, self.view.as_ref()) {
            if let Some(result) = execute_search(
                self.view.as_ref(),
                self.coord.as_ref(),
                &target,
                req.max_host_lvl,
                req.reserve_request_id,
            )
            .await
            {
                self.deliver_search_response(&req.origin, req.pkt_id, result)
                    .await;
            }
            return;
        }
        if let Some(stub) = self
            .stubs
            .gateway_stub(tuple_to_hc(&target, self.view.as_ref()))
        {
            let _ = stub.route_search_request(req).await;
        }
    }

    async fn deliver_search_response(
        &self,
        origin: &crate::domain::TupleGNode,
        pkt_id: i32,
        result: SearchStepResult,
    ) {
        let resp = SearchMigrationPathResponse {
            pkt_id,
            origin: origin.clone(),
            min_host_lvl: result.min_host_lvl,
            set_adjacent: result.set_adjacent,
            final_host_lvl: result.final_host_lvl,
            real_new_pos: result.real_new_pos,
            real_new_eldership: result.real_new_eldership,
            new_conn_vir_pos: result.new_conn_vir_pos,
            new_eldership: result.new_eldership,
        };
        self.route_search_response(resp).await;
    }

    /// `route_search_error` (`message_routing.vala:379-417`).
    pub async fn route_search_error(&self, pkt: SearchMigrationPathErrorPkt) {
        if i_am_inside(&pkt.origin, self.view.as_ref()) {
            if let Some(tx) = self.pending_search.lock().await.remove(&pkt.pkt_id) {
                let _ = tx.send(Err(()));
            }
            return;
        }
        if let Some(stub) = self
            .stubs
            .gateway_stub(tuple_to_hc(&pkt.origin, self.view.as_ref()))
        {
            let _ = stub.route_search_error(pkt).await;
        }
    }

    /// `route_search_response` (`message_routing.vala:447-485`).
    pub async fn route_search_response(&self, resp: SearchMigrationPathResponse) {
        if i_am_inside(&resp.origin, self.view.as_ref()) {
            if let Some(tx) = self.pending_search.lock().await.remove(&resp.pkt_id) {
                let _ = tx.send(Ok(resp));
            }
            return;
        }
        if let Some(stub) = self
            .stubs
            .gateway_stub(tuple_to_hc(&resp.origin, self.view.as_ref()))
        {
            let _ = stub.route_search_response(resp).await;
        }
    }

    // -- Inbound: route_explore_request / _response --

    /// `route_explore_request` (`message_routing.vala:547-669`).
    pub async fn route_explore_request(&self, req: ExploreGNodeRequest) {
        let Some(hop) = req.path_hops.last() else {
            return;
        };
        let target = hop.visiting_gnode.clone();
        if i_am_inside(&target, self.view.as_ref()) {
            let result = crate::search::execute_explore(req.requested_lvl, self.view.as_ref());
            let resp = ExploreGNodeResponse {
                pkt_id: req.pkt_id,
                origin: req.origin.clone(),
                result,
            };
            self.route_explore_response(resp).await;
            return;
        }
        if let Some(stub) = self
            .stubs
            .gateway_stub(tuple_to_hc(&target, self.view.as_ref()))
        {
            let _ = stub.route_explore_request(req).await;
        }
    }

    /// `route_explore_response` (`message_routing.vala:699-737`).
    pub async fn route_explore_response(&self, resp: ExploreGNodeResponse) {
        if i_am_inside(&resp.origin, self.view.as_ref()) {
            if let Some(tx) = self.pending_explore.lock().await.remove(&resp.pkt_id) {
                let _ = tx.send(resp.result);
            }
            return;
        }
        if let Some(stub) = self
            .stubs
            .gateway_stub(tuple_to_hc(&resp.origin, self.view.as_ref()))
        {
            let _ = stub.route_explore_response(resp).await;
        }
    }

    // -- Inbound: route_delete_reserve_request --

    /// `route_delete_reserve_request` (`message_routing.vala:790-817`).
    pub async fn route_delete_reserve_request(&self, req: DeleteReservationRequest) {
        if i_am_inside(&req.dest_gnode, self.view.as_ref()) {
            execute_delete_reserve(
                self.coord.as_ref(),
                self.view.as_ref(),
                &req.dest_gnode,
                req.reserve_request_id,
            )
            .await;
            return;
        }
        if let Some(stub) = self
            .stubs
            .gateway_stub(tuple_to_hc(&req.dest_gnode, self.view.as_ref()))
        {
            let _ = stub.route_delete_reserve_request(req).await;
        }
    }

    // -- Inbound: route_mig_request / _response --

    /// `route_mig_request` (`message_routing.vala:859-901`).
    pub async fn route_mig_request(&self, req: RequestPacket) {
        if i_am_inside(&req.dest, self.view.as_ref()) {
            let (pkt_id, src) = (req.pkt_id, req.src.clone());
            execute_mig(self.coord.as_ref(), self.view.as_ref(), &req).await;
            let resp = ResponsePacket { pkt_id, dest: src };
            self.route_mig_response(resp).await;
            return;
        }
        if let Some(stub) = self
            .stubs
            .gateway_stub(tuple_to_hc(&req.dest, self.view.as_ref()))
        {
            let _ = stub.route_mig_request(req).await;
        }
    }

    /// `route_mig_response` (`message_routing.vala:931-968`).
    pub async fn route_mig_response(&self, resp: ResponsePacket) {
        if i_am_inside(&resp.dest, self.view.as_ref()) {
            if let Some(tx) = self.pending_mig.lock().await.remove(&resp.pkt_id) {
                let _ = tx.send(());
            }
            return;
        }
        if let Some(stub) = self
            .stubs
            .gateway_stub(tuple_to_hc(&resp.dest, self.view.as_ref()))
        {
            let _ = stub.route_mig_response(resp).await;
        }
    }
}

impl SearchRouter for MessageRouting {
    /// `send_search_request` (`message_routing.vala:93-164`).
    fn send_search_request(
        &self,
        path_hops: Vec<crate::domain::PathHop>,
        max_host_lvl: usize,
        reserve_request_id: i32,
    ) -> BoxFuture<'_, Result<SearchStepResult, RoutingError>> {
        Box::pin(async move {
            let target = path_hops.last().ok_or(RoutingError)?.visiting_gnode.clone();
            if i_am_inside(&target, self.view.as_ref()) {
                return execute_search(
                    self.view.as_ref(),
                    self.coord.as_ref(),
                    &target,
                    max_host_lvl,
                    reserve_request_id,
                )
                .await
                .ok_or(RoutingError);
            }
            let pkt_id = crate::idgen::next_i32();
            let (tx, rx) = oneshot::channel();
            self.pending_search.lock().await.insert(pkt_id, tx);
            let Some(stub) = self
                .stubs
                .gateway_stub(tuple_to_hc(&target, self.view.as_ref()))
            else {
                self.pending_search.lock().await.remove(&pkt_id);
                return Err(RoutingError);
            };
            let req = SearchMigrationPathRequest {
                pkt_id,
                origin: self.my_tuple(),
                caller: self.my_tuple(),
                path_hops,
                max_host_lvl,
                reserve_request_id,
            };
            if stub.route_search_request(req).await.is_err() {
                self.pending_search.lock().await.remove(&pkt_id);
                return Err(RoutingError);
            }
            match tokio::time::timeout(self.response_timeout, rx).await {
                Ok(Ok(Ok(resp))) => Ok(SearchStepResult {
                    min_host_lvl: resp.min_host_lvl,
                    final_host_lvl: resp.final_host_lvl,
                    real_new_pos: resp.real_new_pos,
                    real_new_eldership: resp.real_new_eldership,
                    set_adjacent: resp.set_adjacent,
                    new_conn_vir_pos: resp.new_conn_vir_pos,
                    new_eldership: resp.new_eldership,
                }),
                _ => {
                    self.pending_search.lock().await.remove(&pkt_id);
                    Err(RoutingError)
                }
            }
        })
    }

    /// `send_explore_request` (`message_routing.vala:487-545`).
    fn send_explore_request(
        &self,
        path_hops: Vec<crate::domain::PathHop>,
        requested_lvl: usize,
    ) -> BoxFuture<'_, Result<crate::domain::TupleGNode, RoutingError>> {
        Box::pin(async move {
            let target = path_hops.last().ok_or(RoutingError)?.visiting_gnode.clone();
            if i_am_inside(&target, self.view.as_ref()) {
                return Ok(crate::search::execute_explore(
                    requested_lvl,
                    self.view.as_ref(),
                ));
            }
            let pkt_id = crate::idgen::next_i32();
            let (tx, rx) = oneshot::channel();
            self.pending_explore.lock().await.insert(pkt_id, tx);
            let Some(stub) = self
                .stubs
                .gateway_stub(tuple_to_hc(&target, self.view.as_ref()))
            else {
                self.pending_explore.lock().await.remove(&pkt_id);
                return Err(RoutingError);
            };
            let req = ExploreGNodeRequest {
                pkt_id,
                origin: self.my_tuple(),
                path_hops,
                requested_lvl,
            };
            if stub.route_explore_request(req).await.is_err() {
                self.pending_explore.lock().await.remove(&pkt_id);
                return Err(RoutingError);
            }
            match tokio::time::timeout(self.response_timeout, rx).await {
                Ok(Ok(result)) => Ok(result),
                _ => {
                    self.pending_explore.lock().await.remove(&pkt_id);
                    Err(RoutingError)
                }
            }
        })
    }

    /// `send_delete_reserve_request` (`message_routing.vala:739-772`):
    /// fire-and-forget, matching upstream's own "no response needed".
    fn send_delete_reserve_request(
        &self,
        dest_gnode: crate::domain::TupleGNode,
        reserve_request_id: i32,
    ) {
        if i_am_inside(&dest_gnode, self.view.as_ref()) {
            let coord = self.coord.clone();
            let view = self.view.clone();
            tokio::spawn(async move {
                execute_delete_reserve(
                    coord.as_ref(),
                    view.as_ref(),
                    &dest_gnode,
                    reserve_request_id,
                )
                .await;
            });
            return;
        }
        if let Some(stub) = self
            .stubs
            .gateway_stub(tuple_to_hc(&dest_gnode, self.view.as_ref()))
        {
            let req = DeleteReservationRequest {
                dest_gnode,
                reserve_request_id,
            };
            tokio::spawn(async move {
                let _ = stub.route_delete_reserve_request(req).await;
            });
        }
    }

    /// `send_mig_request` (`message_routing.vala:819-857`).
    fn send_mig_request(
        &self,
        mut packet: RequestPacket,
    ) -> BoxFuture<'_, Result<(), RoutingError>> {
        Box::pin(async move {
            if i_am_inside(&packet.dest, self.view.as_ref()) {
                execute_mig(self.coord.as_ref(), self.view.as_ref(), &packet).await;
                return Ok(());
            }
            let pkt_id = crate::idgen::next_i32();
            packet.pkt_id = pkt_id;
            packet.src = self.my_tuple();
            let (tx, rx) = oneshot::channel();
            self.pending_mig.lock().await.insert(pkt_id, tx);
            let Some(stub) = self
                .stubs
                .gateway_stub(tuple_to_hc(&packet.dest, self.view.as_ref()))
            else {
                self.pending_mig.lock().await.remove(&pkt_id);
                return Err(RoutingError);
            };
            if stub.route_mig_request(packet).await.is_err() {
                self.pending_mig.lock().await.remove(&pkt_id);
                return Err(RoutingError);
            }
            match tokio::time::timeout(self.response_timeout, rx).await {
                Ok(Ok(())) => Ok(()),
                _ => {
                    self.pending_mig.lock().await.remove(&pkt_id);
                    Err(RoutingError)
                }
            }
        })
    }
}

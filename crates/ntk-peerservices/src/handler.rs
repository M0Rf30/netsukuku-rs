//! [`PeersRpcHandler`]: the inbound [`ntk_rpc::RpcHandler`] dispatching the 12
//! `MethodCall::peers_*` arms (`ntk-proto/proto/ntk.proto`) to a [`Handle`].
//!
//! Every arm's real work — routing, participation bookkeeping, gossip — can itself make further
//! outbound calls (`forward_peer_message` may hop for a while; `set_participant`/
//! `give_participant_maps` reflood). Those are spawned as independent tasks rather than awaited
//! inline, so one connection's dispatch loop is never blocked waiting on a multi-hop network
//! round trip a `notify`-style caller isn't even waiting for.

use std::fmt;

use futures::future::BoxFuture;
use ntk_common::Topology;
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::response_payload::Value as ResponseValue;
use ntk_proto::v1::{
    CallerContext, Empty, ErrorDomain, MethodCall, RemoteError, ResponsePayload, TypedValue,
};
use ntk_rpc::RpcHandler;

use crate::actor::{GetRequestOutcome, Handle};
use crate::error::Error;
use crate::service::{Refusal, ServiceId};
use crate::tuple::{TupleGNode, TupleNode};
use crate::wire::{
    pack_participant_set, unpack_forwarder, unpack_participant_set, unpack_tuple_gnode,
    unpack_tuple_node,
};

fn malformed(message: impl Into<String>) -> RemoteError {
    RemoteError {
        domain: ErrorDomain::Deserialize as i32,
        message: message.into(),
    }
}

fn decode_err(e: Error) -> RemoteError {
    malformed(e.to_string())
}

fn remote_err(domain: ErrorDomain, message: impl Into<String>) -> RemoteError {
    RemoteError {
        domain: domain as i32,
        message: message.into(),
    }
}

fn empty_ok() -> ResponsePayload {
    ResponsePayload {
        value: Some(ResponseValue::Empty(Empty::VALUE)),
    }
}

fn require_tuple_node(
    topology: &Topology,
    tv: Option<TypedValue>,
) -> Result<TupleNode, RemoteError> {
    let tv = tv.ok_or_else(|| malformed("missing respondant"))?;
    unpack_tuple_node(topology, &tv).map_err(decode_err)
}

fn require_tuple_gnode(
    topology: &Topology,
    tv: Option<TypedValue>,
) -> Result<TupleGNode, RemoteError> {
    let tv = tv.ok_or_else(|| malformed("missing tuple"))?;
    unpack_tuple_gnode(topology, &tv).map_err(decode_err)
}

/// Dispatches the PeerServices method surface onto a [`Handle`]. One instance, shared via
/// `Arc`, serves every connection an `ntk_rpc::TcpServer` (or `FakeRpcClient`) accepts —
/// matches `ntk_rpc::RpcHandler`'s own contract.
pub struct PeersRpcHandler {
    handle: Handle,
}

impl fmt::Debug for PeersRpcHandler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PeersRpcHandler").finish_non_exhaustive()
    }
}

impl PeersRpcHandler {
    /// Dispatches onto `handle`.
    #[must_use]
    pub fn new(handle: Handle) -> Self {
        Self { handle }
    }
}

impl RpcHandler for PeersRpcHandler {
    fn handle<'a>(
        &'a self,
        _caller: CallerContext,
        _unicast_id: TypedValue,
        call: MethodCall,
        // Hop-auth (hop-by-hop, `ntk-rpc`/`ntk-neighborhood`'s own concern) is orthogonal to
        // this crate's origin-auth (`crate::routing`'s `PeerMessageForwarder::auth`, verified
        // once by the servant deep inside `forward_msg`, not per RPC arm) — not consulted here.
        _auth: Option<ntk_proto::v1::Auth>,
    ) -> BoxFuture<'a, Result<ResponsePayload, RemoteError>> {
        Box::pin(async move {
            let topology = self.handle.topology().clone();
            let arm = call
                .call
                .ok_or_else(|| malformed("MethodCall.call unset"))?;
            match arm {
                Call::PeersForwardPeerMessage(tv) => {
                    let mf = unpack_forwarder(&topology, &tv).map_err(decode_err)?;
                    let handle = self.handle.clone();
                    tokio::spawn(async move { handle.forward_msg(mf).await });
                    Ok(empty_ok())
                }
                Call::PeersGetRequest(args) => {
                    let respondant = require_tuple_node(&topology, args.respondant)?;
                    match self.handle.get_request(args.msg_id, respondant).await {
                        Ok(payload) => Ok(ResponsePayload {
                            value: Some(ResponseValue::Typed(payload)),
                        }),
                        Err(GetRequestOutcome::UnknownMessage) => Err(remote_err(
                            ErrorDomain::PeersUnknownMessage,
                            "unknown msg_id",
                        )),
                        Err(GetRequestOutcome::InvalidRequest) => Err(remote_err(
                            ErrorDomain::PeersInvalidRequest,
                            "not the same g-node of research",
                        )),
                    }
                }
                Call::PeersSetResponse(args) => {
                    let response = args
                        .response
                        .ok_or_else(|| malformed("PeersSetResponseArgs.response unset"))?;
                    let respondant = require_tuple_node(&topology, args.respondant)?;
                    self.handle
                        .set_response(args.msg_id, response, respondant)
                        .await;
                    Ok(empty_ok())
                }
                Call::PeersSetRefuseMessage(args) => {
                    let respondant = require_tuple_node(&topology, args.respondant)?;
                    let level =
                        usize::try_from(args.e_lvl).map_err(|_| malformed("e_lvl out of range"))?;
                    // `e_lvl` names an ancestor level of `respondant`'s own scope
                    // (`tuple_gnode_containing`'s precondition, `tuple.rs`) — a peer-supplied
                    // `int32` with no other bound, so a level at or beyond `respondant.top()` is
                    // refused here rather than reaching the routing layer's fallback.
                    if level >= respondant.top() {
                        return Err(decode_err(Error::LevelOutOfRange {
                            level,
                            levels: respondant.top(),
                        }));
                    }
                    self.handle
                        .set_refuse_message(
                            args.msg_id,
                            Refusal {
                                level,
                                message: args.refuse_message,
                            },
                            respondant,
                        )
                        .await;
                    Ok(empty_ok())
                }
                Call::PeersSetRedoFromStart(args) => {
                    let respondant = require_tuple_node(&topology, args.respondant)?;
                    self.handle
                        .set_redo_from_start(args.msg_id, respondant)
                        .await;
                    Ok(empty_ok())
                }
                Call::PeersSetNextDestination(args) => {
                    let tuple = require_tuple_gnode(&topology, args.tuple)?;
                    self.handle.set_next_destination(args.msg_id, tuple).await;
                    Ok(empty_ok())
                }
                Call::PeersSetFailure(args) => {
                    let tuple = require_tuple_gnode(&topology, args.tuple)?;
                    self.handle.set_failure(args.msg_id, tuple).await;
                    Ok(empty_ok())
                }
                Call::PeersSetNonParticipant(args) => {
                    let tuple = require_tuple_gnode(&topology, args.tuple)?;
                    self.handle.set_non_participant(args.msg_id, tuple).await;
                    Ok(empty_ok())
                }
                Call::PeersSetMissingOptionalMaps(msg_id) => {
                    self.handle.set_missing_optional_maps(msg_id).await;
                    Ok(empty_ok())
                }
                Call::PeersSetParticipant(args) => {
                    let tuple = require_tuple_gnode(&topology, args.tuple)?;
                    let p_id = ServiceId::try_from(args.p_id).map_err(decode_err)?;
                    let handle = self.handle.clone();
                    tokio::spawn(async move { handle.handle_set_participant(p_id, tuple).await });
                    Ok(empty_ok())
                }
                Call::PeersGiveParticipantMaps(tv) => {
                    let maps = unpack_participant_set(&topology, &tv).map_err(decode_err)?;
                    let handle = self.handle.clone();
                    tokio::spawn(async move { handle.handle_give_participant_maps(maps).await });
                    Ok(empty_ok())
                }
                Call::PeersAskParticipantMaps(Empty {}) => {
                    let maps = self.handle.ask_participant_maps().await;
                    Ok(ResponsePayload {
                        value: Some(ResponseValue::Typed(pack_participant_set(&maps))),
                    })
                }
                _ => Err(malformed("not a PeerServices method")),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ntk_common::{HCoord, Naddr, Topology};
    use ntk_proto::v1::{PeersSetRefuseMessageArgs, TypedValue};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::actor::Manager;
    use crate::config::Config;
    use crate::stub::{PeersStub, RoutingEnv};
    use crate::wire::pack_tuple_node;

    /// Never actually reached by any test below: every case either gets refused before the
    /// handler touches routing state, or is a fire-and-forget `set_refuse_message` cast that
    /// never consults the environment either.
    struct NoopEnv;

    impl RoutingEnv for NoopEnv {
        fn gnode_exists(&self, _hc: HCoord) -> bool {
            false
        }

        fn gateway(
            &self,
            _hc: HCoord,
            _failed: Option<&Arc<dyn PeersStub>>,
        ) -> Option<Arc<dyn PeersStub>> {
            None
        }

        fn dial(&self, _n: &TupleNode) -> Option<Arc<dyn PeersStub>> {
            None
        }

        fn nodes_in_my_group(&self, _level: usize) -> usize {
            1
        }

        fn neighbors(&self) -> Vec<Arc<dyn PeersStub>> {
            Vec::new()
        }
    }

    /// A single-node handler over `gsizes`, positioned at `pos`. The backing actor is spawned
    /// (not just constructed) so the positive-control case's `set_refuse_message` cast has a
    /// live receiver, exactly like a real deployment.
    fn handler_for(gsizes: &[u32], pos: Vec<u32>) -> (PeersRpcHandler, CancellationToken) {
        let topology = Topology::new(gsizes.iter().copied()).unwrap();
        let my_pos = Naddr::new(topology.clone(), pos).unwrap();
        let (manager, handle) = Manager::new(
            topology.clone(),
            my_pos,
            Arc::new(NoopEnv),
            Config::default(),
            topology.levels(),
        );
        let cancel = CancellationToken::new();
        tokio::spawn(manager.run(cancel.child_token()));
        (PeersRpcHandler::new(handle), cancel)
    }

    fn refuse_call(topology: &Topology, respondant_pos: Vec<u32>, e_lvl: i32) -> MethodCall {
        let respondant = TupleNode::new(topology.clone(), respondant_pos).unwrap();
        MethodCall {
            call: Some(Call::PeersSetRefuseMessage(PeersSetRefuseMessageArgs {
                msg_id: 1,
                refuse_message: String::new(),
                e_lvl,
                respondant: Some(pack_tuple_node(&respondant)),
            })),
        }
    }

    fn caller() -> CallerContext {
        CallerContext {
            source_id: None,
            src_nic: None,
        }
    }

    fn unicast() -> TypedValue {
        TypedValue::new(String::new(), Vec::new())
    }

    // -----------------------------------------------------------------------------------------
    // Hostile / corrupt wire input: `e_lvl` (`PeersSetRefuseMessageArgs.e_lvl`) is an untrusted
    // wire `int32` with no bound of its own; every case below must be refused right here, not
    // left to `routing::tuple_gnode_containing`'s coarse fallback three layers downstream.
    // -----------------------------------------------------------------------------------------

    /// The respondant spans only 2 of the topology's 3 levels (`top() == 2`) — proves the bound
    /// checked is `respondant.top()`, not `topology.levels()` (which would wrongly accept `2`).
    #[tokio::test]
    async fn refuses_e_lvl_at_the_respondants_own_top() {
        let topology = Topology::new([2, 2, 2]).unwrap();
        let (handler, _cancel) = handler_for(&[2, 2, 2], vec![0, 0, 0]);
        let call = refuse_call(&topology, vec![0, 0], 2);
        let err = handler
            .handle(caller(), unicast(), call, None)
            .await
            .unwrap_err();
        assert_eq!(err.domain, ErrorDomain::Deserialize as i32);
    }

    #[tokio::test]
    async fn refuses_e_lvl_just_past_the_respondants_own_top() {
        let topology = Topology::new([2, 2]).unwrap();
        let (handler, _cancel) = handler_for(&[2, 2], vec![0, 0]);
        let call = refuse_call(&topology, vec![0, 0], 3);
        let err = handler
            .handle(caller(), unicast(), call, None)
            .await
            .unwrap_err();
        assert_eq!(err.domain, ErrorDomain::Deserialize as i32);
    }

    #[tokio::test]
    async fn refuses_e_lvl_far_past_the_respondants_own_top() {
        let topology = Topology::new([2, 2]).unwrap();
        let (handler, _cancel) = handler_for(&[2, 2], vec![0, 0]);
        let call = refuse_call(&topology, vec![0, 0], i32::MAX);
        let err = handler
            .handle(caller(), unicast(), call, None)
            .await
            .unwrap_err();
        assert_eq!(err.domain, ErrorDomain::Deserialize as i32);
    }

    #[tokio::test]
    async fn refuses_a_negative_e_lvl() {
        let topology = Topology::new([2, 2]).unwrap();
        let (handler, _cancel) = handler_for(&[2, 2], vec![0, 0]);
        let call = refuse_call(&topology, vec![0, 0], -1);
        let err = handler
            .handle(caller(), unicast(), call, None)
            .await
            .unwrap_err();
        assert_eq!(err.domain, ErrorDomain::Deserialize as i32);
    }

    #[tokio::test]
    async fn accepts_e_lvl_strictly_below_the_respondants_own_top() {
        let topology = Topology::new([2, 2]).unwrap();
        let (handler, _cancel) = handler_for(&[2, 2], vec![0, 0]);
        let call = refuse_call(&topology, vec![0, 0], 1);
        handler
            .handle(caller(), unicast(), call, None)
            .await
            .unwrap();
    }
}

//! Wire encoding: conversions between this crate's domain types and its own generated
//! `ntk.peerservices.v1` protobuf messages (`crate::v1`), plus [`RpcPeersStub`] — the
//! [`crate::PeersStub`] implementation that adapts a real `ntk_rpc::RpcClient` (or
//! `ntk_rpc::FakeRpcClient`, for tests) to this crate's typed outbound-call surface.

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use futures::future::BoxFuture;
use ntk_common::{HCoord, Topology};
use ntk_proto::domain::{from_typed_value, typed_value};
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::response_payload::Value as ResponseValue;
use ntk_proto::v1::{
    CallerContext, Empty, ErrorDomain, MethodCall, PeersMsgIdRespondantArgs, PeersMsgIdTupleArgs,
    PeersSetParticipantArgs, PeersSetRefuseMessageArgs, PeersSetResponseArgs, TypedValue,
};
use ntk_rpc::{RpcClient, RpcError};

use crate::error::Error;
use crate::participation::{ParticipantMap, ParticipantSet};
use crate::service::{Refusal, ServiceId};
use crate::stub::{GetRequestError, PeerMessageForwarder, PeersStub, StubCallError};
use crate::tuple::{TupleGNode, TupleNode};
use crate::v1 as wire;

const TAG_TUPLE_NODE: &str = "peerservices.PeerTupleNode";
const TAG_TUPLE_GNODE: &str = "peerservices.PeerTupleGNode";
const TAG_FORWARDER: &str = "peerservices.PeerMessageForwarder";
const TAG_PARTICIPANT_SET: &str = "peerservices.PeerParticipantSet";

impl From<&TupleNode> for wire::PeerTupleNode {
    fn from(t: &TupleNode) -> Self {
        wire::PeerTupleNode {
            pos: t.positions().to_vec(),
        }
    }
}

fn tuple_node_from_wire(topology: &Topology, w: &wire::PeerTupleNode) -> Result<TupleNode, Error> {
    TupleNode::new(topology.clone(), w.pos.clone())
}

impl From<&TupleGNode> for wire::PeerTupleGNode {
    fn from(t: &TupleGNode) -> Self {
        wire::PeerTupleGNode {
            pos: t.positions().to_vec(),
            top: t.top() as u32,
        }
    }
}

fn tuple_gnode_from_wire(
    topology: &Topology,
    w: &wire::PeerTupleGNode,
) -> Result<TupleGNode, Error> {
    TupleGNode::new(topology.clone(), w.top as usize, w.pos.clone())
}

impl From<&PeerMessageForwarder> for wire::PeerMessageForwarder {
    fn from(mf: &PeerMessageForwarder) -> Self {
        wire::PeerMessageForwarder {
            inside_level: mf.inside_level as u32,
            n: Some((&mf.n).into()),
            x_macron: mf.x_macron.as_ref().map(Into::into),
            lvl: mf.lvl as u32,
            pos: mf.pos,
            p_id: mf.p_id.into(),
            msg_id: mf.msg_id,
            exclude_tuple_list: mf.exclude_tuple_list.iter().map(Into::into).collect(),
            non_participant_tuple_list: mf
                .non_participant_tuple_list
                .iter()
                .map(Into::into)
                .collect(),
            auth: mf.auth.clone(),
        }
    }
}

fn forwarder_from_wire(
    topology: &Topology,
    w: &wire::PeerMessageForwarder,
) -> Result<PeerMessageForwarder, Error> {
    let n = tuple_node_from_wire(topology, w.n.as_ref().ok_or(Error::MissingField("n"))?)?;
    let x_macron = w
        .x_macron
        .as_ref()
        .map(|xm| tuple_node_from_wire(topology, xm))
        .transpose()?;
    let exclude_tuple_list = w
        .exclude_tuple_list
        .iter()
        .map(|t| tuple_gnode_from_wire(topology, t))
        .collect::<Result<Vec<_>, _>>()?;
    let non_participant_tuple_list = w
        .non_participant_tuple_list
        .iter()
        .map(|t| tuple_gnode_from_wire(topology, t))
        .collect::<Result<Vec<_>, _>>()?;
    // `lvl`/`pos` name the next hop's target g-node coordinate as a bare wire
    // `(level, position)` pair — `ntk_common::HCoord` carries no topology of its own to
    // revalidate against, so unlike every other field decoded here it isn't checked by a
    // `TupleNode`/`TupleGNode` constructor. `forward_msg` indexes `my_pos[lvl]` on the very
    // first line of its dispatch (`routing.rs`), and later builds a `TupleGNode` straight from
    // `HCoord::new(lvl, pos)` without revalidating `pos` either — both unconditionally reachable
    // from an untrusted `PeersForwardPeerMessage` notify. Bounded here, the one place this
    // function already has the topology in hand to check them.
    let lvl = w.lvl as usize;
    let gsize = topology.gsize(lvl).ok_or(Error::LevelOutOfRange {
        level: lvl,
        levels: topology.levels(),
    })?;
    if w.pos >= gsize {
        return Err(Error::PositionOutOfRange {
            level: lvl,
            pos: w.pos,
            gsize,
        });
    }
    Ok(PeerMessageForwarder {
        inside_level: w.inside_level as usize,
        n,
        x_macron,
        lvl,
        pos: w.pos,
        p_id: ServiceId::try_from(w.p_id)?,
        msg_id: w.msg_id,
        exclude_tuple_list,
        non_participant_tuple_list,
        auth: w.auth.clone(),
    })
}

fn participant_map_to_wire(m: &ParticipantMap) -> wire::PeerParticipantMap {
    wire::PeerParticipantMap {
        participant_list: m
            .participants()
            .map(ntk_proto::domain::v1::HCoord::from)
            .collect(),
    }
}

fn participant_map_from_wire(w: &wire::PeerParticipantMap) -> Result<ParticipantMap, Error> {
    w.participant_list
        .iter()
        .map(|h| HCoord::try_from(h).map_err(Error::from))
        .collect()
}

fn participant_set_to_wire(s: &ParticipantSet) -> wire::PeerParticipantSet {
    wire::PeerParticipantSet {
        retrieved_below_level: s.retrieved_below_level as u32,
        my_pos: s.my_pos.clone(),
        participant_set: s
            .participant_set
            .iter()
            .map(|(&p_id, m)| (i32::from(p_id), participant_map_to_wire(m)))
            .collect(),
    }
}

fn participant_set_from_wire(
    topology: &Topology,
    w: &wire::PeerParticipantSet,
) -> Result<ParticipantSet, Error> {
    let mut participant_set = BTreeMap::new();
    for (&p_id, m) in &w.participant_set {
        participant_set.insert(ServiceId::try_from(p_id)?, participant_map_from_wire(m)?);
    }
    let set = ParticipantSet {
        retrieved_below_level: w.retrieved_below_level as usize,
        my_pos: w.my_pos.clone(),
        participant_set,
    };
    // Every consumer of a decoded `ParticipantSet` assumes `my_pos.len() == topology.levels()`
    // (`fold_to_my_granularity` asserts it outright) and every coordinate in it is a
    // representable level/position — a peer's `my_pos`/`retrieved_below_level`/participant
    // coordinates are otherwise wire `uint32`s with no inherent bound. `ask_participant_maps`
    // already revalidates its own response leg this way (`check_valid`,
    // `research/impl/vala/peerservices/map_handler.vala:216-220`); this closes the same gap on
    // the inbound `PeersGiveParticipantMaps` leg, which previously skipped it entirely and could
    // reach `fold_to_my_granularity`'s assert with an attacker-controlled `my_pos` length.
    if !set.is_valid(topology) {
        return Err(Error::InvalidParticipantSet);
    }
    Ok(set)
}

pub(crate) fn pack_tuple_node(t: &TupleNode) -> TypedValue {
    typed_value(TAG_TUPLE_NODE, &wire::PeerTupleNode::from(t))
}

pub(crate) fn unpack_tuple_node(topology: &Topology, tv: &TypedValue) -> Result<TupleNode, Error> {
    let w: wire::PeerTupleNode = from_typed_value(tv, TAG_TUPLE_NODE)?;
    tuple_node_from_wire(topology, &w)
}

pub(crate) fn pack_tuple_gnode(t: &TupleGNode) -> TypedValue {
    typed_value(TAG_TUPLE_GNODE, &wire::PeerTupleGNode::from(t))
}

pub(crate) fn unpack_tuple_gnode(
    topology: &Topology,
    tv: &TypedValue,
) -> Result<TupleGNode, Error> {
    let w: wire::PeerTupleGNode = from_typed_value(tv, TAG_TUPLE_GNODE)?;
    tuple_gnode_from_wire(topology, &w)
}

pub(crate) fn pack_forwarder(mf: &PeerMessageForwarder) -> TypedValue {
    typed_value(TAG_FORWARDER, &wire::PeerMessageForwarder::from(mf))
}

pub(crate) fn unpack_forwarder(
    topology: &Topology,
    tv: &TypedValue,
) -> Result<PeerMessageForwarder, Error> {
    let w: wire::PeerMessageForwarder = from_typed_value(tv, TAG_FORWARDER)?;
    forwarder_from_wire(topology, &w)
}

pub(crate) fn pack_participant_set(s: &ParticipantSet) -> TypedValue {
    typed_value(TAG_PARTICIPANT_SET, &participant_set_to_wire(s))
}

pub(crate) fn unpack_participant_set(
    topology: &Topology,
    tv: &TypedValue,
) -> Result<ParticipantSet, Error> {
    let w: wire::PeerParticipantSet = from_typed_value(tv, TAG_PARTICIPANT_SET)?;
    participant_set_from_wire(topology, &w)
}

fn stub_err(e: RpcError) -> StubCallError {
    StubCallError(e.to_string())
}

/// Adapts an [`ntk_rpc::RpcClient`] (real `TcpRpcClient` or, for tests, `FakeRpcClient`) into
/// this crate's [`PeersStub`] surface: builds the `MethodCall` arm for each of the 12
/// PeerServices methods and encodes/decodes their `TypedValue` payloads. This is the "typed
/// per-method stub" half of the split `zcd`'s `rpcdesign` codegen used to produce in one shot
/// (`research/notes/02-vala-services-daemon.md` §1) — hand-written here since this workspace has
/// no IDL codegen.
pub struct RpcPeersStub {
    client: Arc<dyn RpcClient>,
    topology: Topology,
}

impl fmt::Debug for RpcPeersStub {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcPeersStub").finish_non_exhaustive()
    }
}

impl RpcPeersStub {
    /// Wraps `client`, decoding responses against `topology`.
    #[must_use]
    pub fn new(client: Arc<dyn RpcClient>, topology: Topology) -> Self {
        Self { client, topology }
    }

    /// The underlying transport connection, stable per real neighbor link. Lets a
    /// [`RoutingEnv`](crate::RoutingEnv) implementation recognize (by `Arc::ptr_eq`) that a
    /// previously-returned stub it now sees as `failed` was built from the same connection as a
    /// fresh candidate, and exclude it — see `RoutingEnv::gateway`'s own doc.
    #[must_use]
    pub fn client(&self) -> &Arc<dyn RpcClient> {
        &self.client
    }

    /// Identity/NIC addressing is out of this crate's scope (no `ntk-identities`/
    /// `ntk-neighborhood` dependency) — every call carries an empty [`CallerContext`], matching
    /// how `ntk_proto::v1::CallerContext`'s own doc comment describes those fields as belonging
    /// to those phase-2 crates.
    fn caller() -> CallerContext {
        CallerContext {
            source_id: None,
            src_nic: None,
        }
    }

    /// Target-identity dispatch (zcd's `unicast-id`) is likewise out of scope; every call
    /// targets an unnamed default identity.
    fn unicast_id() -> TypedValue {
        TypedValue::new(String::new(), Vec::new())
    }

    fn call(&self, call: Call) -> BoxFuture<'_, Result<ntk_proto::v1::ResponsePayload, RpcError>> {
        self.client.call(
            Self::caller(),
            Self::unicast_id(),
            MethodCall { call: Some(call) },
        )
    }

    fn notify(&self, call: Call) -> BoxFuture<'_, Result<(), RpcError>> {
        self.client.notify(
            Self::caller(),
            Self::unicast_id(),
            MethodCall { call: Some(call) },
        )
    }
}

fn domain_error(remote: &ntk_proto::v1::RemoteError) -> Option<ErrorDomain> {
    ErrorDomain::try_from(remote.domain).ok()
}

impl PeersStub for RpcPeersStub {
    fn forward_peer_message(
        &self,
        msg: PeerMessageForwarder,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let call = Call::PeersForwardPeerMessage(pack_forwarder(&msg));
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }

    fn get_request(
        &self,
        msg_id: i32,
        respondant: TupleNode,
    ) -> BoxFuture<'_, Result<TypedValue, GetRequestError>> {
        let call = Call::PeersGetRequest(PeersMsgIdRespondantArgs {
            msg_id,
            respondant: Some(pack_tuple_node(&respondant)),
        });
        Box::pin(async move {
            match self.call(call).await {
                Ok(payload) => match payload.value {
                    Some(ResponseValue::Typed(tv)) => Ok(tv),
                    _ => Err(GetRequestError::Call(StubCallError(
                        "get_request: malformed response".to_owned(),
                    ))),
                },
                Err(RpcError::Remote(remote)) => match domain_error(&remote) {
                    Some(ErrorDomain::PeersUnknownMessage) => Err(GetRequestError::UnknownMessage),
                    Some(ErrorDomain::PeersInvalidRequest) => Err(GetRequestError::InvalidRequest),
                    _ => Err(GetRequestError::Call(StubCallError(remote.message))),
                },
                Err(e) => Err(GetRequestError::Call(stub_err(e))),
            }
        })
    }

    fn set_response(
        &self,
        msg_id: i32,
        response: TypedValue,
        respondant: TupleNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let call = Call::PeersSetResponse(PeersSetResponseArgs {
            msg_id,
            response: Some(response),
            respondant: Some(pack_tuple_node(&respondant)),
        });
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }

    fn set_refuse_message(
        &self,
        msg_id: i32,
        refusal: Refusal,
        respondant: TupleNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let call = Call::PeersSetRefuseMessage(PeersSetRefuseMessageArgs {
            msg_id,
            refuse_message: refusal.message,
            e_lvl: refusal.level as i32,
            respondant: Some(pack_tuple_node(&respondant)),
        });
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }

    fn set_redo_from_start(
        &self,
        msg_id: i32,
        respondant: TupleNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let call = Call::PeersSetRedoFromStart(PeersMsgIdRespondantArgs {
            msg_id,
            respondant: Some(pack_tuple_node(&respondant)),
        });
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }

    fn set_next_destination(
        &self,
        msg_id: i32,
        tuple: TupleGNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let call = Call::PeersSetNextDestination(PeersMsgIdTupleArgs {
            msg_id,
            tuple: Some(pack_tuple_gnode(&tuple)),
        });
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }

    fn set_failure(
        &self,
        msg_id: i32,
        tuple: TupleGNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let call = Call::PeersSetFailure(PeersMsgIdTupleArgs {
            msg_id,
            tuple: Some(pack_tuple_gnode(&tuple)),
        });
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }

    fn set_non_participant(
        &self,
        msg_id: i32,
        tuple: TupleGNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let call = Call::PeersSetNonParticipant(PeersMsgIdTupleArgs {
            msg_id,
            tuple: Some(pack_tuple_gnode(&tuple)),
        });
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }

    fn set_missing_optional_maps(&self, msg_id: i32) -> BoxFuture<'_, Result<(), StubCallError>> {
        let call = Call::PeersSetMissingOptionalMaps(msg_id);
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }

    fn set_participant(
        &self,
        p_id: ServiceId,
        tuple: TupleGNode,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let call = Call::PeersSetParticipant(PeersSetParticipantArgs {
            p_id: p_id.into(),
            tuple: Some(pack_tuple_gnode(&tuple)),
        });
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }

    fn give_participant_maps(
        &self,
        maps: ParticipantSet,
    ) -> BoxFuture<'_, Result<(), StubCallError>> {
        let call = Call::PeersGiveParticipantMaps(pack_participant_set(&maps));
        Box::pin(async move { self.notify(call).await.map_err(stub_err) })
    }

    fn ask_participant_maps(&self) -> BoxFuture<'_, Result<ParticipantSet, StubCallError>> {
        let call = Call::PeersAskParticipantMaps(Empty::VALUE);
        Box::pin(async move {
            let payload = self.call(call).await.map_err(stub_err)?;
            match payload.value {
                // `unpack_participant_set` re-validates against `self.topology` before trusting
                // a peer's gossip snapshot (`check_valid`,
                // `research/impl/vala/peerservices/map_handler.vala:216-220`).
                Some(ResponseValue::Typed(tv)) => unpack_participant_set(&self.topology, &tv)
                    .map_err(|e| StubCallError(e.to_string())),
                _ => Err(StubCallError(
                    "ask_participant_maps: malformed response".to_owned(),
                )),
            }
        })
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use ntk_proto::domain::v1::HCoord as WireHCoord;

    use super::*;

    fn topology(gsizes: &[u32]) -> Topology {
        Topology::new(gsizes.iter().copied()).unwrap()
    }

    fn forwarder_wire(lvl: u32, pos: u32) -> wire::PeerMessageForwarder {
        wire::PeerMessageForwarder {
            inside_level: 0,
            n: Some(wire::PeerTupleNode { pos: vec![0, 0] }),
            x_macron: None,
            lvl,
            pos,
            p_id: 1,
            msg_id: 1,
            exclude_tuple_list: Vec::new(),
            non_participant_tuple_list: Vec::new(),
            auth: None,
        }
    }

    fn participant_set_wire(
        retrieved_below_level: u32,
        my_pos: Vec<u32>,
    ) -> wire::PeerParticipantSet {
        wire::PeerParticipantSet {
            retrieved_below_level,
            my_pos,
            participant_set: std::collections::HashMap::new(),
        }
    }

    // -----------------------------------------------------------------------------------------
    // Hostile / corrupt wire input: every case below must be REJECTED at the decode boundary,
    // never silently coerced into a valid-looking domain value or left to panic downstream
    // (`routing::Handle::forward_msg` indexes `my_pos[lvl]` on its very first line;
    // `tuple::make_tuple_gnode` builds a `TupleGNode` straight from `HCoord::new(lvl, pos)`
    // without revalidating `pos`; `participation::fold_to_my_granularity` asserts
    // `incoming.my_pos.len() == topology.levels()`).
    // -----------------------------------------------------------------------------------------

    #[test]
    fn rejects_forwarder_lvl_at_the_topology_boundary() {
        let t = topology(&[2, 2]); // 2 levels: valid lvl is 0 or 1.
        let w = forwarder_wire(2, 0); // lvl == topology.levels(): the first invalid value.
        let err = forwarder_from_wire(&t, &w).unwrap_err();
        assert!(matches!(
            err,
            Error::LevelOutOfRange {
                level: 2,
                levels: 2
            }
        ));
    }

    #[test]
    fn rejects_forwarder_lvl_just_past_the_topology_boundary() {
        let t = topology(&[2, 2]);
        let w = forwarder_wire(3, 0);
        let err = forwarder_from_wire(&t, &w).unwrap_err();
        assert!(matches!(
            err,
            Error::LevelOutOfRange {
                level: 3,
                levels: 2
            }
        ));
    }

    #[test]
    fn rejects_forwarder_lvl_far_past_the_topology_boundary() {
        let t = topology(&[2, 2]);
        let w = forwarder_wire(u32::MAX, 0);
        let err = forwarder_from_wire(&t, &w).unwrap_err();
        assert!(matches!(
            err,
            Error::LevelOutOfRange { level, levels: 2 } if level == u32::MAX as usize
        ));
    }

    /// `lvl` alone is a perfectly ordinary level; `pos` alone is a perfectly ordinary `u32`. The
    /// two together are inconsistent — `pos` has no meaning at a level whose g-node size is 3.
    #[test]
    fn rejects_forwarder_pos_inconsistent_with_its_own_level() {
        let t = topology(&[2, 3]);
        let w = forwarder_wire(1, 5);
        let err = forwarder_from_wire(&t, &w).unwrap_err();
        assert!(matches!(
            err,
            Error::PositionOutOfRange {
                level: 1,
                pos: 5,
                gsize: 3
            }
        ));
    }

    #[test]
    fn accepts_a_forwarder_with_a_valid_lvl_and_pos() {
        let t = topology(&[2, 3]);
        let w = forwarder_wire(1, 2);
        let mf = forwarder_from_wire(&t, &w).unwrap();
        assert_eq!(mf.lvl, 1);
        assert_eq!(mf.pos, 2);
    }

    #[test]
    fn rejects_participant_set_whose_my_pos_length_disagrees_with_the_topology() {
        // The exact shape that used to reach `fold_to_my_granularity`'s
        // `assert_eq!(incoming.my_pos.len(), levels)` and panic a live connection.
        let t = topology(&[2, 2, 2]);
        let w = participant_set_wire(0, vec![0, 0]); // 2 entries, topology has 3 levels.
        let err = participant_set_from_wire(&t, &w).unwrap_err();
        assert!(matches!(err, Error::InvalidParticipantSet));
    }

    #[test]
    fn rejects_participant_set_with_an_empty_my_pos() {
        let t = topology(&[2, 2, 2]);
        let w = participant_set_wire(0, Vec::new());
        let err = participant_set_from_wire(&t, &w).unwrap_err();
        assert!(matches!(err, Error::InvalidParticipantSet));
    }

    #[test]
    fn rejects_participant_set_with_retrieved_below_level_far_past_the_topology() {
        let t = topology(&[2, 2]);
        let w = participant_set_wire(u32::MAX, vec![0, 0]);
        let err = participant_set_from_wire(&t, &w).unwrap_err();
        assert!(matches!(err, Error::InvalidParticipantSet));
    }

    #[test]
    fn rejects_participant_set_with_an_out_of_range_participant_position() {
        let t = topology(&[2, 2]);
        let mut w = participant_set_wire(2, vec![0, 0]);
        w.participant_set.insert(
            1,
            wire::PeerParticipantMap {
                participant_list: vec![WireHCoord { level: 0, pos: 99 }],
            },
        );
        let err = participant_set_from_wire(&t, &w).unwrap_err();
        assert!(matches!(err, Error::InvalidParticipantSet));
    }

    #[test]
    fn accepts_a_well_formed_participant_set() {
        let t = topology(&[2, 2]);
        let mut w = participant_set_wire(2, vec![0, 0]);
        w.participant_set.insert(
            1,
            wire::PeerParticipantMap {
                participant_list: vec![WireHCoord { level: 1, pos: 1 }],
            },
        );
        let set = participant_set_from_wire(&t, &w).unwrap();
        assert_eq!(set.retrieved_below_level, 2);
        assert_eq!(set.my_pos, vec![0, 0]);
    }
}

//! Wire codec: converts between this crate's domain types
//! ([`crate::domain`]) and the `prost`-generated `proto/hooking.proto`
//! types ([`v1`]). `From` (domain -> wire) is infallible; `TryFrom`
//! (wire -> domain) never trusts a peer's shape (missing nested messages
//! are rejected, not defaulted).

use ntk_proto::domain::{DomainDecodeError, from_typed_value, typed_value};
use ntk_proto::v1::TypedValue;
use thiserror::Error;

use crate::domain::{
    DeleteReservationRequest, EntryData, ExploreGNodeRequest, ExploreGNodeResponse, MigOp,
    NetworkData, PairTupleGNodeInt, PathHop, RequestPacket, ResponsePacket,
    SearchMigrationPathErrorPkt, SearchMigrationPathRequest, SearchMigrationPathResponse,
    TupleGNode,
};

/// Generated protobuf types for `proto/hooking.proto` (package
/// `ntk.hooking.v1`). Doc comments on individual messages/fields are
/// copied from the `.proto` source by `prost-build`.
#[allow(clippy::doc_markdown)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/ntk.hooking.v1.rs"));
}

/// `type_tag` convention: `"hooking.<TypeName>"`.
pub const NETWORK_DATA_TAG: &str = "hooking.NetworkData";
pub const ENTRY_DATA_TAG: &str = "hooking.EntryData";
pub const SEARCH_REQUEST_TAG: &str = "hooking.SearchMigrationPathRequest";
pub const SEARCH_ERROR_TAG: &str = "hooking.SearchMigrationPathErrorPkt";
pub const SEARCH_RESPONSE_TAG: &str = "hooking.SearchMigrationPathResponse";
pub const EXPLORE_REQUEST_TAG: &str = "hooking.ExploreGNodeRequest";
pub const EXPLORE_RESPONSE_TAG: &str = "hooking.ExploreGNodeResponse";
pub const DELETE_RESERVE_REQUEST_TAG: &str = "hooking.DeleteReservationRequest";
pub const MIG_REQUEST_TAG: &str = "hooking.RequestPacket";
pub const MIG_RESPONSE_TAG: &str = "hooking.ResponsePacket";

/// Everything that can go wrong decoding a wire hooking message. Extends
/// [`DomainDecodeError`] (which has no generic "missing field" vocabulary,
/// only `Naddr`/`Cost`-specific variants) with the one this crate's own
/// messages need: an absent nested message where upstream's Vala classes
/// always construct their nested fields eagerly.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WireError {
    #[error(transparent)]
    Domain(#[from] DomainDecodeError),
    #[error("required field `{0}` was absent")]
    MissingField(&'static str),
    #[error("unrecognized enum value for `{0}`")]
    BadEnum(&'static str),
}

// ---------------------------------------------------------------------------
// TupleGNode / PathHop / PairTupleGNodeInt
// ---------------------------------------------------------------------------

impl From<&TupleGNode> for v1::TupleGNode {
    fn from(t: &TupleGNode) -> Self {
        v1::TupleGNode {
            pos: t.pos.clone(),
            eldership: t.eldership.clone(),
        }
    }
}

impl From<&v1::TupleGNode> for TupleGNode {
    fn from(t: &v1::TupleGNode) -> Self {
        TupleGNode {
            pos: t.pos.clone(),
            eldership: t.eldership.clone(),
        }
    }
}

impl From<&PathHop> for v1::PathHop {
    fn from(h: &PathHop) -> Self {
        v1::PathHop {
            visiting_gnode: Some(v1::TupleGNode::from(&h.visiting_gnode)),
            previous_migrating_gnode: h
                .previous_migrating_gnode
                .as_ref()
                .map(v1::TupleGNode::from),
        }
    }
}

impl TryFrom<&v1::PathHop> for PathHop {
    type Error = WireError;
    fn try_from(h: &v1::PathHop) -> Result<Self, Self::Error> {
        let visiting_gnode = h
            .visiting_gnode
            .as_ref()
            .map(TupleGNode::from)
            .ok_or(WireError::MissingField("PathHop.visiting_gnode"))?;
        Ok(PathHop {
            visiting_gnode,
            previous_migrating_gnode: h.previous_migrating_gnode.as_ref().map(TupleGNode::from),
        })
    }
}

impl From<&PairTupleGNodeInt> for v1::PairTupleGNodeInt {
    fn from(p: &PairTupleGNodeInt) -> Self {
        v1::PairTupleGNodeInt {
            gnode: Some(v1::TupleGNode::from(&p.gnode)),
            border_real_pos: p.border_real_pos,
        }
    }
}

impl TryFrom<&v1::PairTupleGNodeInt> for PairTupleGNodeInt {
    type Error = WireError;
    fn try_from(p: &v1::PairTupleGNodeInt) -> Result<Self, Self::Error> {
        let gnode = p
            .gnode
            .as_ref()
            .map(TupleGNode::from)
            .ok_or(WireError::MissingField("PairTupleGNodeInt.gnode"))?;
        Ok(PairTupleGNodeInt {
            gnode,
            border_real_pos: p.border_real_pos,
        })
    }
}

fn encode_hops(hops: &[PathHop]) -> Vec<v1::PathHop> {
    hops.iter().map(v1::PathHop::from).collect()
}

fn decode_hops(hops: &[v1::PathHop]) -> Result<Vec<PathHop>, WireError> {
    hops.iter().map(PathHop::try_from).collect()
}

fn encode_adjacent(set: &[PairTupleGNodeInt]) -> Vec<v1::PairTupleGNodeInt> {
    set.iter().map(v1::PairTupleGNodeInt::from).collect()
}

fn decode_adjacent(set: &[v1::PairTupleGNodeInt]) -> Result<Vec<PairTupleGNodeInt>, WireError> {
    set.iter().map(PairTupleGNodeInt::try_from).collect()
}

// ---------------------------------------------------------------------------
// NetworkData / EntryData
// ---------------------------------------------------------------------------

impl From<&NetworkData> for v1::NetworkData {
    fn from(n: &NetworkData) -> Self {
        v1::NetworkData {
            network_id: n.network_id,
            neighbor_n_nodes: n.neighbor_n_nodes,
            neighbor_min_level: u32::try_from(n.neighbor_min_level).unwrap_or(u32::MAX),
            gsizes: n.gsizes.clone(),
            neighbor_pos: n.neighbor_pos.clone(),
        }
    }
}

impl From<&v1::NetworkData> for NetworkData {
    fn from(n: &v1::NetworkData) -> Self {
        NetworkData {
            network_id: n.network_id,
            neighbor_n_nodes: n.neighbor_n_nodes,
            neighbor_min_level: n.neighbor_min_level as usize,
            gsizes: n.gsizes.clone(),
            neighbor_pos: n.neighbor_pos.clone(),
        }
    }
}

impl From<&EntryData> for v1::EntryData {
    fn from(e: &EntryData) -> Self {
        v1::EntryData {
            network_id: e.network_id,
            pos: e.pos.clone(),
            elderships: e.elderships.clone(),
        }
    }
}

impl From<&v1::EntryData> for EntryData {
    fn from(e: &v1::EntryData) -> Self {
        EntryData {
            network_id: e.network_id,
            pos: e.pos.clone(),
            elderships: e.elderships.clone(),
        }
    }
}

/// Encodes `n` as a [`TypedValue`] tagged [`NETWORK_DATA_TAG`].
#[must_use]
pub fn encode_network_data(n: &NetworkData) -> TypedValue {
    typed_value(NETWORK_DATA_TAG, &v1::NetworkData::from(n))
}

/// # Errors
/// [`WireError`] on a `type_tag` mismatch or decode failure.
pub fn decode_network_data(tv: &TypedValue) -> Result<NetworkData, WireError> {
    let n: v1::NetworkData = from_typed_value(tv, NETWORK_DATA_TAG)?;
    Ok(NetworkData::from(&n))
}

/// Encodes `e` as a [`TypedValue`] tagged [`ENTRY_DATA_TAG`].
#[must_use]
pub fn encode_entry_data(e: &EntryData) -> TypedValue {
    typed_value(ENTRY_DATA_TAG, &v1::EntryData::from(e))
}

/// # Errors
/// [`WireError`] on a `type_tag` mismatch or decode failure.
pub fn decode_entry_data(tv: &TypedValue) -> Result<EntryData, WireError> {
    let e: v1::EntryData = from_typed_value(tv, ENTRY_DATA_TAG)?;
    Ok(EntryData::from(&e))
}

// ---------------------------------------------------------------------------
// Search / explore / delete-reserve / mig envelopes
// ---------------------------------------------------------------------------

impl From<&SearchMigrationPathRequest> for v1::SearchMigrationPathRequest {
    fn from(r: &SearchMigrationPathRequest) -> Self {
        v1::SearchMigrationPathRequest {
            pkt_id: r.pkt_id,
            origin: Some(v1::TupleGNode::from(&r.origin)),
            caller: Some(v1::TupleGNode::from(&r.caller)),
            path_hops: encode_hops(&r.path_hops),
            max_host_lvl: u32::try_from(r.max_host_lvl).unwrap_or(u32::MAX),
            reserve_request_id: r.reserve_request_id,
        }
    }
}

impl TryFrom<&v1::SearchMigrationPathRequest> for SearchMigrationPathRequest {
    type Error = WireError;
    fn try_from(r: &v1::SearchMigrationPathRequest) -> Result<Self, Self::Error> {
        Ok(SearchMigrationPathRequest {
            pkt_id: r.pkt_id,
            origin: r
                .origin
                .as_ref()
                .map(TupleGNode::from)
                .ok_or(WireError::MissingField("SearchMigrationPathRequest.origin"))?,
            caller: r
                .caller
                .as_ref()
                .map(TupleGNode::from)
                .ok_or(WireError::MissingField("SearchMigrationPathRequest.caller"))?,
            path_hops: decode_hops(&r.path_hops)?,
            max_host_lvl: r.max_host_lvl as usize,
            reserve_request_id: r.reserve_request_id,
        })
    }
}

impl From<&SearchMigrationPathErrorPkt> for v1::SearchMigrationPathErrorPkt {
    fn from(p: &SearchMigrationPathErrorPkt) -> Self {
        v1::SearchMigrationPathErrorPkt {
            pkt_id: p.pkt_id,
            origin: Some(v1::TupleGNode::from(&p.origin)),
        }
    }
}

impl TryFrom<&v1::SearchMigrationPathErrorPkt> for SearchMigrationPathErrorPkt {
    type Error = WireError;
    fn try_from(p: &v1::SearchMigrationPathErrorPkt) -> Result<Self, Self::Error> {
        Ok(SearchMigrationPathErrorPkt {
            pkt_id: p.pkt_id,
            origin: p
                .origin
                .as_ref()
                .map(TupleGNode::from)
                .ok_or(WireError::MissingField(
                    "SearchMigrationPathErrorPkt.origin",
                ))?,
        })
    }
}

fn opt_i32(v: Option<u32>) -> i32 {
    v.map_or(-1, |x| i32::try_from(x).unwrap_or(-1))
}
fn opt_i32_signed(v: Option<i32>) -> i32 {
    v.unwrap_or(-1)
}
fn i32_opt_u32(v: i32) -> Option<u32> {
    u32::try_from(v).ok()
}
fn i32_opt_i32(v: i32) -> Option<i32> {
    (v != -1).then_some(v)
}

impl From<&SearchMigrationPathResponse> for v1::SearchMigrationPathResponse {
    fn from(r: &SearchMigrationPathResponse) -> Self {
        v1::SearchMigrationPathResponse {
            pkt_id: r.pkt_id,
            origin: Some(v1::TupleGNode::from(&r.origin)),
            min_host_lvl: u32::try_from(r.min_host_lvl).unwrap_or(u32::MAX),
            set_adjacent: encode_adjacent(&r.set_adjacent),
            final_host_lvl: i32::try_from(r.final_host_lvl).unwrap_or(i32::MAX),
            real_new_pos: opt_i32(r.real_new_pos),
            real_new_eldership: opt_i32_signed(r.real_new_eldership),
            new_conn_vir_pos: opt_i32(r.new_conn_vir_pos),
            new_eldership: opt_i32_signed(r.new_eldership),
        }
    }
}

impl TryFrom<&v1::SearchMigrationPathResponse> for SearchMigrationPathResponse {
    type Error = WireError;
    fn try_from(r: &v1::SearchMigrationPathResponse) -> Result<Self, Self::Error> {
        Ok(SearchMigrationPathResponse {
            pkt_id: r.pkt_id,
            origin: r
                .origin
                .as_ref()
                .map(TupleGNode::from)
                .ok_or(WireError::MissingField(
                    "SearchMigrationPathResponse.origin",
                ))?,
            min_host_lvl: r.min_host_lvl as usize,
            set_adjacent: decode_adjacent(&r.set_adjacent)?,
            final_host_lvl: r.final_host_lvl.max(0) as usize,
            real_new_pos: i32_opt_u32(r.real_new_pos),
            real_new_eldership: i32_opt_i32(r.real_new_eldership),
            new_conn_vir_pos: i32_opt_u32(r.new_conn_vir_pos),
            new_eldership: i32_opt_i32(r.new_eldership),
        })
    }
}

impl From<&ExploreGNodeRequest> for v1::ExploreGNodeRequest {
    fn from(r: &ExploreGNodeRequest) -> Self {
        v1::ExploreGNodeRequest {
            pkt_id: r.pkt_id,
            origin: Some(v1::TupleGNode::from(&r.origin)),
            path_hops: encode_hops(&r.path_hops),
            requested_lvl: u32::try_from(r.requested_lvl).unwrap_or(u32::MAX),
        }
    }
}

impl TryFrom<&v1::ExploreGNodeRequest> for ExploreGNodeRequest {
    type Error = WireError;
    fn try_from(r: &v1::ExploreGNodeRequest) -> Result<Self, Self::Error> {
        Ok(ExploreGNodeRequest {
            pkt_id: r.pkt_id,
            origin: r
                .origin
                .as_ref()
                .map(TupleGNode::from)
                .ok_or(WireError::MissingField("ExploreGNodeRequest.origin"))?,
            path_hops: decode_hops(&r.path_hops)?,
            requested_lvl: r.requested_lvl as usize,
        })
    }
}

impl From<&ExploreGNodeResponse> for v1::ExploreGNodeResponse {
    fn from(r: &ExploreGNodeResponse) -> Self {
        v1::ExploreGNodeResponse {
            pkt_id: r.pkt_id,
            origin: Some(v1::TupleGNode::from(&r.origin)),
            result: Some(v1::TupleGNode::from(&r.result)),
        }
    }
}

impl TryFrom<&v1::ExploreGNodeResponse> for ExploreGNodeResponse {
    type Error = WireError;
    fn try_from(r: &v1::ExploreGNodeResponse) -> Result<Self, Self::Error> {
        Ok(ExploreGNodeResponse {
            pkt_id: r.pkt_id,
            origin: r
                .origin
                .as_ref()
                .map(TupleGNode::from)
                .ok_or(WireError::MissingField("ExploreGNodeResponse.origin"))?,
            result: r
                .result
                .as_ref()
                .map(TupleGNode::from)
                .ok_or(WireError::MissingField("ExploreGNodeResponse.result"))?,
        })
    }
}

impl From<&DeleteReservationRequest> for v1::DeleteReservationRequest {
    fn from(r: &DeleteReservationRequest) -> Self {
        v1::DeleteReservationRequest {
            dest_gnode: Some(v1::TupleGNode::from(&r.dest_gnode)),
            reserve_request_id: r.reserve_request_id,
        }
    }
}

impl TryFrom<&v1::DeleteReservationRequest> for DeleteReservationRequest {
    type Error = WireError;
    fn try_from(r: &v1::DeleteReservationRequest) -> Result<Self, Self::Error> {
        Ok(DeleteReservationRequest {
            dest_gnode: r.dest_gnode.as_ref().map(TupleGNode::from).ok_or(
                WireError::MissingField("DeleteReservationRequest.dest_gnode"),
            )?,
            reserve_request_id: r.reserve_request_id,
        })
    }
}

impl From<MigOp> for v1::MigOp {
    fn from(op: MigOp) -> Self {
        match op {
            MigOp::PrepareMigration => v1::MigOp::PrepareMigration,
            MigOp::FinishMigration => v1::MigOp::FinishMigration,
        }
    }
}

impl TryFrom<i32> for MigOp {
    type Error = WireError;
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v1::MigOp::try_from(v).ok() {
            Some(v1::MigOp::PrepareMigration) => Ok(MigOp::PrepareMigration),
            Some(v1::MigOp::FinishMigration) => Ok(MigOp::FinishMigration),
            _ => Err(WireError::BadEnum("RequestPacket.operation")),
        }
    }
}

impl From<&RequestPacket> for v1::RequestPacket {
    fn from(p: &RequestPacket) -> Self {
        v1::RequestPacket {
            pkt_id: p.pkt_id,
            dest: Some(v1::TupleGNode::from(&p.dest)),
            src: Some(v1::TupleGNode::from(&p.src)),
            operation: v1::MigOp::from(p.operation) as i32,
            migration_id: p.migration_id,
            conn_gnode_pos: p.conn_gnode_pos,
            host_gnode: Some(v1::TupleGNode::from(&p.host_gnode)),
            real_new_pos: p.real_new_pos,
            real_new_eldership: p.real_new_eldership,
        }
    }
}

impl TryFrom<&v1::RequestPacket> for RequestPacket {
    type Error = WireError;
    fn try_from(p: &v1::RequestPacket) -> Result<Self, Self::Error> {
        Ok(RequestPacket {
            pkt_id: p.pkt_id,
            dest: p
                .dest
                .as_ref()
                .map(TupleGNode::from)
                .ok_or(WireError::MissingField("RequestPacket.dest"))?,
            src: p
                .src
                .as_ref()
                .map(TupleGNode::from)
                .ok_or(WireError::MissingField("RequestPacket.src"))?,
            operation: MigOp::try_from(p.operation)?,
            migration_id: p.migration_id,
            conn_gnode_pos: p.conn_gnode_pos,
            host_gnode: p
                .host_gnode
                .as_ref()
                .map(TupleGNode::from)
                .ok_or(WireError::MissingField("RequestPacket.host_gnode"))?,
            real_new_pos: p.real_new_pos,
            real_new_eldership: p.real_new_eldership,
        })
    }
}

impl From<&ResponsePacket> for v1::ResponsePacket {
    fn from(p: &ResponsePacket) -> Self {
        v1::ResponsePacket {
            pkt_id: p.pkt_id,
            dest: Some(v1::TupleGNode::from(&p.dest)),
        }
    }
}

impl TryFrom<&v1::ResponsePacket> for ResponsePacket {
    type Error = WireError;
    fn try_from(p: &v1::ResponsePacket) -> Result<Self, Self::Error> {
        Ok(ResponsePacket {
            pkt_id: p.pkt_id,
            dest: p
                .dest
                .as_ref()
                .map(TupleGNode::from)
                .ok_or(WireError::MissingField("ResponsePacket.dest"))?,
        })
    }
}

macro_rules! typed_codec {
    ($encode:ident, $decode:ident, $domain:ty, $wire:ty, $tag:expr) => {
        #[must_use]
        pub fn $encode(v: &$domain) -> TypedValue {
            typed_value($tag, &<$wire>::from(v))
        }
        /// # Errors
        /// [`WireError`] on a `type_tag` mismatch or decode failure.
        pub fn $decode(tv: &TypedValue) -> Result<$domain, WireError> {
            let wire: $wire = from_typed_value(tv, $tag)?;
            <$domain>::try_from(&wire)
        }
    };
}

typed_codec!(
    encode_search_request,
    decode_search_request,
    SearchMigrationPathRequest,
    v1::SearchMigrationPathRequest,
    SEARCH_REQUEST_TAG
);
typed_codec!(
    encode_search_error,
    decode_search_error,
    SearchMigrationPathErrorPkt,
    v1::SearchMigrationPathErrorPkt,
    SEARCH_ERROR_TAG
);
typed_codec!(
    encode_search_response,
    decode_search_response,
    SearchMigrationPathResponse,
    v1::SearchMigrationPathResponse,
    SEARCH_RESPONSE_TAG
);
typed_codec!(
    encode_explore_request,
    decode_explore_request,
    ExploreGNodeRequest,
    v1::ExploreGNodeRequest,
    EXPLORE_REQUEST_TAG
);
typed_codec!(
    encode_explore_response,
    decode_explore_response,
    ExploreGNodeResponse,
    v1::ExploreGNodeResponse,
    EXPLORE_RESPONSE_TAG
);
typed_codec!(
    encode_delete_reserve_request,
    decode_delete_reserve_request,
    DeleteReservationRequest,
    v1::DeleteReservationRequest,
    DELETE_RESERVE_REQUEST_TAG
);
typed_codec!(
    encode_mig_request,
    decode_mig_request,
    RequestPacket,
    v1::RequestPacket,
    MIG_REQUEST_TAG
);
typed_codec!(
    encode_mig_response,
    decode_mig_response,
    ResponsePacket,
    v1::ResponsePacket,
    MIG_RESPONSE_TAG
);

//! Crate-wide construction, validation, and wire-decode error type.

use thiserror::Error;

/// Everything that can go wrong constructing this crate's own domain types or decoding them off
/// the wire. Protocol-level *outcomes* (a servant's refusal, a routing failure, a call timeout)
/// are their own types ([`crate::service::ExecError`], [`crate::routing::ContactPeerError`],
/// [`crate::stub::StubCallError`]) — this enum is only for "the data was malformed."
#[derive(Debug, Error)]
pub enum Error {
    /// A [`crate::TupleNode`]/[`crate::TupleGNode`] was asked to span more levels than its
    /// [`ntk_common::Topology`] has.
    #[error("tuple spans {top} levels, topology has only {levels}")]
    TopOutOfRange { top: usize, levels: usize },

    /// A position was `>=` its level's g-node size (`check_valid`,
    /// `research/impl/vala/peerservices/serializables.vala:84-94,179-190`).
    #[error("position {pos} at level {level} is out of range: g-node size is {gsize}")]
    PositionOutOfRange { level: usize, pos: u32, gsize: u32 },

    /// A [`crate::TupleGNode`] must name at least one level (`check_valid`, `serializables.vala:181`).
    #[error("g-node tuple must name at least one level")]
    EmptyGNodeTuple,

    /// A [`crate::TupleGNode`]'s tuple cannot be longer than its own `top`
    /// (`check_valid`, `serializables.vala:183`).
    #[error("g-node tuple has {len} entries, which exceeds top={top}")]
    GNodeTupleTooLong { len: usize, top: usize },

    /// [`crate::service::ServiceId`] must fit RFC 0014 §Definition 2.2's PID space,
    /// `0..=2^16-1` ("The Netsukuku network can host up to 2^16 different P2P services").
    #[error("service id {0} exceeds the RFC 0014 PID space (0..=65535)")]
    ServiceIdOutOfRange(u32),

    /// A wire-decoded level (bare, or paired with a position naming a g-node) is not a valid
    /// level for the topology or tuple scope it was decoded against — e.g.
    /// `PeerMessageForwarder.lvl` against this node's own [`ntk_common::Topology`], or
    /// `PeersSetRefuseMessageArgs.e_lvl` against the respondant tuple's own scope
    /// (`handler.rs`'s `PeersSetRefuseMessage` arm). Both travel as an untrusted wire integer
    /// with no inherent upper bound; this is the crate's single point of refusal for that class
    /// of violation.
    #[error("level {level} is out of range: only {levels} level(s) are valid here")]
    LevelOutOfRange { level: usize, levels: usize },

    /// A decoded [`crate::ParticipantSet`] does not revalidate against the topology it was
    /// decoded against: `my_pos`'s length, `retrieved_below_level`, or a participant coordinate
    /// is out of range (`check_valid`, `research/impl/vala/peerservices/serializables.vala:
    /// 503-518`).
    #[error("participant set is invalid for this topology")]
    InvalidParticipantSet,

    /// A wire message was missing a field this implementation requires.
    #[error("wire message missing required field {0:?}")]
    MissingField(&'static str),

    /// Re-validating a decoded [`ntk_proto::domain::v1`] value against `ntk-common` failed.
    #[error("domain decode error: {0}")]
    Domain(#[from] ntk_proto::domain::DomainDecodeError),

    /// A [`ntk_proto::v1::TypedValue`] carried an unexpected `type_tag`.
    #[error("typed_value tag mismatch: expected {expected:?}, got {actual:?}")]
    TypeTagMismatch { expected: String, actual: String },

    /// A `TypedValue.payload` did not parse as the expected protobuf message.
    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),
}

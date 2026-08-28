//! Shared domain wire codec.
//!
//! Converts between `ntk-common`'s validated in-memory domain types
//! (`Topology`, `Naddr`, `HCoord`, `Fingerprint`, `Cost`) and their
//! `proto/domain.proto` (`ntk.domain.v1`) wire form, and provides the
//! `TypedValue` encode/decode helpers every phase-2 module payload type uses
//! to travel inside [`crate::v1::TypedValue`] (`type_tag = "<module>.<Type>"`).
//!
//! `From` conversions (domain -> wire) are infallible: an `ntk-common` value
//! is already valid by construction, so encoding it cannot fail. `TryFrom`
//! conversions (wire -> domain) are fallible and *revalidate* through
//! `ntk-common`'s own constructors — a decoded value never bypasses the
//! invariants `Topology`/`Naddr`/`Fingerprint` enforce at construction time,
//! because a wire message may have come from a hostile or buggy peer.

/// Generated protobuf types for the domain vocabulary (`proto/domain.proto`,
/// package `ntk.domain.v1`). Doc comments on individual messages/fields are
/// copied from the `.proto` source by `prost-build`.
#[allow(clippy::doc_markdown)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/ntk.domain.v1.rs"));
}

use thiserror::Error;

/// Everything that can go wrong decoding a wire domain value. Either
/// `ntk-common` rejected the revalidated value ([`DomainDecodeError::Invalid`]),
/// or the wire message itself was structurally incomplete in a way
/// `ntk_common::Error` has no vocabulary for — an absent `oneof` arm, a
/// missing required nested message — because those states cannot arise from
/// an in-memory `ntk-common` value in the first place.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DomainDecodeError {
    /// The decoded value failed one of `ntk-common`'s own validity checks
    /// (out-of-range position, zero g-node size, mismatched topology, ...).
    #[error(transparent)]
    Invalid(#[from] ntk_common::Error),

    /// [`v1::Cost`]'s `value` oneof had no arm set.
    #[error("Cost message is missing its `value` oneof")]
    MissingCostValue,

    /// [`v1::Naddr`]'s `topology` field was absent.
    #[error("Naddr message is missing its `topology` field")]
    MissingTopology,

    /// A wire `uint64` did not fit in the target native integer width (only
    /// reachable on a host where `usize` is narrower than 64 bits).
    #[error("value {0} does not fit in the target integer width")]
    IntegerOutOfRange(u64),

    /// [`from_typed_value`] was given a [`crate::v1::TypedValue`]-shaped message
    /// (`crate::v1::TypedValue`) whose `type_tag` did not match what the
    /// caller expected.
    #[error("TypedValue type_tag mismatch: expected {expected:?}, got {actual:?}")]
    TypeTagMismatch {
        /// The tag the caller asked for.
        expected: String,
        /// The tag actually present on the wire value.
        actual: String,
    },

    /// [`from_typed_value`]'s `M::decode` failed on the `payload` bytes.
    #[error("failed to decode TypedValue payload: {0}")]
    PayloadDecode(String),

    /// [`v1::UnicastId`]'s `kind` oneof had no arm set.
    #[error("UnicastId message is missing its `kind` oneof")]
    MissingUnicastIdKind,
}

// ---------------------------------------------------------------------------
// Topology
// ---------------------------------------------------------------------------

impl From<&ntk_common::Topology> for v1::Topology {
    fn from(topology: &ntk_common::Topology) -> Self {
        v1::Topology {
            gsizes: topology.gsizes().to_vec(),
        }
    }
}

impl TryFrom<&v1::Topology> for ntk_common::Topology {
    type Error = DomainDecodeError;

    fn try_from(wire: &v1::Topology) -> Result<Self, Self::Error> {
        Ok(ntk_common::Topology::new(wire.gsizes.iter().copied())?)
    }
}

// ---------------------------------------------------------------------------
// Naddr
// ---------------------------------------------------------------------------

impl From<&ntk_common::Naddr> for v1::Naddr {
    fn from(naddr: &ntk_common::Naddr) -> Self {
        v1::Naddr {
            topology: Some(v1::Topology::from(naddr.topology())),
            pos: naddr.positions().to_vec(),
        }
    }
}

/// Decodes a peer-supplied address, revalidating its level count against the
/// topology it carries — a peer is never trusted to send a well-formed one.
///
/// Uses [`ntk_common::Naddr::new_allowing_virtual`] rather than
/// [`ntk_common::Naddr::new`]: a *virtual* position (`pos >= gsize(level)`)
/// is a legitimate protocol state, not malformed input. It is how a g-node
/// mid-migration describes itself before its entry completes
/// (`is_real_from_to`,
/// `research/impl/vala/qspn/testsuites/system_peer/serializables.vala:20-25`),
/// and `ntk-qspn`'s entering identities put exactly such an address on the
/// wire. Rejecting it here would make migration traffic undecodable the moment
/// it crossed a real socket, while still passing every fake-transport test.
/// The level-count check is retained, so a structurally wrong address is still
/// refused.
impl TryFrom<&v1::Naddr> for ntk_common::Naddr {
    type Error = DomainDecodeError;

    fn try_from(wire: &v1::Naddr) -> Result<Self, Self::Error> {
        let topology_wire = wire
            .topology
            .as_ref()
            .ok_or(DomainDecodeError::MissingTopology)?;
        let topology = ntk_common::Topology::try_from(topology_wire)?;
        Ok(ntk_common::Naddr::new_allowing_virtual(
            topology,
            wire.pos.iter().copied(),
        )?)
    }
}

// ---------------------------------------------------------------------------
// HCoord
// ---------------------------------------------------------------------------

impl From<ntk_common::HCoord> for v1::HCoord {
    fn from(hcoord: ntk_common::HCoord) -> Self {
        v1::HCoord {
            level: hcoord.level as u64,
            pos: hcoord.pos,
        }
    }
}

impl TryFrom<&v1::HCoord> for ntk_common::HCoord {
    type Error = DomainDecodeError;

    fn try_from(wire: &v1::HCoord) -> Result<Self, Self::Error> {
        let level = usize::try_from(wire.level)
            .map_err(|_| DomainDecodeError::IntegerOutOfRange(wire.level))?;
        Ok(ntk_common::HCoord::new(level, wire.pos))
    }
}

// ---------------------------------------------------------------------------
// Cost
// ---------------------------------------------------------------------------

impl From<ntk_common::Cost> for v1::Cost {
    fn from(cost: ntk_common::Cost) -> Self {
        use v1::cost::Value;
        let value = match cost {
            ntk_common::Cost::Null => Value::Null(true),
            ntk_common::Cost::Finite(magnitude) => Value::Finite(magnitude),
            ntk_common::Cost::Dead => Value::Dead(true),
        };
        v1::Cost { value: Some(value) }
    }
}

impl TryFrom<&v1::Cost> for ntk_common::Cost {
    type Error = DomainDecodeError;

    fn try_from(wire: &v1::Cost) -> Result<Self, Self::Error> {
        use v1::cost::Value;
        match wire.value {
            Some(Value::Null(_)) => Ok(ntk_common::Cost::Null),
            Some(Value::Finite(magnitude)) => Ok(ntk_common::Cost::Finite(magnitude)),
            Some(Value::Dead(_)) => Ok(ntk_common::Cost::Dead),
            None => Err(DomainDecodeError::MissingCostValue),
        }
    }
}

// ---------------------------------------------------------------------------
// Fingerprint
// ---------------------------------------------------------------------------

impl From<&ntk_common::Fingerprint<Vec<u8>>> for v1::Fingerprint {
    fn from(fingerprint: &ntk_common::Fingerprint<Vec<u8>>) -> Self {
        let parts = fingerprint.to_parts();
        v1::Fingerprint {
            id: parts.id,
            level: parts.level as u64,
            eldership: parts.eldership,
            pending_elderships: parts.pending_elderships,
            elderships_seed: parts
                .elderships_seed
                .into_iter()
                .map(|value| v1::OptionalEldership { value })
                .collect(),
        }
    }
}

impl TryFrom<&v1::Fingerprint> for ntk_common::Fingerprint<Vec<u8>> {
    type Error = DomainDecodeError;

    fn try_from(wire: &v1::Fingerprint) -> Result<Self, Self::Error> {
        let level = usize::try_from(wire.level)
            .map_err(|_| DomainDecodeError::IntegerOutOfRange(wire.level))?;
        let parts = ntk_common::FingerprintParts {
            id: wire.id.clone(),
            level,
            eldership: wire.eldership,
            pending_elderships: wire.pending_elderships.clone(),
            elderships_seed: wire
                .elderships_seed
                .iter()
                .map(|entry| entry.value)
                .collect(),
        };
        Ok(ntk_common::Fingerprint::from_parts(parts)?)
    }
}

// ---------------------------------------------------------------------------
// TypedValue helpers
// ---------------------------------------------------------------------------

/// Encodes `msg` as a [`crate::v1::TypedValue`] tagged `type_tag`. Every
/// phase-2 module wraps its own payload messages this way to travel inside
/// [`crate::v1::MethodCall`]/[`crate::v1::ResponsePayload`]'s `TypedValue`
/// arms (`type_tag` convention: `"<module>.<TypeName>"`).
pub fn typed_value<M: prost::Message>(type_tag: &str, msg: &M) -> crate::v1::TypedValue {
    crate::v1::TypedValue::new(type_tag, msg.encode_to_vec())
}

/// Decodes a [`crate::v1::TypedValue`] as `M`, first checking that its
/// `type_tag` matches `expected_tag` — a mismatch means the payload was
/// produced by a different type than the caller assumes, which must never be
/// silently decoded as if it were `M`.
///
/// # Errors
/// [`DomainDecodeError::TypeTagMismatch`] if `tv.type_tag != expected_tag`;
/// [`DomainDecodeError::PayloadDecode`] if `M::decode` fails on `tv.payload`.
pub fn from_typed_value<M: prost::Message + Default>(
    tv: &crate::v1::TypedValue,
    expected_tag: &str,
) -> Result<M, DomainDecodeError> {
    if tv.type_tag != expected_tag {
        return Err(DomainDecodeError::TypeTagMismatch {
            expected: expected_tag.to_owned(),
            actual: tv.type_tag.clone(),
        });
    }
    M::decode(tv.payload.as_slice())
        .map_err(|err| DomainDecodeError::PayloadDecode(err.to_string()))
}

// ---------------------------------------------------------------------------
// UnicastId
// ---------------------------------------------------------------------------

/// `type_tag` [`UnicastId::to_typed_value`] uses — this crate's own module name in the
/// `"<module>.<TypeName>"` convention (see the "TypedValue helpers" doc above).
pub const UNICAST_ID_TAG: &str = "proto.UnicastId";

/// Which local identity a `Request` addresses — upstream's three `IUnicastID` implementers
/// (research/impl/vala/ntkd/serializables.vala:405-492); see [`crate::v1::UnicastId`]'s own doc
/// for the exact wire shape and the v0.1.5 compatibility rule [`Self::from_typed_value`]
/// implements. `WholeNode`/`IdentityAware` carry their id as an opaque [`crate::v1::TypedValue`]
/// — exactly like [`crate::v1::CallerContext`]'s own `source_id`/`src_nic` — because decoding it
/// is a phase-2 crate's job; this crate depends on none of them and must not start now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnicastId {
    /// Addresses this node's node-level (neighborhood/identities) skeleton, not any one
    /// identity. Carries the caller's own id (`WholeNodeUnicastID(neighbour_id)`).
    WholeNode(crate::v1::TypedValue),
    /// Addresses exactly the local identity whose own id matches the carried payload
    /// (`IdentityAwareUnicastID(NodeID)`).
    IdentityAware(crate::v1::TypedValue),
    /// Addresses whichever local identity currently is main (`MainIdentityUnicastID`). Also
    /// what an absent/empty `TypedValue` decodes as — see [`Self::from_typed_value`]'s doc.
    MainIdentity,
}

impl UnicastId {
    /// Encodes as the `TypedValue` that populates [`crate::v1::Request::unicast_id`].
    #[must_use]
    pub fn to_typed_value(&self) -> crate::v1::TypedValue {
        use crate::v1::unicast_id::Kind;
        let kind = match self {
            UnicastId::WholeNode(id) => Kind::WholeNode(id.clone()),
            UnicastId::IdentityAware(id) => Kind::IdentityAware(id.clone()),
            UnicastId::MainIdentity => Kind::MainIdentity(crate::v1::Empty::VALUE),
        };
        typed_value(UNICAST_ID_TAG, &crate::v1::UnicastId { kind: Some(kind) })
    }

    /// Decodes `tv` — the wire value of [`crate::v1::Request::unicast_id`].
    ///
    /// # Compatibility
    /// An empty `type_tag` — what `tv` always is on a `Request` an unmodified v0.1.5 peer (or
    /// any peer that has never heard of `UnicastId`) sent, since such a peer never sets
    /// `unicast_id` at all and proto3's zero value for an unset message field is exactly this
    /// empty struct — decodes as [`UnicastId::MainIdentity`], not [`DomainDecodeError::TypeTagMismatch`].
    /// Breaking that would partition this node from every already-deployed peer.
    ///
    /// # Errors
    /// [`DomainDecodeError::TypeTagMismatch`] if `type_tag` is set but is not [`UNICAST_ID_TAG`];
    /// [`DomainDecodeError::PayloadDecode`] if the payload does not decode as
    /// [`crate::v1::UnicastId`]; [`DomainDecodeError::MissingUnicastIdKind`] if it decodes but no
    /// `kind` oneof arm is set.
    pub fn from_typed_value(tv: &crate::v1::TypedValue) -> Result<Self, DomainDecodeError> {
        use crate::v1::unicast_id::Kind;
        if tv.type_tag.is_empty() {
            return Ok(UnicastId::MainIdentity);
        }
        let wire: crate::v1::UnicastId = from_typed_value(tv, UNICAST_ID_TAG)?;
        match wire.kind {
            Some(Kind::WholeNode(id)) => Ok(UnicastId::WholeNode(id)),
            Some(Kind::IdentityAware(id)) => Ok(UnicastId::IdentityAware(id)),
            Some(Kind::MainIdentity(_)) => Ok(UnicastId::MainIdentity),
            None => Err(DomainDecodeError::MissingUnicastIdKind),
        }
    }
}

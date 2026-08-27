//! Crate-wide wire-decode and DHT-proxy error types.

use thiserror::Error;

/// Everything that can go wrong decoding this crate's own wire messages, or constructing a
/// domain value (a `top` outside `1..=levels`, say). Not the *outcome* of a reserve attempt
/// (that is [`crate::ReserveError`], a normal, non-exceptional fk_database.vala answer) nor a
/// routing failure ([`ntk_peerservices::ContactPeerError`], surfaced directly through
/// [`crate::ProxyError`]).
#[derive(Debug, Error)]
pub enum Error {
    /// A [`ntk_proto::v1::TypedValue`] carried an unexpected `type_tag`, or its payload did not
    /// parse as the expected protobuf message.
    #[error("wire decode error: {0}")]
    Domain(#[from] ntk_proto::domain::DomainDecodeError),

    /// A `CoordinatorRequest`/`CoordinatorResponse`/`CoordinatorExecuteArgs.tuple` wire message
    /// was missing its `oneof`/required field.
    #[error("wire message missing required field {0:?}")]
    MissingField(&'static str),

    /// A wire `top`/`level` did not fit `usize` (never happens on real hardware; guards against
    /// a malicious or buggy peer sending a negative/oversized value).
    #[error("level/top value {0} does not fit usize")]
    LevelOutOfRange(i64),

    /// `top` is not a valid `CoordinatorKey` for this topology: it must be `1..=levels`
    /// (`research/impl/vala/coordinator/serializables.vala:156-173`,
    /// `CoordDatabaseDescriptor.is_valid_key`, `fk_database.vala:47-55`).
    #[error("top {top} is out of range for a topology with {levels} levels")]
    InvalidTop { top: usize, levels: usize },
}

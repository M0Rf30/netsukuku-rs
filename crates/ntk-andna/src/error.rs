//! Crate-wide construction, validation, and wire-decode error type.

use thiserror::Error;

/// Everything that can go wrong constructing this crate's own domain types or decoding them off
/// the wire. Protocol-level *outcomes* — a rejected registration
/// ([`crate::record::RegisterRejected`]), a denied counter reservation
/// ([`crate::counter::CounterRejected`]) — are their own types: this enum is only for "the data
/// was malformed."
#[derive(Debug, Error)]
pub enum Error {
    /// A [`crate::hostname::Hostname`] was empty.
    #[error("hostname must not be empty")]
    EmptyHostname,

    /// A [`crate::hostname::Hostname`] exceeded the maximum length (`ANDNA_MAX_HNAME_LEN - 1`,
    /// `research/impl/c/netsukuku/src/andna_cache.h:34`).
    #[error("hostname is {len} bytes, longer than the {max}-byte limit")]
    HostnameTooLong { len: usize, max: usize },

    /// A [`crate::hostname::Hostname`] contained a character outside the documented "bounded
    /// length case-insensitive alnum name" charset (`research/notes/02-vala-services-daemon.md`
    /// §4, `DemoneNTKD/RisoluzioneNomi.md`).
    #[error("hostname must be ASCII alphanumeric")]
    InvalidHostnameChar,

    /// A [`crate::snsd::SnsdRecord`] weight exceeded NTK_RFC 0009's "less than 128" bound
    /// (`research/specs/vala-doc--rfc-Ntk_SNSD`; C: `SNSD_WEIGHT` 0x7f mask,
    /// `research/impl/c/netsukuku/src/snsd_cache.h:45-46`).
    #[error("SNSD weight {0} exceeds the RFC 0009 limit of 127")]
    WeightTooLarge(u8),

    /// A [`crate::record::RegisterRequest`] carried an explicit `service == 0` entry in
    /// `snsd_records` — the zero record is a dedicated, non-reassignable field
    /// (`owner_naddr`/`zero_priority`/`zero_weight`), not a general SNSD slot.
    #[error("snsd_records must not contain an explicit service-0 (zero record) entry")]
    ReservedServiceZero,

    /// A hostname's total SNSD record count would exceed NTK_RFC 0009's 256-total limit
    /// (`SNSD_MAX_RECORDS`, `research/impl/c/netsukuku/src/snsd_cache.h:31`).
    #[error("hostname already has the maximum {0} total SNSD records")]
    TooManySnsdRecords(usize),

    /// One SNSD service number's record count would exceed NTK_RFC 0009's 16-per-service limit
    /// (`SNSD_MAX_REC_SERV`, `research/impl/c/netsukuku/src/snsd_cache.h:36`).
    #[error("service {service} already has the maximum {max} SNSD records")]
    TooManyRecordsForService { service: u32, max: usize },

    /// A signed request's signature did not verify against its claimed `owner_key` — either a
    /// forged/tampered request, or a renewal attempted with the wrong key.
    #[error("signature verification failed")]
    InvalidSignature,

    /// A wire message was missing a field this implementation requires.
    #[error("wire message missing required field {0:?}")]
    MissingField(&'static str),

    /// A wire `uint32`/`uint64` field held a value outside the target narrower integer's range
    /// (e.g. `priority`/`weight`/`service` are `u8`/`u16` in this crate's domain types but
    /// `uint32` on the wire, since protobuf has no narrower integer types).
    #[error("field {0:?} value out of range for its domain type")]
    FieldOutOfRange(&'static str),

    /// Re-validating a decoded [`ntk_proto::domain::v1`] value against `ntk-common` failed.
    #[error("domain decode error: {0}")]
    Domain(#[from] ntk_proto::domain::DomainDecodeError),

    /// A [`ntk_proto::v1::TypedValue`] carried an unexpected `type_tag`.
    #[error("typed_value tag mismatch: expected {expected:?}, got {actual:?}")]
    TypeTagMismatch { expected: String, actual: String },

    /// A `TypedValue.payload` did not parse as the expected protobuf message.
    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),

    /// A wire `owner_key`/`signature` byte slice had the wrong length for its cryptographic type.
    #[error("malformed {what}: expected {expected} bytes, got {actual}")]
    MalformedKeyMaterial {
        what: &'static str,
        expected: usize,
        actual: usize,
    },
}

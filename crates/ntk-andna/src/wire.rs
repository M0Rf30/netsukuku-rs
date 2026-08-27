//! Wire encoding: conversions between this crate's domain types and its own generated
//! `ntk.andna.v1` protobuf messages (`crate::v1`), plus the `TypedValue` pack/unpack helpers
//! [`crate::service`] uses to carry ANDNA's own request/reply schema over the generic
//! `PeerService::exec` path (see `proto/andna.proto`'s module doc comment for why these are not
//! `ntk_proto::v1::MethodCall` arms).

use ed25519_dalek::{Signature, VerifyingKey};
use ntk_proto::domain::{from_typed_value, typed_value};
use ntk_proto::v1::TypedValue;

use crate::counter::CounterRejected;
use crate::error::Error;
use crate::hostname::{Hostname, HostnameHash};
use crate::record::{RegisterOutcome, RegisterRejected, RegisterRequest};
use crate::snsd::{SnsdRecord, SnsdTarget};
use crate::v1 as wire;

const TAG_REGISTER_REQUEST: &str = "andna.RegisterRequest";
const TAG_REGISTER_REPLY: &str = "andna.RegisterReply";
const TAG_RESOLVE_REQUEST: &str = "andna.ResolveRequest";
const TAG_RESOLVE_REPLY: &str = "andna.ResolveReply";
const TAG_COUNTER_REQUEST: &str = "andna.CounterRequest";
const TAG_COUNTER_REPLY: &str = "andna.CounterReply";

fn snsd_record_to_wire(r: &SnsdRecord) -> wire::SnsdRecord {
    let target = match &r.target {
        SnsdTarget::Address(naddr) => wire::snsd_record::Target::Address(wire::AndnaAddress {
            value: vec![naddr.into()],
        }),
        SnsdTarget::Alias(hostname) => {
            wire::snsd_record::Target::Hostname(hostname.as_str().to_owned())
        }
    };
    wire::SnsdRecord {
        service: u32::from(r.service),
        priority: u32::from(r.priority),
        weight: u32::from(r.weight),
        target: Some(target),
    }
}

fn snsd_record_from_wire(w: &wire::SnsdRecord) -> Result<SnsdRecord, Error> {
    let target = match w.target.as_ref().ok_or(Error::MissingField("target"))? {
        wire::snsd_record::Target::Address(wrapped) => {
            let naddr = wrapped
                .value
                .first()
                .ok_or(Error::MissingField("address.value"))?;
            SnsdTarget::Address(ntk_common::Naddr::try_from(naddr)?)
        }
        wire::snsd_record::Target::Hostname(name) => SnsdTarget::Alias(Hostname::new(name)?),
    };
    let service =
        u16::try_from(w.service).map_err(|_| Error::FieldOutOfRange("snsd_record.service"))?;
    let priority =
        u8::try_from(w.priority).map_err(|_| Error::FieldOutOfRange("snsd_record.priority"))?;
    let weight =
        u8::try_from(w.weight).map_err(|_| Error::FieldOutOfRange("snsd_record.weight"))?;
    Ok(SnsdRecord {
        service,
        priority,
        weight,
        target,
    })
}

fn key_from_wire(bytes: &[u8]) -> Result<VerifyingKey, Error> {
    let arr: [u8; 32] = bytes.try_into().map_err(|_| Error::MalformedKeyMaterial {
        what: "owner_key",
        expected: 32,
        actual: bytes.len(),
    })?;
    VerifyingKey::from_bytes(&arr).map_err(|_| Error::InvalidSignature)
}

fn signature_from_wire(bytes: &[u8]) -> Result<Signature, Error> {
    let arr: [u8; 64] = bytes.try_into().map_err(|_| Error::MalformedKeyMaterial {
        what: "signature",
        expected: 64,
        actual: bytes.len(),
    })?;
    Ok(Signature::from_bytes(&arr))
}

pub(crate) fn pack_register_request(req: &RegisterRequest) -> TypedValue {
    let w = wire::RegisterRequest {
        hostname: req.hostname.as_str().to_owned(),
        owner_key: req.owner_key.to_bytes().to_vec(),
        owner_naddr: vec![(&req.owner_naddr).into()],
        sequence: req.sequence,
        timestamp_unix: req.timestamp_unix,
        zero_priority: u32::from(req.zero_priority),
        zero_weight: u32::from(req.zero_weight),
        snsd_records: req.snsd_records.iter().map(snsd_record_to_wire).collect(),
        signature: req.signature.to_bytes().to_vec(),
    };
    typed_value(TAG_REGISTER_REQUEST, &w)
}

pub(crate) fn unpack_register_request(tv: &TypedValue) -> Result<RegisterRequest, Error> {
    let w: wire::RegisterRequest = from_typed_value(tv, TAG_REGISTER_REQUEST)?;
    let owner_naddr = ntk_common::Naddr::try_from(
        w.owner_naddr
            .first()
            .ok_or(Error::MissingField("owner_naddr"))?,
    )?;
    Ok(RegisterRequest {
        hostname: Hostname::new(&w.hostname)?,
        owner_key: key_from_wire(&w.owner_key)?,
        owner_naddr,
        sequence: w.sequence,
        timestamp_unix: w.timestamp_unix,
        zero_priority: u8::try_from(w.zero_priority).unwrap_or(u8::MAX),
        zero_weight: u8::try_from(w.zero_weight).unwrap_or(u8::MAX),
        snsd_records: w
            .snsd_records
            .iter()
            .map(snsd_record_from_wire)
            .collect::<Result<_, _>>()?,
        signature: signature_from_wire(&w.signature)?,
    })
}

pub(crate) fn pack_register_reply(
    outcome: Result<RegisterOutcome, &RegisterRejected>,
) -> TypedValue {
    use wire::register_reply::Outcome;
    let outcome = match outcome {
        Ok(RegisterOutcome::Registered { expires_at }) => {
            Outcome::Accepted(wire::RegisterAccepted {
                expires_at_unix: expires_at,
                renewed: false,
            })
        }
        Ok(RegisterOutcome::Renewed { expires_at }) => Outcome::Accepted(wire::RegisterAccepted {
            expires_at_unix: expires_at,
            renewed: true,
        }),
        Err(rejected) => Outcome::Rejected(wire::RegisterRejectedReply {
            reason: rejected.to_string(),
        }),
    };
    typed_value(
        TAG_REGISTER_REPLY,
        &wire::RegisterReply {
            outcome: Some(outcome),
        },
    )
}

/// Decodes a [`RegisterReply`]: `Ok(Ok(outcome))` on acceptance, `Ok(Err(reason))` on an
/// application-level rejection (the reason string, since the concrete [`RegisterRejected`]
/// variant is not reconstructible off the wire), `Err` on a malformed reply.
pub(crate) fn unpack_register_reply(
    tv: &TypedValue,
) -> Result<Result<RegisterOutcome, String>, Error> {
    use wire::register_reply::Outcome;
    let w: wire::RegisterReply = from_typed_value(tv, TAG_REGISTER_REPLY)?;
    match w.outcome.ok_or(Error::MissingField("outcome"))? {
        Outcome::Accepted(a) if a.renewed => Ok(Ok(RegisterOutcome::Renewed {
            expires_at: a.expires_at_unix,
        })),
        Outcome::Accepted(a) => Ok(Ok(RegisterOutcome::Registered {
            expires_at: a.expires_at_unix,
        })),
        Outcome::Rejected(r) => Ok(Err(r.reason)),
    }
}

pub(crate) fn pack_resolve_request(hostname: &Hostname, service: u16) -> TypedValue {
    typed_value(
        TAG_RESOLVE_REQUEST,
        &wire::ResolveRequest {
            hostname: hostname.as_str().to_owned(),
            service: u32::from(service),
        },
    )
}

pub(crate) fn unpack_resolve_request(tv: &TypedValue) -> Result<(Hostname, u16), Error> {
    let w: wire::ResolveRequest = from_typed_value(tv, TAG_RESOLVE_REQUEST)?;
    let hostname = Hostname::new(&w.hostname)?;
    let service = u16::try_from(w.service).map_err(|_| Error::FieldOutOfRange("service"))?;
    Ok((hostname, service))
}

pub(crate) fn pack_resolve_reply(records: &[SnsdRecord]) -> TypedValue {
    typed_value(
        TAG_RESOLVE_REPLY,
        &wire::ResolveReply {
            records: records.iter().map(snsd_record_to_wire).collect(),
        },
    )
}

pub(crate) fn unpack_resolve_reply(tv: &TypedValue) -> Result<Vec<SnsdRecord>, Error> {
    let w: wire::ResolveReply = from_typed_value(tv, TAG_RESOLVE_REPLY)?;
    w.records.iter().map(snsd_record_from_wire).collect()
}

pub(crate) fn pack_counter_request(hash: HostnameHash) -> TypedValue {
    typed_value(
        TAG_COUNTER_REQUEST,
        &wire::CounterRequest {
            hostname_hash: hash.as_bytes().to_vec(),
        },
    )
}

pub(crate) fn unpack_counter_request(tv: &TypedValue) -> Result<HostnameHash, Error> {
    let w: wire::CounterRequest = from_typed_value(tv, TAG_COUNTER_REQUEST)?;
    let arr: [u8; 32] =
        w.hostname_hash
            .as_slice()
            .try_into()
            .map_err(|_| Error::MalformedKeyMaterial {
                what: "hostname_hash",
                expected: 32,
                actual: w.hostname_hash.len(),
            })?;
    Ok(HostnameHash::from_bytes(arr))
}

pub(crate) fn pack_counter_reply(outcome: Result<usize, &CounterRejected>) -> TypedValue {
    use wire::counter_reply::Outcome;
    let outcome = match outcome {
        Ok(count) => Outcome::ReservedCount(count as u32),
        Err(rejected) => Outcome::DeniedReason(rejected.to_string()),
    };
    typed_value(
        TAG_COUNTER_REPLY,
        &wire::CounterReply {
            outcome: Some(outcome),
        },
    )
}

/// `Ok(count)` on a successful reservation, `Err(reason)` on denial (the reason string, since
/// [`CounterRejected`]'s structured form is not reconstructible off the wire).
pub(crate) fn unpack_counter_reply(tv: &TypedValue) -> Result<Result<usize, String>, Error> {
    use wire::counter_reply::Outcome;
    let w: wire::CounterReply = from_typed_value(tv, TAG_COUNTER_REPLY)?;
    match w.outcome.ok_or(Error::MissingField("outcome"))? {
        Outcome::ReservedCount(count) => Ok(Ok(count as usize)),
        Outcome::DeniedReason(reason) => Ok(Err(reason)),
    }
}

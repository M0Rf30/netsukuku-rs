//! Wire codec: converts between this crate's [`EtpMessage`]/[`EtpPath`] and
//! the `prost`-generated `proto/qspn.proto` types ([`v1`]), reusing
//! `ntk-proto`'s shared domain codec for the embedded
//! `Naddr`/`HCoord`/`Fingerprint`/`Cost` values. `From` (domain -> wire) is
//! infallible; `TryFrom` (wire -> domain) revalidates through `ntk-common`
//! and never trusts a peer's shape.

use ntk_common::HCoord;
use ntk_proto::domain::{from_typed_value, typed_value};
use ntk_proto::v1::TypedValue;

use crate::arc::ArcId;
use crate::error::QspnError;
use crate::path::{EtpMessage, EtpPath};

/// Generated protobuf types for `proto/qspn.proto` (package `ntk.qspn.v1`).
/// Doc comments on individual messages/fields are copied from the `.proto`
/// source by `prost-build`.
#[allow(clippy::doc_markdown)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/ntk.qspn.v1.rs"));
}

/// `type_tag` this module's [`EtpMessage`] travels under inside a
/// [`TypedValue`] (`"<module>.<TypeName>"` convention, `ntk-proto`'s domain
/// module docs) — used for both `qspn_get_full_etp`'s response and
/// `QspnSendEtpArgs.etp`.
pub const ETP_MESSAGE_TAG: &str = "qspn.EtpMessage";
/// `type_tag` a bare `get_full_etp` request address travels under
/// (`qspn_get_full_etp`'s `TypedValue` argument, reusing
/// `ntk.domain.v1.Naddr` directly rather than adding a wrapper message).
pub const NADDR_TAG: &str = "qspn.Naddr";

impl From<&EtpPath> for v1::EtpPath {
    fn from(p: &EtpPath) -> Self {
        v1::EtpPath {
            hops: p.hops.iter().map(|&h| h.into()).collect(),
            arcs: p.arcs.iter().map(|a| a.as_u32()).collect(),
            cost: Some(p.cost.into()),
            fingerprint: Some((&p.fingerprint).into()),
            nodes_inside: p.nodes_inside,
            ignore_outside: p.ignore_outside.clone(),
        }
    }
}

impl TryFrom<&v1::EtpPath> for EtpPath {
    type Error = QspnError;

    fn try_from(p: &v1::EtpPath) -> Result<Self, Self::Error> {
        let mut hops = Vec::with_capacity(p.hops.len());
        for h in &p.hops {
            hops.push(HCoord::try_from(h).map_err(QspnError::Domain)?);
        }
        let cost = p
            .cost
            .as_ref()
            .ok_or(QspnError::MalformedEtp("EtpPath.cost is missing"))?;
        let fingerprint = p
            .fingerprint
            .as_ref()
            .ok_or(QspnError::MalformedEtp("EtpPath.fingerprint is missing"))?;
        Ok(EtpPath {
            hops,
            arcs: p.arcs.iter().map(|&a| ArcId::from(a)).collect(),
            cost: cost.try_into().map_err(QspnError::Domain)?,
            fingerprint: fingerprint.try_into().map_err(QspnError::Domain)?,
            nodes_inside: p.nodes_inside,
            ignore_outside: p.ignore_outside.clone(),
        })
    }
}

impl From<&EtpMessage> for v1::EtpMessage {
    fn from(m: &EtpMessage) -> Self {
        v1::EtpMessage {
            node_address: Some((&m.node_address).into()),
            fingerprints: m.fingerprints.iter().map(Into::into).collect(),
            nodes_inside: m.nodes_inside.clone(),
            hops: m.hops.iter().map(|&h| h.into()).collect(),
            paths: m.paths.iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<&v1::EtpMessage> for EtpMessage {
    type Error = QspnError;

    fn try_from(m: &v1::EtpMessage) -> Result<Self, Self::Error> {
        let node_address = m.node_address.as_ref().ok_or(QspnError::MalformedEtp(
            "EtpMessage.node_address is missing",
        ))?;
        let mut fingerprints = Vec::with_capacity(m.fingerprints.len());
        for f in &m.fingerprints {
            fingerprints.push(f.try_into().map_err(QspnError::Domain)?);
        }
        let mut hops = Vec::with_capacity(m.hops.len());
        for h in &m.hops {
            hops.push(HCoord::try_from(h).map_err(QspnError::Domain)?);
        }
        let mut paths = Vec::with_capacity(m.paths.len());
        for p in &m.paths {
            paths.push(p.try_into()?);
        }
        Ok(EtpMessage {
            node_address: node_address.try_into().map_err(QspnError::Domain)?,
            fingerprints,
            nodes_inside: m.nodes_inside.clone(),
            hops,
            paths,
        })
    }
}

/// Encodes `m` as a [`TypedValue`] tagged [`ETP_MESSAGE_TAG`], for the
/// `qspn_get_full_etp` response and `QspnSendEtpArgs.etp`.
#[must_use]
pub fn encode_etp_message(m: &EtpMessage) -> TypedValue {
    typed_value(ETP_MESSAGE_TAG, &v1::EtpMessage::from(m))
}

/// Decodes a [`TypedValue`] tagged [`ETP_MESSAGE_TAG`] as an [`EtpMessage`].
///
/// # Errors
/// [`QspnError::Domain`]/[`QspnError::MalformedEtp`] on a `type_tag`
/// mismatch, a `prost` decode failure, or a domain revalidation failure
/// (e.g. an out-of-range position).
pub fn decode_etp_message(tv: &TypedValue) -> Result<EtpMessage, QspnError> {
    let wire: v1::EtpMessage = from_typed_value(tv, ETP_MESSAGE_TAG).map_err(QspnError::Domain)?;
    (&wire).try_into()
}

/// Encodes `naddr` as a [`TypedValue`] tagged [`NADDR_TAG`], for
/// `qspn_get_full_etp`'s request argument.
#[must_use]
pub fn encode_naddr(naddr: &ntk_common::Naddr) -> TypedValue {
    typed_value(NADDR_TAG, &ntk_proto::domain::v1::Naddr::from(naddr))
}

/// Decodes a [`TypedValue`] tagged [`NADDR_TAG`] as an [`ntk_common::Naddr`].
///
/// # Errors
/// [`QspnError::Domain`] on a `type_tag` mismatch, a `prost` decode failure,
/// or a domain revalidation failure.
pub fn decode_naddr(tv: &TypedValue) -> Result<ntk_common::Naddr, QspnError> {
    let wire: ntk_proto::domain::v1::Naddr =
        from_typed_value(tv, NADDR_TAG).map_err(QspnError::Domain)?;
    (&wire).try_into().map_err(QspnError::Domain)
}

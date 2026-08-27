//! Origin-auth: the originator of a `contact_peer` request signs its own claimed position
//! (`client_tuple`), service id, and request payload exactly once; only the servant that
//! finally executes the request ([`crate::actor::Handle::exec_local`], reached via
//! [`crate::actor::Handle::forward_msg`]'s self-loop) verifies it — never each relay
//! `forward_msg` hops through on the way there.
//!
//! This closes the audit finding that `client_tuple` (`crate::PeerMessageForwarder::n`) travels
//! end to end through relays and is never authenticated: a malicious relay can rewrite `n`/
//! `p_id` as it forwards, but doing so invalidates a signature computed once, by the true
//! originator, over the untampered values — [`ntk_proto::auth::verify`] recomputes the digest
//! from whatever the servant actually holds, so any relay-introduced mismatch surfaces as
//! [`ntk_proto::auth::AuthError::SignatureMismatch`]. The same coverage stops a signature being
//! transplanted onto a different service or a different request: `p_id` and the request payload
//! are inside the signed bytes too.
//!
//! Gated entirely by [`crate::Config::require_auth`]. With it `false` (the default),
//! `contact_peer` still signs opportunistically whenever a signing key is configured (harmless —
//! the servant never checks it), but a missing/invalid `Auth` is never itself a reason to
//! reject; with no signing key configured at all, `PeerMessageForwarder::auth` stays unset —
//! byte-for-byte the pre-auth wire shape.

use ntk_proto::v1::TypedValue;

use crate::service::ServiceId;

/// Stable discriminant binding an origin-auth signature to this exact scheme — see
/// [`ntk_proto::auth::sign`]'s own doc for why `method` must never be reused across call
/// shapes. `p_id` is *not* folded into this constant (it travels inside the signed payload
/// instead, see [`origin_signing_payload`]), so one constant serves every registered
/// [`crate::PeerService`] rather than needing a per-service method string.
pub(crate) const ORIGIN_AUTH_METHOD: &str = "ntk-peerservices/v1/origin-request";

/// The exact bytes an origin-auth signature covers: the claimed `client_tuple` (length-prefixed
/// `u32`-LE positions), the target [`ServiceId`], and the opaque request payload's `type_tag`/
/// `payload` (both length-prefixed) — deliberately everything a relay could otherwise rewrite or
/// a signature could otherwise be transplanted onto. Length-prefixing every variable-width field
/// follows the same discipline `ntk_proto::auth`'s own digest cites: two distinct inputs can
/// never sign identically through a concatenation-boundary ambiguity.
pub(crate) fn origin_signing_payload(
    client_tuple: &[u32],
    p_id: ServiceId,
    request: &TypedValue,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(
        4 + client_tuple.len() * 4 + 2 + 4 + request.type_tag.len() + 4 + request.payload.len(),
    );
    buf.extend_from_slice(&(client_tuple.len() as u32).to_le_bytes());
    for &pos in client_tuple {
        buf.extend_from_slice(&pos.to_le_bytes());
    }
    buf.extend_from_slice(&p_id.get().to_le_bytes());
    buf.extend_from_slice(&(request.type_tag.len() as u32).to_le_bytes());
    buf.extend_from_slice(request.type_tag.as_bytes());
    buf.extend_from_slice(&(request.payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&request.payload);
    buf
}

/// Everything that can make [`crate::actor::Handle::verify_origin`] reject a request, gated
/// entirely by [`crate::Config::require_auth`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum OriginAuthError {
    /// `require_auth` is set but this request carried no `Auth` block at all.
    #[error("require_auth is enabled but this request carried no Auth block")]
    Missing,
    /// The signature, or the replay sequence behind it, didn't check out.
    #[error(transparent)]
    Auth(#[from] ntk_proto::auth::AuthError),
    /// The actor already shut down mid-verification — nothing left to check the replay sequence
    /// against; treated as a rejection rather than a silent bypass.
    #[error("peerservices actor is shutting down")]
    ActorShutDown,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tv(type_tag: &str, payload: &[u8]) -> TypedValue {
        TypedValue {
            type_tag: type_tag.to_owned(),
            payload: payload.to_vec(),
        }
    }

    #[test]
    fn differs_when_client_tuple_differs() {
        let a = origin_signing_payload(&[1, 2], ServiceId::new(1), &tv("x", b"y"));
        let b = origin_signing_payload(&[1, 3], ServiceId::new(1), &tv("x", b"y"));
        assert_ne!(a, b);
    }

    #[test]
    fn differs_when_service_id_differs() {
        let a = origin_signing_payload(&[1, 2], ServiceId::new(1), &tv("x", b"y"));
        let b = origin_signing_payload(&[1, 2], ServiceId::new(2), &tv("x", b"y"));
        assert_ne!(a, b);
    }

    #[test]
    fn differs_when_request_payload_differs() {
        let a = origin_signing_payload(&[1, 2], ServiceId::new(1), &tv("x", b"y"));
        let b = origin_signing_payload(&[1, 2], ServiceId::new(1), &tv("x", b"z"));
        assert_ne!(a, b);
    }

    /// A `(type_tag, payload)` boundary shift ("x" + "y") vs ("xy" + "") must not collide —
    /// exactly the concatenation-ambiguity `ntk_proto::auth::digest`'s own doc warns about.
    #[test]
    fn does_not_collide_across_a_type_tag_payload_boundary_shift() {
        let a = origin_signing_payload(&[1], ServiceId::new(1), &tv("x", b"y"));
        let b = origin_signing_payload(&[1], ServiceId::new(1), &tv("xy", b""));
        assert_ne!(a, b);
    }
}

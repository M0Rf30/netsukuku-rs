//! Wire glue beyond [`crate::NodeId`]: [`NicRef`] (the other module payload
//! type), `CallerContext`/`ResponsePayload` builders, and the
//! `unicast_id`/`broadcast_id` placeholder every call needs.
//!
//! # Why `CallerContext` carries an explicit `NicRef`
//! Upstream resolves "which arc/NIC did this call arrive on" by inspecting
//! the physical transport (`CallerInfo`, `ntkdrpc/caller_info.vala:23-134`):
//! `query_caller_info.is_from_unicast(rpc_caller, arcs)` matches the
//! *incoming TCP connection* to a known arc. `ntk_rpc::RpcHandler::handle`
//! does not receive the peer's socket address at all (`TcpServer::serve`
//! discards it, `crates/ntk-rpc/src/server.rs:127`), and `FakeRpcClient` has
//! no socket whatsoever — so for `can_you_export`/`nop` (the two methods
//! whose upstream argument list carries no identity fields, relying
//! entirely on `CallerInfo`) the *only* channel left to identify the caller
//! is `Request.caller`/`BroadcastRequest.caller`. This crate therefore
//! populates `CallerContext.source_id`/`.src_nic` with the caller's own
//! [`crate::NodeId`]/[`NicRef`] on every call it makes, and the inbound
//! handler resolves `can_you_export`/`nop`'s arc from those fields instead
//! of from transport introspection. For `here_i_am`/`request_arc`/
//! `remove_arc` — whose args already carry the sender's identity explicitly
//! — `CallerContext` is populated identically for uniformity but is not
//! consulted by the handler.
use std::sync::atomic::{AtomicU64, Ordering};

use ed25519_dalek::{SigningKey, VerifyingKey};
use ntk_proto::domain::{from_typed_value, typed_value};
use ntk_proto::v1::response_payload::Value as ResponseValue;
use ntk_proto::v1::{
    Auth, CallerContext, Empty, ErrorDomain, MethodCall, RemoteError, ResponsePayload, TypedValue,
};
use prost::Message;

use crate::error::NeighborhoodError;
use crate::node_id::NodeId;
use crate::v1;

/// `type_tag` this crate uses for [`NicRef`]'s `TypedValue` payload.
pub const NIC_REF_TAG: &str = "neighborhood.NicRef";

/// `type_tag` for the `unicast_id`/`broadcast_id` placeholder — see
/// [`default_identity_marker`].
pub const DEFAULT_IDENTITY_TAG: &str = "neighborhood.DefaultIdentity";

/// Domain form of a local NIC's fixed discovery address: MAC plus the
/// linklocal address assigned by `new_linklocal_address`
/// (`neighborhood.vala:106-134`). Carried in [`CallerContext::src_nic`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NicRef {
    /// Hardware (MAC) address.
    pub mac: String,
    /// Fixed linklocal address assigned to this NIC.
    pub nic_addr: String,
}

impl NicRef {
    pub(crate) fn to_typed_value(&self) -> TypedValue {
        typed_value(
            NIC_REF_TAG,
            &v1::NicRef {
                mac: self.mac.clone(),
                nic_addr: self.nic_addr.clone(),
            },
        )
    }

    /// Decodes and re-validates a peer-supplied `TypedValue`.
    ///
    /// # Errors
    /// Returns [`NeighborhoodError`] if the tag/payload do not decode, or
    /// either field is empty (a value upstream's transport layer would
    /// never produce).
    pub(crate) fn from_typed_value(tv: &TypedValue) -> Result<Self, NeighborhoodError> {
        let wire: v1::NicRef = from_typed_value(tv, NIC_REF_TAG)?;
        if wire.mac.is_empty() || wire.nic_addr.is_empty() {
            return Err(NeighborhoodError::MalformedWire(
                "NicRef: mac and nic_addr must be non-empty".to_owned(),
            ));
        }
        Ok(Self {
            mac: wire.mac,
            nic_addr: wire.nic_addr,
        })
    }
}

/// Builds the `CallerContext` this crate attaches to every outbound call —
/// see the module doc comment for why both fields are always populated.
pub(crate) fn caller_context(my_id: NodeId, my_nic: &NicRef) -> CallerContext {
    CallerContext {
        source_id: Some(my_id.to_typed_value()),
        src_nic: Some(my_nic.to_typed_value()),
    }
}

/// Placeholder `unicast_id`/`broadcast_id`. `Request`/`BroadcastRequest`
/// require this field (it names "which local identity object should handle
/// this call when the daemon hosts more than one",
/// `crates/ntk-proto/proto/ntk.proto` `Request.unicast_id` doc); this crate
/// has no multi-identity concept (`ntk-identities`, a sibling crate, owns
/// that — explicitly out of scope here), so every call names the same
/// constant "default identity" sentinel.
pub(crate) fn default_identity_marker() -> TypedValue {
    typed_value(DEFAULT_IDENTITY_TAG, &Empty::VALUE)
}

pub(crate) fn empty_response() -> ResponsePayload {
    ResponsePayload {
        value: Some(ResponseValue::Empty(Empty::VALUE)),
    }
}

pub(crate) fn boolean_response(value: bool) -> ResponsePayload {
    ResponsePayload {
        value: Some(ResponseValue::Boolean(value)),
    }
}

/// Builds a [`RemoteError`] in the `DESERIALIZE` domain — this crate's
/// equivalent of `ntk-rpc`'s own `server::malformed` helper, duplicated
/// because it is private to that crate.
pub(crate) fn malformed(message: impl Into<String>) -> RemoteError {
    RemoteError {
        domain: ErrorDomain::Deserialize as i32,
        message: message.into(),
    }
}

/// `ntk_proto::auth::sign`/`verify`'s `method` discriminant for each of this crate's 5 outbound
/// call sites — shared by [`sign_call`] (the signing side, `crate::manager`) and [`verify_auth`]
/// (the verifying side, `crate::handler`) so both name the exact same string for a given call;
/// a mismatch here would make every signature fail to verify.
pub(crate) const METHOD_HERE_I_AM: &str = "ntk-neighborhood/here_i_am";
pub(crate) const METHOD_REQUEST_ARC: &str = "ntk-neighborhood/request_arc";
pub(crate) const METHOD_CAN_YOU_EXPORT: &str = "ntk-neighborhood/can_you_export";
pub(crate) const METHOD_REMOVE_ARC: &str = "ntk-neighborhood/remove_arc";
pub(crate) const METHOD_NOP: &str = "ntk-neighborhood/nop";

/// Signs `call`'s canonical encoding (`ntk_proto::auth::sign`) under `key` at the next value
/// drawn from `sequence`, or `None` when `key` is absent — the vanilla-reference default of
/// leaving outbound traffic unsigned. `sequence` is a single counter shared across every
/// outbound neighbourhood call this node makes (actor-inline call sites and the independent
/// `run_radar`/`run_arc_monitor`/`run_arc_confirmation` background tasks alike): this node has
/// exactly one signing identity, and `ntk_proto::auth::SequenceGuard` tracks one
/// strictly-increasing sequence per signer key, not per call site or per peer, so every signed
/// message this node ever sends must draw from the same counter regardless of which of the 5
/// methods it is or which task produced it.
pub(crate) fn sign_call(
    key: Option<&SigningKey>,
    sequence: &AtomicU64,
    method: &str,
    call: &MethodCall,
) -> Option<Auth> {
    let key = key?;
    let next = sequence.fetch_add(1, Ordering::Relaxed) + 1;
    Some(ntk_proto::auth::sign(
        key,
        next,
        method,
        &call.encode_to_vec(),
    ))
}

/// Stateless half of inbound sender authentication: verifies `auth` (when present) against
/// `method`/`payload`, returning the signer's verified key and the sequence it claimed. `None`
/// when the inbound `Envelope` carried no `Auth` at all — accepted or rejected by
/// `crate::manager::Manager::authenticate`'s `require_auth` policy, not here. A *present but
/// invalid* `Auth` (bad length, malformed key, signature mismatch) is always a hard reject:
/// unlike an absent `Auth` block (an old/unmodified peer that has never heard of this scheme),
/// a present-but-broken one means either tampering or a real bug, never something safe to
/// silently downgrade to "unauthenticated".
///
/// # Errors
/// A [`RemoteError`] when `auth` is present but does not verify.
pub(crate) fn verify_auth(
    auth: Option<&Auth>,
    method: &str,
    payload: &[u8],
) -> Result<Option<(VerifyingKey, u64)>, RemoteError> {
    let Some(auth) = auth else {
        return Ok(None);
    };
    let key = ntk_proto::auth::verify(auth, method, payload)
        .map_err(|error| malformed(format!("auth: {error}")))?;
    Ok(Some((key, auth.sequence)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn sample_call() -> MethodCall {
        MethodCall {
            call: Some(ntk_proto::v1::method_call::Call::NeighborhoodNop(
                Empty::VALUE,
            )),
        }
    }

    /// The default, vanilla-reference behaviour: with no signing key configured, every one of
    /// this crate's 6 outbound call sites (all of which route through this one function) must
    /// leave the outbound `Envelope` byte-identical to today — no `Auth` block attached, and no
    /// observable side effect (the sequence counter is never touched) either.
    #[test]
    fn sign_call_with_no_signing_key_produces_no_auth_and_does_not_touch_the_sequence_counter() {
        let sequence = AtomicU64::new(0);
        let call = sample_call();
        assert_eq!(sign_call(None, &sequence, METHOD_NOP, &call), None);
        assert_eq!(sequence.load(Ordering::Relaxed), 0);
    }

    /// Proves [`sign_call`] and [`verify_auth`] agree on both the `method` string and the
    /// exact payload bytes for every one of this crate's 5 outbound call kinds — a mismatch
    /// here (e.g. one side hashing the raw args instead of the whole `MethodCall`) would make
    /// every real signature this crate ever produces fail to verify.
    #[test]
    fn sign_call_then_verify_auth_round_trips_for_every_method_constant() {
        let signing_key = key(1);
        let verifying_key = signing_key.verifying_key();
        let call = sample_call();
        for method in [
            METHOD_HERE_I_AM,
            METHOD_REQUEST_ARC,
            METHOD_CAN_YOU_EXPORT,
            METHOD_REMOVE_ARC,
            METHOD_NOP,
        ] {
            let sequence = AtomicU64::new(0);
            let auth = sign_call(Some(&signing_key), &sequence, method, &call)
                .expect("a configured signing key must produce an Auth block");
            assert_eq!(auth.sequence, 1);
            let verified = verify_auth(Some(&auth), method, &call.encode_to_vec())
                .expect("a signature sign_call just produced must verify")
                .expect("Some(auth) must never verify to None");
            assert_eq!(verified, (verifying_key, 1));
        }
    }

    /// A signature bound to one method must never verify against a different one — the same
    /// cross-method transplant protection `ntk_proto::auth` itself already covers, pinned here
    /// against this crate's own constants so a future accidental alias (two constants with the
    /// same string) would be caught.
    #[test]
    fn a_signature_for_one_method_does_not_verify_for_another() {
        let signing_key = key(2);
        let sequence = AtomicU64::new(0);
        let call = sample_call();
        let auth = sign_call(Some(&signing_key), &sequence, METHOD_HERE_I_AM, &call).unwrap();
        let error = verify_auth(Some(&auth), METHOD_REQUEST_ARC, &call.encode_to_vec())
            .expect_err("a here_i_am signature must not verify as a request_arc signature");
        assert_eq!(error.domain, ErrorDomain::Deserialize as i32);
    }

    /// [`verify_auth`] must return `Ok(None)`, not an error, when the peer sent no `Auth` at
    /// all — the interoperability contract with an unmodified/older peer.
    #[test]
    fn verify_auth_with_no_auth_block_is_ok_none() {
        let call = sample_call();
        assert_eq!(
            verify_auth(None, METHOD_NOP, &call.encode_to_vec()),
            Ok(None)
        );
    }
}

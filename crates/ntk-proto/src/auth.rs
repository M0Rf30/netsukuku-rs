//! Sender-authentication primitives for [`crate::v1::Auth`]: a canonical
//! signing encoding, `sign`/`verify`, and a bounded replay guard.
//!
//! **Scope.** This module supplies the cryptographic contract only — how a
//! sender proves it produced a given `(method, payload, sequence)` triple,
//! and how a receiver checks that proof. It does not decide *when* a
//! receiver requires `Auth` to be present (that stays a per-deployment,
//! per-call-site policy so an unauthenticated peer keeps interoperating —
//! see [`crate::v1::Envelope`]'s doc comment), and it does not decide how a
//! consumer maps its own RPC methods to a `method` string, or where a
//! [`SequenceGuard`] lives — those are the next layer's job (`ntk-rpc`'s
//! per-arc hop auth, `ntk-peerservices`/`ntk-coordinator`'s per-origin-request
//! auth).
//!
//! **Cost.** Ed25519 verify is commonly quoted at ~100-150 microseconds;
//! measured on the reference machine here it is faster (26.78 µs) but still
//! roughly two orders of magnitude past the pre-authentication envelope
//! codec path (139.89 ns encode / 350.56 ns decode / 503.11 ns round trip —
//! `benches/codec.rs`'s baseline, with the `auth` group's numbers measured
//! alongside it). That is why this module is a pair of *callable*
//! primitives rather than something wired into every `Envelope` decode:
//! authentication is meant to run per-arc (hop auth, amortized over a
//! link's lifetime) or per origin-request (not per relay hop), never
//! blindly per-message.
//!
//! **Precedent and deliberate improvement.** Shape and replay policy mirror
//! `ntk_andna::record::RegisterRequest`: a self-asserted public key
//! traveling with the message (`record.rs:150-151`), a `sequence` token
//! that must strictly increase per signer (`record.rs:16-20`), and a
//! length-prefixed canonical signing encoding distinct from the protobuf
//! wire format (`record.rs:124-128`). One thing this module does that
//! precedent does not: `DOMAIN_TAG` domain-separates the signature so it
//! can never be replayed into a different protocol context. ANDNA's own
//! `signing_bytes` has no such separator — an oversight this module does
//! not repeat.

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use std::collections::HashMap;
use thiserror::Error;

use crate::v1::Auth;

/// Mixed into every signature's input so a signature produced for this
/// scheme can never be replayed as valid input in a different protocol
/// context — a different message type, a different major protocol version,
/// or an entirely unrelated system that happens to reuse the same signing
/// key. Trailing NUL guarantees this constant cannot collide with any
/// human-typed prefix of itself.
const DOMAIN_TAG: &[u8] = b"netsukuku-rs/v1/rpc-auth\0";

/// Wire length of an ed25519 [`VerifyingKey`].
const VERIFYING_KEY_LEN: usize = 32;
/// Wire length of an ed25519 [`Signature`].
const SIGNATURE_LEN: usize = 64;

/// Everything that can go wrong producing or checking sender authentication.
/// Peer-supplied bytes never panic here: a short/long `signer_key` or
/// `signature` is [`AuthError::BadKeyLength`]/[`AuthError::BadSignatureLength`],
/// never an `unwrap`.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum AuthError {
    /// `Auth::signer_key` was not exactly `VERIFYING_KEY_LEN` bytes.
    #[error("signer_key must be {VERIFYING_KEY_LEN} bytes, got {0}")]
    BadKeyLength(usize),

    /// `Auth::signature` was not exactly `SIGNATURE_LEN` bytes.
    #[error("signature must be {SIGNATURE_LEN} bytes, got {0}")]
    BadSignatureLength(usize),

    /// `Auth::signer_key`'s bytes are the right length but not a valid
    /// compressed Edwards point.
    #[error("signer_key is not a valid ed25519 public key")]
    MalformedKey,

    /// The signature does not verify against `(method, payload, sequence)`
    /// under the claimed `signer_key` — wrong key, tampered payload,
    /// mismatched method, or mismatched sequence.
    #[error("signature verification failed")]
    SignatureMismatch,

    /// [`SequenceGuard::observe`] saw a sequence that is not strictly
    /// greater than the highest one already recorded for this signer.
    #[error("sequence {got} is not greater than last-seen {last_seen} for this signer")]
    Replayed {
        /// The rejected sequence.
        got: u64,
        /// The highest sequence previously accepted for this signer.
        last_seen: u64,
    },
}

/// BLAKE3 digest of `method` and `payload`, each length-prefixed so two
/// distinct `(method, payload)` pairs can never hash identically
/// (concatenation/canonicalization ambiguity) — the same discipline as
/// `ntk_andna::record::RegisterRequest::signing_bytes`'s length-prefixed
/// vector, applied at the point the variable-length fields are combined.
///
/// Hashing first, rather than feeding `method`/`payload` straight into
/// [`sign`]/[`verify`], bounds what ed25519 itself processes to a fixed 32
/// bytes regardless of `payload`'s size. That matters because plain (not
/// "ph"-prehashed) Ed25519 hashes its whole input message *twice* with
/// SHA-512 (once for nonce derivation, once for the challenge) — for a
/// large `payload` (a `Fingerprint` climbed to 16 levels, a batch of SNSD
/// records, ...) that is real, avoidable cost. BLAKE3 hashes it once, and
/// is substantially faster per byte than SHA-512 to begin with.
fn digest(method: &str, payload: &[u8]) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(method.len() as u32).to_le_bytes());
    hasher.update(method.as_bytes());
    hasher.update(&(payload.len() as u32).to_le_bytes());
    hasher.update(payload);
    hasher.finalize()
}

/// The canonical, unambiguous byte encoding [`sign`]/[`verify`] operate
/// over — deliberately *not* this crate's protobuf wire encoding
/// (`Envelope`/`Auth`), whose byte layout is not a serializer-stability
/// guarantee this scheme should depend on.
///
/// Layout, field by field, in order, every field fixed-width (so there is
/// no framing ambiguity left to resolve at this level — the only
/// variable-width inputs, `method` and `payload`, are already collapsed
/// into a fixed-size digest by [`digest`]):
///
/// 1. [`DOMAIN_TAG`] — fixed 25 bytes, verbatim.
/// 2. [`digest(method, payload)`](digest) — 32 bytes, BLAKE3, binding both
///    the method/call discriminant and the full payload.
/// 3. `sequence` — 8 bytes, little-endian, this request's replay token.
///
/// `signer_key` and `signature` are deliberately absent from this
/// encoding: the key is *asserted* by the caller of [`verify`] (ANDNA-style
/// "ownership is provable from the request alone"), and a signature can
/// never cover itself.
fn signing_bytes(method: &str, payload: &[u8], sequence: u64) -> Vec<u8> {
    let digest = digest(method, payload);
    let mut out = Vec::with_capacity(DOMAIN_TAG.len() + blake3::OUT_LEN + 8);
    out.extend_from_slice(DOMAIN_TAG);
    out.extend_from_slice(digest.as_bytes());
    out.extend_from_slice(&sequence.to_le_bytes());
    out
}

/// Signs `payload` (already encoded by the caller — this module has no
/// opinion on its shape) for RPC `method` at `sequence`, producing a wire
/// [`Auth`] block ready to attach to an [`crate::v1::Envelope`].
///
/// `method` should be a stable discriminant for the call this signature is
/// bound to (e.g. a fully-qualified RPC method name); it, along with
/// `payload`, is exactly what [`verify`] must be given to accept this
/// signature — a signature produced for one `method` never verifies
/// against another (see the transplant test in this module).
#[must_use]
pub fn sign(signing_key: &SigningKey, sequence: u64, method: &str, payload: &[u8]) -> Auth {
    let signature = signing_key.sign(&signing_bytes(method, payload, sequence));
    Auth {
        signer_key: signing_key.verifying_key().to_bytes().to_vec(),
        sequence,
        signature: signature.to_bytes().to_vec(),
    }
}

/// Verifies `auth` was produced by [`sign`] for this exact
/// `(method, payload, auth.sequence)` triple, returning the signer's
/// [`VerifyingKey`] on success — callers use the returned key as the now-
/// authenticated sender identity, e.g. to key a [`SequenceGuard`] or to pin
/// against a previously-claimed identity.
///
/// Does **not** check replay/staleness: this function only proves *who*
/// signed a message, never how many other messages that signer has sent.
/// Callers that need replay rejection own a [`SequenceGuard`] and call
/// [`SequenceGuard::observe`] themselves with the key this function
/// returns.
///
/// # Errors
/// [`AuthError::BadKeyLength`] / [`AuthError::BadSignatureLength`] if
/// `auth.signer_key` / `auth.signature` are not exactly 32 / 64 bytes;
/// [`AuthError::MalformedKey`] if `signer_key`'s bytes are not a valid
/// compressed Edwards point; [`AuthError::SignatureMismatch`] if the
/// signature does not verify against `(method, payload, auth.sequence)`.
pub fn verify(auth: &Auth, method: &str, payload: &[u8]) -> Result<VerifyingKey, AuthError> {
    let key_bytes: [u8; VERIFYING_KEY_LEN] = auth
        .signer_key
        .as_slice()
        .try_into()
        .map_err(|_| AuthError::BadKeyLength(auth.signer_key.len()))?;
    let sig_bytes: [u8; SIGNATURE_LEN] = auth
        .signature
        .as_slice()
        .try_into()
        .map_err(|_| AuthError::BadSignatureLength(auth.signature.len()))?;
    let signer_key = VerifyingKey::from_bytes(&key_bytes).map_err(|_| AuthError::MalformedKey)?;
    let signature = Signature::from_bytes(&sig_bytes);
    signer_key
        .verify(&signing_bytes(method, payload, auth.sequence), &signature)
        .map_err(|_| AuthError::SignatureMismatch)?;
    Ok(signer_key)
}

/// Default cap on distinct signer keys tracked at once by [`SequenceGuard::new`]
/// — see [`SequenceGuard`]'s doc comment for the policy this bounds.
pub const DEFAULT_MAX_SIGNERS: usize = 4096;

/// Tracks the highest sequence [`verify`]-accepted per signer key and
/// rejects a replay (an already-seen or lower sequence). *Not* wired into
/// [`verify`] itself: [`verify`] only proves who signed a message, never
/// which signers a given call site should even be tracking, or for how
/// long — the consumer (an arc registry for hop auth, a servant for origin
/// auth) owns that decision and this guard's storage.
///
/// **Bounded, because it is keyed by peer-supplied data.** `signer_key` in
/// every [`Auth`] is attacker-controlled: an unbounded map here would be a
/// fresh remote-memory-exhaustion defect of exactly the family this whole
/// change closes (a hostile peer mints an unlimited number of ed25519
/// keypairs, each used once, each permanently occupying an entry). This
/// guard caps its table at `max_signers` entries ([`DEFAULT_MAX_SIGNERS`]
/// unless overridden via [`SequenceGuard::with_capacity`]) and, once full,
/// evicts the least-recently-*updated* signer to admit a new one — bounded
/// LRU, not "refuse new signers".
///
/// This is a deliberate trade-off, not an oversight: eviction forgets that
/// signer's high-water mark, so a verbatim replay of an old message from an
/// *evicted* signer would pass this guard again. That window only opens
/// under sustained pressure from more distinct signers than `max_signers`
/// permits at once, and closes again the moment that signer is next
/// observed. Callers whose signer cardinality is naturally bounded by
/// topology — hop auth: current neighbors; origin auth: peers with an
/// in-flight request — should size `max_signers` comfortably above that
/// real cardinality so eviction never triggers in normal operation, and
/// treat a triggered eviction as a signal worth counting/alerting on, not
/// silently absorbing.
#[derive(Debug)]
pub struct SequenceGuard {
    max_signers: usize,
    clock: u64,
    seen: HashMap<VerifyingKey, (u64, u64)>,
}

impl SequenceGuard {
    /// A guard capped at [`DEFAULT_MAX_SIGNERS`] distinct signers.
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_MAX_SIGNERS)
    }

    /// A guard capped at `max_signers` distinct signers (clamped to at
    /// least 1).
    #[must_use]
    pub fn with_capacity(max_signers: usize) -> Self {
        Self {
            max_signers: max_signers.max(1),
            clock: 0,
            seen: HashMap::new(),
        }
    }

    /// Number of distinct signers currently tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.seen.len()
    }

    /// Whether no signer is currently tracked.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.seen.is_empty()
    }

    /// Records `sequence` as seen for `signer`. Mirrors ANDNA's own
    /// strictly-increasing replay policy (`ntk_andna::record` module doc,
    /// contrasted there with upstream's looser `>` check): a
    /// byte-for-byte replay of the most recently accepted sequence is
    /// rejected exactly like an older one.
    ///
    /// # Errors
    /// [`AuthError::Replayed`] if `sequence` is not strictly greater than
    /// the last-seen sequence already recorded for `signer`.
    pub fn observe(&mut self, signer: VerifyingKey, sequence: u64) -> Result<(), AuthError> {
        self.clock += 1;
        let clock = self.clock;

        if let Some((last_seen, touched)) = self.seen.get_mut(&signer) {
            if sequence <= *last_seen {
                return Err(AuthError::Replayed {
                    got: sequence,
                    last_seen: *last_seen,
                });
            }
            *last_seen = sequence;
            *touched = clock;
            return Ok(());
        }

        if self.seen.len() >= self.max_signers
            && let Some(oldest) = self
                .seen
                .iter()
                .min_by_key(|&(_, &(_, touched))| touched)
                .map(|(key, _)| *key)
        {
            self.seen.remove(&oldest);
        }
        self.seen.insert(signer, (sequence, clock));
        Ok(())
    }
}

impl Default for SequenceGuard {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    #[test]
    fn a_valid_signature_verifies_and_returns_the_signer() {
        let signing_key = key(1);
        let auth = sign(&signing_key, 1, "neighborhood.here_i_am", b"payload");
        let signer = verify(&auth, "neighborhood.here_i_am", b"payload").unwrap();
        assert_eq!(signer, signing_key.verifying_key());
    }

    #[test]
    fn a_tampered_payload_fails_verification() {
        let signing_key = key(2);
        let auth = sign(&signing_key, 1, "neighborhood.here_i_am", b"payload");
        let err = verify(&auth, "neighborhood.here_i_am", b"tampered").unwrap_err();
        assert_eq!(err, AuthError::SignatureMismatch);
    }

    #[test]
    fn a_signature_transplanted_onto_a_different_method_fails() {
        let signing_key = key(3);
        let auth = sign(&signing_key, 1, "neighborhood.here_i_am", b"payload");
        let err = verify(&auth, "coordinator.execute_prepare_enter", b"payload").unwrap_err();
        assert_eq!(err, AuthError::SignatureMismatch);
    }

    #[test]
    fn a_signature_transplanted_onto_a_different_sequence_fails() {
        let signing_key = key(4);
        let auth = sign(&signing_key, 1, "neighborhood.here_i_am", b"payload");
        let mut replayed = auth.clone();
        replayed.sequence = 2;
        let err = verify(&replayed, "neighborhood.here_i_am", b"payload").unwrap_err();
        assert_eq!(err, AuthError::SignatureMismatch);
    }

    #[test]
    fn a_malformed_key_length_is_an_error_not_a_panic() {
        let signing_key = key(5);
        let mut auth = sign(&signing_key, 1, "m", b"p");
        auth.signer_key.truncate(31);
        assert_eq!(
            verify(&auth, "m", b"p").unwrap_err(),
            AuthError::BadKeyLength(31)
        );
    }

    #[test]
    fn a_malformed_signature_length_is_an_error_not_a_panic() {
        let signing_key = key(6);
        let mut auth = sign(&signing_key, 1, "m", b"p");
        auth.signature.push(0);
        assert_eq!(
            verify(&auth, "m", b"p").unwrap_err(),
            AuthError::BadSignatureLength(65)
        );
    }

    #[test]
    fn a_malformed_key_that_is_not_a_valid_point_is_an_error_not_a_panic() {
        let signing_key = key(7);
        let mut auth = sign(&signing_key, 1, "m", b"p");
        // 0x00.. with a trailing 0xFF byte does not decompress to any
        // point on the curve.
        auth.signer_key = {
            let mut b = vec![0u8; 32];
            b[31] = 0xFF;
            b
        };
        assert_eq!(
            verify(&auth, "m", b"p").unwrap_err(),
            AuthError::MalformedKey
        );
    }

    #[test]
    fn sequence_guard_accepts_strictly_increasing_sequences() {
        let mut guard = SequenceGuard::new();
        let signer = key(8).verifying_key();
        guard.observe(signer, 1).unwrap();
        guard.observe(signer, 2).unwrap();
        guard.observe(signer, 100).unwrap();
        assert_eq!(guard.len(), 1);
    }

    #[test]
    fn sequence_guard_rejects_an_exact_replay() {
        let mut guard = SequenceGuard::new();
        let signer = key(9).verifying_key();
        guard.observe(signer, 5).unwrap();
        let err = guard.observe(signer, 5).unwrap_err();
        assert_eq!(
            err,
            AuthError::Replayed {
                got: 5,
                last_seen: 5
            }
        );
    }

    #[test]
    fn sequence_guard_rejects_a_stale_sequence() {
        let mut guard = SequenceGuard::new();
        let signer = key(10).verifying_key();
        guard.observe(signer, 10).unwrap();
        let err = guard.observe(signer, 3).unwrap_err();
        assert_eq!(
            err,
            AuthError::Replayed {
                got: 3,
                last_seen: 10
            }
        );
    }

    #[test]
    fn sequence_guard_tracks_independent_signers_independently() {
        let mut guard = SequenceGuard::new();
        let alice = key(11).verifying_key();
        let bob = key(12).verifying_key();
        guard.observe(alice, 50).unwrap();
        // Bob's first sequence is unrelated to Alice's history.
        guard.observe(bob, 1).unwrap();
        assert_eq!(guard.len(), 2);
    }

    #[test]
    fn sequence_guard_evicts_the_least_recently_updated_signer_once_at_capacity() {
        let mut guard = SequenceGuard::with_capacity(2);
        let a = key(20).verifying_key();
        let b = key(21).verifying_key();
        let c = key(22).verifying_key();

        guard.observe(a, 1).unwrap();
        guard.observe(b, 1).unwrap();
        assert_eq!(guard.len(), 2);

        // `a` is now the least-recently-updated; admitting `c` evicts it.
        guard.observe(c, 1).unwrap();
        assert_eq!(guard.len(), 2);

        // `a`'s history is forgotten: a value that would have been a
        // replay is now accepted again (the documented eviction trade-off).
        guard.observe(a, 1).unwrap();
        assert_eq!(guard.len(), 2);
    }

    #[test]
    fn sequence_guard_capacity_is_clamped_to_at_least_one() {
        let guard = SequenceGuard::with_capacity(0);
        assert_eq!(guard.max_signers, 1);
    }
}

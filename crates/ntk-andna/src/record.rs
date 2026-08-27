//! Signed registration/renewal requests, the record a hash-node holds for one hostname, and the
//! collision/replay/TTL policy that decides whether a request is accepted
//! (`andna_reg_pkt`/`andna_cache`, `research/impl/c/netsukuku/src/andna.h:101-116`,
//! `andna_cache.h:89-103`).
//!
//! **Cryptography, deliberate deviation from upstream**: upstream signs with RSA-1024
//! (`ANDNA_PRIVKEY_BITS`, `research/impl/c/netsukuku/src/andna_cache.h:24`); this crate's
//! assignment specifies `ed25519-dalek` instead — smaller keys/signatures, no RSA padding-oracle
//! history, and a modern, audited implementation. Wire compatibility with the Vala/C daemon is a
//! stated non-goal for this whole port.
//!
//! **Replay protection, deliberate deviation from upstream**: upstream's own update check is
//! `if (acq->hname_updates > req->hname_updates) reject` (`research/impl/c/netsukuku/src/andna.c`,
//! per the update-validation path) — note the `>`, not `>=`: replaying the *most recent* signed
//! renewal verbatim passes this check unchanged (`hname_updates` unchanged means "not greater"),
//! so upstream's own scheme only rate-limits replay via the separate `ANDNA_MIN_UPDATE_TIME`
//! cooldown, not by rejecting it outright. This crate requires `sequence` to *strictly* increase
//! on every accepted registration/renewal, so a byte-for-byte replay of any prior accepted
//! request is unconditionally rejected as [`RegisterRejected::StaleSequence`], independent of the
//! cooldown.

use std::collections::BTreeMap;
use std::time::Duration;

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use ntk_common::Naddr;

use crate::error::Error;
use crate::hostname::Hostname;
use crate::snsd::{SnsdRecord, SnsdTable, SnsdTarget, ZERO_SERVICE};

/// Deterministic domain-level encoding of a target, for [`RegisterRequest::signing_bytes`] —
/// distinct from this crate's protobuf wire format, which is not guaranteed byte-stable and is
/// not what ownership is proven over.
fn encode_target(out: &mut Vec<u8>, target: &SnsdTarget) {
    match target {
        SnsdTarget::Address(naddr) => {
            out.push(0);
            out.extend((naddr.positions().len() as u32).to_le_bytes());
            for gsize in naddr.topology().gsizes() {
                out.extend(gsize.to_le_bytes());
            }
            for pos in naddr.positions() {
                out.extend(pos.to_le_bytes());
            }
        }
        SnsdTarget::Alias(hostname) => {
            out.push(1);
            let bytes = hostname.as_str().as_bytes();
            out.extend((bytes.len() as u32).to_le_bytes());
            out.extend(bytes);
        }
    }
}

/// A signed registration or renewal of `hostname` — ANDNA's `register_node` role
/// (`andna_reg_pkt`, `research/impl/c/netsukuku/src/andna.h:101-116`).
#[derive(Clone, Debug)]
pub struct RegisterRequest {
    /// The name being registered or renewed.
    pub hostname: Hostname,
    /// The registrant's ed25519 public key — its persistent identity for this hostname.
    pub owner_key: VerifyingKey,
    /// The address the zero (service-0) SNSD record resolves to.
    pub owner_naddr: Naddr,
    /// Strictly increasing across every accepted registration/renewal of `hostname` by
    /// `owner_key` — this request's replay-protection token.
    pub sequence: u64,
    /// Informational client clock reading; never trusted for TTL/rate-limit accounting (see
    /// [`crate::actor::unix_now`]'s doc comment).
    pub timestamp_unix: u64,
    /// Priority of the zero (service-0) record.
    pub zero_priority: u8,
    /// Weight of the zero (service-0) record; `0` disables it.
    pub zero_weight: u8,
    /// Additional SNSD records (NTK_RFC 0009). MUST NOT contain a `service == 0` entry — the
    /// zero record is `owner_naddr`/`zero_priority`/`zero_weight` instead.
    pub snsd_records: Vec<SnsdRecord>,
    /// Ed25519 signature over [`RegisterRequest::signing_bytes`].
    pub signature: Signature,
}

impl RegisterRequest {
    /// Builds and signs a request with `signing_key`. The resulting [`RegisterRequest::verify`]
    /// always succeeds against the corresponding [`SigningKey::verifying_key`].
    ///
    /// # Errors
    /// [`Error::ReservedServiceZero`] if `snsd_records` contains an explicit `service == 0`
    /// entry; [`Error::WeightTooLarge`] if `zero_weight` exceeds [`crate::snsd::MAX_WEIGHT`].
    #[allow(clippy::too_many_arguments)]
    pub fn sign(
        signing_key: &SigningKey,
        hostname: Hostname,
        owner_naddr: Naddr,
        sequence: u64,
        timestamp_unix: u64,
        zero_priority: u8,
        zero_weight: u8,
        snsd_records: Vec<SnsdRecord>,
    ) -> Result<Self, Error> {
        if snsd_records.iter().any(|r| r.service == ZERO_SERVICE) {
            return Err(Error::ReservedServiceZero);
        }
        if zero_weight > crate::snsd::MAX_WEIGHT {
            return Err(Error::WeightTooLarge(zero_weight));
        }
        let owner_key = signing_key.verifying_key();
        let mut unsigned = Self {
            hostname,
            owner_key,
            owner_naddr,
            sequence,
            timestamp_unix,
            zero_priority,
            zero_weight,
            snsd_records,
            signature: Signature::from_bytes(&[0u8; 64]),
        };
        let signature = signing_key.sign(&unsigned.signing_bytes());
        unsigned.signature = signature;
        Ok(unsigned)
    }

    /// The deterministic byte encoding [`RegisterRequest::sign`]/[`RegisterRequest::verify`]
    /// sign/check — every field except `signature` itself, in a fixed order with explicit
    /// length prefixes on variable-length data. This is *not* this crate's protobuf wire
    /// encoding (`crate::wire`): signing over a purpose-built, unambiguous encoding avoids
    /// depending on any serializer's byte-stability guarantees.
    fn signing_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        let name = self.hostname.as_str().as_bytes();
        out.extend((name.len() as u32).to_le_bytes());
        out.extend(name);
        out.extend(self.owner_key.to_bytes());
        encode_target(&mut out, &SnsdTarget::Address(self.owner_naddr.clone()));
        out.extend(self.sequence.to_le_bytes());
        out.extend(self.timestamp_unix.to_le_bytes());
        out.push(self.zero_priority);
        out.push(self.zero_weight);
        out.extend((self.snsd_records.len() as u32).to_le_bytes());
        for record in &self.snsd_records {
            out.extend(record.service.to_le_bytes());
            out.push(record.priority);
            out.push(record.weight);
            encode_target(&mut out, &record.target);
        }
        out
    }

    /// Verifies this request's signature against its own `owner_key` — ownership is provable
    /// from the request alone, with no side channel to the registrant required.
    ///
    /// # Errors
    /// [`Error::InvalidSignature`] if the signature does not match.
    pub fn verify(&self) -> Result<(), Error> {
        self.owner_key
            .verify(&self.signing_bytes(), &self.signature)
            .map_err(|_| Error::InvalidSignature)
    }
}

/// What a hash-node stores for one live hostname (`andna_cache`,
/// `research/impl/c/netsukuku/src/andna_cache.h:89-103`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostedRecord {
    /// The owner's persistent identity; a renewal MUST be signed by this same key.
    pub owner_key: VerifyingKey,
    /// Last accepted request's replay-protection token.
    pub sequence: u64,
    /// When this hostname was first registered (unix seconds).
    pub registered_at: u64,
    /// When this hostname was last registered or renewed (unix seconds) — the
    /// [`crate::config::Config::min_renewal_interval`] anchor.
    pub renewed_at: u64,
    /// When this record stops resolving/renewing without action (unix seconds).
    pub expires_at: u64,
    /// This hostname's SNSD record set (NTK_RFC 0009), including the zero record.
    pub snsd: SnsdTable,
}

impl HostedRecord {
    /// True once `now` has reached or passed this record's TTL — the record is up for a fresh
    /// registration (possibly by a different owner) at that point.
    #[must_use]
    pub fn is_expired(&self, now: u64) -> bool {
        now >= self.expires_at
    }
}

/// A successfully applied registration or renewal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegisterOutcome {
    Registered { expires_at: u64 },
    Renewed { expires_at: u64 },
}

/// Why [`Cache::register`] declined a request.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RegisterRejected {
    #[error("signature verification failed")]
    InvalidSignature,
    #[error("snsd_records must not contain an explicit service-0 entry")]
    ReservedServiceZero,
    /// The hostname is currently live under a different owner key — covers both a fresh
    /// collision (two different registrants racing the same name) and a "renewal" signed with
    /// the wrong key, which are indistinguishable to the hash-node: both are a request claiming
    /// a hostname that someone else currently, verifiably, owns. First valid registration wins;
    /// this is the tiebreak (`research/notes/02-vala-services-daemon.md` §4's "first-come-wins"
    /// framing; upstream's own per-hostname `andna_cache_queue` collision arbitration among
    /// multiple simultaneously-claimed pubkeys was not fully traceable to a resolve-time tiebreak
    /// rule from the C source read for this port, so this crate implements the simpler, fully
    /// specified single-owner-per-hostname model instead of porting that ambiguity).
    #[error("hostname is currently owned by a different key")]
    OwnedByOther,
    /// `sequence` did not strictly increase past the stored record's — a stale, out-of-order, or
    /// replayed request.
    #[error("sequence {given} is not greater than the stored sequence {stored}")]
    StaleSequence { given: u64, stored: u64 },
    /// A renewal arrived before [`crate::config::Config::min_renewal_interval`] elapsed since the
    /// last one.
    #[error("renewed too recently; retry after {retry_after:?}")]
    RenewalTooSoon { retry_after: Duration },
    #[error("SNSD cap exceeded: {0}")]
    SnsdCap(#[from] SnsdCapError),
    /// This node's `Andna` role already holds [`crate::config::Config::max_hosted_records`]
    /// distinct hostname keys — see that field's own doc for why this is a per-node bound,
    /// independent of [`RegisterRejected`]'s other, per-registrant/per-hostname checks. Never
    /// raised for a renewal or takeover of a hostname key this cache already holds (live,
    /// stale, or not-yet-purged) — only a brand-new key hits this once the cache is full.
    #[error("this node already hosts the maximum {cap} hostname records")]
    HostCapacityExceeded { cap: usize },
}

/// SNSD-cap violations, surfaced separately from [`Error`] since they are a registration-request
/// outcome, not a construction/decode failure.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SnsdCapError {
    #[error("hostname already has the maximum {0} total SNSD records")]
    Total(usize),
    #[error("service {service} already has the maximum {max} SNSD records")]
    PerService { service: u32, max: usize },
}

fn snsd_error(e: Error) -> SnsdCapError {
    match e {
        Error::TooManySnsdRecords(max) => SnsdCapError::Total(max),
        Error::TooManyRecordsForService { service, max } => {
            SnsdCapError::PerService { service, max }
        }
        other => unreachable!("SnsdTable::insert never returns {other:?} for a capacity check"),
    }
}

/// The Andna service's per-node hostname store: every hostname this node holds as hash-node or
/// replica.
#[derive(Clone, Debug, Default)]
pub struct Cache {
    records: BTreeMap<Hostname, HostedRecord>,
}

impl Cache {
    /// An empty cache: this node holds no hostnames yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Applies `req`, per the collision/replay/TTL policy documented on
    /// [`RegisterRejected`]/this module.
    #[allow(clippy::too_many_arguments)]
    pub fn register(
        &mut self,
        req: &RegisterRequest,
        now: u64,
        ttl: Duration,
        min_renewal_interval: Duration,
        max_snsd_per_service: usize,
        max_snsd_total: usize,
        max_hosted_records: usize,
    ) -> Result<RegisterOutcome, RegisterRejected> {
        req.verify()
            .map_err(|_| RegisterRejected::InvalidSignature)?;
        if req.snsd_records.iter().any(|r| r.service == ZERO_SERVICE) {
            return Err(RegisterRejected::ReservedServiceZero);
        }

        // A brand-new hostname key would grow `self.records` past its cap — refuse it outright.
        // A renewal/takeover of a key already present here (live, stale, or not yet purged by
        // `run_expiry_reclaimer`) never grows the map, so it is never blocked by this check —
        // only ever-larger *distinct-key* growth is (`Config::max_hosted_records`'s own doc).
        if !self.records.contains_key(&req.hostname) && self.records.len() >= max_hosted_records {
            return Err(RegisterRejected::HostCapacityExceeded {
                cap: max_hosted_records,
            });
        }

        let is_fresh_slot = self
            .records
            .get(&req.hostname)
            .is_none_or(|existing| existing.is_expired(now) || existing.owner_key != req.owner_key);

        if !is_fresh_slot {
            let existing = self.records.get(&req.hostname).expect("checked above");
            if req.sequence <= existing.sequence {
                return Err(RegisterRejected::StaleSequence {
                    given: req.sequence,
                    stored: existing.sequence,
                });
            }
            let min_next = existing
                .renewed_at
                .saturating_add(min_renewal_interval.as_secs());
            if now < min_next {
                return Err(RegisterRejected::RenewalTooSoon {
                    retry_after: Duration::from_secs(min_next - now),
                });
            }
        } else if let Some(existing) = self.records.get(&req.hostname)
            && !existing.is_expired(now)
            && existing.owner_key != req.owner_key
        {
            return Err(RegisterRejected::OwnedByOther);
        }

        let mut snsd = SnsdTable::new();
        snsd.set_zero_record(req.owner_naddr.clone(), req.zero_priority, req.zero_weight);
        for record in req.snsd_records.clone() {
            snsd.insert(record, max_snsd_per_service, max_snsd_total)
                .map_err(|e| RegisterRejected::SnsdCap(snsd_error(e)))?;
        }

        let expires_at = now + ttl.as_secs();
        let outcome = if is_fresh_slot {
            self.records.insert(
                req.hostname.clone(),
                HostedRecord {
                    owner_key: req.owner_key,
                    sequence: req.sequence,
                    registered_at: now,
                    renewed_at: now,
                    expires_at,
                    snsd,
                },
            );
            RegisterOutcome::Registered { expires_at }
        } else {
            let record = self
                .records
                .get_mut(&req.hostname)
                .expect("is_fresh_slot=false implies a live record exists");
            record.sequence = req.sequence;
            record.renewed_at = now;
            record.expires_at = expires_at;
            record.snsd = snsd;
            RegisterOutcome::Renewed { expires_at }
        };
        Ok(outcome)
    }

    /// Resolves `hostname`'s records for `service` (see [`SnsdTable::resolve`]); an unknown or
    /// expired hostname resolves to an empty list.
    pub fn resolve(
        &self,
        hostname: &Hostname,
        service: u16,
        now: u64,
        rng: &mut impl rand::Rng,
    ) -> Vec<SnsdRecord> {
        match self.records.get(hostname) {
            Some(record) if !record.is_expired(now) => record.snsd.resolve(service, rng),
            _ => Vec::new(),
        }
    }

    /// Drops every expired record, returning the hostnames freed.
    pub fn purge_expired(&mut self, now: u64) -> Vec<Hostname> {
        let expired: Vec<Hostname> = self
            .records
            .iter()
            .filter(|(_, r)| r.is_expired(now))
            .map(|(h, _)| h.clone())
            .collect();
        for hostname in &expired {
            self.records.remove(hostname);
        }
        expired
    }

    /// A read-only view of every hostname currently held, for [`crate::actor::Snapshot`].
    #[must_use]
    pub fn records(&self) -> &BTreeMap<Hostname, HostedRecord> {
        &self.records
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ntk_common::Topology;

    fn naddr(pos: u32) -> Naddr {
        Naddr::new(Topology::new([8]).unwrap(), [pos]).unwrap()
    }

    fn signed(key: &SigningKey, name: &str, seq: u64, now: u64) -> RegisterRequest {
        RegisterRequest::sign(
            key,
            Hostname::new(name).unwrap(),
            naddr(0),
            seq,
            now,
            16,
            1,
            Vec::new(),
        )
        .unwrap()
    }

    fn key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    #[test]
    fn valid_signature_registers() {
        let mut cache = Cache::new();
        let k = key(1);
        let req = signed(&k, "angelica", 1, 1000);
        let out = cache
            .register(
                &req,
                1000,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                usize::MAX,
            )
            .unwrap();
        assert!(matches!(out, RegisterOutcome::Registered { .. }));
    }

    #[test]
    fn tampered_field_rejected() {
        let k = key(1);
        let mut req = signed(&k, "angelica", 1, 1000);
        req.sequence = 999; // mutate after signing, without re-signing
        let mut cache = Cache::new();
        let err = cache
            .register(
                &req,
                1000,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                usize::MAX,
            )
            .unwrap_err();
        assert_eq!(err, RegisterRejected::InvalidSignature);
    }

    #[test]
    fn wrong_key_renewal_rejected() {
        let mut cache = Cache::new();
        let owner = key(1);
        let attacker = key(2);
        cache
            .register(
                &signed(&owner, "angelica", 1, 1000),
                1000,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                usize::MAX,
            )
            .unwrap();
        let err = cache
            .register(
                &signed(&attacker, "angelica", 2, 1001),
                1001,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                usize::MAX,
            )
            .unwrap_err();
        assert_eq!(err, RegisterRejected::OwnedByOther);
    }

    #[test]
    fn replayed_request_rejected() {
        let mut cache = Cache::new();
        let owner = key(1);
        let req = signed(&owner, "angelica", 1, 1000);
        cache
            .register(
                &req,
                1000,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                usize::MAX,
            )
            .unwrap();
        let err = cache
            .register(
                &req,
                1005,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                usize::MAX,
            )
            .unwrap_err();
        assert_eq!(
            err,
            RegisterRejected::StaleSequence {
                given: 1,
                stored: 1
            }
        );
    }

    #[test]
    fn renewal_extends_ttl_and_bumps_sequence() {
        let mut cache = Cache::new();
        let owner = key(1);
        cache
            .register(
                &signed(&owner, "angelica", 1, 1000),
                1000,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                usize::MAX,
            )
            .unwrap();
        let out = cache
            .register(
                &signed(&owner, "angelica", 2, 1050),
                1050,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(out, RegisterOutcome::Renewed { expires_at: 1150 });
    }

    #[test]
    fn expired_record_can_be_taken_by_a_new_owner() {
        let mut cache = Cache::new();
        let owner = key(1);
        let other = key(2);
        cache
            .register(
                &signed(&owner, "angelica", 1, 1000),
                1000,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                usize::MAX,
            )
            .unwrap();
        // now = 1100 >= expires_at (1100): expired.
        let out = cache
            .register(
                &signed(&other, "angelica", 1, 1100),
                1100,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                usize::MAX,
            )
            .unwrap();
        assert!(matches!(out, RegisterOutcome::Registered { .. }));
    }

    #[test]
    fn resolve_unknown_hostname_is_empty() {
        let cache = Cache::new();
        let mut rng = rand::rng();
        let hits = cache.resolve(&Hostname::new("nope").unwrap(), 0, 1000, &mut rng);
        assert!(hits.is_empty());
    }

    #[test]
    fn resolve_expired_hostname_is_empty() {
        let mut cache = Cache::new();
        let owner = key(1);
        cache
            .register(
                &signed(&owner, "angelica", 1, 1000),
                1000,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                usize::MAX,
            )
            .unwrap();
        let mut rng = rand::rng();
        let hits = cache.resolve(&Hostname::new("angelica").unwrap(), 0, 1200, &mut rng);
        assert!(hits.is_empty());
    }

    /// Sub-defect A regression: before this cap existed, an inbound `RegisterRequest` for a
    /// brand-new hostname had no capacity check at all on this path — `Cache::register` would
    /// insert unconditionally. Fails before the `max_hosted_records` parameter/check existed
    /// (any call compiled with the old 6-argument signature accepted every distinct hostname
    /// unconditionally); passes after.
    #[test]
    fn a_brand_new_hostname_is_refused_once_the_node_is_at_capacity() {
        let mut cache = Cache::new();
        let owner = key(1);
        cache
            .register(
                &signed(&owner, "angelica", 1, 1000),
                1000,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                1,
            )
            .unwrap();
        let err = cache
            .register(
                &signed(&owner, "frenzu", 2, 1000),
                1000,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                1,
            )
            .unwrap_err();
        assert_eq!(err, RegisterRejected::HostCapacityExceeded { cap: 1 });
    }

    /// The host-capacity cap bounds *distinct hostname keys*, not registration traffic: renewing
    /// an already-tracked hostname must never be refused just because the node is "full" of that
    /// same one hostname.
    #[test]
    fn a_renewal_of_an_existing_hostname_is_never_blocked_by_the_host_capacity_cap() {
        let mut cache = Cache::new();
        let owner = key(1);
        cache
            .register(
                &signed(&owner, "angelica", 1, 1000),
                1000,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                1,
            )
            .unwrap();
        let out = cache
            .register(
                &signed(&owner, "angelica", 2, 1050),
                1050,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                1,
            )
            .unwrap();
        assert_eq!(out, RegisterOutcome::Renewed { expires_at: 1150 });
    }

    /// A hostname key already present (even expired) never counts as "new" for the capacity
    /// check — taking it over in place doesn't grow `records`, so it must succeed even when the
    /// node is otherwise completely at capacity.
    #[test]
    fn expired_hostname_can_be_taken_over_even_when_the_node_is_at_capacity() {
        let mut cache = Cache::new();
        let owner = key(1);
        let other = key(2);
        cache
            .register(
                &signed(&owner, "angelica", 1, 1000),
                1000,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                1,
            )
            .unwrap();
        // now = 1100 >= expires_at (1100): expired, but the key still occupies the one slot.
        let out = cache
            .register(
                &signed(&other, "angelica", 1, 1100),
                1100,
                Duration::from_secs(100),
                Duration::ZERO,
                16,
                256,
                1,
            )
            .unwrap();
        assert!(matches!(out, RegisterOutcome::Registered { .. }));
    }
}

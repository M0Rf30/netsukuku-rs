//! The Counter service's per-registrant reservation cache: enforces NTK_RFC 0007/upstream's
//! 256-live-hostnames-per-registrant cap (`counter_c`/`counter_c_hashes`,
//! `research/impl/c/netsukuku/src/andna_cache.h:106-143`).
//!
//! **Identity, deliberate deviation from upstream**: the scouted C source keys its counter
//! record by the registrant's **public key** (`counter_c_findpubk`,
//! `research/impl/c/netsukuku/src/andna_cache.c`) even though NTK_RFC 0007's whole stated
//! purpose is routing to the counter_gnode *by address* specifically so a registrant can't evade
//! the cap by minting new keypairs. Keying the stored reservation set by pubkey too would still
//! let one physical node bypass the cap (mint a new keypair, land on the same counter_gnode by
//! address, get a fresh empty per-pubkey record). This module instead keys reservations by the
//! requester's network position (`client_tuple`, populated by `ntk-peerservices`' own routing —
//! not a self-declared, spoofable payload field), which actually realizes RFC 0007's stated
//! intent. This is this crate's own reasoned correction, not a literal port.
//!
//! Reservations carry their own TTL, aged out independently of the Andna service's hostname
//! cache (mirroring `cc_hashes_del_expired`'s independent expiry,
//! `research/impl/c/netsukuku/src/andna_cache.h:445-446`) — the two services never message each
//! other directly, exactly as two independent `PeerService`s registered on the same substrate
//! should.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::hostname::HostnameHash;

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum CounterRejected {
    #[error("registrant already has the maximum {cap} live hostnames")]
    CapExceeded { cap: usize },
    /// This node's `Counter` role already tracks
    /// [`crate::config::Config::max_counter_registrants`] distinct registrant identities — see
    /// that field's own doc for why the registrant identity space itself needs an independent
    /// bound (virtual positions make minting a "new" registrant cheap). Never raised for a
    /// registrant already tracked here, however many reservations it holds.
    #[error("this node already tracks the maximum {cap} distinct registrants")]
    TooManyRegistrants { cap: usize },
}

#[derive(Clone, Debug, Default)]
struct Reservations {
    /// hostname hash -> expiry (unix seconds).
    hashes: BTreeMap<HostnameHash, u64>,
}

impl Reservations {
    fn live_count(&self, now: u64) -> usize {
        self.hashes.values().filter(|&&exp| exp > now).count()
    }
}

/// The Counter service's per-node state: one [`Reservations`] set per registrant network
/// position seen.
#[derive(Clone, Debug, Default)]
pub struct CounterCache {
    by_registrant: BTreeMap<Vec<u32>, Reservations>,
}

impl CounterCache {
    /// An empty cache: no registrant has reserved anything yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Reserves `hash` for `registrant` (its DHT-routing-verified position). Reserving an
    /// already-live hash for the same registrant is idempotent — it refreshes that
    /// reservation's own TTL without counting against the cap a second time (this is what makes
    /// a renewal's repeated Counter contact free of cap pressure, matching the real C
    /// implementation's own update path).
    ///
    /// # Errors
    /// [`CounterRejected::CapExceeded`] if `registrant` already holds `cap` other live
    /// reservations. [`CounterRejected::TooManyRegistrants`] if `registrant` is not already
    /// tracked and this node already tracks `max_registrants` other distinct registrants.
    pub fn try_reserve(
        &mut self,
        registrant: &[u32],
        hash: HostnameHash,
        now: u64,
        ttl: Duration,
        cap: usize,
        max_registrants: usize,
    ) -> Result<usize, CounterRejected> {
        if !self.by_registrant.contains_key(registrant)
            && self.by_registrant.len() >= max_registrants
        {
            return Err(CounterRejected::TooManyRegistrants {
                cap: max_registrants,
            });
        }
        let entry = self.by_registrant.entry(registrant.to_vec()).or_default();
        let is_live_renewal = entry.hashes.get(&hash).is_some_and(|&exp| exp > now);
        if !is_live_renewal && entry.live_count(now) >= cap {
            return Err(CounterRejected::CapExceeded { cap });
        }
        entry.hashes.insert(hash, now + ttl.as_secs());
        Ok(entry.live_count(now))
    }

    /// Drops every expired reservation across every registrant, for periodic hygiene
    /// ([`crate::actor::Handle::purge_expired`]).
    pub fn purge_expired(&mut self, now: u64) {
        for reservations in self.by_registrant.values_mut() {
            reservations.hashes.retain(|_, &mut exp| exp > now);
        }
        self.by_registrant.retain(|_, r| !r.hashes.is_empty());
    }

    /// A read-only view of every registrant's live reservation count, for
    /// [`crate::actor::Snapshot`].
    #[must_use]
    pub fn snapshot(&self, now: u64) -> BTreeMap<Vec<u32>, usize> {
        self.by_registrant
            .iter()
            .map(|(k, r)| (k.clone(), r.live_count(now)))
            .filter(|(_, count)| *count > 0)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(n: u32) -> HostnameHash {
        HostnameHash::from_bytes(*blake3::hash(&n.to_le_bytes()).as_bytes())
    }

    fn count(cache: &CounterCache, registrant: &[u32], now: u64) -> usize {
        cache.snapshot(now).get(registrant).copied().unwrap_or(0)
    }

    #[test]
    fn reserves_up_to_cap() {
        let mut cache = CounterCache::new();
        for i in 0..3u32 {
            cache
                .try_reserve(&[0], hash(i), 0, Duration::from_secs(100), 3, usize::MAX)
                .unwrap();
        }
        assert_eq!(count(&cache, &[0], 0), 3);
    }

    #[test]
    fn the_257th_distinct_hostname_is_rejected() {
        let mut cache = CounterCache::new();
        for i in 0..256u32 {
            cache
                .try_reserve(&[0], hash(i), 0, Duration::from_secs(100), 256, usize::MAX)
                .unwrap();
        }
        let err = cache
            .try_reserve(
                &[0],
                hash(256),
                0,
                Duration::from_secs(100),
                256,
                usize::MAX,
            )
            .unwrap_err();
        assert_eq!(err, CounterRejected::CapExceeded { cap: 256 });
    }

    #[test]
    fn renewing_an_already_reserved_hash_does_not_count_twice() {
        let mut cache = CounterCache::new();
        cache
            .try_reserve(&[0], hash(1), 0, Duration::from_secs(100), 1, usize::MAX)
            .unwrap();
        // Same hash again, well within the cap of 1: must succeed (it's a refresh, not a new
        // reservation) even though the cap is already "full".
        let count = cache
            .try_reserve(&[0], hash(1), 50, Duration::from_secs(100), 1, usize::MAX)
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn different_registrants_have_independent_caps() {
        let mut cache = CounterCache::new();
        cache
            .try_reserve(&[0], hash(1), 0, Duration::from_secs(100), 1, usize::MAX)
            .unwrap();
        cache
            .try_reserve(&[1], hash(1), 0, Duration::from_secs(100), 1, usize::MAX)
            .unwrap();
        assert_eq!(count(&cache, &[0], 0), 1);
        assert_eq!(count(&cache, &[1], 0), 1);
    }

    #[test]
    fn expired_reservation_frees_a_slot() {
        let mut cache = CounterCache::new();
        cache
            .try_reserve(&[0], hash(1), 0, Duration::from_secs(100), 1, usize::MAX)
            .unwrap();
        // At t=200 the reservation (expires at 100) is stale; a new hash may take the slot.
        cache
            .try_reserve(&[0], hash(2), 200, Duration::from_secs(100), 1, usize::MAX)
            .unwrap();
        assert_eq!(count(&cache, &[0], 200), 1);
    }

    #[test]
    fn purge_expired_removes_stale_registrants() {
        let mut cache = CounterCache::new();
        cache
            .try_reserve(&[0], hash(1), 0, Duration::from_secs(100), 1, usize::MAX)
            .unwrap();
        cache.purge_expired(200);
        assert!(cache.snapshot(200).is_empty());
    }

    /// Sub-defect C regression: before this cap existed, `try_reserve` tracked a brand-new
    /// registrant identity unconditionally, however many distinct identities `by_registrant`
    /// already held — an attacker varying `owner_naddr`/its virtual position per request grew
    /// this map without bound even though each individual registrant stayed under its own
    /// per-registrant cap. Fails before the `max_registrants` parameter/check existed; passes
    /// after.
    #[test]
    fn a_new_registrant_is_refused_once_this_node_tracks_the_maximum_distinct_registrants() {
        let mut cache = CounterCache::new();
        cache
            .try_reserve(&[0], hash(1), 0, Duration::from_secs(100), 256, 1)
            .unwrap();
        let err = cache
            .try_reserve(&[1], hash(1), 0, Duration::from_secs(100), 256, 1)
            .unwrap_err();
        assert_eq!(err, CounterRejected::TooManyRegistrants { cap: 1 });
    }

    /// The distinct-registrant cap bounds *identity churn*, not an already-tracked registrant's
    /// own traffic: a registrant already tracked here must keep reserving/renewing up to its own
    /// per-registrant cap even while the node is at its distinct-registrant limit.
    #[test]
    fn an_already_tracked_registrant_is_never_blocked_by_the_distinct_registrant_cap() {
        let mut cache = CounterCache::new();
        cache
            .try_reserve(&[0], hash(1), 0, Duration::from_secs(100), 256, 1)
            .unwrap();
        let count = cache
            .try_reserve(&[0], hash(2), 0, Duration::from_secs(100), 256, 1)
            .unwrap();
        assert_eq!(count, 2);
    }
}

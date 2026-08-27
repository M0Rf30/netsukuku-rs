//! SNSD (NTK_RFC 0009, "Scattered Name Service Disgregation"): per-service-number arrays of
//! alias/backup records, priority/weight selection.
//!
//! **Scope, honored in full**: this module implements every mechanic NTK_RFC 0009 specifies
//! completely from the spec text (`research/specs/vala-doc--rfc-Ntk_SNSD`) — service/priority/
//! weight semantics, the 16-per-service/256-total caps, the immutable-by-default zero record,
//! and weight-based selection (`snsd_choose_wrand`,
//! `research/impl/c/netsukuku/src/snsd_cache.c:897-930`, generalized here from "pick one" to "a
//! full weighted ordering" since [`SnsdTable::resolve`] returns every candidate for the caller to
//! try in order, not just the first pick).
//!
//! **Left out, deliberately, and documented rather than stubbed**: the RFC's *optional*
//! pubkey liveness-challenge feature ("the register_node needs the ANDNA pubkey of the SNSD node
//! to send a periodical challenge... if the node fails to reply, delete the record"). The RFC
//! text itself frames this as optional ("can *also* choose to use an *optional* SNSD feature"),
//! and the only C-side trace of it is an unused `pubkey` field never populated by any
//! challenge/verify code (confirmed by reading `snsd_cache.c` in full: the field is copied
//! around but no periodic-probe logic exists in that module). Implementing it properly needs a
//! scheduled outbound liveness probe against an arbitrary SNSD target — a background job wired
//! to whatever transport reaches that node, which belongs with whichever crate owns node
//! liveness (`ntk-neighborhood`'s domain, not this DHT-registered service's). Declining to
//! half-build it here (e.g. a probe with no real transport) is this crate's choice, not a gap in
//! the spec.
//!
//! **Chain resolution**: NTK_RFC 0009's own worked example ("the browser will resolve
//! `depausceve`... the ftp client... will get...") describes the *caller* performing at most one
//! extra [`SnsdTable::resolve`]-shaped lookup when a picked record's target is itself a hostname,
//! not a server-side chain walk — "chains are ignored" literally means no second hop is ever
//! taken automatically. [`SnsdTable::resolve`] already returns whichever target (address or
//! alias hostname) was selected; following an alias is exactly one more call from the caller,
//! not a distinct mechanism this module needs to add.

use ntk_common::Naddr;
use rand::{Rng, RngExt};

use crate::error::Error;
use crate::hostname::Hostname;

/// NTK_RFC 0009: "The weight number has to be less than 128." (`SNSD_WEIGHT` 0x7f mask,
/// `research/impl/c/netsukuku/src/snsd_cache.h:45-46`).
pub const MAX_WEIGHT: u8 = 127;

/// The reserved "zero record" service number: the plain hostname -> address mapping
/// (`SNSD_DEFAULT_SERVICE`, `research/impl/c/netsukuku/src/snsd_cache.h:40`).
pub const ZERO_SERVICE: u16 = 0;

/// NTK_RFC 0009's default zero-record priority/weight (`SNSD_DEFAULT_PRIO` = 16,
/// `SNSD_DEFAULT_WEIGHT` = 1, `research/impl/c/netsukuku/src/snsd_cache.h:43-44`).
pub const ZERO_DEFAULT_PRIORITY: u8 = 16;
/// See [`ZERO_DEFAULT_PRIORITY`]'s doc comment.
pub const ZERO_DEFAULT_WEIGHT: u8 = 1;

/// What one SNSD record resolves to: a normal address, or an alias to another hostname
/// (resolved again, by the caller, as its own lookup).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnsdTarget {
    Address(Naddr),
    Alias(Hostname),
}

/// One SNSD record: an entry in `service`'s priority/weight-ordered array.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnsdRecord {
    /// The service (port) number this record is scoped to; `0` is the zero record.
    pub service: u16,
    /// Higher priority is tried first among a service's records.
    pub priority: u8,
    /// Selection weight among same-priority records; `0` disables the record entirely ("It is
    /// also possible to use a weight equal to zero to disable a record").
    pub weight: u8,
    /// What this record resolves to.
    pub target: SnsdTarget,
}

impl SnsdRecord {
    /// # Errors
    /// [`Error::WeightTooLarge`] if `weight` exceeds [`MAX_WEIGHT`].
    pub fn new(service: u16, priority: u8, weight: u8, target: SnsdTarget) -> Result<Self, Error> {
        if weight > MAX_WEIGHT {
            return Err(Error::WeightTooLarge(weight));
        }
        Ok(Self {
            service,
            priority,
            weight,
            target,
        })
    }
}

/// The full set of SNSD records registered under one hostname, capped per NTK_RFC 0009 (16 per
/// service, 256 total).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct SnsdTable {
    by_service: std::collections::BTreeMap<u16, Vec<SnsdRecord>>,
    total: usize,
}

impl SnsdTable {
    /// An empty table: no SNSD records registered yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Total record count across every service number.
    #[must_use]
    pub fn total(&self) -> usize {
        self.total
    }

    /// Every service number with at least one record.
    pub fn services(&self) -> impl Iterator<Item = u16> + '_ {
        self.by_service.keys().copied()
    }

    /// Inserts `record`, enforcing NTK_RFC 0009's per-service and total caps.
    ///
    /// # Errors
    /// [`Error::TooManyRecordsForService`] or [`Error::TooManySnsdRecords`].
    pub fn insert(
        &mut self,
        record: SnsdRecord,
        max_per_service: usize,
        max_total: usize,
    ) -> Result<(), Error> {
        if self.total >= max_total {
            return Err(Error::TooManySnsdRecords(max_total));
        }
        let service = record.service;
        let bucket = self.by_service.entry(service).or_default();
        if bucket.len() >= max_per_service {
            return Err(Error::TooManyRecordsForService {
                service: u32::from(service),
                max: max_per_service,
            });
        }
        bucket.push(record);
        self.total += 1;
        Ok(())
    }

    /// Replaces the entire zero-service (service 0) entry, keeping the invariant that its
    /// address is always the registrant's own — NTK_RFC 0009: "it isn't allowed to change the
    /// main IP... it can be disabled by setting its weight number to 0."
    pub fn set_zero_record(&mut self, owner_naddr: Naddr, priority: u8, weight: u8) {
        if let Some(old) = self.by_service.remove(&ZERO_SERVICE) {
            self.total -= old.len();
        }
        // weight is separately capped to MAX_WEIGHT by callers constructing the request; a
        // malformed value here is clamped rather than panicking, since this is a purely internal
        // reconciliation step (the caller-facing validation already happened in `RegisterRequest`).
        let weight = weight.min(MAX_WEIGHT);
        self.by_service.insert(
            ZERO_SERVICE,
            vec![SnsdRecord {
                service: ZERO_SERVICE,
                priority,
                weight,
                target: SnsdTarget::Address(owner_naddr),
            }],
        );
        self.total += 1;
    }

    /// Resolves `service`: every enabled (`weight > 0`) record for `service`, falling back to
    /// the zero record if `service` has no records of its own ("If Y tries to resolve a service
    /// which hasn't been associated to anything, it will get the mainip"), grouped by descending
    /// priority and weight-randomized within each priority tier.
    pub fn resolve(&self, service: u16, rng: &mut impl Rng) -> Vec<SnsdRecord> {
        let records = self
            .by_service
            .get(&service)
            .filter(|records| !records.is_empty())
            .or_else(|| self.by_service.get(&ZERO_SERVICE));
        let Some(records) = records else {
            return Vec::new();
        };

        let mut by_priority: std::collections::BTreeMap<std::cmp::Reverse<u8>, Vec<&SnsdRecord>> =
            std::collections::BTreeMap::new();
        for record in records.iter().filter(|r| r.weight > 0) {
            by_priority
                .entry(std::cmp::Reverse(record.priority))
                .or_default()
                .push(record);
        }

        let mut ordered = Vec::with_capacity(records.len());
        for tier in by_priority.into_values() {
            ordered.extend(weighted_shuffle(tier, rng).into_iter().cloned());
        }
        ordered
    }
}

/// Repeated weighted pick-without-replacement over `records` (`snsd_choose_wrand`,
/// `research/impl/c/netsukuku/src/snsd_cache.c:897-930`, generalized from "pick one" to "order
/// them all" — see this module's doc comment). A record with total weight 0 in the pool can't
/// happen here since [`SnsdTable::resolve`] already filters `weight > 0` before calling this.
fn weighted_shuffle<'a>(
    mut records: Vec<&'a SnsdRecord>,
    rng: &mut impl Rng,
) -> Vec<&'a SnsdRecord> {
    let mut ordered = Vec::with_capacity(records.len());
    while !records.is_empty() {
        let total_weight: u32 = records.iter().map(|r| u32::from(r.weight)).sum();
        let mut pick = rng.random_range(1..=total_weight);
        let mut chosen = 0;
        for (i, record) in records.iter().enumerate() {
            let w = u32::from(record.weight);
            if pick <= w {
                chosen = i;
                break;
            }
            pick -= w;
        }
        ordered.push(records.remove(chosen));
    }
    ordered
}

#[cfg(test)]
mod tests {
    use super::*;

    fn naddr(n: u32) -> Naddr {
        let topo = ntk_common::Topology::new([4, 4]).unwrap();
        Naddr::new(topo, [n % 4, (n / 4) % 4]).unwrap()
    }

    #[test]
    fn rejects_weight_over_limit() {
        assert!(matches!(
            SnsdRecord::new(80, 1, 128, SnsdTarget::Address(naddr(0))),
            Err(Error::WeightTooLarge(128))
        ));
    }

    #[test]
    fn per_service_cap_enforced() {
        let mut table = SnsdTable::new();
        for i in 0..3 {
            table
                .insert(
                    SnsdRecord::new(80, 1, 10, SnsdTarget::Address(naddr(i))).unwrap(),
                    3,
                    100,
                )
                .unwrap();
        }
        let err = table
            .insert(
                SnsdRecord::new(80, 1, 10, SnsdTarget::Address(naddr(9))).unwrap(),
                3,
                100,
            )
            .unwrap_err();
        assert!(matches!(err, Error::TooManyRecordsForService { .. }));
    }

    #[test]
    fn total_cap_enforced_across_services() {
        let mut table = SnsdTable::new();
        table
            .insert(
                SnsdRecord::new(80, 1, 10, SnsdTarget::Address(naddr(0))).unwrap(),
                16,
                1,
            )
            .unwrap();
        let err = table
            .insert(
                SnsdRecord::new(21, 1, 10, SnsdTarget::Address(naddr(1))).unwrap(),
                16,
                1,
            )
            .unwrap_err();
        assert!(matches!(err, Error::TooManySnsdRecords(1)));
    }

    #[test]
    fn falls_back_to_zero_record_when_service_unset() {
        let mut table = SnsdTable::new();
        table.set_zero_record(naddr(7), ZERO_DEFAULT_PRIORITY, ZERO_DEFAULT_WEIGHT);
        let mut rng = rand::rng();
        let picked = table.resolve(80, &mut rng);
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].target, SnsdTarget::Address(naddr(7)));
    }

    #[test]
    fn disabled_zero_weight_record_never_selected() {
        let mut table = SnsdTable::new();
        table.set_zero_record(naddr(0), ZERO_DEFAULT_PRIORITY, 0);
        table
            .insert(
                SnsdRecord::new(0, 20, 5, SnsdTarget::Address(naddr(1))).unwrap(),
                16,
                256,
            )
            .unwrap();
        let mut rng = rand::rng();
        let picked = table.resolve(0, &mut rng);
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].target, SnsdTarget::Address(naddr(1)));
    }

    #[test]
    fn higher_priority_tier_ordered_first() {
        let mut table = SnsdTable::new();
        table
            .insert(
                SnsdRecord::new(80, 1, 10, SnsdTarget::Address(naddr(1))).unwrap(),
                16,
                256,
            )
            .unwrap();
        table
            .insert(
                SnsdRecord::new(80, 5, 10, SnsdTarget::Address(naddr(2))).unwrap(),
                16,
                256,
            )
            .unwrap();
        let mut rng = rand::rng();
        let picked = table.resolve(80, &mut rng);
        assert_eq!(picked[0].target, SnsdTarget::Address(naddr(2)));
        assert_eq!(picked[1].target, SnsdTarget::Address(naddr(1)));
    }

    #[test]
    fn heavier_weight_is_picked_first_more_often() {
        let mut table = SnsdTable::new();
        table
            .insert(
                SnsdRecord::new(80, 1, 100, SnsdTarget::Address(naddr(1))).unwrap(),
                16,
                256,
            )
            .unwrap();
        table
            .insert(
                SnsdRecord::new(80, 1, 1, SnsdTarget::Address(naddr(2))).unwrap(),
                16,
                256,
            )
            .unwrap();
        let mut rng = rand::rng();
        let mut heavy_first = 0;
        for _ in 0..200 {
            let picked = table.resolve(80, &mut rng);
            if picked[0].target == SnsdTarget::Address(naddr(1)) {
                heavy_first += 1;
            }
        }
        assert!(heavy_first > 150, "heavy_first={heavy_first}");
    }

    proptest::proptest! {
        #[test]
        fn resolve_returns_exactly_the_enabled_records_for_the_service(
            weights in proptest::collection::vec(0u8..=MAX_WEIGHT, 1..12),
        ) {
            let mut table = SnsdTable::new();
            for (i, &weight) in weights.iter().enumerate() {
                table
                    .insert(
                        SnsdRecord::new(80, (i % 5) as u8, weight, SnsdTarget::Address(naddr(i as u32)))
                            .unwrap(),
                        weights.len(),
                        weights.len(),
                    )
                    .unwrap();
            }
            let expected_enabled = weights.iter().filter(|&&w| w > 0).count();
            let mut rng = rand::rng();
            let resolved = table.resolve(80, &mut rng);

            proptest::prop_assert_eq!(resolved.len(), expected_enabled);
            proptest::prop_assert!(resolved.iter().all(|r| r.weight > 0));
            let mut targets: Vec<_> = resolved.iter().map(|r| r.target.clone()).collect();
            targets.sort_by_key(|t| match t {
                SnsdTarget::Address(a) => a.positions().to_vec(),
                SnsdTarget::Alias(_) => unreachable!("test only inserts Address targets"),
            });
            targets.dedup();
            proptest::prop_assert_eq!(targets.len(), expected_enabled, "no target appears twice");
        }
    }
}

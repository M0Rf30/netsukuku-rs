//! Injectable timing and capacity constants, transcribed from
//! `research/notes/02-vala-services-daemon.md` §4, NTK_RFC 0009, and
//! `research/impl/c/netsukuku/src/andna_cache.h`/`snsd_cache.h`, rather than hard-coded at their
//! use sites.

use std::time::Duration;

/// Tuning knobs every domain function ([`crate::record::Cache::register`],
/// [`crate::counter::CounterCache::try_reserve`], ...) takes explicitly rather than reading a
/// global — construct via [`Config::default`] for this crate's own documented values, or
/// override individual fields for tests/deployments that need different numbers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// How long a registered hostname stays live without renewal.
    ///
    /// **Deviation, deliberate**: the only Vala-corpus source that states a number
    /// (`research/notes/02-vala-services-daemon.md` §4, citing
    /// `documentation/ita/DemoneNTKD/RisoluzioneNomi.md:50-52`) says **30 days**; the C
    /// implementation's `ANDNA_EXPIRATION_TIME` is 259200s (3 days,
    /// `research/impl/c/netsukuku/src/andna_cache.h:38`). The two normative-for-this-port
    /// sources disagree; this crate follows the Vala-era design doc's number as the more
    /// current spec (`research/notes/03-specs-and-rfcs.md`'s own stratification ranks the
    /// vala-era docs above the frozen-2013 C tree).
    pub name_ttl: Duration,
    /// Minimum interval between two accepted renewals of the same hostname, independent of
    /// sequence-number validity — an anti-abuse rate limit, not a replay-protection mechanism
    /// (`ANDNA_MIN_UPDATE_TIME`, `research/impl/c/netsukuku/src/andna_cache.h:39-41`).
    pub min_renewal_interval: Duration,
    /// Per-registrant cap on live (non-expired) hostnames, enforced by the Counter service
    /// (`ANDNA_MAX_HOSTNAMES`, `research/impl/c/netsukuku/src/andna_cache.h:35`;
    /// `research/notes/02-vala-services-daemon.md` §4).
    pub max_hostnames_per_registrant: usize,
    /// Per-service-number cap on SNSD records under one hostname (NTK_RFC 0009: "up to 16
    /// records to a single service"; `SNSD_MAX_REC_SERV`,
    /// `research/impl/c/netsukuku/src/snsd_cache.h:36`).
    pub max_snsd_records_per_service: usize,
    /// Total cap on SNSD records (across every service number) under one hostname (NTK_RFC
    /// 0009: "maximum number of total records which can be registered is 256";
    /// `SNSD_MAX_RECORDS`, `research/impl/c/netsukuku/src/snsd_cache.h:31`).
    pub max_snsd_records_total: usize,
    /// How many nodes closest to a hostname's hash target receive a registration
    /// ([`crate::actor::Handle::register`] calls `ntk_peerservices::Handle::replicate` with this
    /// as `q`).
    ///
    /// **Deviation, deliberate**: upstream's ANDNA-specific `ANDNA_MAX_BACKUP_GNODES` is 2
    /// (`research/impl/c/netsukuku/src/andna_cache.h:32`). This crate is built as a
    /// `PeerService` on the generic PeerServices substrate precisely so it can use *that*
    /// substrate's own redundancy rule (RFC 0014 §2.2 step 5: "send it to 31 nodes") instead of
    /// re-deriving an ANDNA-specific backup count — the task assignment's own instruction ("plus
    /// replication per RFC 0014's redundancy rule"). Still fully overridable per deployment.
    ///
    /// **Deviation, deliberate (revised)**: `ntk_peerservices::Handle::replicate` walks the DHT
    /// serially, by necessity (each replica excludes every node already collected, so replicas
    /// stay distinct — see that method's own doc), so the literal RFC 0014 figure of 31 at this
    /// crate's [`Config::call_timeout`] default of 5s could serialize to ~155s worst case for
    /// one registration — an audit finding this crate's own change fixes from two directions.
    /// `ntk_peerservices::Config::replicate_deadline_multiplier` now caps `replicate`'s overall
    /// wall clock independent of `q`, so a large `q` can no longer stall unboundedly; this field
    /// is additionally lowered from 31 to **7**, well above upstream's own 2-node floor while
    /// bounding the serial *common* case (not just the pathological one) to `7 x call_timeout`
    /// = 35s worst case even before the deadline multiplier engages. Still fully overridable per
    /// deployment.
    pub replication_factor: u32,
    /// Timeout for a single outbound `contact_peer`/`replicate` call this crate makes.
    pub call_timeout: Duration,
    /// Hard cap on how many live-or-not-yet-purged hostname records a single node's `Andna`
    /// role ([`crate::record::Cache`]) will hold at once.
    ///
    /// **Why this exists, and why it is new**: neither NTK_RFC 0007 nor the only capacity number
    /// this port's normative sources state (`research/specs/vala-doc--olddoc-main_doc-andna.pdf`
    /// §3.5, "the maximum number of hostnames, which can be registered is 256") bounds *this*
    /// quantity — that 256 figure is [`Config::max_hostnames_per_registrant`], a per-registrant
    /// limit enforced by the Counter service, not a per-node limit on how many *other* peers'
    /// hostnames a hash-node/replica stores. Upstream's `andna.pdf` §2 intro states only a
    /// worst-case *expectation* ("every node will have to use few hundred kilobytes of memory"),
    /// never a hard number, and RFC 0007's own counter-node design assumes the per-registrant
    /// cap alone is sufficient because minting a new registrant identity was assumed costly (a
    /// real IP change) — an assumption this port's virtual positions break (see
    /// [`Config::max_counter_registrants`]'s doc). Without an independent per-node cap, an
    /// inbound `RegisterRequest` a remote peer sends straight to this node's `AndnaService` has
    /// no capacity gate at all (unlike a *self*-issued registration, which already goes through
    /// [`Config::max_hostnames_per_registrant`] outbound before it ever reaches a hash-node) —
    /// this field closes that gap directly at the resource actually being exhausted
    /// (`Cache::records`), independent of registrant identity, so it holds even against an
    /// attacker who mints a fresh registrant identity per request.
    ///
    /// **Default, justified**: 65536 records. At the low end of this port's own per-record size
    /// estimate (~254 bytes for a bare hostname, no extra SNSD records) that is ~16 MiB; at the
    /// pathological high end (256 SNSD records per hostname, this crate's own
    /// [`Config::max_snsd_records_total`] ceiling) it is bounded at a few hundred MiB — large
    /// relative to upstream's own "few hundred kilobytes" worst-case expectation (this port's
    /// SNSD surface is considerably richer than the 2008 spec's), but still a hard, finite
    /// ceiling instead of the unbounded growth this field replaces. Renewals of hostnames
    /// already tracked here (including ones only *logically*, not yet physically, expired) are
    /// never blocked by this cap — only a brand-new hostname key is, and only once every
    /// existing key is exhausted. Fully overridable per deployment.
    pub max_hosted_records: usize,
    /// Hard cap on how many distinct registrant identities a single node's `Counter` role
    /// ([`crate::counter::CounterCache`]) will track at once.
    ///
    /// **Why this exists**: [`Config::max_hostnames_per_registrant`] bounds how many hostnames
    /// *one* registrant may hold, but not how many *registrants* this node tracks — and the
    /// registrant identity NTK_RFC 0007 relies on (a caller's own network position, chosen
    /// specifically because upstream assumed it was expensive to change) is exactly what this
    /// port's virtual positions (`Naddr::new_allowing_virtual`, load-bearing for legitimate
    /// mid-migration g-node splits/merges — see `mesh::isolated_merge`) make cheap to multiply:
    /// nothing this crate owns can reject a virtual position without also rejecting a genuine
    /// migrating node, so the fix is not a tighter identity check but a hard ceiling on how many
    /// *distinct* identities get tracked at all. Once at capacity, only a never-before-seen
    /// registrant is refused (refuse-new, matching every other cap in this crate) — every
    /// already-tracked registrant keeps renewing and registering up to its own
    /// [`Config::max_hostnames_per_registrant`] exactly as before, so a legitimate node that
    /// hooked before the cap filled is never evicted.
    ///
    /// **Default, justified**: 4096 registrants. Combined with
    /// [`Config::max_hostnames_per_registrant`]'s default (256), the worst case — every tracked
    /// registrant simultaneously holding the maximum — is bounded at 4096 × 256 = 1,048,576
    /// reservation entries (a `HostnameHash` plus an expiry `u64`, tens of bytes each): tens of
    /// MiB, not the unbounded growth this field replaces. Reaching that worst case requires an
    /// attacker to actually complete that many *successful* signed registrations (each
    /// signature-verification-limited), not merely mint identities, so realistic occupancy is
    /// far lower. Fully overridable per deployment.
    pub max_counter_registrants: usize,
    /// How often [`crate::actor::run_expiry_reclaimer`] calls [`crate::actor::Handle::purge_expired`]
    /// in the running daemon.
    ///
    /// **Why this exists**: lazy expiry (`Cache::resolve`/`Cache::register` both check
    /// `is_expired` on access) makes reclamation optional for *correctness*, but
    /// [`Config::max_hosted_records`]/[`Config::max_counter_registrants`] only stay meaningful
    /// caps — rather than permanent, unreclaimable garbage once an attacker fills them with
    /// short-lived registrations — if something actually calls `purge_expired` on a live
    /// daemon; nothing did before this field existed (see the type's own module doc). No
    /// upstream/RFC source states a reclamation cadence (only [`Config::name_ttl`]'s expiry
    /// *duration* is specified), so this default is this crate's own choice.
    ///
    /// **Default, justified**: 5 minutes — two orders of magnitude shorter than
    /// [`Config::name_ttl`]'s default (30 days) or [`Config::min_renewal_interval`]'s (1 hour),
    /// so an attacker's expired garbage vacates its capacity slot quickly relative to how long a
    /// legitimate registration is expected to sit unrenewed, while a single `BTreeMap` scan over
    /// at most `max_hosted_records`/(`max_counter_registrants` × `max_hostnames_per_registrant`)
    /// entries every 5 minutes is negligible CPU cost.
    pub expiry_purge_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            name_ttl: Duration::from_secs(30 * 24 * 3600),
            min_renewal_interval: Duration::from_secs(3600),
            max_hostnames_per_registrant: 256,
            max_snsd_records_per_service: 16,
            max_snsd_records_total: 256,
            replication_factor: 7,
            call_timeout: Duration::from_secs(5),
            max_hosted_records: 65_536,
            max_counter_registrants: 4_096,
            expiry_purge_interval: Duration::from_secs(5 * 60),
        }
    }
}

//! Crate-wide error type.

use thiserror::Error;

/// Everything that can go wrong constructing or operating on the shared address,
/// topology and fingerprint types in this crate.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum Error {
    /// A [`crate::Topology`] must describe at least one level.
    #[error("topology must have at least one level")]
    EmptyTopology,

    /// Every level's g-node size must be strictly positive. The legacy spec fixed
    /// this at `MAXGROUPNODE = 256` for every level; the current spec allows an
    /// arbitrary per-level size but never zero (`research/notes/03-specs-and-rfcs.md`
    /// §Topology/addressing parameters).
    #[error("g-node size at level {level} must be greater than zero")]
    ZeroGsize {
        /// The offending level index.
        level: usize,
    },

    /// A [`crate::Naddr`] must carry exactly one position per level of its
    /// [`crate::Topology`] (`IQspnNaddr`, `research/impl/vala/qspn/api.vala:23-28`,
    /// backed by the reference `Naddr(pos, sizes)` ctor assertion,
    /// `research/impl/vala/qspn/testsuites/system_peer/serializables.vala:11-18`).
    #[error("address has {actual} level positions, topology has {expected} levels")]
    LevelCountMismatch {
        /// Number of levels the topology declares.
        expected: usize,
        /// Number of positions actually supplied.
        actual: usize,
    },

    /// A position was `>=` its level's g-node size. [`crate::Naddr::new`]
    /// rejects this — every caller that only ever expects fully-resolved
    /// addresses keeps that guarantee — while
    /// [`crate::Naddr::new_allowing_virtual`] accepts it, modeling upstream's
    /// *virtual position* mechanism for mid-migration g-nodes
    /// (`pos >= gsize(i)`, `research/notes/03-specs-and-rfcs.md`
    /// §Topology/addressing parameters, "virtual positions" row).
    #[error("position {pos} at level {level} is out of range: g-node size is {gsize}")]
    PositionOutOfRange {
        /// The level at which the position was checked.
        level: usize,
        /// The offending position.
        pos: u32,
        /// The g-node size at that level.
        gsize: u32,
    },

    /// A level index was requested that does not exist in the topology/address.
    #[error("level {level} is out of range: topology has {levels} levels")]
    LevelOutOfRange {
        /// The requested level.
        level: usize,
        /// Number of levels available.
        levels: usize,
    },

    /// Two [`crate::Naddr`] values were compared but belong to different
    /// [`crate::Topology`] instances (different level count and/or g-node sizes) —
    /// they cannot share a hierarchy, so no coordinate relates them.
    #[error("addresses belong to different topologies")]
    TopologyMismatch,

    /// [`std::str::FromStr`] for [`crate::Naddr`] was given text that does not
    /// match the canonical `pos/gsize` per-level form (see the `Naddr` type docs).
    #[error("malformed Naddr text: {0:?}")]
    ParseNaddr(String),

    /// [`crate::Fingerprint::elder_seed`] requires an aggregated (level > 0)
    /// fingerprint on both sides — a level-0 fingerprint names a single real node,
    /// not a g-node, and upstream never compares two of those for eldership
    /// (`i_qspn_elder_seed`, `research/impl/vala/qspn/testsuites/system_peer/serializables.vala:231-244`).
    #[error("elder_seed requires an aggregated fingerprint (level > 0), got level 0")]
    FingerprintBaseLevel,

    /// [`crate::Fingerprint::elder_seed`] was called on two fingerprints that were
    /// aggregated to different hierarchy levels; they are not comparable.
    #[error("cannot compare fingerprints at different levels ({self_level} vs {other_level})")]
    FingerprintLevelMismatch {
        /// The level of `self`.
        self_level: usize,
        /// The level of the fingerprint compared against.
        other_level: usize,
    },

    /// The two fingerprints' eldership-seed trails were identical at every
    /// position, so neither outranks the other. Upstream treats this as
    /// unreachable given well-behaved (non-colliding) identities
    /// (`assert_not_reached()`, `research/impl/vala/qspn/testsuites/system_peer/serializables.vala:260`);
    /// this crate reports it as an error instead of panicking.
    #[error("fingerprints are indistinguishable by eldership seed")]
    IndistinguishableFingerprints,

    /// [`crate::Fingerprint::construct`] was called on a fingerprint that already
    /// has no more levels to climb (`assert(elderships.size > 0)`,
    /// `research/impl/vala/qspn/testsuites/system_peer/serializables.vala:273`).
    #[error("fingerprint is already at the top of the hierarchy, cannot construct further")]
    TopOfHierarchy,

    /// [`crate::Fingerprint::from_parts`] was given a [`crate::FingerprintParts`]
    /// whose `elderships_seed` length does not equal `level`. Every
    /// [`crate::Fingerprint::construct`] call pushes exactly one seed entry,
    /// so a well-formed fingerprint at level `level` always has exactly
    /// `level` seed entries (empty at level 0) — a decoder cannot otherwise
    /// tell a hostile/corrupt wire value from a real one here.
    #[error(
        "fingerprint at level {level} has {seed_len} elderships-seed entries, expected {level}"
    )]
    FingerprintSeedLength {
        /// The fingerprint's claimed level.
        level: usize,
        /// The actual number of `elderships_seed` entries supplied.
        seed_len: usize,
    },
}

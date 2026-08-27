//! Arc identity and QSPN-local id allocation.

use std::sync::atomic::{AtomicU64, Ordering};

/// Opaque per-arc identifier. Analogous to upstream's random 31-bit `arc_id`
/// (`research/impl/vala/qspn/qspn.vala:101,290-300,727-731`), which keys
/// `id_arc_map` and is stamped onto every ETP hop this node originates.
///
/// Unlike upstream's `IQspnArc` (cost + equality + `i_qspn_comes_from`
/// bundled into one object, `research/impl/vala/qspn/api.vala:114-119`), this
/// crate is deliberately decoupled from Neighborhood: an `ArcId` carries no
/// behavior of its own. Cost lives in the actor's arc table
/// ([`crate::state::ArcEntry`]); resolving an inbound RPC caller to the arc it
/// arrived on ("comes_from") is delegated to an injectable arc resolver owned
/// by whichever crate (Neighborhood, out of this crate's scope) knows the
/// physical/NIC mapping.
///
/// `arcs[1..]` inside a received [`crate::EtpPath`] are **foreign** ids
/// minted by other nodes and carried through unchanged (`revise_etp` only
/// ever inserts at position 0, `qspn.vala:1129`); disjoint-path overlap
/// detection compares them as opaque tokens (`qspn.vala:1579-1600`), so this
/// type is never reinterpreted as "one of my own arcs" past position 0.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ArcId(u32);

impl ArcId {
    /// The raw wire value (`prost`'s `uint32`).
    #[must_use]
    pub fn as_u32(self) -> u32 {
        self.0
    }
}

impl From<u32> for ArcId {
    fn from(v: u32) -> Self {
        Self(v)
    }
}

/// Pluggable arc-id generator. Upstream itself defers `arc_id` generation to
/// an injectable `IRandomNumberGenerator`
/// (`research/impl/vala/qspn/rngen.vala:24-63`) rather than hard-coding a
/// source, precisely so tests can inject a deterministic one; this trait is
/// the same seam.
pub trait ArcIdSource: Send + Sync {
    /// A value in `1..=0x7FFF_FFFF` (upstream excludes 0 and stays within a
    /// 31-bit positive `int`, `qspn.vala:292,426-429`).
    fn next(&self) -> u32;
}

/// A small, dependency-free `SplitMix64`-based [`ArcIdSource`], seeded from
/// the process's default hasher state. No security property is claimed or
/// needed here — only "practically unique among this node's live arcs",
/// exactly upstream's own requirement on its injected default (glib's PRNG,
/// `rngen.vala:66-83`).
#[derive(Debug)]
pub struct DefaultArcIdSource(AtomicU64);

impl DefaultArcIdSource {
    #[must_use]
    pub fn new() -> Self {
        use std::collections::hash_map::RandomState;
        use std::hash::{BuildHasher, Hasher};
        let seed = RandomState::new().build_hasher().finish() | 1;
        Self(AtomicU64::new(seed))
    }
}

impl Default for DefaultArcIdSource {
    fn default() -> Self {
        Self::new()
    }
}

impl ArcIdSource for DefaultArcIdSource {
    fn next(&self) -> u32 {
        loop {
            let mut z = self
                .0
                .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
                .wrapping_add(0x9E37_79B9_7F4A_7C15);
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^= z >> 31;
            let candidate = (z as u32) & 0x7FFF_FFFF;
            if candidate != 0 {
                return candidate;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_source_never_yields_zero_and_varies() {
        let src = DefaultArcIdSource::new();
        let a = src.next();
        let b = src.next();
        assert_ne!(a, 0);
        assert_ne!(b, 0);
        assert_ne!(a, b);
        assert!(a <= 0x7FFF_FFFF);
    }

    #[test]
    fn arc_id_round_trips_u32() {
        let id: ArcId = 42u32.into();
        assert_eq!(id.as_u32(), 42);
    }
}

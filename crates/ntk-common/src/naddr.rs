//! `Naddr`: a hierarchical Netsukuku address.

use std::fmt;
use std::str::FromStr;

use crate::error::Error;
use crate::hcoord::HCoord;
use crate::topology::Topology;

/// A hierarchical Netsukuku address: one position per level of a [`Topology`].
///
/// Direct port of the reference `Naddr{pos[], sizes[]}` model
/// (`IQspnNaddr`/`IQspnMyNaddr`, `research/impl/vala/qspn/api.vala:23-33`; concrete
/// reference implementation `research/impl/vala/qspn/testsuites/system_peer/serializables.vala:7-108`).
/// This decouples "position in the hierarchy" from "shape of the hierarchy"
/// (`research/notes/03-specs-and-rfcs.md` §Topology/addressing parameters,
/// "address format" row) — the shape lives in the shared [`Topology`], the
/// position vector is unique per address.
///
/// **Virtual positions**: upstream allows a position to be *virtual*
/// (`pos >= gsize(level)`), naming a reserved-but-not-yet-placed slot for a
/// g-node mid-migration (`is_real_from_to`,
/// `research/impl/vala/qspn/testsuites/system_peer/serializables.vala:20-25`;
/// `research/impl/vala/documentation/ita/ModuloQspn/AnalisiFunzionale.md:406-408,425-426`).
/// [`Naddr::new`] still rejects out-of-range positions — every caller that
/// only ever expects fully-resolved addresses keeps that guarantee — while
/// [`Naddr::new_allowing_virtual`] permits them for the migration/hooking/
/// coordinator code that must construct and carry a virtual address.
/// [`Naddr::is_virtual_at`], [`Naddr::is_virtual`] and
/// [`Naddr::is_real_from_to`] tell a virtual position apart from a real one
/// after construction. Upstream's own analogous "no real value yet" marker is
/// [`crate::FingerprintParts::eldership`]`: None` — a g-node's own eldership
/// claim is null exactly when its position is virtual (`update_clusters`,
/// `research/notes/01-vala-core-routing.md` §3) — the two mechanisms describe
/// the same migration state from two different angles and are never expected
/// to disagree.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Naddr {
    topology: Topology,
    pos: Box<[u32]>,
}

impl Naddr {
    /// Builds an address from a position per level of `topology`.
    ///
    /// # Errors
    /// [`Error::LevelCountMismatch`] if `pos` does not have exactly
    /// `topology.levels()` entries; [`Error::PositionOutOfRange`] if any position
    /// is not strictly less than its level's g-node size.
    pub fn new(topology: Topology, pos: impl IntoIterator<Item = u32>) -> Result<Self, Error> {
        let pos: Vec<u32> = pos.into_iter().collect();
        if pos.len() != topology.levels() {
            return Err(Error::LevelCountMismatch {
                expected: topology.levels(),
                actual: pos.len(),
            });
        }
        for (level, (&p, &gsize)) in pos.iter().zip(topology.gsizes()).enumerate() {
            if p >= gsize {
                return Err(Error::PositionOutOfRange {
                    level,
                    pos: p,
                    gsize,
                });
            }
        }
        Ok(Self {
            topology,
            pos: pos.into(),
        })
    }

    /// Builds an address from a position per level of `topology`, permitting
    /// *virtual* positions (`pos >= gsize(level)`) — see the type docs. Used
    /// by migration/hooking/coordinator code that must name a
    /// reserved-but-unplaced slot; every other caller should prefer
    /// [`Naddr::new`].
    ///
    /// # Errors
    /// [`Error::LevelCountMismatch`] if `pos` does not have exactly
    /// `topology.levels()` entries — a structural property of the topology,
    /// not a realness question, so it is still checked here.
    pub fn new_allowing_virtual(
        topology: Topology,
        pos: impl IntoIterator<Item = u32>,
    ) -> Result<Self, Error> {
        let pos: Vec<u32> = pos.into_iter().collect();
        if pos.len() != topology.levels() {
            return Err(Error::LevelCountMismatch {
                expected: topology.levels(),
                actual: pos.len(),
            });
        }
        Ok(Self {
            topology,
            pos: pos.into(),
        })
    }

    /// The topology this address is bound to.
    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    /// Number of levels in this address (`i_qspn_get_levels`,
    /// `research/impl/vala/qspn/api.vala:25`).
    pub fn levels(&self) -> usize {
        self.pos.len()
    }

    /// The position held at `level`, or `None` if out of range
    /// (`i_qspn_get_pos`, `research/impl/vala/qspn/api.vala:27`).
    pub fn pos(&self, level: usize) -> Option<u32> {
        self.pos.get(level).copied()
    }

    /// All per-level positions, index 0 first.
    pub fn positions(&self) -> &[u32] {
        &self.pos
    }

    /// True if the position at `level` is virtual (`pos >= gsize(level)`, see
    /// the type docs); `None` if `level` is out of range.
    pub fn is_virtual_at(&self, level: usize) -> Option<bool> {
        let p = self.pos.get(level).copied()?;
        let gsize = self.topology.gsize(level)?;
        Some(p >= gsize)
    }

    /// True if any level holds a virtual position.
    pub fn is_virtual(&self) -> bool {
        (0..self.levels()).any(|level| self.is_virtual_at(level) == Some(true))
    }

    /// True iff every level in the **inclusive** range `from..=to` holds a
    /// real (non-virtual) position. Direct port of upstream's
    /// `is_real_from_to`
    /// (`research/impl/vala/qspn/testsuites/system_peer/serializables.vala:20-25`):
    /// `for (int i = from; i <= to; i++) if (pos[i] >= sizes[i]) return
    /// false;` — both ends inclusive, and an empty or inverted range
    /// (`from > to`) never enters the loop, so it is vacuously `true`
    /// (nothing in an empty range can be non-real). This port keeps that
    /// literally rather than treating `from > to` as a caller error.
    ///
    /// # Errors
    /// [`Error::LevelOutOfRange`] if `to` is not a valid level index.
    /// Upstream has no such check (an out-of-range Vala array read would
    /// fail); this crate reports it instead. `from` needs no separate bound:
    /// once `to` is in range, `from > to` short-circuits to `Ok(true)` before
    /// any indexing, and every in-range `from <= to` is already covered by
    /// the `to` check.
    pub fn is_real_from_to(&self, from: usize, to: usize) -> Result<bool, Error> {
        if to >= self.levels() {
            return Err(Error::LevelOutOfRange {
                level: to,
                levels: self.levels(),
            });
        }
        Ok((from..=to).all(|level| self.is_virtual_at(level) == Some(false)))
    }

    /// True if this address and `other` belong to the same g-node at `level` —
    /// i.e. it holds the g-node named by `hc` as an ancestor (positions agree at
    /// `hc.level` and at every level above it).
    ///
    /// No literal upstream method name matches this; it is the containment
    /// relation implied by the `gsize(i)`-hierarchy model
    /// (`research/notes/03-specs-and-rfcs.md` §Topology/addressing parameters),
    /// analogous in spirit to Hooking's `i_am_inside(TupleGNode, …)`
    /// (`research/impl/vala/hooking/structs.vala:126-129`), which operates on a
    /// later-phase path structure not modeled in this crate.
    ///
    /// If `self`'s position at `hc.level` is virtual (see the type docs), this
    /// still compares by raw position value — a virtual position never denotes
    /// real membership upstream, so callers that care about the distinction
    /// should check [`Naddr::is_virtual_at`] first.
    ///
    /// # Errors
    /// [`Error::LevelOutOfRange`] if `hc.level` exceeds this address's levels.
    pub fn is_inside(&self, hc: HCoord) -> Result<bool, Error> {
        let p = self.pos(hc.level).ok_or(Error::LevelOutOfRange {
            level: hc.level,
            levels: self.levels(),
        })?;
        Ok(p == hc.pos)
    }

    /// The hierarchical coordinate of `dest` relative to `self`: the highest
    /// level at which the two addresses' positions differ, i.e. the position
    /// `dest` holds in the smallest g-node that is a common ancestor of both
    /// (`i_qspn_get_coord_by_address`, `research/impl/vala/qspn/api.vala:32`,
    /// reference behavior `research/impl/vala/qspn/testsuites/system_peer/serializables.vala:86-96`).
    ///
    /// Since positions above the returned level are, by construction, always
    /// equal, this is simultaneously "the highest level of divergence" and "the
    /// lowest level whose parent g-node contains both addresses" — the two
    /// phrasings name the same level.
    ///
    /// Returns `Ok(None)` if `self` and `dest` are the same address (upstream
    /// returns the sentinel `HCoord(-1, -1)` for this case,
    /// `qspn/testsuites/system_peer/serializables.vala:94-95`; `Option` is the
    /// idiomatic Rust equivalent).
    ///
    /// The returned [`HCoord`]'s `pos` may be virtual (`dest`'s position at
    /// that level might be `>= gsize(level)`); upstream is explicit that a
    /// virtual `HCoord` is never itself a destination
    /// (`research/impl/vala/documentation/ita/ModuloQspn/AnalisiFunzionale.md:329-330`)
    /// — callers must confirm realness (e.g. via [`Naddr::is_virtual_at`] on
    /// `dest`) before treating it as one.
    ///
    /// # Errors
    /// [`Error::TopologyMismatch`] if `self` and `dest` are bound to different
    /// topologies (they then share no coordinate space to compute a divergence in).
    pub fn hcoord(&self, dest: &Naddr) -> Result<Option<HCoord>, Error> {
        if self.topology != dest.topology {
            return Err(Error::TopologyMismatch);
        }
        for level in (0..self.pos.len()).rev() {
            if self.pos[level] != dest.pos[level] {
                return Ok(Some(HCoord::new(level, dest.pos[level])));
            }
        }
        Ok(None)
    }
}

impl fmt::Display for Naddr {
    /// Canonical textual form: `pos/gsize` per level, joined with `.`, level 0
    /// first. No literal wire/text form for `Naddr` exists upstream — it is only
    /// ever (de)serialized as JSON object fields
    /// (`serialize_object`/`deserialize_object`, `research/impl/vala/qspn/serializables.vala:219-263`)
    /// — so this format is netsukuku-rs's own choice. It is deliberately
    /// self-describing (each level carries its own g-node size) so that
    /// [`FromStr`] can reconstruct a full [`Naddr`] — [`Topology`] included —
    /// from the string alone, with no external context required for the
    /// [`FromStr`] contract.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, (&p, &g)) in self.pos.iter().zip(self.topology.gsizes()).enumerate() {
            if i > 0 {
                write!(f, ".")?;
            }
            write!(f, "{p}/{g}")?;
        }
        Ok(())
    }
}

impl FromStr for Naddr {
    type Err = Error;

    /// Parses the canonical form documented on [`Naddr`]'s `Display` impl.
    /// Accepts virtual positions (`pos >= gsize`) exactly as `Display` prints
    /// them, via [`Naddr::new_allowing_virtual`] — the text form doesn't
    /// distinguish virtual from real by shape (only by the printed numbers),
    /// so there is nothing to lose by allowing what `Display` can already
    /// produce.
    fn from_str(s: &str) -> Result<Self, Error> {
        let malformed = || Error::ParseNaddr(s.to_string());
        let mut pos = Vec::new();
        let mut gsizes = Vec::new();
        for level in s.split('.') {
            let (p, g) = level.split_once('/').ok_or_else(malformed)?;
            pos.push(p.parse::<u32>().map_err(|_| malformed())?);
            gsizes.push(g.parse::<u32>().map_err(|_| malformed())?);
        }
        let topology = Topology::new(gsizes)?;
        Naddr::new_allowing_virtual(topology, pos)
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Naddr {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Naddr {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology() -> Topology {
        Topology::new([8, 16, 4]).unwrap()
    }

    #[test]
    fn rejects_wrong_level_count() {
        assert_eq!(
            Naddr::new(topology(), [1, 2]),
            Err(Error::LevelCountMismatch {
                expected: 3,
                actual: 2
            })
        );
    }

    #[test]
    fn rejects_position_out_of_range() {
        assert_eq!(
            Naddr::new(topology(), [1, 20, 2]),
            Err(Error::PositionOutOfRange {
                level: 1,
                pos: 20,
                gsize: 16
            })
        );
    }

    #[test]
    fn accessors_report_positions() {
        let a = Naddr::new(topology(), [1, 2, 3]).unwrap();
        assert_eq!(a.levels(), 3);
        assert_eq!(a.pos(1), Some(2));
        assert_eq!(a.pos(3), None);
        assert_eq!(a.positions(), &[1, 2, 3]);
    }

    #[test]
    fn display_and_parse_round_trip() {
        let a = Naddr::new(topology(), [1, 2, 3]).unwrap();
        assert_eq!(a.to_string(), "1/8.2/16.3/4");
        assert_eq!(a.to_string().parse::<Naddr>().unwrap(), a);
    }

    #[test]
    fn parse_rejects_malformed_text() {
        assert!(matches!(
            "garbage".parse::<Naddr>(),
            Err(Error::ParseNaddr(_))
        ));
        assert!(matches!("".parse::<Naddr>(), Err(Error::ParseNaddr(_))));
    }

    #[test]
    fn hcoord_rejects_mismatched_topology() {
        let a = Naddr::new(topology(), [1, 2, 3]).unwrap();
        let b = Naddr::new(Topology::new([8, 16]).unwrap(), [1, 2]).unwrap();
        assert_eq!(a.hcoord(&b), Err(Error::TopologyMismatch));
    }

    #[test]
    fn hcoord_is_none_for_identical_addresses() {
        let a = Naddr::new(topology(), [1, 2, 3]).unwrap();
        assert_eq!(a.hcoord(&a), Ok(None));
    }

    #[test]
    fn hcoord_finds_the_highest_divergent_level() {
        let t = topology();
        // Differ only at the innermost level: they share everything above it.
        let a = Naddr::new(t.clone(), [1, 2, 3]).unwrap();
        let b = Naddr::new(t.clone(), [5, 2, 3]).unwrap();
        assert_eq!(a.hcoord(&b), Ok(Some(HCoord::new(0, 5))));

        // Differ at the outermost level: they share nothing in this topology.
        let c = Naddr::new(t.clone(), [1, 2, 0]).unwrap();
        assert_eq!(a.hcoord(&c), Ok(Some(HCoord::new(2, 0))));

        // Differ at multiple levels: the highest one wins, lower ones are moot.
        let d = Naddr::new(t, [7, 9, 0]).unwrap();
        assert_eq!(a.hcoord(&d), Ok(Some(HCoord::new(2, 0))));
    }

    #[test]
    fn is_inside_checks_the_named_level() {
        let a = Naddr::new(topology(), [1, 2, 3]).unwrap();
        assert_eq!(a.is_inside(HCoord::new(1, 2)), Ok(true));
        assert_eq!(a.is_inside(HCoord::new(1, 9)), Ok(false));
        assert_eq!(
            a.is_inside(HCoord::new(9, 0)),
            Err(Error::LevelOutOfRange {
                level: 9,
                levels: 3
            })
        );
    }

    #[test]
    fn new_rejects_virtual_but_new_allowing_virtual_accepts_it() {
        let t = topology();
        assert_eq!(
            Naddr::new(t.clone(), [1, 20, 2]),
            Err(Error::PositionOutOfRange {
                level: 1,
                pos: 20,
                gsize: 16
            })
        );
        let a = Naddr::new_allowing_virtual(t, [1, 20, 2]).unwrap();
        assert_eq!(a.positions(), &[1, 20, 2]);
    }

    #[test]
    fn new_allowing_virtual_still_checks_level_count() {
        assert_eq!(
            Naddr::new_allowing_virtual(topology(), [1, 2]),
            Err(Error::LevelCountMismatch {
                expected: 3,
                actual: 2
            })
        );
    }

    #[test]
    fn is_virtual_at_reports_real_and_virtual_levels() {
        // gsizes = [8, 16, 4]; pos == gsize-1 is real, pos == gsize is virtual.
        let a = Naddr::new_allowing_virtual(topology(), [7, 16, 3]).unwrap();
        assert_eq!(a.is_virtual_at(0), Some(false)); // 7 < 8: real, boundary
        assert_eq!(a.is_virtual_at(1), Some(true)); // 16 >= 16: virtual, boundary
        assert_eq!(a.is_virtual_at(2), Some(false)); // 3 < 4: real
        assert_eq!(a.is_virtual_at(9), None);
        assert!(a.is_virtual());

        let all_real = Naddr::new(topology(), [1, 2, 3]).unwrap();
        assert!(!all_real.is_virtual());
        assert_eq!(all_real.is_virtual_at(0), Some(false));
    }

    #[test]
    fn is_real_from_to_checks_the_inclusive_range() {
        let a = Naddr::new_allowing_virtual(topology(), [1, 16, 3]).unwrap();
        // Level 1 is virtual; ranges that include it are false, ranges that
        // exclude it are true.
        assert_eq!(a.is_real_from_to(0, 0), Ok(true));
        assert_eq!(a.is_real_from_to(1, 1), Ok(false));
        assert_eq!(a.is_real_from_to(0, 2), Ok(false));
        assert_eq!(a.is_real_from_to(2, 2), Ok(true));
    }

    #[test]
    fn is_real_from_to_empty_or_inverted_range_is_vacuously_true() {
        let a = Naddr::new_allowing_virtual(topology(), [1, 16, 3]).unwrap();
        // from > to: the loop never runs, matching upstream's `for (i=from; i<=to; i++)`.
        assert_eq!(a.is_real_from_to(2, 0), Ok(true));
        assert_eq!(a.is_real_from_to(1, 0), Ok(true));
    }

    #[test]
    fn is_real_from_to_rejects_out_of_range_upper_bound() {
        let a = Naddr::new(topology(), [1, 2, 3]).unwrap();
        assert_eq!(
            a.is_real_from_to(0, 3),
            Err(Error::LevelOutOfRange {
                level: 3,
                levels: 3
            })
        );
    }

    #[test]
    fn display_and_parse_round_trip_a_virtual_address() {
        let a = Naddr::new_allowing_virtual(topology(), [1, 16, 3]).unwrap();
        assert_eq!(a.to_string(), "1/8.16/16.3/4");
        let parsed: Naddr = a.to_string().parse().unwrap();
        assert_eq!(parsed, a);
        assert!(parsed.is_virtual_at(1).unwrap());
    }

    proptest::proptest! {
        #[test]
        fn display_parse_round_trips_any_position_real_or_virtual(
            p0 in 0u32..12, p1 in 0u32..24, p2 in 0u32..6,
        ) {
            let a = Naddr::new_allowing_virtual(topology(), [p0, p1, p2]).unwrap();
            let parsed: Naddr = a.to_string().parse().unwrap();
            proptest::prop_assert_eq!(parsed, a);
        }
    }
}

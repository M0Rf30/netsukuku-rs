//! Property-based invariants for `ntk-common`'s pure algorithmic types
//! (per `research/notes/06-rust-stack.md` §Deterministic-simulation testing item 3).

use ntk_common::{Cost, Fingerprint, Naddr, Topology};
use proptest::prelude::*;

fn topology_strategy() -> impl Strategy<Value = Topology> {
    prop::collection::vec(1u32..=32, 1..=6).prop_map(|gsizes| Topology::new(gsizes).unwrap())
}

/// A valid `Naddr` for an arbitrary topology: raw values reduced modulo each
/// level's g-node size so every position is in range by construction.
fn naddr_strategy() -> impl Strategy<Value = Naddr> {
    topology_strategy().prop_flat_map(|topology| {
        let levels = topology.levels();
        prop::collection::vec(any::<u32>(), levels).prop_map(move |raw| {
            let pos: Vec<u32> = raw
                .iter()
                .zip(topology.gsizes())
                .map(|(&r, &g)| r % g)
                .collect();
            Naddr::new(topology.clone(), pos).unwrap()
        })
    })
}

/// Two valid `Naddr` values sharing one topology, for divergence-symmetry checks.
fn naddr_pair_strategy() -> impl Strategy<Value = (Naddr, Naddr)> {
    topology_strategy().prop_flat_map(|topology| {
        let levels = topology.levels();
        (
            prop::collection::vec(any::<u32>(), levels),
            prop::collection::vec(any::<u32>(), levels),
        )
            .prop_map(move |(raw_a, raw_b)| {
                let pos_a: Vec<u32> = raw_a
                    .iter()
                    .zip(topology.gsizes())
                    .map(|(&r, &g)| r % g)
                    .collect();
                let pos_b: Vec<u32> = raw_b
                    .iter()
                    .zip(topology.gsizes())
                    .map(|(&r, &g)| r % g)
                    .collect();
                (
                    Naddr::new(topology.clone(), pos_a).unwrap(),
                    Naddr::new(topology.clone(), pos_b).unwrap(),
                )
            })
    })
}

/// A group of `(id, eldership)` pairs with pairwise-distinct ids and
/// pairwise-distinct elderships, i.e. no tie-break ambiguity in a
/// [`Fingerprint::construct`] race.
fn distinct_eldership_group() -> impl Strategy<Value = Vec<(u32, u32)>> {
    prop::collection::vec((any::<u32>(), any::<u32>()), 1..=6).prop_filter(
        "ids and elderships must each be pairwise distinct",
        |v| {
            let mut ids: Vec<_> = v.iter().map(|&(id, _)| id).collect();
            let mut eld: Vec<_> = v.iter().map(|&(_, e)| e).collect();
            ids.sort_unstable();
            ids.dedup();
            eld.sort_unstable();
            eld.dedup();
            ids.len() == v.len() && eld.len() == v.len()
        },
    )
}

proptest! {
    /// `Naddr`'s canonical text form round-trips through `Display`/`FromStr`.
    #[test]
    fn naddr_display_fromstr_round_trip(a in naddr_strategy()) {
        let parsed: Naddr = a.to_string().parse().unwrap();
        prop_assert_eq!(parsed, a);
    }

    /// `hcoord(a, b)`'s level agrees with `hcoord(b, a)`'s level: divergence is a
    /// property of the pair, not of which side asks.
    #[test]
    fn hcoord_divergence_level_is_symmetric((a, b) in naddr_pair_strategy()) {
        let a_to_b = a.hcoord(&b).unwrap().map(|hc| hc.level);
        let b_to_a = b.hcoord(&a).unwrap().map(|hc| hc.level);
        prop_assert_eq!(a_to_b, b_to_a);
    }

    /// Adding a segment never lowers a path's cost.
    #[test]
    fn cost_saturating_add_is_monotonic(a in any::<u64>(), b in any::<u64>()) {
        let ca = Cost::Finite(a);
        let cb = Cost::Finite(b);
        prop_assert!(ca.saturating_add(cb) >= ca);
        prop_assert!(cb.saturating_add(ca) >= cb);
    }

    /// Finite addition matches checked arithmetic when it fits, and saturates to
    /// `u64::MAX` instead of wrapping when it doesn't.
    #[test]
    #[allow(clippy::manual_saturating_arithmetic)] // deliberately an independent oracle, not a missed `saturating_add`
    fn cost_saturating_add_matches_checked_or_max(a in any::<u64>(), b in any::<u64>()) {
        let sum = Cost::Finite(a).saturating_add(Cost::Finite(b));
        let expected = Cost::Finite(a.checked_add(b).unwrap_or(u64::MAX));
        prop_assert_eq!(sum, expected);
    }

    /// With no tied eldership claims, `construct` always selects the global
    /// minimum claim regardless of sibling order — the "good" half of the
    /// associativity notes/06 flags as an open assumption; the tied case is
    /// proven *not* order-independent in `fingerprint.rs`'s unit tests instead.
    #[test]
    fn fingerprint_construct_without_ties_picks_the_global_minimum(group in distinct_eldership_group()) {
        let fps: Vec<Fingerprint<u32>> = group
            .iter()
            .map(|&(id, eldership)| Fingerprint::new(id, eldership, vec![0]))
            .collect();
        let (me, siblings) = fps.split_first().unwrap();
        let expected_id = group.iter().min_by_key(|&&(_, eldership)| eldership).unwrap().0;

        let forward = me.construct(siblings, false).unwrap();
        prop_assert_eq!(*forward.id(), expected_id);

        let mut reversed = siblings.to_vec();
        reversed.reverse();
        let backward = me.construct(&reversed, false).unwrap();
        prop_assert_eq!(*backward.id(), expected_id);
    }
}

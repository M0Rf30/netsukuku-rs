//! The size-based merge-direction heuristic —
//! `research/impl/vala/hooking/arc_handler.vala:150-214` — extracted as
//! pure functions so the decision boundary (smaller/larger/within-10x/tie)
//! is unit-testable without any actor/RPC machinery.

/// The outcome of comparing my own network size against a newly-discovered
/// neighbor network's *self-reported* size (`arc_handler.vala:155-178`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeDecision {
    /// The neighbor's network is more than 10x mine — proceed
    /// unconditionally (`arc_handler.vala:159-162`).
    Proceed,
    /// Sizes are equal, or within the 10x band either direction — ask the
    /// Coordinator for an authoritative recount and re-decide via
    /// [`merge_tiebreak`] (`arc_handler.vala:156,163-166,174-177`).
    AskCoordinator,
    /// My own network is more than 10x the neighbor's — wait and redo from
    /// start (`arc_handler.vala:169-173`).
    Wait,
}

/// `arc_handler.vala:155-178`: the *local* (self-reported sizes) merge
/// decision, before any Coordinator round-trip.
#[must_use]
pub fn merge_direction(my_n_nodes: u64, neighbor_n_nodes: u64) -> MergeDecision {
    if neighbor_n_nodes == my_n_nodes {
        MergeDecision::AskCoordinator
    } else if neighbor_n_nodes > my_n_nodes {
        if neighbor_n_nodes > my_n_nodes.saturating_mul(10) {
            MergeDecision::Proceed
        } else {
            MergeDecision::AskCoordinator
        }
    } else if my_n_nodes > neighbor_n_nodes.saturating_mul(10) {
        MergeDecision::Wait
    } else {
        MergeDecision::AskCoordinator
    }
}

/// `arc_handler.vala:183-208`: re-decides using the Coordinator's
/// authoritative node counts (`my_n_nodes` from `coord.get_n_nodes()`,
/// `neighbor_n_nodes` from re-asking the peer with `ask_coord = true`) and,
/// on an exact tie, breaks it deterministically on `network_id` so both
/// sides of the tie agree which one proceeds — larger network id proceeds
/// (`arc_handler.vala:207`).
#[must_use]
pub fn merge_tiebreak(
    my_n_nodes: u64,
    neighbor_n_nodes: u64,
    my_network_id: i64,
    neighbor_network_id: i64,
) -> bool {
    if neighbor_n_nodes > my_n_nodes {
        true
    } else {
        neighbor_n_nodes == my_n_nodes && neighbor_network_id > my_network_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smaller_neighbor_within_10x_asks_coordinator() {
        assert_eq!(merge_direction(100, 150), MergeDecision::AskCoordinator);
        assert_eq!(merge_direction(150, 100), MergeDecision::AskCoordinator);
    }

    #[test]
    fn neighbor_over_10x_larger_proceeds_unconditionally() {
        assert_eq!(merge_direction(10, 101), MergeDecision::Proceed);
    }

    #[test]
    fn neighbor_at_exactly_10x_still_asks_coordinator() {
        // `> my_n_nodes * 10`, not `>=` (arc_handler.vala:159).
        assert_eq!(merge_direction(10, 100), MergeDecision::AskCoordinator);
    }

    #[test]
    fn my_network_over_10x_larger_waits() {
        assert_eq!(merge_direction(101, 10), MergeDecision::Wait);
    }

    #[test]
    fn my_network_at_exactly_10x_still_asks_coordinator() {
        assert_eq!(merge_direction(100, 10), MergeDecision::AskCoordinator);
    }

    #[test]
    fn exact_tie_asks_coordinator() {
        assert_eq!(merge_direction(42, 42), MergeDecision::AskCoordinator);
    }

    #[test]
    fn tiebreak_prefers_strictly_larger_authoritative_count() {
        assert!(merge_tiebreak(10, 20, 1, 2));
        assert!(!merge_tiebreak(20, 10, 1, 2));
    }

    #[test]
    fn tiebreak_on_exact_authoritative_tie_uses_network_id() {
        // The actual tiebreak path: equal authoritative counts, decided by
        // which network_id is numerically larger (arc_handler.vala:207).
        assert!(merge_tiebreak(10, 10, 1, 2));
        assert!(!merge_tiebreak(10, 10, 2, 1));
        assert!(!merge_tiebreak(10, 10, 5, 5));
    }
}

#[cfg(test)]
mod proptests {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        /// Swapping which side is "mine" flips `Proceed`/`Wait` and keeps
        /// `AskCoordinator` — the two nodes on an arc must never both
        /// independently decide to `Proceed` (double-merge) nor both
        /// `Wait` (arc permanently ignored).
        #[test]
        fn merge_direction_is_antisymmetric(a in 1u64..1_000_000, b in 1u64..1_000_000) {
            match merge_direction(a, b) {
                MergeDecision::Proceed => prop_assert_eq!(merge_direction(b, a), MergeDecision::Wait),
                MergeDecision::Wait => prop_assert_eq!(merge_direction(b, a), MergeDecision::Proceed),
                MergeDecision::AskCoordinator => {
                    prop_assert_eq!(merge_direction(b, a), MergeDecision::AskCoordinator);
                }
            }
        }

        /// Exactly one side of a tiebreak proceeds — both nodes evaluating
        /// the same authoritative counts/network-ids from their own
        /// perspective must agree on a single winner.
        #[test]
        fn merge_tiebreak_picks_exactly_one_side(
            a in 0u64..1000, b in 0u64..1000, id_a in 0i64..1000, id_b in 0i64..1000,
        ) {
            prop_assume!(!(a == b && id_a == id_b));
            let mine_proceeds = merge_tiebreak(a, b, id_a, id_b);
            let theirs_proceeds = merge_tiebreak(b, a, id_b, id_a);
            prop_assert_ne!(mine_proceeds, theirs_proceeds);
        }
    }
}

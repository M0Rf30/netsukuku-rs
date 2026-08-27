//! Pure-function protocol invariants, fuzzed with `proptest`: path costs
//! never decrease when accumulating, `revise_etp` never lets a hop equal to
//! this node's own g-node position survive into an admitted path (the
//! acyclic rule) yet a cyclic path never takes the rest of its message down
//! with it, implicit withdrawal only ever fires for a full ETP and only for
//! paths it genuinely omits, `Destination::evaluate`'s elder-seed
//! fingerprint selection is order-independent (the fold's "history" of who
//! has been compared so far never changes the winner), indistinguishable
//! fingerprints (tied eldership, distinct identity — the ordinary outcome
//! between two real members of one g-node) never fail `update_map`, a
//! directly-reachable own-g-node member is always exposed, and a split
//! signal fires if and only if two admitted fingerprints for one
//! destination are actually distinguishable by elder-seed.

use ntk_common::{Cost, Fingerprint, HCoord, Naddr, Topology};
use ntk_qspn::{
    ArcId, Destination, EtpMessage, EtpPath, NodePath, QspnConfig, QspnState,
    check_incoming_message, revise_etp,
};
use proptest::prelude::*;

fn cost_strategy() -> impl Strategy<Value = Cost> {
    prop_oneof![
        Just(Cost::Null),
        Just(Cost::Dead),
        any::<u64>().prop_map(Cost::Finite)
    ]
}

proptest! {
    /// Accumulating cost along a path never decreases the running total and
    /// never panics — the type-level guarantee (`Cost` has no negative
    /// representation) plus `saturating_add`'s documented semantics
    /// (`Dead` absorbs, `Null` is identity, `Finite` saturates instead of
    /// wrapping).
    #[test]
    fn cost_accumulation_never_decreases_or_panics(a in cost_strategy(), b in cost_strategy()) {
        let sum = a.saturating_add(b);
        match (a, b) {
            (Cost::Dead, _) | (_, Cost::Dead) => prop_assert_eq!(sum, Cost::Dead),
            (Cost::Null, x) | (x, Cost::Null) => prop_assert_eq!(sum, x),
            (Cost::Finite(x), Cost::Finite(y)) => {
                let expect = x.saturating_add(y);
                prop_assert_eq!(sum, Cost::Finite(expect));
                prop_assert!(expect >= x && expect >= y, "accumulation must never decrease below either segment");
            }
        }
    }
}

fn single_level_topology() -> Topology {
    Topology::new([3]).unwrap()
}

fn hop_path_strategy() -> impl Strategy<Value = Vec<u32>> {
    prop::collection::vec(0u32..3, 1..=2)
}

proptest! {
    /// `revise_etp` never lets a path survive that visits this node's own
    /// g-node position — the acyclic rule (`qspn.vala:1132-1153`). Every
    /// generated path either omits position 0 (this node's own position)
    /// entirely, in which case it survives (possibly regrouped), or
    /// contains it, in which case it MUST be dropped from the output.
    #[test]
    fn revise_etp_never_returns_a_path_through_my_own_position(
        sender_pos in 1u32..3,
        raw_positions in hop_path_strategy(),
    ) {
        let topo = single_level_topology();
        let my_naddr = Naddr::new(topo.clone(), [0]).unwrap();
        let sender = Naddr::new(topo.clone(), [sender_pos]).unwrap();
        let levels = topo.levels();
        let fp = Fingerprint::new(vec![sender_pos as u8], 0, vec![0u32; levels]);

        let hops: Vec<HCoord> = raw_positions.iter().map(|&p| HCoord::new(0, p)).collect();
        let arcs: Vec<ArcId> = (0..hops.len()).map(|i| ArcId::from(100 + i as u32)).collect();
        let path = EtpPath {
            hops: hops.clone(),
            arcs,
            cost: Cost::Finite(1),
            fingerprint: fp.clone(),
            nodes_inside: 1,
            ignore_outside: vec![false; levels],
        };
        let msg = EtpMessage {
            node_address: sender,
            fingerprints: (0..=levels).map(|_| fp.clone()).collect(),
            nodes_inside: vec![1; levels + 1],
            hops: Vec::new(),
            paths: vec![path],
        };
        let arc = ArcId::from(1);
        let contains_my_pos = raw_positions.contains(&0);

        let Ok(revised) = revise_etp(&my_naddr, msg, arc, None, false, &[]) else {
            // A top-level AcyclicError can only fire from `m.hops`, which is
            // always empty here; this branch should be unreachable.
            prop_assert!(false, "unexpected top-level AcyclicError");
            return Ok(());
        };

        for np in &revised.paths {
            prop_assert!(
                !np.path.hops.iter().any(|h| my_naddr.pos(h.level) == Some(h.pos)),
                "revise_etp returned a path through my own position: {:?}",
                np.path.hops
            );
        }
        if contains_my_pos {
            prop_assert!(
                !revised.paths.iter().any(|np| np.path.hops[1..] == hops[..]),
                "the cyclic candidate path must not survive in any form"
            );
        }
    }
}

fn hop_positions_possibly_out_of_range() -> impl Strategy<Value = Vec<u32>> {
    // `single_level_topology()`'s gsize(0) == 3; the upper half of this
    // range (3..6) is deliberately out of bounds, to exercise both
    // acceptance and rejection by `check_incoming_message`.
    prop::collection::vec(0u32..6, 1..=2)
}

proptest! {
    /// No ETP `check_incoming_message` accepts may carry a hop position
    /// outside its level's `gsize` -- the defect this task closes. Before
    /// the fix, `check_hop_list` validated hop levels only ("Positions are
    /// always non-negative in this port ... so only the level checks
    /// apply" -- the reasoning error previously at `validate.rs:13-14`,
    /// since corrected), so an out-of-range `pos` sailed straight through
    /// into `update_map` and every published `RouteSnapshot`. This test
    /// fails before that fix (an out-of-range `raw_positions` entry is
    /// wrongly accepted) and passes after.
    #[test]
    fn accepted_etp_never_carries_an_out_of_range_position(
        sender_pos in 1u32..3,
        raw_positions in hop_positions_possibly_out_of_range(),
    ) {
        let topo = single_level_topology();
        let levels = topo.levels();
        let gsize = topo.gsize(0).unwrap();
        let my_naddr = Naddr::new(topo.clone(), [0]).unwrap();
        let sender = Naddr::new(topo.clone(), [sender_pos]).unwrap();
        let fp = Fingerprint::new(vec![sender_pos as u8], 0, vec![0u32; levels]);

        let hops: Vec<HCoord> = raw_positions.iter().map(|&p| HCoord::new(0, p)).collect();
        let arcs: Vec<ArcId> = (0..hops.len()).map(|i| ArcId::from(100 + i as u32)).collect();
        let path = EtpPath {
            hops: hops.clone(),
            arcs,
            cost: Cost::Finite(1),
            fingerprint: fp.clone(),
            nodes_inside: 1,
            ignore_outside: vec![false; levels],
        };
        let msg = EtpMessage {
            node_address: sender,
            fingerprints: (0..=levels).map(|_| fp.clone()).collect(),
            nodes_inside: vec![1; levels + 1],
            hops: hops.clone(),
            paths: vec![path],
        };

        let any_out_of_range = raw_positions.iter().any(|&p| p >= gsize);
        let accepted = check_incoming_message(&msg, &my_naddr);

        if any_out_of_range {
            prop_assert!(!accepted, "an out-of-range position must never be accepted");
        }
        if accepted {
            for hop in msg.hops.iter().chain(msg.paths.iter().flat_map(|p| p.hops.iter())) {
                prop_assert!(
                    hop.pos < topo.gsize(hop.level).unwrap(),
                    "accepted ETP carried out-of-range hop {hop:?}"
                );
            }
        }
    }
}

fn two_level_topology() -> Topology {
    Topology::new([6, 4]).unwrap()
}

proptest! {
    /// Implicit withdrawal is a full-ETP-only mechanism, and only ever
    /// withdraws a path the message genuinely omits — it must never
    /// resurrect (i.e. leave untouched with a live cost) a path absent from
    /// a full ETP, and must never synthesize a withdrawal at all for a
    /// partial ETP.
    #[test]
    fn implicit_withdrawal_only_for_full_etp_and_only_for_omitted_paths(
        mentioned in prop::collection::vec(any::<bool>(), 3),
        is_full in any::<bool>(),
    ) {
        let topo = two_level_topology();
        let my_naddr = Naddr::new(topo.clone(), [0, 0]).unwrap();
        let sender = Naddr::new(topo.clone(), [1, 0]).unwrap();
        let levels = topo.levels();
        let arc = ArcId::from(1);
        let sender_fp = Fingerprint::new(vec![9u8], 0, vec![0u32; levels]);
        let positions = [2u32, 3, 4]; // distinct from my own pos 0 and sender pos 1, within gsize(0)=6

        let mut state = QspnState::new(my_naddr.clone(), Fingerprint::new(vec![1u8], 0, vec![0u32; levels]), QspnConfig::default());
        state.add_arc(arc, Cost::Finite(5));
        state.record_peer_naddr(arc, sender.clone());

        // First establish `v` (the peer itself) as a known destination —
        // `update_map` drops any candidate whose intermediate hop isn't yet
        // known (`qspn.vala:1466-1486`), and every multi-hop path below
        // passes through `v`.
        let v = HCoord::new(0, 1);
        let v_path = EtpPath {
            hops: vec![v],
            arcs: vec![arc],
            cost: Cost::Null,
            fingerprint: sender_fp.clone(),
            nodes_inside: 1,
            ignore_outside: vec![false, false],
        };
        state.update_map(std::slice::from_ref(&NodePath::new(arc, v_path)), None).unwrap();

        // Seed one existing path per position, always shaped as `[v, pos]`
        // with `arc` then a foreign id — the shape `revise_etp`'s grouping
        // (prepend v/arc) produces for a message that *does* mention it.
        let mut existing = Vec::new();
        for &pos in &positions {
            let foreign = ArcId::from(1000 + pos);
            let path = EtpPath {
                hops: vec![v, HCoord::new(0, pos)],
                arcs: vec![arc, foreign],
                cost: Cost::Finite(1),
                fingerprint: Fingerprint::new(vec![pos as u8], 0, vec![0u32; levels]),
                nodes_inside: 1,
                ignore_outside: vec![false, true],
            };
            state.update_map(std::slice::from_ref(&NodePath::new(arc, path.clone())), None).unwrap();
            existing.push((pos, foreign, path));
        }

        let mut msg_paths = Vec::new();
        for (i, &(pos, foreign, _)) in existing.iter().enumerate() {
            if mentioned[i] {
                msg_paths.push(EtpPath {
                    hops: vec![HCoord::new(0, pos)],
                    arcs: vec![foreign],
                    cost: Cost::Finite(2),
                    fingerprint: Fingerprint::new(vec![pos as u8], 0, vec![0u32; levels]),
                    nodes_inside: 1,
                    ignore_outside: vec![false, true],
                });
            }
        }
        let msg = EtpMessage {
            node_address: sender,
            fingerprints: (0..=levels).map(|_| sender_fp.clone()).collect(),
            nodes_inside: vec![1; levels + 1],
            hops: Vec::new(),
            paths: msg_paths,
        };

        let existing_via_arc = state.paths_via_arc0(arc);
        let revised = revise_etp(&my_naddr, msg, arc, state.peer_naddr(arc).cloned().as_ref(), is_full, &existing_via_arc).unwrap();

        for (i, (_, _, path)) in existing.iter().enumerate() {
            let withdrawn = revised.paths.iter().any(|np| np.path.hops == path.hops && np.path.arcs == path.arcs && np.path.cost.is_dead());
            if is_full && !mentioned[i] {
                prop_assert!(withdrawn, "full ETP omitting an existing path must synthesize its withdrawal");
            } else {
                prop_assert!(!withdrawn, "no withdrawal may be synthesized when the path is mentioned or the ETP is partial");
            }
        }
    }
}

proptest! {
    /// Generalizes `implicit_withdrawal_only_for_full_etp_and_only_for_omitted_paths`
    /// across a second, unrelated arc: whatever a full ETP on `arc` does or
    /// does not withdraw, it must *never* produce any candidate — live or a
    /// synthesized withdrawal — for a path this node learned through a
    /// completely different arc (`existing_paths_via_arc`/`paths_via_arc0`
    /// filters on `path.arcs[0] == arc`, `qspn.vala:1185-1196`'s `m_a_set`).
    /// The daemon defect this crate was dispatched to investigate
    /// (session `QspnWithdrawFix`) would have violated exactly this
    /// invariant had it been an `ntk-qspn` bug; it traced instead to
    /// `ntkd`'s own arc-resolution glue feeding `revise_etp` a mislabeled
    /// `arc` parameter, outside anything this invariant can observe.
    #[test]
    fn full_etp_never_yields_a_candidate_for_a_path_learned_via_a_different_arc(
        mentioned in any::<bool>(),
        is_full in any::<bool>(),
    ) {
        let topo = two_level_topology();
        let my_naddr = Naddr::new(topo.clone(), [0, 0]).unwrap();
        let sender = Naddr::new(topo.clone(), [1, 0]).unwrap();
        let levels = topo.levels();
        let arc = ArcId::from(1);
        let other_arc = ArcId::from(2);
        let sender_fp = Fingerprint::new(vec![9u8], 0, vec![0u32; levels]);

        let mut state = QspnState::new(my_naddr.clone(), Fingerprint::new(vec![1u8], 0, vec![0u32; levels]), QspnConfig::default());
        state.add_arc(arc, Cost::Finite(5));
        state.record_peer_naddr(arc, sender.clone());
        state.add_arc(other_arc, Cost::Finite(9));
        state.record_peer_naddr(other_arc, Naddr::new(topo.clone(), [3, 0]).unwrap());

        // A path learned only through `other_arc` — must be invisible to
        // any `revise_etp` call scoped to `arc`.
        let foreign_path = EtpPath {
            hops: vec![HCoord::new(0, 2)],
            arcs: vec![other_arc],
            cost: Cost::Finite(3),
            fingerprint: Fingerprint::new(vec![7u8], 0, vec![0u32; levels]),
            nodes_inside: 1,
            ignore_outside: vec![false, true],
        };
        state.update_map(std::slice::from_ref(&NodePath::new(other_arc, foreign_path.clone())), None).unwrap();

        let msg_paths = if mentioned {
            vec![EtpPath {
                hops: vec![HCoord::new(0, 4)],
                arcs: vec![ArcId::from(1000)],
                cost: Cost::Finite(2),
                fingerprint: sender_fp.clone(),
                nodes_inside: 1,
                ignore_outside: vec![false, true],
            }]
        } else {
            Vec::new()
        };
        let msg = EtpMessage {
            node_address: sender,
            fingerprints: (0..=levels).map(|_| sender_fp.clone()).collect(),
            nodes_inside: vec![1; levels + 1],
            hops: Vec::new(),
            paths: msg_paths,
        };

        let existing_via_arc = state.paths_via_arc0(arc);
        prop_assert!(existing_via_arc.is_empty(), "arc has learned nothing yet; foreign_path must not leak in");
        let revised = revise_etp(&my_naddr, msg, arc, state.peer_naddr(arc).cloned().as_ref(), is_full, &existing_via_arc).unwrap();

        prop_assert!(
            !revised.paths.iter().any(|np| np.path.hops == foreign_path.hops && np.path.arcs == foreign_path.arcs),
            "a full ETP on `arc` must never produce any candidate for a path learned via `other_arc`"
        );
    }
}

proptest! {
    /// `Destination::evaluate`'s elder-seed fingerprint selection picks the
    /// same winner regardless of the order its candidate paths are stored
    /// in — the aggregation history leading up to any one comparison never
    /// changes the outcome (selecting the maximum of a total order by
    /// iterated pairwise comparison is order-independent). Ids and
    /// elderships are both generated pairwise-distinct: upstream's own
    /// elder-seed comparison only promises a defined answer for
    /// "well-behaved (non-colliding) identities"
    /// (`ntk_common::Error::IndistinguishableFingerprints` docs) — colliding
    /// elderships across different ids can make the *error/success* outcome
    /// itself order-dependent (which candidate meets which first), a
    /// distinct, already-documented degenerate case this test deliberately
    /// avoids rather than mischaracterizing as a fingerprint-selection bug.
    #[test]
    fn destination_evaluate_fingerprint_choice_is_order_independent(
        distinct_elderships in prop::collection::hash_set(0u32..1000, 2..=5),
    ) {
        let n = distinct_elderships.len();
        let paths: Vec<NodePath> = distinct_elderships
            .into_iter()
            .enumerate()
            .map(|(i, eldership)| {
                let base = Fingerprint::new(vec![i as u8], eldership, vec![0u32]);
                let fp1 = base.construct(&[], false).unwrap();
                let path = EtpPath {
                    hops: vec![HCoord::new(1, 0)],
                    arcs: vec![ArcId::from(1)],
                    cost: Cost::Finite(i as u64 + 1),
                    fingerprint: fp1,
                    nodes_inside: 1,
                    ignore_outside: vec![false],
                };
                NodePath::new(ArcId::from(1), path)
            })
            .collect();
        prop_assume!(n >= 2);

        let forward = Destination { coord: HCoord::new(1, 0), paths: paths.clone() };
        let (fp_forward, _, _) = forward.evaluate(|_| Cost::Null).expect("pairwise-distinct elderships never tie");

        let mut reversed_paths = paths;
        reversed_paths.reverse();
        let reversed = Destination { coord: HCoord::new(1, 0), paths: reversed_paths };
        let (fp_reversed, _, _) = reversed.evaluate(|_| Cost::Null).expect("pairwise-distinct elderships never tie");

        prop_assert!(fp_forward.identity_eq(&fp_reversed), "winning fingerprint must not depend on candidate order");
    }
}

proptest! {
    /// A single ETP message carrying both a cyclic path (one whose hop list
    /// loops back through this node's own g-node position) and a genuinely
    /// acyclic one must still yield the acyclic path. The per-path acyclic
    /// rule (`qspn.vala:1132-1153`) drops only the offending path; dropping
    /// the *whole* message is reserved for a cyclic message *header*
    /// (`m.hops`, `qspn.vala:1096-1104`) — always empty for a fresh message
    /// like this one, so it can never itself trigger `QspnError::Acyclic`.
    #[test]
    fn cyclic_path_is_dropped_without_discarding_the_rest_of_the_message(
        sender_pos in 1u32..3,
        survivor_pos in 1u32..3,
    ) {
        let topo = single_level_topology();
        let my_naddr = Naddr::new(topo.clone(), [0]).unwrap();
        let sender = Naddr::new(topo.clone(), [sender_pos]).unwrap();
        let levels = topo.levels();
        let fp = Fingerprint::new(vec![sender_pos as u8], 0, vec![0u32; levels]);

        let cyclic = EtpPath {
            hops: vec![HCoord::new(0, 0)],
            arcs: vec![ArcId::from(100)],
            cost: Cost::Finite(1),
            fingerprint: fp.clone(),
            nodes_inside: 1,
            ignore_outside: vec![false; levels],
        };
        let survivor = EtpPath {
            hops: vec![HCoord::new(0, survivor_pos)],
            arcs: vec![ArcId::from(101)],
            cost: Cost::Finite(2),
            fingerprint: fp.clone(),
            nodes_inside: 1,
            ignore_outside: vec![false; levels],
        };
        let msg = EtpMessage {
            node_address: sender,
            fingerprints: (0..=levels).map(|_| fp.clone()).collect(),
            nodes_inside: vec![1; levels + 1],
            hops: Vec::new(),
            paths: vec![cyclic.clone(), survivor.clone()],
        };
        let arc = ArcId::from(1);

        let revised = revise_etp(&my_naddr, msg, arc, None, false, &[])
            .expect("m.hops is always empty here, so the message header is never cyclic");

        prop_assert!(
            !revised.paths.iter().any(|np| np.path.cost == cyclic.cost),
            "the cyclic path must be dropped"
        );
        prop_assert!(
            revised.paths.iter().any(|np| np.path.cost == survivor.cost),
            "the acyclic path carried in the same message must still survive"
        );
    }
}

proptest! {
    /// Two admitted paths to the same destination whose fingerprints are
    /// genuinely indistinguishable by eldership seed — distinct identity,
    /// identical elderships_seed — must never fail `update_map`. This is
    /// the ordinary outcome between two real members of the very same
    /// g-node under a tied eldership claim: `Fingerprint::construct`'s
    /// champion race starts from each member's own fingerprint as champion
    /// and only a *candidate* sibling can depose it, so it is not the
    /// anomaly `Fingerprint::elder_seed`'s upstream `assert_not_reached()`
    /// assumes (`ntk_common::Error::IndistinguishableFingerprints` docs).
    #[test]
    fn indistinguishable_fingerprints_never_fail_update_map(
        id_a in 0u8..250,
        id_b in 0u8..250,
        eldership in 0u32..1000,
        cost_a in 1u64..100,
        cost_b in 1u64..100,
    ) {
        prop_assume!(id_a != id_b);
        let topo = two_level_topology();
        let my_naddr = Naddr::new(topo.clone(), [0, 0]).unwrap();
        let levels = topo.levels();

        // Both constructed from no known siblings, so each keeps itself as
        // champion: distinct ids, but (since `eldership` is shared) an
        // identical single-entry elderships_seed.
        let fp_a = Fingerprint::new(vec![id_a], eldership, vec![0u32; levels])
            .construct(&[], false)
            .unwrap();
        let fp_b = Fingerprint::new(vec![id_b], eldership, vec![0u32; levels])
            .construct(&[], false)
            .unwrap();
        prop_assert!(!fp_a.identity_eq(&fp_b));
        prop_assert_eq!(fp_a.elder_seed(&fp_b), Err(ntk_common::Error::IndistinguishableFingerprints));

        let mut state = QspnState::new(
            my_naddr,
            Fingerprint::new(vec![1u8], 0, vec![0u32; levels]),
            QspnConfig::default(),
        );
        let arc_a = ArcId::from(1);
        let arc_b = ArcId::from(2);
        state.add_arc(arc_a, Cost::Finite(cost_a));
        state.add_arc(arc_b, Cost::Finite(cost_b));

        let d = HCoord::new(1, 3);
        let path_a = NodePath::new(arc_a, EtpPath {
            hops: vec![d],
            arcs: vec![arc_a],
            cost: Cost::Null,
            fingerprint: fp_a,
            nodes_inside: 1,
            ignore_outside: vec![false; levels],
        });
        let path_b = NodePath::new(arc_b, EtpPath {
            hops: vec![d],
            arcs: vec![arc_b],
            cost: Cost::Null,
            fingerprint: fp_b,
            nodes_inside: 1,
            ignore_outside: vec![false; levels],
        });

        let result = state.update_map(&[path_a, path_b], None);
        prop_assert!(result.is_ok(), "update_map must not fail on indistinguishable fingerprints: {result:?}");
        prop_assert!(state.snapshot().is_ok());
    }
}

proptest! {
    /// A level-0 destination reached by exactly one direct, live-cost path
    /// (the simplest possible admission: a real g-node member one hop
    /// away) is always exposed — `QspnState::exposed_paths` returns it
    /// unconditionally at level 0 (no fingerprint gate applies there, since
    /// a level-0 fingerprint names a single real node, not a branch of an
    /// aggregated g-node) and `snapshot()` carries it through to the
    /// consumer-facing route set with its exact cost.
    #[test]
    fn direct_own_gnode_member_is_always_exposed(
        id in 0u8..250,
        pos in 1u32..4,
        cost in 1u64..1000,
    ) {
        let topo = two_level_topology();
        let my_naddr = Naddr::new(topo.clone(), [0, 0]).unwrap();
        let levels = topo.levels();
        let mut state = QspnState::new(
            my_naddr,
            Fingerprint::new(vec![1u8], 0, vec![0u32; levels]),
            QspnConfig::default(),
        );
        let arc = ArcId::from(1);
        state.add_arc(arc, Cost::Finite(cost));

        let d = HCoord::new(0, pos);
        let path = NodePath::new(arc, EtpPath {
            hops: vec![d],
            arcs: vec![arc],
            cost: Cost::Null,
            fingerprint: Fingerprint::new(vec![id], 0, vec![0u32; levels]),
            nodes_inside: 1,
            ignore_outside: vec![false; levels],
        });
        state.update_map(std::slice::from_ref(&path), None).unwrap();

        let exposed = state.exposed_paths(d).unwrap();
        prop_assert_eq!(exposed.len(), 1, "the direct path must be exposed exactly once: {:?}", exposed);
        prop_assert_eq!(exposed[0].total_cost(state.arc_cost(arc)), Cost::Finite(cost));

        let snapshot = state.snapshot().unwrap();
        let entry = snapshot.levels[0].iter().find(|e| e.destination == d)
            .expect("direct sibling must appear in the level-0 route snapshot");
        prop_assert_eq!(entry.paths.len(), 1);
        prop_assert_eq!(entry.paths[0].cost, Cost::Finite(cost));
    }
}

proptest! {
    /// Two admitted paths to the same level-above-0 destination, reached
    /// via two genuinely disjoint arcs, must signal a split
    /// (`UpdateMapOutcome::split_signals`) if and only if their
    /// fingerprints are actually distinguishable by elder-seed. Tied
    /// (`elder_seed` indistinguishable) or identical fingerprints are the
    /// ordinary outcome of a densely-connected g-node observed through more
    /// than one gateway (see `Fingerprint::same_branch`'s docs) and must
    /// never be reported as a fork; genuinely orderable fingerprints from
    /// two distinct g-nodes must still be reported. This pins the defect
    /// `crates/ntk-qspn/tests/k4_mesh.rs` found: the split check used to
    /// dedupe by bare `Fingerprint::identity_eq`, so two tied-but-distinct
    /// identities always looked like two different g-nodes in conflict.
    #[test]
    fn split_signal_fires_iff_fingerprints_are_actually_distinguishable(
        id_a in 0u8..250,
        id_b in 0u8..250,
        eldership_a in 0u32..1000,
        // Weighted so the tied (indistinguishable) branch — the one the
        // fix targets — is exercised about as often as the genuinely
        // distinguishable one, rather than relying on two independent
        // `0..1000` draws to collide by chance.
        tie_eldership in prop::bool::ANY,
        eldership_b_if_untied in 0u32..1000,
        cost_a in 1u64..100,
        cost_b in 1u64..100,
    ) {
        prop_assume!(id_a != id_b);
        let eldership_b = if tie_eldership { eldership_a } else { eldership_b_if_untied };
        let topo = two_level_topology();
        let my_naddr = Naddr::new(topo.clone(), [0, 0]).unwrap();
        let levels = topo.levels();

        let fp_a = Fingerprint::new(vec![id_a], eldership_a, vec![0u32; levels])
            .construct(&[], false)
            .unwrap();
        let fp_b = Fingerprint::new(vec![id_b], eldership_b, vec![0u32; levels])
            .construct(&[], false)
            .unwrap();
        let distinguishable = !matches!(
            fp_a.elder_seed(&fp_b),
            Err(ntk_common::Error::IndistinguishableFingerprints)
        );

        let mut state = QspnState::new(
            my_naddr,
            Fingerprint::new(vec![1u8], 0, vec![0u32; levels]),
            QspnConfig::default(),
        );
        let arc_a = ArcId::from(1);
        let arc_b = ArcId::from(2);
        state.add_arc(arc_a, Cost::Finite(cost_a));
        state.add_arc(arc_b, Cost::Finite(cost_b));

        let d = HCoord::new(1, 3);
        let path_a = NodePath::new(arc_a, EtpPath {
            hops: vec![d],
            arcs: vec![arc_a],
            cost: Cost::Null,
            fingerprint: fp_a,
            nodes_inside: 1,
            ignore_outside: vec![false; levels],
        });
        let path_b = NodePath::new(arc_b, EtpPath {
            hops: vec![d],
            arcs: vec![arc_b],
            cost: Cost::Null,
            fingerprint: fp_b,
            nodes_inside: 1,
            ignore_outside: vec![false; levels],
        });

        let outcome = state.update_map(&[path_a, path_b], None).unwrap();
        if distinguishable {
            prop_assert!(
                !outcome.split_signals.is_empty(),
                "two genuinely distinguishable fingerprints for one destination must signal a split"
            );
        } else {
            prop_assert!(
                outcome.split_signals.is_empty(),
                "tied/indistinguishable fingerprints must never signal a false split: {:?}",
                outcome.split_signals
            );
        }
    }
}

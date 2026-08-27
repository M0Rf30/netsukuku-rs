//! Migration-specific invariants, fuzzed with `proptest`: [`QspnState::new_entering`]'s
//! internal-arc remap never lets a path survive through an arc outside the
//! freshly constructed identity's own arc set (implicit withdrawal's
//! arc-scoping property, generalized to the migration import path,
//! `qspn.vala:253-283,308-330`), and `revise_etp`'s acyclic rule
//! (`qspn.vala:1132-1153`) is unaffected by `my_naddr` holding a virtual
//! position — it compares raw per-level positions regardless of realness.

use std::collections::HashMap;

use ntk_common::{Cost, Fingerprint, HCoord, Naddr, Topology};
use ntk_qspn::{
    ArcId, Destination, EtpMessage, EtpPath, InternalArc, NodePath, QspnConfig, QspnState,
    revise_etp,
};
use proptest::prelude::*;

fn topology2() -> Topology {
    Topology::new([4, 4]).unwrap()
}

fn arc_id_strategy() -> impl Strategy<Value = ArcId> {
    (1u32..8).prop_map(ArcId::from)
}

proptest! {
    /// `new_entering`'s arc remap never leaves a [`NodePath`] pointing at an
    /// arc outside the freshly constructed identity's own arc set: a path
    /// through the one internal arc that actually survived migration is
    /// remapped and kept; a path through any other (migration-dropped) arc
    /// id is dropped, never smuggled through with a stale id.
    #[test]
    fn new_entering_never_yields_a_path_through_an_unmapped_arc(
        previous_arc in arc_id_strategy(),
        candidate_arc in arc_id_strategy(),
        new_arc in arc_id_strategy(),
        maps in prop::bool::ANY,
    ) {
        let topology = topology2();
        let peer = Naddr::new(topology.clone(), [1, 0]).unwrap();
        let internal_arcs = if maps {
            vec![InternalArc {
                previous_arc,
                new_arc,
                peer_naddr: peer,
                cost: Cost::Finite(1),
            }]
        } else {
            Vec::new()
        };

        let path = EtpPath {
            hops: vec![HCoord::new(0, 2)],
            arcs: vec![candidate_arc],
            cost: Cost::Finite(1),
            fingerprint: Fingerprint::new(vec![9u8], 0, vec![]),
            nodes_inside: 1,
            ignore_outside: vec![false, false],
        };
        let mut level0 = HashMap::new();
        level0.insert(
            2u32,
            Destination {
                coord: HCoord::new(0, 2),
                paths: vec![NodePath::new(candidate_arc, path)],
            },
        );
        let previous_destinations = vec![level0, HashMap::new()];

        let my_naddr = Naddr::new(topology, [0, 0]).unwrap();
        let entering = QspnState::new_entering(
            my_naddr,
            Fingerprint::new(vec![1u8], 0, vec![0u32, 0u32]),
            QspnConfig::default(),
            &internal_arcs,
            &[],
            1,
            2,
            (0, 0),
            &previous_destinations,
        )
        .expect("valid enter_net construction");

        let new_arc_set: Vec<ArcId> = internal_arcs.iter().map(|a| a.new_arc).collect();
        for arc in entering.arcs() {
            prop_assert!(new_arc_set.contains(&arc), "registered arc must be in the new arc set");
        }

        let survived = candidate_arc == previous_arc && maps;
        match entering.destination(0, 2) {
            Some(d) => {
                prop_assert!(survived, "a destination must not survive an unmapped arc");
                for np in &d.paths {
                    prop_assert!(new_arc_set.contains(&np.arc), "path arc must be in the new arc set");
                    prop_assert_eq!(np.path.arcs[0], np.arc, "path.arcs[0] must match the remapped arc");
                }
            }
            None => {
                prop_assert!(!survived, "a mapped arc's destination must survive migration");
            }
        }
    }
}

fn wide_hop_path_strategy() -> impl Strategy<Value = Vec<u32>> {
    prop::collection::vec(0u32..6, 1..=2)
}

proptest! {
    /// `revise_etp`'s acyclic rule compares raw per-level positions
    /// (`Naddr::pos`), never `Naddr::is_virtual_at` — a virtual `my_naddr`
    /// (an entering identity mid-migration) rejects a path through its own
    /// position exactly like a fully-real one would. `raw_positions` is
    /// drawn wide enough (`0..6`) to actually hit the virtual position `5`
    /// itself (`gsize(0) == 3`), not just the real range.
    #[test]
    fn acyclic_rule_holds_for_a_virtual_my_naddr(
        sender_pos in 1u32..3,
        raw_positions in wide_hop_path_strategy(),
    ) {
        let topo = Topology::new([3]).unwrap();
        // Virtual: 5 >= gsize(0) == 3.
        let my_naddr = Naddr::new_allowing_virtual(topo.clone(), [5]).unwrap();
        prop_assert_eq!(my_naddr.is_virtual_at(0), Some(true));
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
        let contains_my_pos = raw_positions.contains(&5);

        let Ok(revised) = revise_etp(&my_naddr, msg, arc, None, false, &[]) else {
            prop_assert!(false, "unexpected top-level AcyclicError");
            return Ok(());
        };
        for np in &revised.paths {
            prop_assert!(
                !np.path.hops.iter().any(|h| my_naddr.pos(h.level) == Some(h.pos)),
                "revise_etp returned a path through my own (virtual) position: {:?}",
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

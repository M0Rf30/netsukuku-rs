//! Pins the exact implicit-withdrawal semantics
//! (`research/impl/vala/qspn/qspn.vala:1182-1223`, notes/01 §3 rule 4): a
//! **full** ETP that stays silent about a previously-known path through the
//! sending arc MUST withdraw it; a **partial** ETP with the same silence
//! MUST NOT.

use ntk_common::{Cost, Fingerprint, HCoord, Naddr, Topology};
use ntk_qspn::{ArcId, EtpMessage, EtpPath, NodePath, QspnConfig, QspnState, revise_etp};

fn topo() -> Topology {
    Topology::new([4, 4]).unwrap()
}

fn addr(t: &Topology, pos: [u32; 2]) -> Naddr {
    Naddr::new(t.clone(), pos).unwrap()
}

fn fp(id: u8, levels: usize) -> Fingerprint<Vec<u8>> {
    Fingerprint::new(vec![id], 0, vec![0u32; levels])
}

/// Builds a fresh state (node A at `[0,0]`) with one already-known path to
/// destination `(0,2)` learned through `arc` (peer B at `[1,0]`), returning
/// `(state, arc, c_path, peer_naddr)`.
fn seed(t: &Topology) -> (QspnState, ArcId, EtpPath, Naddr) {
    let mut state = QspnState::new(addr(t, [0, 0]), fp(1, t.levels()), QspnConfig::default());
    let arc = ArcId::from(1);
    state.add_arc(arc, Cost::Finite(5));
    let peer_naddr = addr(t, [1, 0]);
    state.record_peer_naddr(arc, peer_naddr.clone());

    let c_path = EtpPath {
        hops: vec![HCoord::new(0, 2)],
        arcs: vec![arc],
        cost: Cost::Finite(1),
        fingerprint: fp(9, t.levels()),
        nodes_inside: 1,
        ignore_outside: vec![false, true],
    };
    state
        .update_map(&[NodePath::new(arc, c_path.clone())], None)
        .unwrap();
    assert!(
        !state.exposed_paths(HCoord::new(0, 2)).unwrap().is_empty(),
        "seed setup failed: destination (0,2) not admitted"
    );
    (state, arc, c_path, peer_naddr)
}

fn silent_etp(peer_naddr: Naddr, levels: usize) -> EtpMessage {
    EtpMessage {
        node_address: peer_naddr,
        fingerprints: (0..=levels).map(|_| fp(2, levels)).collect(),
        nodes_inside: vec![1; levels + 1],
        hops: Vec::new(),
        paths: Vec::new(),
    }
}

#[test]
fn full_etp_silent_about_a_known_path_withdraws_it() {
    let t = topo();
    let (mut state, arc, c_path, peer_naddr) = seed(&t);
    let existing = state.paths_via_arc0(arc);
    assert_eq!(
        existing.len(),
        1,
        "expected exactly the seeded path to (0,2)"
    );

    let etp = silent_etp(peer_naddr, t.levels());
    let revised = revise_etp(
        state.my_naddr(),
        etp,
        arc,
        state.peer_naddr(arc).cloned().as_ref(),
        true,
        &existing,
    )
    .expect("revise_etp");

    let withdrawal = revised
        .paths
        .iter()
        .find(|np| np.path.hops == c_path.hops && np.path.arcs == c_path.arcs);
    assert!(
        withdrawal.is_some(),
        "a full ETP silent about (0,2) must synthesize its withdrawal"
    );
    assert_eq!(
        withdrawal.unwrap().path.cost,
        Cost::Dead,
        "the synthesized withdrawal must carry Cost::Dead"
    );

    state.update_map(&revised.paths, None).unwrap();
    assert!(
        state.exposed_paths(HCoord::new(0, 2)).unwrap().is_empty(),
        "destination (0,2) MUST be gone after a full ETP silent about it"
    );
}

#[test]
fn partial_etp_silent_about_a_known_path_does_not_withdraw_it() {
    let t = topo();
    let (mut state, arc, c_path, peer_naddr) = seed(&t);
    let existing = state.paths_via_arc0(arc);

    let etp = silent_etp(peer_naddr, t.levels());
    let revised = revise_etp(
        state.my_naddr(),
        etp,
        arc,
        state.peer_naddr(arc).cloned().as_ref(),
        false,
        &existing,
    )
    .expect("revise_etp");

    let withdrawal = revised
        .paths
        .iter()
        .find(|np| np.path.hops == c_path.hops && np.path.arcs == c_path.arcs);
    assert!(
        withdrawal.is_none(),
        "a partial ETP MUST NOT synthesize a withdrawal for a path it simply didn't mention"
    );

    state.update_map(&revised.paths, None).unwrap();
    assert!(
        !state.exposed_paths(HCoord::new(0, 2)).unwrap().is_empty(),
        "destination (0,2) MUST survive a partial ETP that doesn't mention it"
    );
}

/// Pins the exact defect surfaced by `ntkd`'s multi-node integration test
/// (`crates/ntkd/tests/multi_node.rs::chain_converges_then_arc_flap_reinstalls_only_the_affected_route`,
/// investigated by session `QspnWithdrawFix`): on a node with **two** arcs,
/// a full ETP arriving on the second arc must withdraw only its *own*
/// omitted paths (`m_a_set`, `qspn.vala:1185-1196`) and must leave every
/// path learned through the *other* arc completely untouched. The
/// investigation confirmed `revise_etp`/`update_map` already scope this
/// correctly (`existing_paths_via_arc`/`paths_via_arc0` filter on
/// `path.arcs[0] == arc`, matching upstream's `d_p.path.arcs[0] == arc_id`
/// exactly) — the daemon's observed symptom traced to `ntkd`'s own
/// arc-resolution glue (`crates/ntkd/src/node/{registry.rs,adapters.rs}`)
/// misattributing one arc's inbound call to a *different* local arc-id
/// (out of this crate's scope), which upstream's `peer_naddr_changed`
/// identity-migration rule (`qspn.vala:1082-1088,1166-1181`) then correctly
/// — but spuriously, given the mislabeled input — treated as "this arc's
/// peer moved". This test locks in that `ntk-qspn`'s own two-arc scoping
/// has no such bug and must never regress into one.
#[test]
fn full_etp_on_one_arc_never_touches_paths_learned_via_another_arc() {
    let t = Topology::new([5, 4]).unwrap();
    let mut state = QspnState::new(addr(&t, [0, 0]), fp(1, t.levels()), QspnConfig::default());

    let arc1 = ArcId::from(1);
    let peer1 = addr(&t, [1, 0]); // B
    state.add_arc(arc1, Cost::Finite(5));
    state.record_peer_naddr(arc1, peer1.clone());

    let arc2 = ArcId::from(2);
    let peer2 = addr(&t, [2, 0]); // C
    state.add_arc(arc2, Cost::Finite(7));
    state.record_peer_naddr(arc2, peer2.clone());

    // D, learned only through arc1; E, learned only through arc2.
    let d_path = EtpPath {
        hops: vec![HCoord::new(0, 3)],
        arcs: vec![arc1],
        cost: Cost::Finite(1),
        fingerprint: fp(9, t.levels()),
        nodes_inside: 1,
        ignore_outside: vec![false, true],
    };
    let e_path = EtpPath {
        hops: vec![HCoord::new(0, 4)],
        arcs: vec![arc2],
        cost: Cost::Finite(1),
        fingerprint: fp(8, t.levels()),
        nodes_inside: 1,
        ignore_outside: vec![false, true],
    };
    state
        .update_map(&[NodePath::new(arc1, d_path.clone())], None)
        .unwrap();
    state
        .update_map(&[NodePath::new(arc2, e_path.clone())], None)
        .unwrap();
    assert!(!state.exposed_paths(HCoord::new(0, 3)).unwrap().is_empty());
    assert!(!state.exposed_paths(HCoord::new(0, 4)).unwrap().is_empty());

    // A full ETP arrives on arc2 (from C), silent about everything —
    // including E, which C itself previously reported.
    let existing_via_arc2 = state.paths_via_arc0(arc2);
    let etp = silent_etp(peer2, t.levels());
    let revised = revise_etp(
        state.my_naddr(),
        etp,
        arc2,
        state.peer_naddr(arc2).cloned().as_ref(),
        true,
        &existing_via_arc2,
    )
    .expect("revise_etp");

    // Direction 1: arc2's own omitted path (E) is withdrawn.
    let e_withdrawn = revised.paths.iter().any(|np| {
        np.path.hops == e_path.hops && np.path.arcs == e_path.arcs && np.path.cost.is_dead()
    });
    assert!(
        e_withdrawn,
        "full ETP on arc2 must withdraw arc2's own omitted path E"
    );
    // Direction 2: arc1's path (D) is not even a candidate in this batch.
    assert!(
        !revised.paths.iter().any(|np| np.path.hops == d_path.hops),
        "full ETP on arc2 must not synthesize anything for D, which arc1 taught us"
    );

    state.update_map(&revised.paths, None).unwrap();
    assert!(
        state.exposed_paths(HCoord::new(0, 4)).unwrap().is_empty(),
        "E must be gone: arc2's full ETP stayed silent about it"
    );
    let d_after = state.exposed_paths(HCoord::new(0, 3)).unwrap();
    assert_eq!(
        d_after.len(),
        1,
        "D, learned via arc1, must survive arc2's full ETP untouched"
    );
    assert_eq!(d_after[0].path.cost, Cost::Finite(1));
    assert_eq!(d_after[0].arc, arc1);
}

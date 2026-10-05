// SPDX-FileCopyrightText: 2026 Gianluca Boiano
// SPDX-License-Identifier: GPL-3.0-or-later

//! A neighbour must not be able to grow one destination's stored paths
//! without bound by repeating a route under many peer-chosen fingerprint ids.

use ntk_common::{Cost, Fingerprint, HCoord, Naddr, Topology};
use ntk_qspn::{ArcId, EtpPath, NodePath, QspnConfig, QspnState};

#[test]
fn repeating_one_route_under_many_fingerprint_ids_stores_it_once() {
    let topo = Topology::new([6, 4]).expect("valid topology");
    let levels = topo.levels();
    let my_naddr = Naddr::new(topo, [0, 0]).expect("valid address");
    let mut state = QspnState::new(
        my_naddr,
        Fingerprint::new(vec![1u8], 0, vec![0u32; levels]),
        QspnConfig::default(),
    );
    let arc = ArcId::from(1);
    state.add_arc(arc, Cost::Finite(1));

    let d = HCoord::new(0, 3);
    let candidates: Vec<NodePath> = (0..50u8)
        .map(|id| {
            NodePath::new(
                arc,
                EtpPath {
                    hops: vec![d],
                    arcs: vec![arc],
                    cost: Cost::Finite(1),
                    fingerprint: Fingerprint::new(vec![id], u32::from(id), vec![0u32; levels]),
                    nodes_inside: 1,
                    ignore_outside: vec![false; levels],
                },
            )
        })
        .collect();
    state
        .update_map(&candidates, None)
        .expect("update_map must accept the candidates");

    let stored = state
        .paths_via_arc0(arc)
        .into_iter()
        .filter(|np| np.path.hops == vec![d])
        .count();
    assert_eq!(stored, 1, "identical routes must be de-duplicated");
}

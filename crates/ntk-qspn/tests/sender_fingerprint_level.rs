// SPDX-FileCopyrightText: 2026 Gianluca Boiano
// SPDX-License-Identifier: GPL-3.0-or-later

//! A peer's per-level sender fingerprints must carry the level of their own
//! slot; a mislevelled one is rejected before it can reach the intrinsic
//! path built by `revise_etp`.

use ntk_common::{Cost, Fingerprint, HCoord, Naddr, Topology};
use ntk_qspn::{ArcId, EtpMessage, check_incoming_message, revise_etp};

fn message(sender_fps: Vec<Fingerprint<Vec<u8>>>, my: &Naddr) -> EtpMessage {
    let topo = my.topology().clone();
    EtpMessage {
        node_address: Naddr::new(topo, vec![0u32, 1]).expect("valid sender address"),
        fingerprints: sender_fps,
        nodes_inside: vec![1; 3],
        hops: vec![],
        paths: vec![],
    }
}

fn setup() -> (Naddr, Vec<Fingerprint<Vec<u8>>>) {
    let topo = Topology::new(vec![2, 2]).expect("valid topology");
    let my = Naddr::new(topo, vec![0u32, 0]).expect("valid address");
    let good = (0..=2usize)
        .map(|level| {
            let mut fp = Fingerprint::new(vec![1u8], 0, vec![0u32; level]);
            for _ in 0..level {
                fp = fp.construct(&[], false).expect("valid champion climb");
            }
            fp
        })
        .collect();
    (my, good)
}

#[test]
fn etp_with_mislevelled_sender_fingerprint_is_rejected_by_validation() {
    let (my, mut fps) = setup();
    assert!(
        check_incoming_message(&message(fps.clone(), &my), &my),
        "well-formed levels must be accepted"
    );
    fps[1] = fps[0].clone();
    assert!(!check_incoming_message(&message(fps, &my), &my));
}

#[test]
fn revise_etp_errors_instead_of_building_a_mislevelled_intrinsic_path() {
    let (my, mut fps) = setup();
    // Divergence level against sender [0, 1] is 1, so fingerprints[1] is used.
    fps[1] = fps[0].clone();
    let result = revise_etp(&my, message(fps, &my), ArcId::from(1), None, false, &[]);
    assert!(result.is_err());
}

#[test]
fn revise_etp_withdraws_the_real_old_coordinate_when_the_divergence_level_changed() {
    let (my, fps) = setup();
    // Peer used to be my level-0 sibling at [1, 0]; it is now at [0, 1],
    // which diverges from me at level 1.
    let old = Naddr::new(my.topology().clone(), vec![1u32, 0]).expect("valid old address");
    let revised = revise_etp(
        &my,
        message(fps, &my),
        ArcId::from(1),
        Some(&old),
        false,
        &[],
    )
    .expect("well-formed ETP");
    assert!(
        revised
            .paths
            .iter()
            .any(|p| p.path.cost == Cost::Dead && p.path.hops == vec![HCoord::new(0, 1)]),
        "the old level-0 coordinate must be withdrawn: {:?}",
        revised.paths
    );
}

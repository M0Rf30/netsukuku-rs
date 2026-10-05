// SPDX-FileCopyrightText: 2026 Gianluca Boiano
// SPDX-License-Identifier: GPL-3.0-or-later

//! Every ETP this actor generates must satisfy the shape its peers enforce:
//! per-level sender fingerprints carry their own level and the message passes
//! `check_incoming_message` on the receiving side.

mod support;

use ntk_common::Cost;
use ntk_qspn::check_incoming_message;
use proptest::prelude::*;
use support::{Node, fast_config, link, naddr, topology};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
    fn generated_full_etp_has_leveled_fingerprints_and_is_accepted_by_the_peer(
        pos0 in 0u32..4,
        pos1 in 0u32..4,
    ) {
        prop_assume!(pos0 != 0 || pos1 != 0);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let topo = topology();
            let a_addr = naddr(&topo, [0, 0]);
            let b_addr = naddr(&topo, [pos0, pos1]);
            let a = Node::spawn(a_addr.clone(), 1, fast_config());
            let b = Node::spawn(b_addr.clone(), 2, fast_config());
            let (_a_arc, b_arc) = link(&a, &b, Cost::Finite(5)).await;

            let etp = b
                .handle
                .handle_get_full_etp(b_arc, a_addr.clone())
                .await
                .expect("a bootstrapped node answers a full-ETP request");
            for (level, fp) in etp.fingerprints.iter().enumerate() {
                prop_assert_eq!(fp.level(), level);
            }
            prop_assert!(check_incoming_message(&etp, &a_addr));
            Ok(())
        })?;
    }
}

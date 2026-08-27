//! ETP construction for outbound sends — `research/impl/vala/qspn/etp_message.vala:24-245`.
//! Building an outgoing `EtpMessage` and the `ignore_outside` pruning pass
//! are read-only over [`QspnState`], so both live here as free functions
//! rather than `QspnState` methods, keeping the actor's mutation surface
//! (`update_map`/`update_clusters`/arc table edits) the only place that
//! writes to the destination map.

use ntk_common::{Cost, HCoord};

use crate::path::{EtpMessage, EtpPath, NodePath};
use crate::state::QspnState;

/// `prepare_new_etp` (`etp_message.vala:193-213`): wraps `paths` plus this
/// node's own fingerprints/`nodes_inside`/address into an outgoing ETP,
/// stamped with `etp_hops` — empty for a self-originated ETP, or the
/// revised sender's hop trail when forwarding (`prepare_fwd_etp`,
/// `etp_message.vala:234-245`).
#[must_use]
pub fn prepare_new_etp(
    state: &QspnState,
    paths: Vec<EtpPath>,
    etp_hops: Vec<HCoord>,
) -> EtpMessage {
    let levels = state.levels();
    EtpMessage {
        node_address: state.my_naddr().clone(),
        fingerprints: (0..=levels)
            .map(|l| state.fingerprint(l).expect("l <= levels").clone())
            .collect(),
        nodes_inside: (0..=levels)
            .map(|l| state.nodes_inside_at(l).expect("l <= levels"))
            .collect(),
        hops: etp_hops,
        paths,
    }
}

/// `prepare_full_etp` (`etp_message.vala:216-232`): every currently-admitted
/// path at every level, fully pruned via [`set_ignore_outside_for_sending`].
#[must_use]
pub fn prepare_full_etp(state: &QspnState) -> EtpMessage {
    let mut paths = Vec::new();
    for level in 0..state.levels() {
        for np in state.all_paths_at(level) {
            let mut p = crate::path::prepare_for_sending(np, state.arc_cost(np.arc));
            set_ignore_outside_for_sending(state, &mut p);
            paths.push(p);
        }
    }
    prepare_new_etp(state, paths, Vec::new())
}

/// `set_ignore_outside_for_sending` (`etp_message.vala:37-116`): for each
/// level `i` above 0 that `p` reaches, decides whether `p`'s fact is still
/// worth telling levels above `i` — `true` ("ignore") when a different,
/// currently-preferred path to the same exit gateway already covers it, so
/// this specific route isn't redundantly (or inconsistently) advertised
/// outward.
pub fn set_ignore_outside_for_sending(state: &QspnState, p: &mut EtpPath) {
    let levels = state.levels();
    let last_level = p.hops.last().expect("path always has >= 1 hop").level;
    let mut ignore_outside = Vec::with_capacity(levels);
    ignore_outside.push(false);
    for i in 1..levels {
        if last_level < i {
            ignore_outside.push(true);
            continue;
        }
        let j = p
            .hops
            .iter()
            .position(|h| h.level >= i)
            .expect("last_level >= i");
        let hop = p.hops[j];
        let Some(d) = state.destination(hop.level, hop.pos) else {
            // Not (yet) a known destination: upstream warns for a live path
            // and accepts silently for a dead one (etp_message.vala:56-67);
            // either way the fact is treated as still valid.
            ignore_outside.push(false);
            continue;
        };
        let mut best: Option<(&NodePath, Cost)> = None;
        for q in &d.paths {
            if q.path.arcs.last() == Some(&p.arcs[j]) {
                let cost = q.total_cost(state.arc_cost(q.arc));
                if best.is_none_or(|(_, best_cost)| cost < best_cost) {
                    best = Some((q, cost));
                }
            }
        }
        let ignore = match best {
            None => false,
            Some((best, _)) => {
                let same = best.path.hops.len() == j + 1
                    && (0..j)
                        .all(|k| best.path.hops[k] == p.hops[k] && best.path.arcs[k] == p.arcs[k]);
                !same
            }
        };
        ignore_outside.push(ignore);
    }
    p.ignore_outside = ignore_outside;
}

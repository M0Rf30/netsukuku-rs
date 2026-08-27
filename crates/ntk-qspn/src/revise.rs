//! `revise_etp` (`research/impl/vala/qspn/qspn.vala:1074-1232`): the single
//! function upstream itself flags as the hardest QSPN rule to port faithfully
//! (notes/01 §3 rule 4) — full-ETP implicit withdrawal. There is deliberately
//! no explicit "withdraw" message: a full ETP's *silence* about a
//! previously-known path through the same arc means that path is now dead.

use ntk_common::{Cost, HCoord, Naddr};

use crate::arc::ArcId;
use crate::error::QspnError;
use crate::path::{EtpMessage, EtpPath, NodePath};

/// The result of [`revise_etp`]: the message's own hop trail after grouping
/// (needed to build a forward message's `hops` field, `etp_message.vala:240-244`)
/// plus every path the message yields.
#[derive(Debug)]
pub struct RevisedEtp {
    /// `m.hops` after the grouping + acyclic rule (`qspn.vala:1090-1104`).
    pub hops: Vec<HCoord>,
    pub paths: Vec<NodePath>,
}

/// Revises `m` (received from `arc`, whose peer address was `old_peer_naddr`
/// before this message) into this node's own coordinate frame, and returns
/// every path the message yields — freshly grouped, acyclic-checked, plus the
/// two intrinsic paths to the sender and (for a full ETP) the synthetic
/// `Cost::Dead` withdrawal for every path this node held through `arc` that
/// the full ETP did not repeat.
///
/// `m` MUST already have passed [`crate::check_incoming_message`] (or, for a
/// self-originated forward, [`crate::check_outgoing_message`]) — this
/// function assumes validated shape (e.g. every path's `fingerprint.level()`
/// matches its own last hop's level).
///
/// `existing_paths_via_arc` MUST be exactly the node's current paths whose
/// `path.arcs[0] == arc` (`qspn.vala:1185-1196`'s `m_a_set`) — the caller
/// (the actor, which owns the destination map) computes this slice since this
/// function stays pure/testable without owning any map state.
///
/// # Errors
/// [`QspnError::Acyclic`] if `m`'s own hop list loops back through this
/// node's g-node (`qspn.vala:1096-1104`) — upstream's `AcyclicError`, fatal to
/// the whole message (individual looping *paths* are silently dropped
/// instead, `qspn.vala:1132-1153`, not surfaced as an error).
/// [`QspnError::EtpFromSelf`] if `m.node_address` is this node's own address
/// (should already be rejected by ingress validation).
pub fn revise_etp(
    my_naddr: &Naddr,
    mut m: EtpMessage,
    arc: ArcId,
    old_peer_naddr: Option<&Naddr>,
    is_full: bool,
    existing_paths_via_arc: &[NodePath],
) -> Result<RevisedEtp, QspnError> {
    let levels = my_naddr.topology().levels();
    if m.fingerprints.len() != levels + 1 || m.nodes_inside.len() != levels + 1 {
        return Err(QspnError::MalformedEtp(
            "fingerprints/nodes_inside length must be levels + 1",
        ));
    }
    let v = my_naddr
        .hcoord(&m.node_address)
        .map_err(QspnError::Common)?
        .ok_or(QspnError::EtpFromSelf)?;

    let peer_naddr_changed =
        old_peer_naddr.is_some_and(|old| old.pos(v.level) != m.node_address.pos(v.level));

    // Grouping rule on m.hops (qspn.vala:1090-1094): drop leading hops this
    // node's level doesn't need to see, then prepend the sender's own coord.
    while m.hops.first().is_some_and(|h| h.level < v.level) {
        m.hops.remove(0);
    }
    m.hops.insert(0, v);
    // Acyclic rule on m.hops (qspn.vala:1096-1104): fatal for the whole message.
    if m.hops.iter().any(|g| my_naddr.pos(g.level) == Some(g.pos)) {
        return Err(QspnError::Acyclic);
    }

    // Drop paths whose fact isn't valid at this node's level (qspn.vala:1107-1119).
    m.paths
        .retain(|p| !p.ignore_outside.get(v.level).copied().unwrap_or(false));

    // Grouping rule per path, then acyclic rule (qspn.vala:1121-1153): a
    // looping *path* is silently dropped, not an error.
    for p in &mut m.paths {
        while p.hops.first().is_some_and(|h| h.level < v.level) {
            p.hops.remove(0);
            p.arcs.remove(0);
        }
        p.hops.insert(0, v);
        p.arcs.insert(0, arc);
    }
    m.paths
        .retain(|p| !p.hops.iter().any(|g| my_naddr.pos(g.level) == Some(g.pos)));

    // Intrinsic zero-cost path straight to the sender (qspn.vala:1154-1165).
    let sender_fp = m.fingerprints[v.level].clone();
    let sender_nn = m.nodes_inside[v.level];
    m.paths.push(EtpPath {
        hops: vec![v],
        arcs: vec![arc],
        cost: Cost::Null,
        fingerprint: sender_fp.clone(),
        nodes_inside: sender_nn,
        ignore_outside: vec![false; levels],
    });

    // If the peer's address at this level moved since the last ETP (identity
    // migrated on the other end), also withdraw the *old* position
    // (qspn.vala:1166-1181).
    if peer_naddr_changed {
        let old_pos = old_peer_naddr
            .and_then(|old| old.pos(v.level))
            .expect("peer_naddr_changed is only true when old_peer_naddr is Some");
        m.paths.push(EtpPath {
            hops: vec![HCoord::new(v.level, old_pos)],
            arcs: vec![arc],
            cost: Cost::Dead,
            fingerprint: sender_fp,
            nodes_inside: sender_nn,
            ignore_outside: vec![false; levels],
        });
    }

    let mut ret = Vec::new();
    // Full-ETP implicit withdrawal (qspn.vala:1182-1223): the single most
    // misportable QSPN rule (notes/01 §3 rule 4). There is no explicit
    // withdraw message — a full ETP that stays silent about a path this node
    // previously held through `arc` IS the withdrawal, encoded here as a
    // synthetic `Cost::Dead` candidate fed into the same `update_map` merge
    // path as any other candidate (so it competes/replaces exactly like a
    // real update would).
    if is_full {
        for np in existing_paths_via_arc {
            let still_present = m
                .paths
                .iter()
                .any(|p| np.path.hops == p.hops && np.path.arcs == p.arcs);
            if !still_present {
                let mut dead = np.path.clone();
                dead.cost = Cost::Dead;
                // A freshly synthesized NodePath always starts unexposed,
                // matching the `NodePath` ctor upstream always uses here
                // (`destinations.vala:26-31`) regardless of the withdrawn
                // path's prior exposed state.
                ret.push(NodePath::new(arc, dead));
            }
        }
    }

    ret.extend(m.paths.into_iter().map(|path| NodePath::new(arc, path)));
    Ok(RevisedEtp {
        hops: m.hops,
        paths: ret,
    })
}

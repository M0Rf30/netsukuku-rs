//! Position tuples scoped to a bounded address prefix, and the Chord-like geometry built on
//! them: `dist`/`approximate` (the key→g-node mapping) plus the small set of tuple-algebra
//! helpers `contact_peer`/`forward_msg` need to translate a target between scopes as a message
//! hops deeper into the hierarchy.
//!
//! Direct port of `research/impl/vala/peerservices/serializables.vala:23-207` (the two tuple
//! types) and `utils.vala` (the algebra) onto [`ntk_common::Topology`]/[`ntk_common::HCoord`].
//!
//! **Scope note**: like [`ntk_common::Naddr`], this module models only fully-resolved
//! (non-virtual) local addresses. Upstream additionally tracks *virtual* positions
//! (`pos >= gsize(level)`) for a node mid-migration and gates routing on `i_am_real_up_to`/
//! `i_am_real_down_to` (`message_routing.vala:229-241,280,285,310,425,540`), aborting the
//! calling tasklet via `client_not_main_id`/`server_not_main_id` when a *virtual* identity tries
//! to act as a client or servant. Since every position this crate accepts is backed by
//! [`ntk_common::Naddr`] (which already rejects out-of-range/virtual positions at construction),
//! those guards are always trivially true here and are omitted rather than half-modeled;
//! whichever future crate implements hooking/migration is where virtual addressing belongs.

use std::fmt;

use ntk_common::{HCoord, Topology};

use crate::error::Error;

/// A fully-resolved node position, one entry per level from 0 up to (but not including) `top`.
/// Rust analogue of upstream's `PeerTupleNode{tuple, top}`
/// (`research/impl/vala/peerservices/serializables.vala:23-111`); `level` is always 0 there
/// (a node's position is a leaf, never a g-node), so unlike [`TupleGNode`] this type has no
/// separate `level`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TupleNode {
    topology: Topology,
    pos: Box<[u32]>,
}

impl TupleNode {
    /// Builds a node tuple spanning levels `0..pos.len()`.
    ///
    /// # Errors
    /// [`Error::TopOutOfRange`] if `pos` has more entries than `topology` has levels;
    /// [`Error::PositionOutOfRange`] if any entry is `>=` its level's g-node size.
    pub fn new(topology: Topology, pos: impl Into<Box<[u32]>>) -> Result<Self, Error> {
        let pos = pos.into();
        if pos.len() > topology.levels() {
            return Err(Error::TopOutOfRange {
                top: pos.len(),
                levels: topology.levels(),
            });
        }
        for (level, &p) in pos.iter().enumerate() {
            let gsize = topology
                .gsize(level)
                .expect("level < pos.len() <= topology.levels()");
            if p >= gsize {
                return Err(Error::PositionOutOfRange {
                    level,
                    pos: p,
                    gsize,
                });
            }
        }
        Ok(Self { topology, pos })
    }

    /// The number of levels this tuple spans (upstream's `top`, always equal to `tuple.size` for
    /// a node tuple).
    #[must_use]
    pub fn top(&self) -> usize {
        self.pos.len()
    }

    /// The per-level positions, index 0 first.
    #[must_use]
    pub fn positions(&self) -> &[u32] {
        &self.pos
    }

    /// The [`Topology`] this tuple is bound to.
    #[must_use]
    pub fn topology(&self) -> &Topology {
        &self.topology
    }
}

impl fmt::Display for TupleNode {
    /// `[p0,p1,...]`, matching upstream's `PeerTupleNode.to_string()` for a node (`level` is
    /// always 0, so no leading `*` markers, `serializables.vala:96-110`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        for (i, p) in self.pos.iter().enumerate() {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{p}")?;
        }
        write!(f, "]")
    }
}

/// A g-node position spanning levels `level..top` (inclusive..exclusive), where
/// `level = top - pos.len()`. Rust analogue of upstream's `PeerTupleGNode{tuple, top}`
/// (`research/impl/vala/peerservices/serializables.vala:113-207`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TupleGNode {
    topology: Topology,
    top: usize,
    pos: Box<[u32]>,
}

impl TupleGNode {
    /// Builds a g-node tuple naming levels `(top - pos.len())..top`.
    ///
    /// # Errors
    /// [`Error::EmptyGNodeTuple`] if `pos` is empty; [`Error::TopOutOfRange`] if `top` exceeds
    /// `topology`'s level count; [`Error::GNodeTupleTooLong`] if `pos` has more entries than
    /// `top`; [`Error::PositionOutOfRange`] if any entry is `>=` its level's g-node size.
    pub fn new(topology: Topology, top: usize, pos: impl Into<Box<[u32]>>) -> Result<Self, Error> {
        let pos = pos.into();
        if pos.is_empty() {
            return Err(Error::EmptyGNodeTuple);
        }
        if top > topology.levels() {
            return Err(Error::TopOutOfRange {
                top,
                levels: topology.levels(),
            });
        }
        if pos.len() > top {
            return Err(Error::GNodeTupleTooLong {
                len: pos.len(),
                top,
            });
        }
        let level = top - pos.len();
        for (i, &p) in pos.iter().enumerate() {
            let lvl = level + i;
            let gsize = topology.gsize(lvl).expect("lvl < top <= topology.levels()");
            if p >= gsize {
                return Err(Error::PositionOutOfRange {
                    level: lvl,
                    pos: p,
                    gsize,
                });
            }
        }
        Ok(Self { topology, top, pos })
    }

    /// The g-node's own hierarchy level (upstream's `level` getter, `top - tuple.size`).
    #[must_use]
    pub fn level(&self) -> usize {
        self.top - self.pos.len()
    }

    /// The address-prefix scope this tuple is expressed in.
    #[must_use]
    pub fn top(&self) -> usize {
        self.top
    }

    /// The per-level positions for `level()..top()`, index 0 first (i.e. the g-node's own level
    /// first).
    #[must_use]
    pub fn positions(&self) -> &[u32] {
        &self.pos
    }

    /// The [`Topology`] this tuple is bound to.
    #[must_use]
    pub fn topology(&self) -> &Topology {
        &self.topology
    }

    /// This g-node's coordinate: `(level(), position at that level)`.
    #[must_use]
    pub fn hcoord(&self) -> HCoord {
        HCoord::new(self.level(), self.pos[0])
    }
}

impl fmt::Display for TupleGNode {
    /// `*,*,...,p0,p1,...`, matching upstream's `PeerTupleGNode.to_string()`
    /// (`serializables.vala:192-206`).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;
        let mut next = "";
        for _ in 0..self.level() {
            write!(f, "{next}*")?;
            next = ",";
        }
        for p in &self.pos {
            write!(f, "{next}{p}")?;
            next = ",";
        }
        write!(f, "]")
    }
}

/// Builds the [`TupleNode`] representing `h` inside my g-node scoped to `top` levels: levels
/// above `h.level` keep my own position, `h.level` itself is overridden by `h.pos`, and levels
/// below `h.level` are set to `0` (upstream: "not important, just have to be in range" —
/// `make_tuple_node`, `research/impl/vala/peerservices/utils.vala:31-50`).
///
/// # Panics
/// If `top <= h.level` or `top` exceeds `my_pos`'s length.
#[must_use]
pub fn make_tuple_node(topology: &Topology, my_pos: &[u32], h: HCoord, top: usize) -> TupleNode {
    assert!(top > h.level, "make_tuple_node: top must exceed h.level");
    let mut out = vec![0u32; top];
    for (level, slot) in out.iter_mut().enumerate().skip(h.level + 1) {
        *slot = my_pos[level];
    }
    out[h.level] = h.pos;
    TupleNode::new(topology.clone(), out).expect("levels above h.level come from a valid address")
}

/// Builds the [`TupleGNode`] representing `h` inside my g-node scoped to `top` levels
/// (`make_tuple_gnode`, `research/impl/vala/peerservices/utils.vala:52-72`).
///
/// # Panics
/// If `top <= h.level` or `top` exceeds `my_pos`'s length.
#[must_use]
pub fn make_tuple_gnode(topology: &Topology, my_pos: &[u32], h: HCoord, top: usize) -> TupleGNode {
    assert!(top > h.level, "make_tuple_gnode: top must exceed h.level");
    let mut out = vec![0u32; top - h.level];
    out[0] = h.pos;
    for (i, slot) in out.iter_mut().enumerate().skip(1) {
        *slot = my_pos[h.level + i];
    }
    TupleGNode::new(topology.clone(), top, out)
        .expect("levels above h.level come from a valid address")
}

/// How a g-node tuple relates to my own address, per `convert_tuple_gnode`
/// (`research/impl/vala/peerservices/utils.vala:74-115`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GNodeRelation {
    /// `t` names one of my own ancestor g-nodes.
    Mine,
    /// `t` names a g-node visible in my topology that I do not belong to.
    Visible,
    /// `t` names a g-node not directly visible in my topology (it diverges from me above its
    /// own level, so only its containing g-node is visible to me).
    Hidden,
}

/// Given `t`, a g-node living inside one of my g-nodes, determines how it relates to my own
/// address and the coordinate of the g-node in *my* map that contains it
/// (`convert_tuple_gnode`, `research/impl/vala/peerservices/utils.vala:74-115`).
#[must_use]
pub fn convert_tuple_gnode(my_pos: &[u32], t: &TupleGNode) -> (GNodeRelation, HCoord) {
    let level = t.level();
    let tuple = t.positions();
    for offset in (0..tuple.len()).rev() {
        let lvl = level + offset;
        if my_pos[lvl] != tuple[offset] {
            let ret = HCoord::new(lvl, tuple[offset]);
            let relation = if offset == 0 {
                GNodeRelation::Visible
            } else {
                GNodeRelation::Hidden
            };
            return (relation, ret);
        }
    }
    (GNodeRelation::Mine, HCoord::new(level, tuple[0]))
}

/// Reinterprets a node tuple as a (level-0) g-node tuple with the same `top`
/// (`tuple_node_to_tuple_gnode`, `utils.vala:117-124`).
#[must_use]
pub fn tuple_node_to_tuple_gnode(t: &TupleNode) -> TupleGNode {
    TupleGNode::new(t.topology().clone(), t.top(), t.positions().to_vec())
        .expect("a valid TupleNode is a valid level-0 TupleGNode")
}

/// Extends `t` (inside my g-node at level `t.top()`) to span up to `new_top`, filling the newly
/// included higher levels with my own position (`rebase_tuple_gnode`, `utils.vala:126-148`).
///
/// # Panics
/// If `new_top < t.top()`.
#[must_use]
pub fn rebase_tuple_gnode(my_pos: &[u32], t: &TupleGNode, new_top: usize) -> TupleGNode {
    assert!(
        t.top() <= new_top,
        "rebase_tuple_gnode: new_top must not shrink the scope"
    );
    let mut out = t.positions().to_vec();
    out.extend_from_slice(&my_pos[t.top()..new_top]);
    TupleGNode::new(t.topology().clone(), new_top, out).expect("extension uses valid positions")
}

/// Extends `t` (a node tuple inside my g-node at `t.top()`) to span up to `new_top`
/// (`rebase_tuple_node`, `utils.vala:161-183`).
///
/// # Panics
/// If `new_top < t.top()`.
#[must_use]
pub fn rebase_tuple_node(my_pos: &[u32], t: &TupleNode, new_top: usize) -> TupleNode {
    assert!(
        t.top() <= new_top,
        "rebase_tuple_node: new_top must not shrink the scope"
    );
    let mut out = t.positions().to_vec();
    out.extend_from_slice(&my_pos[t.top()..new_top]);
    TupleNode::new(t.topology().clone(), out).expect("extension uses valid positions")
}

/// The g-node of level `new_level` (an ancestor of `t`, same `top`) that contains `t`
/// (`tuple_gnode_containing`, `utils.vala:150-159`).
///
/// # Errors
/// [`Error::EmptyGNodeTuple`] if `new_level >= t.top()`: a g-node tuple always names at least
/// one level (`check_valid`, `serializables.vala:181`), and there is no ancestor *at* or beyond
/// the scope's own boundary — `top` is the exclusive upper bound of the levels this tuple can
/// name, not itself a level. This is not the identity case (that is `new_level == t.level()`,
/// handled below without removing anything): upstream's own downstream consumer,
/// `convert_tuple_gnode`, asserts its input tuple is non-empty (`utils.vala:93`,
/// `assert(i > 0)`), so an empty-tuple result would only move this function's crash into the
/// next call. Upstream's sole real caller also never supplies a level this coarse
/// (`message_routing.vala:505`'s `waiting_answer.e_lvl` always names a genuine ancestor level
/// strictly below `top`, e.g. `guest_gnode_level`/`common_lvl` in `databases.vala:576,595,635,
/// 670,691,730,757,911,931`) — reaching `top` here means the level came from elsewhere (in this
/// port, an untrusted wire field, `handler.rs`'s `PeersSetRefuseMessage` decode), so it is
/// reported as an error rather than silently degenerating.
///
/// # Panics
/// If `new_level < t.level()`.
pub fn tuple_gnode_containing(t: &TupleGNode, new_level: usize) -> Result<TupleGNode, Error> {
    assert!(
        new_level >= t.level(),
        "tuple_gnode_containing: new_level must not be deeper than t"
    );
    let pos = t.positions();
    // Clamp rather than index-panic: any `new_level >= t.top()` (exactly at the boundary, or an
    // untrusted value beyond it) removes every position, which `TupleGNode::new` below rejects.
    let remove = (new_level - t.level()).min(pos.len());
    TupleGNode::new(t.topology().clone(), t.top(), pos[remove..].to_vec())
}

/// True if some node inside my g-node of level `lvl` can see `t` well enough to know it exists
/// (`visible_by_someone_inside_my_gnode`, `utils.vala:185-219`).
#[must_use]
pub fn visible_by_someone_inside_my_gnode(my_pos: &[u32], t: &TupleGNode, lvl: usize) -> bool {
    let level = t.level();
    let l = if lvl == 0 || level >= lvl - 1 {
        level + 1
    } else {
        lvl
    };
    if t.top() <= l {
        return true;
    }
    let remove = l - level;
    let h = TupleGNode::new(
        t.topology().clone(),
        t.top(),
        t.positions()[remove..].to_vec(),
    )
    .expect("a suffix of a valid tuple is valid");
    matches!(convert_tuple_gnode(my_pos, &h).0, GNodeRelation::Mine)
}

/// True if `container` (same `top` as `contained`) is an ancestor g-node of `contained`
/// (`contains`, `utils.vala:221-237`).
///
/// # Panics
/// If `container.top() != contained.top()`.
#[must_use]
pub fn contains(container: &TupleGNode, contained: &TupleGNode) -> bool {
    assert_eq!(
        container.top(),
        contained.top(),
        "contains: tuples must share a top"
    );
    let c_pos = container.positions();
    let d_pos = contained.positions();
    c_pos.len() <= d_pos.len() && c_pos == &d_pos[d_pos.len() - c_pos.len()..]
}

/// Circular mixed-radix distance from `x_macron` to `x`: per level `j`, the digit is
/// `(x[j] - x̄[j]) mod gsize(j)`, computed independently (no borrow across levels); the digits
/// combine via ordinary positional (Horner) encoding with level `top-1` most significant
/// (`dist`, `research/impl/vala/peerservices/message_routing.vala:155-167`).
///
/// This is **not a modular distance over one combined ring**: because each level's remainder is
/// computed independently rather than via one multi-digit subtraction with borrowing, `dist` is
/// really a lexicographic combination of per-level one-directional circular distances — the
/// outermost (most significant) level's divergence dominates the result, exactly the Chord-like
/// "route toward the coarsest matching region first" behavior `research/notes/02-vala-services-
/// daemon.md` §3 describes.
///
/// **Metric properties** (verified against the definition above, not assumed):
/// - **Identity holds**: `dist(x, x) == 0` for any `x` (every per-level remainder is `0`).
/// - **Symmetry does NOT hold**: `dist(x̄, x) != dist(x, x̄)` in general — it is a one-directional
///   ("clockwise") distance, confirmed by upstream's own test
///   (`research/impl/vala/peerservices/testsuites/message_routing/test_message_routing.vala:166-174`,
///   `assert(m.dist(y,x) != m.dist(y,z))`).
/// - **Triangle inequality holds**: `dist(a, c) <= dist(a, b) + dist(b, c)`. Proof: for any single
///   level `j`, one-directional modular distance satisfies `d_j(a,c) <= d_j(a,b) + d_j(b,c)`
///   (both sides are congruent mod `gsize(j)`, the left side is reduced into `[0, gsize(j))`, so
///   it cannot exceed a sum of two such non-negative reduced values). `dist` is a non-negative
///   weighted sum of these independent per-level terms, and a non-negative weighted sum of
///   subadditive terms is itself subadditive.
///
/// # Panics
/// If `x_macron.top() != x.top()`.
#[must_use]
pub fn dist(topology: &Topology, x_macron: &TupleNode, x: &TupleNode) -> u128 {
    assert_eq!(
        x_macron.top(),
        x.top(),
        "dist: tuples must span the same levels"
    );
    let mut distance: u128 = 0;
    for level in (0..x.top()).rev() {
        let gsize = u128::from(
            topology
                .gsize(level)
                .expect("level < x.top() <= topology.levels()"),
        );
        let xj = u128::from(x.positions()[level]);
        let xbarj = u128::from(x_macron.positions()[level]);
        let d = if xj >= xbarj {
            xj - xbarj
        } else {
            xj + gsize - xbarj
        };
        distance = distance * gsize + d;
    }
    distance
}

/// `x = H(x̄)`: the key→g-node mapping (RFC 0014 §2, Definition 2.3) restricted to the levels
/// visible in my own topology. Scans every g-node known to exist (via `gnode_exists`) at each
/// level below `x_macron`'s scope, plus myself, and returns whichever has minimum [`dist`] from
/// `x_macron`, excluding anything in `exclude_list` (`approximate`,
/// `research/impl/vala/peerservices/message_routing.vala:169-227`). Returns `None` if `x_macron`
/// is absent/empty (meaning "route to myself") and I am excluded, or if nothing visible remains
/// after exclusions (RFC 0014 §2.2 note 1: "There are no participants").
#[must_use]
pub fn approximate(
    topology: &Topology,
    my_pos: &[u32],
    x_macron: Option<&TupleNode>,
    exclude_list: &[HCoord],
    mut gnode_exists: impl FnMut(HCoord) -> bool,
) -> Option<HCoord> {
    let valid_levels = match x_macron {
        None => 0,
        Some(x) => x.top(),
    };
    if valid_levels == 0 {
        let me = HCoord::new(0, my_pos[0]);
        return (!exclude_list.contains(&me)).then_some(me);
    }
    let x_macron = x_macron.expect("valid_levels > 0 implies x_macron is Some");
    debug_assert!(valid_levels <= topology.levels());

    let mut best: Option<(HCoord, u128)> = None;
    for level in 0..valid_levels {
        let gsize = topology
            .gsize(level)
            .expect("level < valid_levels <= topology.levels()");
        for p in 0..gsize {
            if p == my_pos[level] {
                continue;
            }
            let h = HCoord::new(level, p);
            if exclude_list.contains(&h) || !gnode_exists(h) {
                continue;
            }
            let tuple_x = make_tuple_node(topology, my_pos, h, valid_levels);
            let d = dist(topology, x_macron, &tuple_x);
            if best.is_none_or(|(_, bd)| d < bd) {
                best = Some((h, d));
            }
        }
    }
    let me = HCoord::new(0, my_pos[0]);
    if !exclude_list.contains(&me) {
        let tuple_x = make_tuple_node(topology, my_pos, me, valid_levels);
        let d = dist(topology, x_macron, &tuple_x);
        if best.is_none_or(|(_, bd)| d < bd) {
            best = Some((me, d));
        }
    }
    best.map(|(h, _)| h)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology(gsizes: &[u32]) -> Topology {
        Topology::new(gsizes.iter().copied()).unwrap()
    }

    fn node(topology: &Topology, pos: &[u32]) -> TupleNode {
        TupleNode::new(topology.clone(), pos.to_vec()).unwrap()
    }

    /// Upstream `test_dist` (`research/impl/vala/peerservices/testsuites/message_routing/
    /// test_message_routing.vala:133-175`): same magnitudes at level 0 vs level 1 are never
    /// equal, and level-1 deltas always dominate level-0 deltas.
    #[test]
    fn dist_matches_upstream_reference_values() {
        let t = topology(&[5, 5, 5]);
        let y = node(&t, &[1, 1, 0]);
        let x = node(&t, &[2, 1, 0]);
        let z = node(&t, &[0, 1, 0]);
        let ga = node(&t, &[1, 2, 0]);

        assert!(dist(&t, &y, &x) < dist(&t, &y, &ga));
        assert_ne!(dist(&t, &y, &x), dist(&t, &y, &z));
    }

    #[test]
    fn dist_identity_is_zero() {
        let t = topology(&[4, 3, 2]);
        let x = node(&t, &[2, 1, 1]);
        assert_eq!(dist(&t, &x, &x), 0);
    }

    #[test]
    fn dist_is_not_symmetric() {
        let t = topology(&[5, 5, 5]);
        let a = node(&t, &[1, 1, 0]);
        let b = node(&t, &[2, 1, 0]);
        assert_ne!(dist(&t, &a, &b), dist(&t, &b, &a));
    }

    /// Upstream `test_approximate` (`test_message_routing.vala:102-130`): target `4:2:2` with
    /// candidate `0:2:1` resolves to `HCoord(1, 2)`.
    #[test]
    fn approximate_matches_upstream_reference_case() {
        let t = topology(&[5, 5, 5]);
        let my_pos = [3u32, 1, 0];
        let nodes_in_network: Vec<TupleNode> = [
            [3, 1, 0],
            [2, 1, 0],
            [1, 1, 0],
            [0, 1, 3],
            [1, 2, 0],
            [1, 3, 0],
        ]
        .into_iter()
        .map(|p: [u32; 3]| node(&t, &p))
        .collect();
        let gnode_exists = |h: HCoord| {
            let gnode = make_tuple_gnode(&t, &my_pos, h, t.levels());
            nodes_in_network.iter().any(|n| {
                let n_gnode = tuple_node_to_tuple_gnode(n);
                contains(&gnode, &n_gnode)
            })
        };
        let x_macron = node(&t, &[2, 2, 4]);
        let h = approximate(&t, &my_pos, Some(&x_macron), &[], gnode_exists).unwrap();
        assert_eq!(h, HCoord::new(1, 2));
    }

    /// Repro for the panic this crate hit for real during the severance-mesh two-level merge
    /// scenario: `ExecError::Refuse` can legitimately carry `level == top` (the coordinator
    /// service refuses with a level scoped to the whole request, `ntk-coordinator/src/
    /// service.rs`'s `SetHookingMemory` precondition), and `tuple_gnode_containing` used to
    /// build an ancestor of that level unconditionally, panicking on the empty g-node tuple.
    /// Before the fix this call panicked; after the fix it returns
    /// [`Error::EmptyGNodeTuple`].
    #[test]
    fn tuple_gnode_containing_at_top_is_an_error_not_a_panic() {
        let t = topology(&[4, 4]);
        let gn = TupleGNode::new(t.clone(), 2, vec![1u32, 2]).unwrap();
        let result = tuple_gnode_containing(&gn, gn.top());
        assert!(matches!(result, Err(Error::EmptyGNodeTuple)));
    }

    /// Same off-by-one, one step further: a `new_level` beyond `top` (reachable in practice
    /// since `PeersSetRefuseMessage`'s `e_lvl` is an untrusted wire `i32`, `handler.rs`) removes
    /// every position too, and must not slice-index-panic either.
    #[test]
    fn tuple_gnode_containing_beyond_top_is_also_an_error() {
        let t = topology(&[4, 4]);
        let gn = TupleGNode::new(t.clone(), 2, vec![1u32, 2]).unwrap();
        let result = tuple_gnode_containing(&gn, gn.top() + 5);
        assert!(matches!(result, Err(Error::EmptyGNodeTuple)));
    }

    #[test]
    fn tuple_gnode_containing_at_own_level_is_identity() {
        let t = topology(&[4, 4, 4]);
        let gn = TupleGNode::new(t.clone(), 3, vec![1u32, 2]).unwrap();
        let result = tuple_gnode_containing(&gn, gn.level()).unwrap();
        assert_eq!(result, gn);
    }

    #[test]
    fn tuple_gnode_containing_strictly_between_level_and_top_is_a_real_ancestor() {
        let t = topology(&[4, 4, 4]);
        let gn = TupleGNode::new(t.clone(), 3, vec![1u32, 2]).unwrap();
        let result = tuple_gnode_containing(&gn, 2).unwrap();
        assert_eq!(result, TupleGNode::new(t, 3, vec![2u32]).unwrap());
    }

    proptest::proptest! {
        #[test]
        fn dist_identity_holds_for_random_topologies(
            gsizes in proptest::collection::vec(1u32..8, 1..5),
        ) {
            let t = topology(&gsizes);
            let pos: Vec<u32> = gsizes.iter().map(|&g| g / 2).collect();
            let x = node(&t, &pos);
            proptest::prop_assert_eq!(dist(&t, &x, &x), 0);
        }

        #[test]
        fn dist_triangle_inequality_holds(
            gsizes in proptest::collection::vec(1u32..8, 1..5),
            seed_a in proptest::collection::vec(0u32..8, 1..5),
            seed_b in proptest::collection::vec(0u32..8, 1..5),
            seed_c in proptest::collection::vec(0u32..8, 1..5),
        ) {
            let levels = gsizes.len();
            let t = topology(&gsizes);
            let clamp = |seed: &[u32]| -> Vec<u32> {
                (0..levels).map(|i| seed[i % seed.len()] % gsizes[i]).collect()
            };
            let a = node(&t, &clamp(&seed_a));
            let b = node(&t, &clamp(&seed_b));
            let c = node(&t, &clamp(&seed_c));
            proptest::prop_assert!(dist(&t, &a, &c) <= dist(&t, &a, &b) + dist(&t, &b, &c));
        }

        #[test]
        fn tuple_gnode_containing_never_panics_across_the_top_boundary(
            gsizes in proptest::collection::vec(1u32..8, 1..5),
            top_seed in 0usize..8,
            size_seed in 0usize..8,
            pos_seed in proptest::collection::vec(0u32..8, 1..5),
            extra in 0usize..4,
        ) {
            let levels = gsizes.len();
            let top = 1 + top_seed % levels;
            let size = 1 + size_seed % top;
            let level = top - size;
            let t = topology(&gsizes);
            let pos: Vec<u32> = (0..size)
                .map(|i| pos_seed[i % pos_seed.len()] % gsizes[level + i])
                .collect();
            let gn = TupleGNode::new(t, top, pos).unwrap();
            // `extra` sweeps `new_level` from the identity case, through every real ancestor,
            // across `top` itself, and past it — the exact domain that used to panic at and
            // beyond `top`.
            let new_level = level + extra;
            let result = tuple_gnode_containing(&gn, new_level);
            if new_level < top {
                let g = result.expect("a level strictly below top is a real ancestor");
                proptest::prop_assert_eq!(g.level(), new_level);
                proptest::prop_assert_eq!(g.top(), top);
            } else {
                proptest::prop_assert!(matches!(result, Err(Error::EmptyGNodeTuple)));
            }
        }
    }
}

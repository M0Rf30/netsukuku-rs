//! Wire-shaped path/ETP domain types — the Rust analogue of
//! `research/impl/vala/qspn/serializables.vala:25-217` and
//! `research/impl/vala/qspn/destinations.vala`.

use ntk_common::{Cost, Fingerprint, HCoord, Naddr};

use crate::arc::ArcId;

/// One hop of a resolved path, as exposed to consumers (`IQspnHop`,
/// `research/impl/vala/qspn/api.vala:121-125`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hop {
    pub arc: ArcId,
    pub coord: HCoord,
}

/// One path to one destination as carried inside an ETP (`EtpPath`,
/// `research/impl/vala/qspn/serializables.vala:116-123`).
///
/// `fingerprint` is fixed to `Fingerprint<Vec<u8>>` — the wire instantiation
/// the shared domain codec (`ntk-proto`'s `domain` module) commits to. QSPN
/// never needs a different `Id` type since every fingerprint it handles
/// either originates locally (this node's own) or arrives already decoded
/// off the wire.
#[derive(Clone, Debug)]
pub struct EtpPath {
    /// G-node coordinates visited, ascending level, terminated by the
    /// destination itself (`qspn.vala:1094,1128`).
    pub hops: Vec<HCoord>,
    /// One local/foreign arc id per hop, same length as `hops`
    /// (`qspn.vala:1129`; see [`ArcId`] docs on why `arcs[1..]` are foreign).
    pub arcs: Vec<ArcId>,
    pub cost: Cost,
    pub fingerprint: Fingerprint<Vec<u8>>,
    pub nodes_inside: u32,
    /// Per-level "this path fact is not valid outside level i" pruning flags,
    /// length = topology levels (`qspn.vala:117-119`,
    /// `etp_message.vala:37-116`).
    pub ignore_outside: Vec<bool>,
}

impl PartialEq for EtpPath {
    fn eq(&self, other: &Self) -> bool {
        self.hops == other.hops
            && self.arcs == other.arcs
            && self.cost == other.cost
            && self.fingerprint.identity_eq(&other.fingerprint)
            && self.nodes_inside == other.nodes_inside
            && self.ignore_outside == other.ignore_outside
    }
}

/// The full ETP message (`research/impl/vala/qspn/serializables.vala:25-31`).
#[derive(Clone, Debug)]
pub struct EtpMessage {
    pub node_address: Naddr,
    /// Sender's own fingerprint per level, index 0 = level 0, length =
    /// `levels + 1`.
    pub fingerprints: Vec<Fingerprint<Vec<u8>>>,
    /// Sender's own `nodes_inside` per level, same indexing as
    /// `fingerprints`.
    pub nodes_inside: Vec<u32>,
    /// G-node coordinates the message has traversed so far, ascending level
    /// (`qspn.vala:1094`).
    pub hops: Vec<HCoord>,
    pub paths: Vec<EtpPath>,
}

impl PartialEq for EtpMessage {
    fn eq(&self, other: &Self) -> bool {
        self.node_address == other.node_address
            && self.fingerprints.len() == other.fingerprints.len()
            && self
                .fingerprints
                .iter()
                .zip(&other.fingerprints)
                .all(|(a, b)| a.identity_eq(b))
            && self.nodes_inside == other.nodes_inside
            && self.hops == other.hops
            && self.paths == other.paths
    }
}

/// A path bound to the local arc it was learned from — the unit the
/// destination map actually stores (`NodePath`,
/// `research/impl/vala/qspn/destinations.vala:24-60`).
#[derive(Clone, Debug)]
pub struct NodePath {
    pub arc: ArcId,
    pub path: EtpPath,
    /// Whether this path has ever been surfaced to consumers via
    /// `PathAdded`/`PathChanged` — upstream's `exposed` flag, the mechanism
    /// the elder-fingerprint gate uses to detect a losing-branch path's
    /// *first* transition to "no longer the elder"
    /// (`destinations.vala:30,34`; set/read at
    /// `qspn.vala:1657,1676,1694,1699,1706,1711`). A freshly constructed
    /// `NodePath` always starts `false` (`destinations.vala:26-31`).
    pub exposed: bool,
}

impl PartialEq for NodePath {
    fn eq(&self, other: &Self) -> bool {
        self.arc == other.arc && self.path == other.path && self.exposed == other.exposed
    }
}

impl NodePath {
    /// Builds a fresh, never-yet-exposed `NodePath` (`NodePath` ctor,
    /// `destinations.vala:26-31`).
    #[must_use]
    pub fn new(arc: ArcId, path: EtpPath) -> Self {
        Self {
            arc,
            path,
            exposed: false,
        }
    }

    /// Total cost: the owning arc's own advertised cost plus the path's own
    /// accumulated cost (`NodePath.cost` getter, `destinations.vala:36-41`).
    /// Upstream re-reads `arc.i_qspn_get_cost()` live on every access since
    /// `IQspnArc` is a stateful object the arc's owner can mutate in place;
    /// this crate's [`ArcId`] carries no cost of its own, so callers pass the
    /// current cost from the actor's arc table explicitly.
    #[must_use]
    pub fn total_cost(&self, arc_cost: Cost) -> Cost {
        arc_cost.saturating_add(self.path.cost)
    }

    /// `hops_arcs_equal`/`hops_arcs_equal_etppath`
    /// (`destinations.vala:42-59`): the identity upstream uses to key
    /// `ArrayList`s of `NodePath` and to match an existing path against a
    /// revised candidate.
    #[must_use]
    pub fn hops_arcs_equal(&self, other: &EtpPath) -> bool {
        self.path.hops == other.hops && self.path.arcs == other.arcs
    }
}

/// One path to a destination, as exposed to downstream consumers (route
/// snapshot, event stream). Mirrors `IQspnNodePath`
/// (`research/impl/vala/qspn/api.vala:127-134`) built by `get_ret_path`
/// (`research/impl/vala/qspn/destinations.vala:212-232`).
#[derive(Clone, Debug, PartialEq)]
pub struct RoutePath {
    pub arc: ArcId,
    pub hops: Vec<Hop>,
    pub cost: Cost,
    pub nodes_inside: u32,
}

/// Builds the consumer-facing [`RoutePath`] for `np`, given its arc's current
/// cost (`get_ret_path`, `destinations.vala:213-232`).
#[must_use]
pub fn to_route_path(np: &NodePath, arc_cost: Cost) -> RoutePath {
    RoutePath {
        arc: np.arc,
        hops: np
            .path
            .hops
            .iter()
            .zip(&np.path.arcs)
            .map(|(&coord, &arc)| Hop { arc, coord })
            .collect(),
        cost: np.total_cost(arc_cost),
        nodes_inside: np.path.nodes_inside,
    }
}

/// `prepare_path_for_sending` (`research/impl/vala/qspn/etp_message.vala:25-36`):
/// the outgoing wire representation of an admitted path, whose `cost` is the
/// *total* accumulated cost (arc + path) so the next hop continues to
/// accumulate correctly. `ignore_outside` is left empty here; the caller
/// fills it in via the pruning pass (`set_ignore_outside_for_sending`,
/// out of scope for this pure helper — see the actor's `finalize_paths`).
#[must_use]
pub fn prepare_for_sending(np: &NodePath, arc_cost: Cost) -> EtpPath {
    EtpPath {
        hops: np.path.hops.clone(),
        arcs: np.path.arcs.clone(),
        cost: np.total_cost(arc_cost),
        fingerprint: np.path.fingerprint.clone(),
        nodes_inside: np.path.nodes_inside,
        ignore_outside: Vec::new(),
    }
}

/// All currently-admitted paths to one destination (`Destination`,
/// `research/impl/vala/qspn/destinations.vala:62-170`). Never stored empty —
/// an empty `Destination` is instead removed from the map
/// (`qspn.vala:1728-1732`).
#[derive(Clone, Debug)]
pub struct Destination {
    pub coord: HCoord,
    pub paths: Vec<NodePath>,
}

impl PartialEq for Destination {
    fn eq(&self, other: &Self) -> bool {
        self.coord == other.coord && self.paths == other.paths
    }
}

/// The elder-seed-winning fingerprint among `paths`
/// (`IQspnFingerprint::i_qspn_elder_seed`-based selection, used identically
/// by `Destination.evaluate`'s `fpd` and the standalone `find_fingerprint`
/// helper, `destinations.vala:77-125` / `qspn.vala:1236-1260`). The two
/// upstream call sites compute the exact same value: selecting the maximum
/// element of a totally-ordered set by iterated pairwise comparison is
/// order-independent, so `evaluate`'s incremental fold and
/// `find_fingerprint`'s independent fold always agree. `Ok(None)` iff `paths`
/// is empty.
///
/// Two real members of the *same* g-node legitimately reach this with
/// differently-identified but numerically tied fingerprints: `construct`'s
/// champion race starts from each member's own fingerprint as the initial
/// "current" and only ever lets a *candidate* sibling depose it
/// (`Fingerprint::construct`'s docs) — for exactly two members with equal
/// eldership claims, each necessarily names the *other* champion, and upstream
/// (`elder_seed`'s `assert_not_reached()`, `serializables.vala:260`) simply
/// assumes real elderships never tie. This port instead treats that specific
/// [`ntk_common::Error::IndistinguishableFingerprints`] outcome the way any
/// other tied/unordered comparison would be treated: no established order, so
/// the fold keeps its current winner rather than erroring — deterministic on
/// `paths`' own order, and no worse than upstream's assumption that this case
/// can't happen. This is the *only* [`ntk_common::Error`] variant swallowed
/// here; anything else (e.g. mismatched levels) still indicates a genuine
/// bug in validated ETP input and is propagated.
///
/// # Errors
/// Propagates [`ntk_common::Error`] from [`Fingerprint::elder_seed`] other
/// than [`ntk_common::Error::IndistinguishableFingerprints`] (e.g. mismatched
/// fingerprint levels — never expected for validated ETP input, see
/// [`crate::validate`]).
pub fn winning_fingerprint(
    paths: &[NodePath],
) -> Result<Option<Fingerprint<Vec<u8>>>, ntk_common::Error> {
    let mut winner: Option<Fingerprint<Vec<u8>>> = None;
    for p in paths {
        let fp = &p.path.fingerprint;
        winner = Some(match winner {
            None => fp.clone(),
            Some(w) => {
                if fp.identity_eq(&w) {
                    w
                } else {
                    match fp.elder_seed(&w) {
                        Ok(true) => fp.clone(),
                        Ok(false) | Err(ntk_common::Error::IndistinguishableFingerprints) => w,
                        Err(e) => return Err(e),
                    }
                }
            }
        });
    }
    Ok(winner)
}

impl Destination {
    /// Winning fingerprint, its `nodes_inside`, and the index of the
    /// cheapest path carrying it (`Destination.evaluate`/`best_path`/
    /// `nodes_inside`/`fingerprint`, `destinations.vala:77-146`). At level 0
    /// "best" is simply lowest total cost; above level 0, the elder-seed
    /// winning fingerprint is selected first (`winning_fingerprint`) and
    /// its cheapest path breaks ties — equivalent to upstream's single-pass
    /// fold (see `winning_fingerprint` docs for why the split is safe).
    ///
    /// # Panics
    /// If `self.paths` is empty — a [`Destination`] is never stored that way
    /// (`qspn.vala:1728-1736`).
    ///
    /// # Errors
    /// Propagates [`ntk_common::Error`] from [`Fingerprint::elder_seed`].
    pub fn evaluate(
        &self,
        arc_cost: impl Fn(ArcId) -> Cost,
    ) -> Result<(Fingerprint<Vec<u8>>, u32, usize), ntk_common::Error> {
        assert!(
            !self.paths.is_empty(),
            "Destination is never stored empty (qspn.vala:1728-1736)"
        );
        if self.coord.level == 0 {
            let (idx, best) = self
                .paths
                .iter()
                .enumerate()
                .min_by_key(|(_, p)| p.total_cost(arc_cost(p.arc)))
                .expect("non-empty");
            return Ok((best.path.fingerprint.clone(), best.path.nodes_inside, idx));
        }
        let winner = winning_fingerprint(&self.paths)?.expect("non-empty");
        let (idx, best) = self
            .paths
            .iter()
            .enumerate()
            .filter(|(_, p)| p.path.fingerprint.identity_eq(&winner))
            .min_by_key(|(_, p)| p.total_cost(arc_cost(p.arc)))
            .expect("winner fingerprint came from one of these paths");
        Ok((winner, best.path.nodes_inside, idx))
    }
}

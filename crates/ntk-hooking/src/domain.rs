//! In-memory payload/helper types — `research/impl/vala/hooking/serializables.vala`
//! and the free helper functions from `research/impl/vala/hooking/structs.vala:76-165`.
//! Wire (de)serialization lives in [`crate::wire`].

use ntk_common::HCoord;

use crate::view::QspnView;

/// Relative position/eldership tuple naming one g-node — `TupleGNode`
/// (`serializables.vala:559-612`). `pos[0]`/`eldership[0]` name the
/// innermost level the tuple covers; the last entry names the outermost
/// (root) level. `eldership` carries upstream's `-1` "not yet known"
/// sentinel (`arc_handler.vala:446-448`), hence the signed element type.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct TupleGNode {
    pub pos: Vec<u32>,
    pub eldership: Vec<i32>,
}

impl TupleGNode {
    pub fn new(pos: Vec<u32>, eldership: Vec<i32>) -> Self {
        Self { pos, eldership }
    }

    /// The hierarchy level this tuple starts at, given the topology's total
    /// `levels` — `level(TupleGNode)` (`structs.vala:120-124`).
    pub fn level(&self, levels: usize) -> usize {
        levels - self.pos.len()
    }

    /// Whether `self` fully contains `outside` as a descendant g-node —
    /// `tuple_contains` (`structs.vala:139-146`): every position `outside`
    /// names must match the corresponding (deeper) position `self` names.
    pub fn contains(&self, outside: &TupleGNode) -> bool {
        if self.pos.len() < outside.pos.len() {
            return false;
        }
        let d = self.pos.len() - outside.pos.len();
        outside
            .pos
            .iter()
            .enumerate()
            .all(|(i, &p)| p == self.pos[i + d])
    }

    /// Truncates this tuple so it starts at `target_level` instead of its
    /// current (necessarily shallower) level — `make_tuple_up_to_level`
    /// (`structs.vala:104-118`).
    ///
    /// **Deviation from upstream**: `structs.vala:108` asserts `levels >
    /// target_level` strictly, rejecting `target_level == levels` even
    /// though that case (truncating to a zero-length "the whole network"
    /// tuple) computes a perfectly well-defined empty result — this only
    /// matters for a degenerate single-level topology's BFS root
    /// (`find_shortest_mig`'s root visits exactly this level), which
    /// upstream's own reference deployments never exercise. This port
    /// allows `target_level == levels` rather than inheriting an assertion
    /// that would gratuitously panic on a legal (if unusual) topology.
    pub fn truncate_to_level(&self, target_level: usize, levels: usize) -> TupleGNode {
        assert!(
            levels >= target_level,
            "target_level must not exceed levels"
        );
        let posnum = levels - target_level;
        assert!(
            self.pos.len() >= posnum,
            "cannot truncate to a shallower level"
        );
        let todel = self.pos.len() - posnum;
        TupleGNode {
            pos: self.pos[todel..].to_vec(),
            eldership: self.eldership[todel..].to_vec(),
        }
    }
}

/// `positions_equal` (`structs.vala:131-137`): upstream's equality
/// comparator for the BFS visited-set `S`, position-only (eldership is
/// ignored — two tuples naming the same g-node may disagree on eldership
/// mid-search).
pub fn positions_equal(a: &TupleGNode, b: &TupleGNode) -> bool {
    a.pos == b.pos
}

/// `make_tuple_from_level` (`structs.vala:76-87`): my own tuple starting at
/// `l`.
///
/// Every current caller already keeps `l <= levels` (validated where the
/// value first enters this crate, e.g. [`crate::rpc::HookingRpcHandler`]'s
/// `search_migration_path` for a peer-supplied level), but this function
/// stays defensive in its own right rather than trusting that forever — see
/// this crate's `#[error("requested level exceeds the known topology")]`
/// history. Upstream's own `make_tuple_from_level` (`structs.vala:76-87`)
/// has no `l`-vs-`levels` check either, but its `for (i = l; i < levels;
/// i++)` loop simply no-ops when `l >= levels`, returning an empty tuple —
/// this port's capacity pre-sizing has no equivalent free no-op, so
/// `saturating_sub` (rather than a bare `levels - l`) is what restores that
/// same "out of range -> empty" behavior instead of underflowing.
pub fn make_tuple_from_level(l: usize, view: &dyn QspnView) -> TupleGNode {
    let levels = view.topology().levels();
    let span = levels.saturating_sub(l);
    let mut pos = Vec::with_capacity(span);
    let mut eldership = Vec::with_capacity(span);
    for i in l..levels {
        pos.push(view.my_pos(i));
        eldership.push(view.my_eldership(i));
    }
    TupleGNode { pos, eldership }
}

/// `make_tuple_from_hc` (`structs.vala:89-102`): the tuple naming the
/// g-node at `hc`, using my own positions above `hc.level`.
pub fn make_tuple_from_hc(hc: HCoord, view: &dyn QspnView) -> TupleGNode {
    let levels = view.topology().levels();
    let mut pos = vec![hc.pos];
    let mut eldership = vec![view.eldership(hc.level, hc.pos)];
    for i in (hc.level + 1)..levels {
        pos.push(view.my_pos(i));
        eldership.push(view.my_eldership(i));
    }
    TupleGNode { pos, eldership }
}

/// `i_am_inside` (`structs.vala:126-129`): whether my own current g-node
/// hierarchy contains `tuple`.
pub fn i_am_inside(tuple: &TupleGNode, view: &dyn QspnView) -> bool {
    make_tuple_from_level(0, view).contains(tuple)
}

/// `tuple_has_virtual_pos` (`hooking.vala:147-154`): whether any position in
/// `tuple` is virtual (`pos >= gsize` at that level) — i.e. `tuple` names a
/// connectivity (not-yet-fully-integrated) g-node.
pub fn tuple_has_virtual_pos(tuple: &TupleGNode, view: &dyn QspnView) -> bool {
    let levels = view.topology().levels();
    let d = levels - tuple.pos.len();
    (d..levels).any(|i| {
        let gsize = view.topology().gsize(i).unwrap_or(0);
        tuple.pos[i - d] >= gsize
    })
}

/// `tuple_to_hc` (`structs.vala:148-164`): the coordinate of the highest
/// level at which `a` diverges from my own current position.
///
/// # Panics
/// If `a` never diverges from my own position (a malformed/self-referential
/// tuple) — matches upstream's own `assert(i >= 0)`/`assert(j >= 0)`.
pub fn tuple_to_hc(a: &TupleGNode, view: &dyn QspnView) -> HCoord {
    let levels = view.topology().levels();
    let mut i = levels;
    let mut j = a.pos.len();
    loop {
        i -= 1;
        j -= 1;
        let my_pos = view.my_pos(i);
        let a_pos = a.pos[j];
        if my_pos != a_pos {
            return HCoord::new(i, a_pos);
        }
        assert!(
            i > 0 && j > 0,
            "tuple_to_hc: tuple never diverges from my own position"
        );
    }
}

/// One step of a `SearchMigrationPathRequest`/`ExploreGNodeRequest` routing
/// envelope — `PathHop` (`serializables.vala:614-671`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathHop {
    pub visiting_gnode: TupleGNode,
    pub previous_migrating_gnode: Option<TupleGNode>,
}

/// One adjacent g-node plus the real position of my own border g-node that
/// borders it — `PairTupleGNodeInt` (`serializables.vala:769-779`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PairTupleGNodeInt {
    pub gnode: TupleGNode,
    pub border_real_pos: u32,
}

/// `INetworkData` (`serializables.vala:23-100`): `retrieve_network_data`'s
/// return value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NetworkData {
    pub network_id: i64,
    pub neighbor_n_nodes: u64,
    pub neighbor_min_level: usize,
    pub gsizes: Vec<u32>,
    pub neighbor_pos: Vec<u32>,
}

/// `IEntryData` (`serializables.vala:451-509`): the resolved new address
/// chain, returned by `search_migration_path` and carried inside
/// [`FinishEnterData`]/[`FinishMigrationData`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryData {
    pub network_id: i64,
    pub pos: Vec<u32>,
    pub elderships: Vec<i32>,
}

/// `FinishEnterData` (`serializables.vala:521-533`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinishEnterData {
    pub enter_id: i32,
    pub entry_data: EntryData,
    pub go_connectivity_position: u32,
}

/// `FinishMigrationData` (`serializables.vala:545-557`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FinishMigrationData {
    pub migration_id: i32,
    pub migration_data: EntryData,
    pub go_connectivity_position: u32,
}

/// `EvaluateEnterData` (`serializables.vala:102-186`): the network-wide
/// evaluation request an arc handler sends once it decides to proceed with
/// a merge.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvaluateEnterRequest {
    pub network_id: i64,
    pub neighbor_pos: Vec<u32>,
    pub neighbor_min_lvl: usize,
    pub min_lvl: usize,
    pub evaluate_enter_id: i32,
}

/// `RequestPacketType` (`structs.vala:241-245`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigOp {
    PrepareMigration,
    FinishMigration,
}

/// `RequestPacket` (`serializables.vala:984-995`): a migration-execute
/// message routed toward `dest` (some `mig_gnode`). `pkt_id` correlates the
/// `ResponsePacket` ack when this hop is routed remotely; `src` names the
/// sender for the (unused by this crate's simplified single-hop routing,
/// see `crate::routing` module docs) reverse path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestPacket {
    pub pkt_id: i32,
    pub dest: TupleGNode,
    pub src: TupleGNode,
    pub operation: MigOp,
    pub migration_id: i32,
    /// Only meaningful for [`MigOp::FinishMigration`].
    pub conn_gnode_pos: i32,
    pub host_gnode: TupleGNode,
    pub real_new_pos: i32,
    pub real_new_eldership: i32,
}

/// `ResponsePacket` (`serializables.vala:997-1001`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResponsePacket {
    pub pkt_id: i32,
    pub dest: TupleGNode,
}

/// `SearchMigrationPathRequest` (`serializables.vala:673-761`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchMigrationPathRequest {
    pub pkt_id: i32,
    pub origin: TupleGNode,
    pub caller: TupleGNode,
    pub path_hops: Vec<PathHop>,
    pub max_host_lvl: usize,
    pub reserve_request_id: i32,
}

/// `SearchMigrationPathErrorPkt` (`serializables.vala:763-767`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchMigrationPathErrorPkt {
    pub pkt_id: i32,
    pub origin: TupleGNode,
}

/// `SearchMigrationPathResponse` (`serializables.vala:781-888`).
/// `final_host_lvl`/`real_new_pos`/`real_new_eldership`/`new_conn_vir_pos`/
/// `new_eldership` are `None` exactly where upstream uses its `-1` "not
/// set" sentinel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchMigrationPathResponse {
    pub pkt_id: i32,
    pub origin: TupleGNode,
    pub min_host_lvl: usize,
    pub set_adjacent: Vec<PairTupleGNodeInt>,
    pub final_host_lvl: usize,
    pub real_new_pos: Option<u32>,
    pub real_new_eldership: Option<i32>,
    pub new_conn_vir_pos: Option<u32>,
    pub new_eldership: Option<i32>,
}

/// `ExploreGNodeRequest` (`serializables.vala:890-969`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExploreGNodeRequest {
    pub pkt_id: i32,
    pub origin: TupleGNode,
    pub path_hops: Vec<PathHop>,
    pub requested_lvl: usize,
}

/// `ExploreGNodeResponse` (`serializables.vala:971-976`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExploreGNodeResponse {
    pub pkt_id: i32,
    pub origin: TupleGNode,
    pub result: TupleGNode,
}

/// `DeleteReservationRequest` (`serializables.vala:978-982`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteReservationRequest {
    pub dest_gnode: TupleGNode,
    pub reserve_request_id: i32,
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedView {
        levels: ntk_common::Topology,
        my_pos: Vec<u32>,
    }
    impl QspnView for FixedView {
        fn topology(&self) -> &ntk_common::Topology {
            &self.levels
        }
        fn network_id(&self) -> i64 {
            0
        }
        fn n_nodes(&self) -> u64 {
            1
        }
        fn my_pos(&self, level: usize) -> u32 {
            self.my_pos[level]
        }
        fn my_eldership(&self, _level: usize) -> i32 {
            0
        }
        fn subnetlevel(&self) -> usize {
            0
        }
        fn epsilon(&self, _level: usize) -> usize {
            0
        }
        fn eldership(&self, _level: usize, _pos: u32) -> i32 {
            0
        }
        fn adjacent_to_my_gnode(
            &self,
            _level_adjacent_gnodes: usize,
            _level_my_gnode: usize,
        ) -> Vec<crate::view::AdjacentGNode> {
            Vec::new()
        }
        fn is_bootstrapped(&self) -> bool {
            true
        }
    }

    fn view() -> FixedView {
        FixedView {
            levels: ntk_common::Topology::new([4, 4, 4]).unwrap(),
            my_pos: vec![1, 2, 3],
        }
    }

    #[test]
    fn make_tuple_from_level_zero_is_my_full_position() {
        let v = view();
        let t = make_tuple_from_level(0, &v);
        assert_eq!(t.pos, vec![1, 2, 3]);
    }

    #[test]
    fn truncate_to_level_drops_inner_levels() {
        let v = view();
        let t = make_tuple_from_level(0, &v);
        let truncated = t.truncate_to_level(1, 3);
        assert_eq!(truncated.pos, vec![2, 3]);
    }

    #[test]
    fn contains_checks_deepest_matching_suffix() {
        let outside = TupleGNode::new(vec![3], vec![0]);
        let inside = TupleGNode::new(vec![2, 3], vec![0, 0]);
        assert!(inside.contains(&outside));
        assert!(!outside.contains(&inside));
    }

    #[test]
    fn i_am_inside_matches_my_own_full_tuple() {
        let v = view();
        let mine = make_tuple_from_level(1, &v);
        assert!(i_am_inside(&mine, &v));
        let other = TupleGNode::new(vec![9], vec![0]);
        assert!(!i_am_inside(&other, &v));
    }

    #[test]
    fn tuple_has_virtual_pos_detects_out_of_range() {
        let v = view();
        let real = make_tuple_from_level(0, &v);
        assert!(!tuple_has_virtual_pos(&real, &v));
        let virt = TupleGNode::new(vec![1, 2, 9], vec![0, 0, 0]);
        assert!(tuple_has_virtual_pos(&virt, &v));
    }
}

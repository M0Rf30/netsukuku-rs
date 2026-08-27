//! Neighborhood-level arc handles and the per-identity view of them.

use crate::identity::IdentityId;

/// Opaque handle for a neighborhood-level arc, minted and owned by the
/// daemon. Stands in for upstream's `IIdmgmtArc` object identity
/// (`identities/identities.vala:44-49,124,136-145`) so this crate never
/// depends on `ntk-neighborhood`'s concrete arc type — only on this id plus
/// the [`ArcInfo`] the caller supplies at [`crate::Handle::add_arc`] time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ArcId(pub u64);

/// The subset of `IIdmgmtArc`'s properties (`identities.vala:46-48`) this
/// crate needs about a neighborhood arc.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArcInfo {
    /// Real network device this arc rides on — the key used to look up
    /// per-device migration data (`arc.get_dev()`, `identities.vala:499`).
    pub dev: String,
    pub peer_mac: String,
    pub peer_linklocal: String,
}

/// One local identity's view of a peer identity reachable across an arc —
/// upstream's `IdentityArc`/`IIdmgmtIdentityArc`
/// (`identities.vala:51-56,954-983`). Several of these may coexist for the
/// same (identity, arc) pair as the peer migrates
/// (`identity_arcs: HashMap<"nodeid-arcid", ArrayList<IdentityArc>>`,
/// `identities.vala:129,182-215`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityArc {
    pub peer_id: IdentityId,
    pub peer_mac: String,
    pub peer_linklocal: String,
}

/// What changed about one identity-arc — folds upstream's four arc-scoped
/// signals (`identity_arc_added`/`_changed`/`_removing`/`_removed`,
/// `identities.vala:771-774`) into one enum; carries the resulting
/// mac/linklocal so a subscriber can drive `ntk-netlink`'s gateway route
/// (`netns_manager.add_gateway`, `identities.vala:560,637,902`) without a
/// follow-up query — the kernel call itself stays out of this crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityArcChange {
    /// `identity_arc_added` (`:771`).
    Added {
        peer_id: IdentityId,
        peer_mac: String,
        peer_linklocal: String,
    },
    /// `identity_arc_changed` (`:772`).
    Changed {
        peer_id: IdentityId,
        peer_mac: String,
        peer_linklocal: String,
        only_neighbour_migrated: bool,
    },
    /// `identity_arc_removing` (`:773`) — about to be removed.
    Removing { peer_id: IdentityId },
    /// `identity_arc_removed` (`:774`).
    Removed { peer_id: IdentityId },
}

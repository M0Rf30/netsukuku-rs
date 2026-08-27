//! Consumer-facing events — the Rust analogue of `QspnManager`'s GObject
//! signals (`research/impl/vala/qspn/qspn.vala:122-147`), published as a
//! `tokio::sync::broadcast` stream rather than callbacks/signals.

use ntk_common::{Fingerprint, HCoord};

use crate::arc::ArcId;
use crate::path::RoutePath;

/// One QSPN protocol event. Each variant documents the exact upstream signal
/// it replaces.
#[derive(Clone, Debug)]
pub enum QspnEvent {
    /// `qspn_bootstrap_complete` (`qspn.vala:122`): this identity has
    /// completed its hook on the network (for the `create_net`-only manager
    /// this crate implements, that is immediately after construction, once
    /// `bootstrap_signal_delay` elapses).
    BootstrapComplete,
    /// `presence_notified` (`qspn.vala:124`): the first full ETP this
    /// identity published should have reached its neighbors.
    PresenceNotified,
    /// `arc_removed` (`qspn.vala:127`): an arc left this node's arc set,
    /// either by explicit removal or a failed/rejected call.
    ArcRemoved { arc: ArcId, bad_link: bool },
    /// `destination_added` (`qspn.vala:130`): first path to a destination.
    DestinationAdded(HCoord),
    /// `destination_removed` (`qspn.vala:133`): last path to a destination
    /// was withdrawn.
    DestinationRemoved(HCoord),
    /// `path_added` (`qspn.vala:135`).
    PathAdded(RoutePath),
    /// `path_changed` (`qspn.vala:137`).
    PathChanged(RoutePath),
    /// `path_removed` (`qspn.vala:139`).
    PathRemoved(RoutePath),
    /// `changed_fp` (`qspn.vala:141`): this node's own g-node fingerprint at
    /// `level` changed.
    ChangedFingerprint(usize),
    /// `changed_nodes_inside` (`qspn.vala:143`): this node's own g-node
    /// `nodes_inside` estimate at `level` changed.
    ChangedNodesInside(usize),
    /// `gnode_splitted` (`qspn.vala:145`): the g-node reached via `arc` at
    /// `destination` has split, and the branch carrying `fingerprint` (not
    /// the eldest) must migrate.
    GnodeSplitted {
        arc: ArcId,
        destination: HCoord,
        fingerprint: Fingerprint<Vec<u8>>,
    },
}

impl PartialEq for QspnEvent {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::BootstrapComplete, Self::BootstrapComplete)
            | (Self::PresenceNotified, Self::PresenceNotified) => true,
            (
                Self::ArcRemoved {
                    arc: a1,
                    bad_link: b1,
                },
                Self::ArcRemoved {
                    arc: a2,
                    bad_link: b2,
                },
            ) => a1 == a2 && b1 == b2,
            (Self::DestinationAdded(a), Self::DestinationAdded(b))
            | (Self::DestinationRemoved(a), Self::DestinationRemoved(b)) => a == b,
            (Self::PathAdded(a), Self::PathAdded(b))
            | (Self::PathChanged(a), Self::PathChanged(b))
            | (Self::PathRemoved(a), Self::PathRemoved(b)) => a == b,
            (Self::ChangedFingerprint(a), Self::ChangedFingerprint(b))
            | (Self::ChangedNodesInside(a), Self::ChangedNodesInside(b)) => a == b,
            (
                Self::GnodeSplitted {
                    arc: a1,
                    destination: d1,
                    fingerprint: f1,
                },
                Self::GnodeSplitted {
                    arc: a2,
                    destination: d2,
                    fingerprint: f2,
                },
            ) => a1 == a2 && d1 == d2 && f1.identity_eq(f2),
            _ => false,
        }
    }
}

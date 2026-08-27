//! Consumer-facing events — the Rust analogue of `HookingManager`'s GObject
//! signals (`research/impl/vala/hooking/hooking.vala:112-122`), published as
//! a `tokio::sync::broadcast` stream rather than callbacks/signals.
//!
//! `do_prepare_enter`/`do_finish_enter`/`do_prepare_migration`/
//! `do_finish_migration` are how upstream's `HookingManager` tells the
//! composition root (in our port: `ntkd`, phase 4) to actually apply a
//! resolved entry/migration against `ntk-identities`/`ntk-qspn` — Hooking
//! itself never touches those crates (dependency-inversion contract). They
//! fire when [`crate::manager::HookingHandle::notify_prepare_enter`] and
//! its three siblings are called, which `ntkd` does upon receiving the
//! corresponding propagation from the real Coordinator module.

use crate::arc::ArcId;
use crate::domain::{FinishEnterData, FinishMigrationData};

/// One Hooking protocol event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HookingEvent {
    /// `failing_arc` (`hooking.vala:112`): this arc's handler hit a
    /// transport/deserialize failure and terminated; the arc should be
    /// considered dead by whoever owns the physical link.
    FailingArc(ArcId),

    /// `same_network` (`hooking.vala:113`): the peer on this arc reported
    /// the same `network_id` as mine — no merge needed, the arc handler
    /// terminated cleanly.
    SameNetwork(ArcId),

    /// `another_network` (`hooking.vala:114`): the peer on this arc belongs
    /// to a different (topology-compatible) network; the merge-direction
    /// heuristic is about to run.
    AnotherNetwork { arc: ArcId, network_id: i64 },

    /// `do_prepare_enter` (`hooking.vala:115-116`): every member of the
    /// g-node at the propagated level must prepare for `enter_id` to be
    /// admitted.
    DoPrepareEnter { enter_id: i32 },
    /// `do_finish_enter` (`hooking.vala:117-119`): `data.entry_data` is the
    /// resolved admission; `guest_gnode_level` is the propagation level;
    /// `data.enter_id` matches a preceding `DoPrepareEnter`.
    DoFinishEnter {
        guest_gnode_level: usize,
        data: FinishEnterData,
    },

    /// `do_prepare_migration` (`hooking.vala:120`).
    DoPrepareMigration { migration_id: i32 },

    /// `do_finish_migration` (`hooking.vala:121-123`).
    DoFinishMigration {
        guest_gnode_level: usize,
        data: FinishMigrationData,
    },
}

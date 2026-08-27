//! Events published by the identity-manager actor.

use crate::arc::{ArcId, IdentityArcChange};
use crate::identity::IdentityId;
use crate::migration::MigrationId;

/// Broadcast events published by the identity-manager actor — the Rust
/// analogue of upstream's GObject signals (`identities.vala:771-775`),
/// delivered as a stream (`tokio::sync::broadcast`,
/// [`crate::Handle::subscribe`]) rather than callbacks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityEvent {
    /// A new identity entered the registry — either the initial main
    /// identity, or a fresh one minted by [`crate::Handle::migrate`]
    /// (`add_identity`'s `new_identity`, `identities.vala:455-456`).
    /// `migration_id` is `None` only for the initial main identity.
    IdentityAdded {
        id: IdentityId,
        migration_id: Option<MigrationId>,
    },
    /// A migration completed: `old_id` is now connectivity-only, `new_id`
    /// took over its main/internal role (`add_identity`'s overall effect,
    /// `identities.vala:441-577`).
    IdentityDuplicated {
        migration_id: MigrationId,
        old_id: IdentityId,
        new_id: IdentityId,
    },
    /// `remove_identity` completed (`identities.vala:685-730`); `id` is no
    /// longer present in the registry.
    IdentityDismissed { id: IdentityId },
    /// [`crate::Handle::abort_migration`] reverted an unsuccessful
    /// migration: `new_id` was dismissed and `old_id` regained its
    /// pre-migration status (and main-identity role, if it held one).
    /// Upstream has no equivalent at this layer (research/notes/01 §5's
    /// "Open questions") — this crate's own recovery path for a successor
    /// that never finished hooking.
    MigrationAborted {
        old_id: IdentityId,
        new_id: IdentityId,
    },
    /// One identity-arc changed; see [`IdentityArcChange`].
    IdentityArc {
        arc: ArcId,
        identity: IdentityId,
        change: IdentityArcChange,
    },
    /// A whole physical/neighborhood arc was torn down — `arc_removed`
    /// (`identities.vala:775`), arc-scoped rather than identity-scoped.
    ArcRemoved { arc: ArcId },
}

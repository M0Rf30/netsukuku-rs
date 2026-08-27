//! Types describing one duplication/migration handshake.

use crate::identity::IdentityId;

/// Caller-chosen correlation id for one migration
/// (`add_identity(migration_id, ...)`, `identities.vala:399,441`). Matches
/// the wire's `int32` (`IdentityMatchDuplicationArgs.migration_id`,
/// `ntk-proto/proto/ntk.proto`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MigrationId(pub i32);

/// Per-device data the caller supplies to [`crate::Handle::migrate`] once
/// it has actually created the old identity's pseudo-device — upstream's
/// `MigrationDeviceData` (`identities.vala:1011-1016`) minus the fields
/// derivable via [`crate::pseudo`]. The kernel operation itself
/// (`netns_manager.create_pseudodev`/`add_address`) is `ntk-netlink`'s job,
/// composed by the daemon, never this crate's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationDeviceInfo {
    pub old_id_new_mac: String,
    pub old_id_new_linklocal: String,
}

/// The peer's answer to `match_duplication` — upstream's
/// `DuplicationData`/`IDuplicationData` (`identities.vala:990-995`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuplicationData {
    pub peer_new_id: IdentityId,
    pub peer_old_id_new_mac: String,
    pub peer_old_id_new_linklocal: String,
}

//! Deterministic pseudo-address / pseudo-device naming.
//!
//! Pure functions only — no namespace, pseudo-device, address, or route is
//! ever created here. The daemon calls these to know what to name things
//! *before* calling `ntk-netlink` to actually create them, then reports the
//! resulting mac/linklocal back to [`crate::Handle::migrate`]. Kernel
//! operations are an explicit non-goal of this crate.

use crate::migration::MigrationId;

/// Deterministic namespace name for the temporary namespace that holds an
/// identity's connectivity fork during `migration_id`
/// (`identities.vala:460`: `ns_temp = "ntkv$(this_namespace)"`).
///
/// Upstream's `this_namespace` is a separate per-manager monotonic counter
/// distinct from `migration_id` (`identities.vala:440,459`). This port
/// reuses the caller-chosen [`MigrationId`] instead of introducing its own
/// counter, keeping the function pure and stateless — sound because one
/// `MigrationId` names exactly one `prepare_migration`/`migrate` pairing in
/// this port (see [`crate::Handle::migrate`]).
#[must_use]
pub fn migration_namespace(migration_id: MigrationId) -> String {
    format!("ntkv{}", migration_id.0)
}

/// Deterministic pseudo-device name for `real_dev` inside `namespace`
/// (`identities.vala:470`: `pseudo_dev = "$(ns_temp)_$(dev)"`).
#[must_use]
pub fn pseudo_device_name(namespace: &str, real_dev: &str) -> String {
    format!("{namespace}_{real_dev}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespace_name_matches_upstream_pattern() {
        assert_eq!(migration_namespace(MigrationId(7)), "ntkv7");
    }

    #[test]
    fn pseudo_device_name_matches_upstream_pattern() {
        let ns = migration_namespace(MigrationId(7));
        assert_eq!(pseudo_device_name(&ns, "eth0"), "ntkv7_eth0");
    }

    #[test]
    fn distinct_migrations_never_collide() {
        let a = migration_namespace(MigrationId(1));
        let b = migration_namespace(MigrationId(2));
        assert_ne!(a, b);
    }
}

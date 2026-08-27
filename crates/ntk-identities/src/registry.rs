//! The pure, synchronous half of upstream's `IdentityManager`
//! (`identities.vala:60-215,344-397`) — the identity map and its
//! invariants, with all async/RPC machinery in [`crate::actor`]. Kept
//! separate so registry invariants (no duplicate ids, a dismissed identity
//! becomes unreachable) are unit-testable without spinning up the actor.

use std::collections::HashMap;

use ntk_common::Naddr;

use crate::error::Error;
use crate::identity::{IdentityId, IdentityRecord, IdentityStatus};
use crate::snapshot::IdentitySnapshot;

/// The identity map plus which entry is `main_id`
/// (`identities.vala:88-90,100-102,125-126`).
#[derive(Debug, Clone)]
pub struct Registry {
    main_id: IdentityId,
    identities: HashMap<IdentityId, IdentityRecord>,
}

impl Registry {
    /// Seeds the registry with a single main identity — the effect of
    /// `IdentityManager`'s constructor before it processes any real NIC
    /// (`identities.vala:99-102`).
    #[must_use]
    pub fn new(main_id: IdentityId, naddr: Option<Naddr>) -> Self {
        let mut identities = HashMap::new();
        identities.insert(
            main_id,
            IdentityRecord {
                id: main_id,
                naddr,
                status: IdentityStatus::Main,
            },
        );
        Self {
            main_id,
            identities,
        }
    }

    #[must_use]
    pub fn main_id(&self) -> IdentityId {
        self.main_id
    }

    #[must_use]
    pub fn get(&self, id: IdentityId) -> Option<&IdentityRecord> {
        self.identities.get(&id)
    }

    /// All currently-reachable identity ids (`get_id_list`,
    /// `identities.vala:349-354`).
    pub fn ids(&self) -> impl Iterator<Item = IdentityId> + '_ {
        self.identities.keys().copied()
    }

    /// Generates a fresh id guaranteed not to collide with any id already
    /// in the registry.
    #[must_use]
    pub fn fresh_id(&self) -> IdentityId {
        loop {
            let candidate = IdentityId::generate();
            if !self.identities.contains_key(&candidate) {
                return candidate;
            }
        }
    }

    /// Adds a new identity record.
    ///
    /// # Errors
    /// [`Error::DuplicateIdentity`] if `record.id` is already present.
    pub fn insert(&mut self, record: IdentityRecord) -> Result<(), Error> {
        if self.identities.contains_key(&record.id) {
            return Err(Error::DuplicateIdentity(record.id));
        }
        self.identities.insert(record.id, record);
        Ok(())
    }

    /// # Errors
    /// [`Error::UnknownIdentity`] if `id` is not present.
    pub fn set_status(&mut self, id: IdentityId, status: IdentityStatus) -> Result<(), Error> {
        let record = self
            .identities
            .get_mut(&id)
            .ok_or(Error::UnknownIdentity(id))?;
        record.status = status;
        Ok(())
    }

    /// # Errors
    /// [`Error::UnknownIdentity`] if `id` is not present.
    pub fn set_naddr(&mut self, id: IdentityId, naddr: Option<Naddr>) -> Result<(), Error> {
        let record = self
            .identities
            .get_mut(&id)
            .ok_or(Error::UnknownIdentity(id))?;
        record.naddr = naddr;
        Ok(())
    }

    /// Hands the main-identity role to `new_main`, e.g. because the
    /// previous main identity just migrated (`if (main_id == old_identity)
    /// main_id = new_identity;`, `identities.vala:464`).
    pub fn reassign_main(&mut self, new_main: IdentityId) {
        self.main_id = new_main;
    }

    /// Removes a non-main identity, making it unreachable via
    /// [`Registry::get`]/[`Registry::ids`] from this point on
    /// (`remove_identity`, `identities.vala:685-730`).
    ///
    /// # Errors
    /// [`Error::CannotRemoveMainIdentity`] if `id == main_id`;
    /// [`Error::UnknownIdentity`] if `id` is not present.
    pub fn dismiss(&mut self, id: IdentityId) -> Result<IdentityRecord, Error> {
        if id == self.main_id {
            return Err(Error::CannotRemoveMainIdentity);
        }
        self.identities
            .remove(&id)
            .ok_or(Error::UnknownIdentity(id))
    }

    #[must_use]
    pub fn snapshot(&self) -> IdentitySnapshot {
        IdentitySnapshot {
            main_id: self.main_id,
            identities: self
                .identities
                .iter()
                .map(|(id, r)| (*id, r.clone()))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology() -> ntk_common::Topology {
        ntk_common::Topology::new([4, 4]).expect("valid topology")
    }

    #[test]
    fn new_seeds_exactly_the_main_identity() {
        let main = IdentityId::from_raw(1);
        let reg = Registry::new(main, None);
        assert_eq!(reg.main_id(), main);
        assert!(reg.get(main).is_some());
        assert_eq!(reg.ids().count(), 1);
    }

    #[test]
    fn insert_rejects_duplicate_ids() {
        let main = IdentityId::from_raw(1);
        let mut reg = Registry::new(main, None);
        let dup = IdentityRecord {
            id: main,
            naddr: None,
            status: IdentityStatus::Connectivity,
        };
        let err = reg.insert(dup).expect_err("duplicate id must be rejected");
        assert!(matches!(err, Error::DuplicateIdentity(id) if id == main));
    }

    #[test]
    fn dismissed_identity_is_unreachable() {
        let main = IdentityId::from_raw(1);
        let other = IdentityId::from_raw(2);
        let mut reg = Registry::new(main, None);
        reg.insert(IdentityRecord {
            id: other,
            naddr: None,
            status: IdentityStatus::Connectivity,
        })
        .unwrap();

        reg.dismiss(other).expect("non-main identity dismissable");

        assert!(reg.get(other).is_none());
        assert_eq!(reg.ids().count(), 1);
    }

    #[test]
    fn dismiss_rejects_main_identity() {
        let main = IdentityId::from_raw(1);
        let mut reg = Registry::new(main, None);
        let err = reg.dismiss(main).expect_err("main identity must survive");
        assert!(matches!(err, Error::CannotRemoveMainIdentity));
    }

    #[test]
    fn dismiss_rejects_unknown_identity() {
        let main = IdentityId::from_raw(1);
        let mut reg = Registry::new(main, None);
        let err = reg
            .dismiss(IdentityId::from_raw(99))
            .expect_err("unknown identity must be rejected");
        assert!(matches!(err, Error::UnknownIdentity(_)));
    }

    #[test]
    fn fresh_id_never_collides_with_existing_identities() {
        let main = IdentityId::from_raw(1);
        let mut reg = Registry::new(main, None);
        for _ in 0..1000 {
            let id = reg.fresh_id();
            reg.insert(IdentityRecord {
                id,
                naddr: None,
                status: IdentityStatus::Connectivity,
            })
            .expect("fresh_id must not collide");
        }
        assert_eq!(reg.ids().count(), 1001);
    }

    #[test]
    fn naddr_round_trips_through_the_registry() {
        let main = IdentityId::from_raw(1);
        let mut reg = Registry::new(main, None);
        let naddr = Naddr::new(topology(), [1, 2]).expect("valid address");
        reg.set_naddr(main, Some(naddr.clone())).unwrap();
        assert_eq!(reg.get(main).unwrap().naddr, Some(naddr));
    }
}

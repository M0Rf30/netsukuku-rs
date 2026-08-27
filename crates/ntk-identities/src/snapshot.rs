//! Read-only, point-in-time view of the identity registry.

use std::collections::BTreeMap;

use crate::identity::{IdentityId, IdentityRecord};

/// A consistent, point-in-time view of the identity registry, published via
/// `tokio::sync::watch` (`research/notes/06-rust-stack.md` §Concurrency) so
/// readers never take a lock over live protocol state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentitySnapshot {
    pub main_id: IdentityId,
    pub identities: BTreeMap<IdentityId, IdentityRecord>,
}

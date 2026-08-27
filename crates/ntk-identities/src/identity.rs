//! [`IdentityId`] and the role an identity plays ([`IdentityStatus`]).

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

use ntk_common::Naddr;

/// A per-identity id — the Rust analogue of upstream's `NodeID{id:int}`
/// (`research/impl/vala/ntkd-common/ntkd_common.vala:37-49`) and the
/// concrete implementer behind the wire's opaque `IIdentityID` marker
/// interface (`ntkdrpc/interfaces.vala:125-127`). Widened to `u64`
/// (upstream is a 31-bit positive `int`, `PRNGen.int_range(1, int.MAX)`)
/// since nothing here constrains it to fit a smaller wire type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IdentityId(u64);

impl IdentityId {
    /// Wraps a caller-chosen value: the deterministic path used by tests
    /// and by wire decoding, standing in for constructing a `NodeID`
    /// directly from a known `int` (`ntkd_common.vala:41-43`).
    #[must_use]
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// The raw numeric value, e.g. for wire encoding.
    #[must_use]
    pub const fn into_raw(self) -> u64 {
        self.0
    }

    /// Generates a fresh, likely-unique id (`Identity()` ctor,
    /// `identities/identities.vala:930-937`: `id = new
    /// NodeID(PRNGen.int_range(1, int.MAX))`).
    ///
    /// Upstream seeds one per-process PRNG, reseedable via
    /// `IdentityManager.init_rngen` for reproducible tests
    /// (`identities/rngen.vala`). The workspace has no PRNG crate in
    /// `[workspace.dependencies]` (`research/notes/06-rust-stack.md`), so
    /// rather than add one or hand-roll a substitute, this derives a value
    /// from the standard library's own randomized hasher seed
    /// (`std::collections::hash_map::RandomState`, itself sourced from OS
    /// entropy) applied to a monotonic counter. Deterministic reproduction
    /// for tests uses [`IdentityId::from_raw`] directly instead of porting
    /// `init_rngen`'s seed injection.
    #[must_use]
    pub fn generate() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(counter);
        match hasher.finish() {
            0 => Self(u64::MAX),
            value => Self(value),
        }
    }
}

impl From<u64> for IdentityId {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<IdentityId> for u64 {
    fn from(id: IdentityId) -> Self {
        id.0
    }
}

/// An identity's role in live g-node migration
/// (`research/notes/01-vala-core-routing.md` §5). Upstream's `Identity`
/// class tracks none of this explicitly — only `IdentityManager.main_id`
/// singles out the main identity (`identities.vala:100,126,344-347`); this
/// registry makes the connectivity and dismissed states first-class so the
/// daemon has one place to query an identity's role instead of re-deriving
/// it from `main_id == id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityStatus {
    /// Owns this node's default network namespace and participates in
    /// hooking (`main_id`).
    Main,
    /// A connectivity-only fork keeping a migrated g-node's external arcs
    /// alive while a new identity re-hooks at the new position — the "old"
    /// identity after `Handle::migrate` (`add_identity`,
    /// `identities.vala:441-577`, described in notes/01 §5).
    Connectivity,
    /// Removed (`remove_identity`, `identities.vala:685-730`). Only ever
    /// observed as the payload of [`crate::IdentityEvent::IdentityDismissed`]
    /// — a dismissed identity is deleted from the registry, not retained
    /// with this status.
    Dismissed,
}

/// Per-identity data the registry owns: which real network position (if
/// any) the identity currently holds, and its role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityRecord {
    pub id: IdentityId,
    /// `None` until hooking negotiates a position for this identity.
    /// While hooking is still resolving a real slot, a fresh migration
    /// target may hold a *virtual* position instead of `None`
    /// (`Naddr::new_allowing_virtual`, `ntk_common::Naddr` type docs;
    /// `is_real_from_to`,
    /// `research/impl/vala/qspn/testsuites/system_peer/serializables.vala:20-25`) —
    /// see [`IdentityRecord::is_hooked`]. This crate never computes or
    /// mutates this field itself — the hooking state machine is an
    /// explicit non-goal — callers set it via [`crate::Handle::set_naddr`].
    pub naddr: Option<Naddr>,
    pub status: IdentityStatus,
}

impl IdentityRecord {
    /// True once this identity holds a real (non-virtual) position —
    /// i.e. is fully hooked, as opposed to holding no position yet or
    /// only the negotiated virtual placeholder a migration target starts
    /// with. The composition root uses this to decide when a migration's
    /// successor is ready and its connectivity fork
    /// ([`IdentityStatus::Connectivity`]) can be retired via
    /// [`crate::Handle::remove_identity`].
    #[must_use]
    pub fn is_hooked(&self) -> bool {
        self.naddr.as_ref().is_some_and(|naddr| !naddr.is_virtual())
    }
}

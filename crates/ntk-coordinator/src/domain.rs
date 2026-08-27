//! In-memory domain types for the fixed-keys database the reserve protocol mutates
//! (`research/impl/vala/coordinator/serializables.vala:156-201`).

use std::collections::BTreeMap;
use std::sync::Arc;

use ntk_proto::v1::TypedValue;
use tokio::time::Instant;

/// One booked (real or virtual) position, keyed by the requester's idempotency token
/// (`Booking`, `research/impl/vala/coordinator/serializables.vala:175-181`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Booking {
    pub reserve_request_id: i64,
    pub new_pos: u32,
    pub new_eldership: u64,
    pub expires_at: Instant,
}

/// The full per-level fixed-keys record the reserve protocol mutates (`CoordGnodeMemory`,
/// `research/impl/vala/coordinator/serializables.vala:183-201`).
#[derive(Clone, Debug, PartialEq)]
pub struct GnodeMemory {
    pub reserve_list: Vec<Booking>,
    /// Monotonically increasing; seeded at this level's g-node size, so the first virtual
    /// allocation is `gsize` itself (`pos >= gsize` is exactly how a virtual position is
    /// recognized, `research/notes/01-vala-core-routing.md` §6 step 6).
    pub max_virtual_pos: u32,
    /// Monotonically increasing, never reused, shared by every booking at this level
    /// (`fk_database.vala:558`).
    pub max_eldership: u64,
    pub n_nodes: Option<(u64, Instant)>,
    pub hooking_memory: Option<TypedValue>,
}

impl GnodeMemory {
    /// The record every level starts from before any reservation happens
    /// (`CoordService.new_coordgnodememory`, `research/impl/vala/coordinator/
    /// peer_service.vala:79-88`).
    #[must_use]
    pub fn fresh(gsize: u32) -> Self {
        Self {
            reserve_list: Vec::new(),
            max_virtual_pos: gsize,
            max_eldership: 0,
            n_nodes: None,
            hooking_memory: None,
        }
    }
}

/// Outcome of a successful reservation (`Reservation`, `research/impl/vala/coordinator/api.vala:75-79`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reservation {
    pub new_pos: u32,
    pub new_eldership: u64,
}

/// `reserve` could not be served at all — a normal, non-exceptional answer distinct from a
/// `ntk_peerservices::ExecError::Refuse` (`ReserveEnterErrorResponse`,
/// `research/impl/vala/coordinator/fk_database.vala:504-505`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReserveError {
    #[error("top {0} is out of range for this topology")]
    TopOutOfRange(usize),
    #[error("cannot reserve at top {0} right now")]
    CannotReserve(usize),
}

/// The (`positions`, `fp_id`, `propagation_id`, `level`, `data`) envelope carried by all 5
/// `CoordinatorManager.execute_*` methods (`research/impl/vala/coordinator/coord.vala:442-553`;
/// wire shape `ntk_proto::v1::CoordinatorExecuteArgs`). `level` is 0-indexed here — a distinct
/// numbering convention from the DHT request surface's 1-indexed `top`
/// (`research/impl/vala/coordinator/coord.vala:229-237` builds `positions`/`fp_id` from a plain
/// 0-indexed level, never translated the way `fk_database.vala:505,539` translate `top - 1`).
#[derive(Clone, Debug)]
pub struct PropagationArgs {
    /// My positions from `level` (inclusive) up to the topology's top level (exclusive).
    pub positions: Vec<u32>,
    pub fp_id: i64,
    pub propagation_id: i32,
    pub level: usize,
    pub data: TypedValue,
}

/// A read-only snapshot of every level's fixed-keys record, published on every mutation
/// (`tokio::sync::watch`, per this crate's actor-model constraints).
pub type Snapshot = Arc<BTreeMap<usize, GnodeMemory>>;

/// A participation-relevant change, published on a [`tokio::sync::broadcast`] stream in place
/// of upstream's GObject signals.
#[derive(Clone, Debug)]
pub enum Event {
    /// A reservation was made or replayed at `top`.
    Reserved {
        top: usize,
        reservation: Reservation,
    },
    /// A booking was explicitly released at `top`.
    ReserveDeleted { top: usize, reserve_request_id: i64 },
    /// The Hooking-owned scratch memory at `top` changed.
    HookingMemoryChanged { top: usize },
}

/// Snapshot of every level's record, handed from a retiring identity's [`crate::Manager`] to
/// the replacement spawned during migration (`CoordService`'s constructor threading
/// `prev_service.fkdd` forward, `research/impl/vala/coordinator/coord.vala:142-146`) — the
/// coordinator hand-off protocol.
///
/// **Scope note**: mirrors only the *continuity* half of upstream's `fixed_keys_db_on_startup`
/// (`research/impl/vala/peerservices/databases.vala:862-897`): passing forward the levels the
/// old and new identity share. The other half — fetching records for levels the new identity
/// does not yet have from the network — is guest/host migration-bootstrap sequencing that
/// belongs to Hooking, exactly like `ntk_peerservices::Manager::new`'s own scope note excludes
/// `enter_net`.
#[derive(Clone, Debug, Default)]
pub struct HandOff(pub(crate) BTreeMap<usize, GnodeMemory>);

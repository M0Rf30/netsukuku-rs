//! Numbered routing-table and rule-priority allocation, replacing
//! `ntkd/table_names.vala`'s `TableNames` — minus the `/etc/iproute2/rt_tables`
//! bookkeeping, which existed only so `ip`(8)'s human-facing output could
//! print a name instead of a number. Netlink route/rule messages always take
//! a numeric table id (see [`crate::RealNetlink`]); this crate never touches
//! `rt_tables`, so there is nothing to `sed -i` and nothing to keep in sync.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::ops::RangeInclusive;

use crate::error::NetlinkError;

/// `RT_TABLE_UNSPEC` — never a valid table for a route or rule.
pub const RT_TABLE_UNSPEC: u32 = 0;
/// `RT_TABLE_DEFAULT`.
pub const RT_TABLE_DEFAULT: u32 = 253;
/// `RT_TABLE_MAIN` — the kernel's default routing table.
pub const RT_TABLE_MAIN: u32 = 254;
/// `RT_TABLE_LOCAL` — the kernel's local/broadcast/anycast table.
pub const RT_TABLE_LOCAL: u32 = 255;

/// Whether `table` is one of the kernel's own reserved tables. Netsukuku
/// must never add, change or remove state in any of these (design decision:
/// `research/README.md` "Netsukuku is an L3 routing protocol" — it owns its
/// *own* tables, it does not rewrite the host's).
pub const fn is_kernel_reserved_table(table: u32) -> bool {
    matches!(
        table,
        RT_TABLE_UNSPEC | RT_TABLE_DEFAULT | RT_TABLE_MAIN | RT_TABLE_LOCAL
    )
}

/// Rejects a kernel-reserved table id. Called by every mutating
/// [`crate::RouteTable`]/[`crate::RuleTable`] method in both
/// [`crate::RealNetlink`] and [`crate::FakeNetlink`], so a caller can never
/// mutate `main`/`local`/`default` even by passing a raw table id that
/// bypasses [`TableAllocator`] entirely.
pub(crate) fn guard_table(table: u32) -> Result<(), NetlinkError> {
    if is_kernel_reserved_table(table) {
        Err(NetlinkError::ReservedTable(table))
    } else {
        Ok(())
    }
}

/// Per-peer dynamic table ids, mirroring `ntk.conf`'s `200..=250`
/// (`research/impl/vala/system-ntkd/ntk.conf:2-53`) and
/// `table_names.vala:51`'s `for (int i = 250; i >= 200; i--) free_tid.add(i)`.
pub const DEFAULT_PEER_TABLE_RANGE: RangeInclusive<u32> = 200..=250;
/// The main identity's fixed table id, mirroring `ntk.conf:1`'s `251 ntk`.
pub const DEFAULT_MAIN_TABLE_ID: u32 = 251;
/// `[INFERENCE]`: upstream never assigns an explicit `ip rule` priority
/// (`identity_ip_commands.vala:89-90,157-158` call `ip rule add table ntk` /
/// `ip rule add fwmark <tid> table <table>` with no `pref`, so the kernel
/// auto-assigns one ahead of its own built-in 32766/32767 rules — leaving
/// the relative order between the main identity's catch-all rule and every
/// per-peer `fwmark` rule unspecified). We fix it deterministically instead:
/// every peer `fwmark` rule (specific) is assigned a priority below this
/// constant, so it is always evaluated *before* the main identity's own
/// catch-all `table <main>` rule (general) — and both are still evaluated
/// long before the kernel's built-in 32766 (`main`) / 32767 (`default`).
pub const DEFAULT_MAIN_RULE_PRIORITY: u32 = 10_000;

/// A table id or rule priority pool is misconfigured or exhausted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TableAllocatorError {
    /// The requested range/main id overlaps a kernel-reserved table.
    #[error("table {0} is a kernel-reserved table and cannot be allocated to Netsukuku")]
    ReservedTable(u32),
    /// The peer range is empty or overlaps the main table id.
    #[error("peer table range is empty or overlaps the main table id {main}")]
    InvalidRange {
        /// The main table id that conflicted with the range.
        main: u32,
    },
    /// Every table id (or rule priority) in the configured range is in use.
    #[error("no free table ids remain in the allocator's range")]
    Exhausted,
    /// `owner` has no table currently allocated.
    #[error("owner has no table allocated")]
    UnknownOwner,
    /// [`TableAllocator::release`] was called while references remain.
    #[error("table still has {0} outstanding reference(s)")]
    StillReferenced(i64),
}

#[derive(Debug)]
struct Allocation {
    table: u32,
    priority: u32,
    refcount: i64,
}

/// Hands out numbered routing-table ids and `ip rule` priorities for
/// Netsukuku's per-identity / per-peer policy routing, replacing
/// `ntkd/table_names.vala`'s `TableNames` class. Generic over an owner key
/// `K` (in the Vala original, always a peer MAC string) so this crate is not
/// coupled to how a future sibling crate (neighborhood/identities) chooses
/// to identify a peer.
#[derive(Debug)]
pub struct TableAllocator<K> {
    peer_range: RangeInclusive<u32>,
    main_table: u32,
    main_rule_priority: u32,
    free_tables: VecDeque<u32>,
    free_priorities: VecDeque<u32>,
    allocated: HashMap<K, Allocation>,
}

impl<K> TableAllocator<K> {
    /// The fixed main-identity table id.
    pub fn main_table(&self) -> u32 {
        self.main_table
    }

    /// The main identity's own catch-all rule priority (always greater than
    /// every allocatable peer-table priority).
    pub fn main_rule_priority(&self) -> u32 {
        self.main_rule_priority
    }

    /// Whether `table` belongs to Netsukuku under this allocator's
    /// configuration — the main table id or anywhere in the peer range.
    /// [`crate::cleanup::cleanup`] uses this as its rule ownership predicate.
    pub fn owns_table(&self, table: u32) -> bool {
        table == self.main_table || self.peer_range.contains(&table)
    }

    /// Every table id this allocator owns: the main table plus the whole
    /// peer range, regardless of which peer tables are currently allocated.
    /// [`crate::cleanup::cleanup`] sweeps exactly these tables for leftover
    /// routes, whether or not `K`'s allocation bookkeeping still remembers
    /// who owned them (e.g. after a daemon restart with a fresh, empty
    /// allocator).
    pub fn owned_tables(&self) -> impl Iterator<Item = u32> + '_ {
        std::iter::once(self.main_table).chain(self.peer_range.clone())
    }
}

impl<K: Clone + Eq + Hash> TableAllocator<K> {
    /// The default allocator: peer tables `200..=250`, main table `251`,
    /// mirroring upstream's `ntk.conf` exactly.
    pub fn new() -> Self {
        Self::with_range(
            DEFAULT_PEER_TABLE_RANGE,
            DEFAULT_MAIN_TABLE_ID,
            DEFAULT_MAIN_RULE_PRIORITY,
        )
        .expect("the default table range is always valid")
    }

    /// Builds an allocator over a custom range, rejecting any overlap with a
    /// kernel-reserved table or between `peer_range` and `main_table`.
    pub fn with_range(
        peer_range: RangeInclusive<u32>,
        main_table: u32,
        main_rule_priority: u32,
    ) -> Result<Self, TableAllocatorError> {
        if peer_range.is_empty() {
            return Err(TableAllocatorError::InvalidRange { main: main_table });
        }
        if is_kernel_reserved_table(main_table) {
            return Err(TableAllocatorError::ReservedTable(main_table));
        }
        if let Some(reserved) = peer_range.clone().find(|id| is_kernel_reserved_table(*id)) {
            return Err(TableAllocatorError::ReservedTable(reserved));
        }
        if peer_range.contains(&main_table) {
            return Err(TableAllocatorError::InvalidRange { main: main_table });
        }
        let free_tables: VecDeque<u32> = peer_range.clone().collect();
        let slots = free_tables.len() as u32;
        let priority_floor = main_rule_priority.saturating_sub(slots);
        let free_priorities: VecDeque<u32> = (priority_floor..main_rule_priority).collect();
        Ok(Self {
            peer_range,
            main_table,
            main_rule_priority,
            free_tables,
            free_priorities,
            allocated: HashMap::new(),
        })
    }

    /// Allocates a `(table id, rule priority)` pair for `owner`, or returns
    /// the existing allocation if `owner` already has one (idempotent, like
    /// `TableNames.get_table`).
    pub fn acquire(&mut self, owner: K) -> Result<(u32, u32), TableAllocatorError> {
        if let Some(existing) = self.allocated.get(&owner) {
            return Ok((existing.table, existing.priority));
        }
        let table = self
            .free_tables
            .pop_front()
            .ok_or(TableAllocatorError::Exhausted)?;
        let priority = match self.free_priorities.pop_front() {
            Some(priority) => priority,
            None => {
                // Unreachable in practice: both pools are built with equal
                // length in `with_range`, but restore the table id rather
                // than leak it if this invariant is ever violated.
                self.free_tables.push_front(table);
                return Err(TableAllocatorError::Exhausted);
            }
        };
        self.allocated.insert(
            owner,
            Allocation {
                table,
                priority,
                refcount: 0,
            },
        );
        Ok((table, priority))
    }

    /// The `(table id, rule priority)` currently allocated to `owner`, if any.
    pub fn table_of(&self, owner: &K) -> Option<(u32, u32)> {
        self.allocated.get(owner).map(|a| (a.table, a.priority))
    }

    /// Increments `owner`'s reference count (a second peer arc sharing the
    /// same already-allocated table), returning the new count.
    pub fn incref(&mut self, owner: &K) -> Result<i64, TableAllocatorError> {
        let allocation = self
            .allocated
            .get_mut(owner)
            .ok_or(TableAllocatorError::UnknownOwner)?;
        allocation.refcount += 1;
        Ok(allocation.refcount)
    }

    /// Decrements `owner`'s reference count, returning the new count. May go
    /// to zero or below, exactly mirroring `TableNames.decref_table` — it is
    /// [`TableAllocator::release`]'s job to refuse freeing a table that is
    /// still referenced.
    pub fn decref(&mut self, owner: &K) -> Result<i64, TableAllocatorError> {
        let allocation = self
            .allocated
            .get_mut(owner)
            .ok_or(TableAllocatorError::UnknownOwner)?;
        allocation.refcount -= 1;
        Ok(allocation.refcount)
    }

    /// Frees `owner`'s table id and rule priority back into the pool, at
    /// the front — a table released this way is the *next* one handed out
    /// by [`TableAllocator::acquire`], mirroring `TableNames.release_table`
    /// (`table_names.vala:108`, `free_tid.insert(0, tid)`) and
    /// `TableNames.get_table` (`table_names.vala:75`,
    /// `free_tid.remove_at(0)`) — both act at index 0. Refuses to do so
    /// while the reference count is above zero.
    pub fn release(&mut self, owner: &K) -> Result<u32, TableAllocatorError> {
        let refcount = self
            .allocated
            .get(owner)
            .ok_or(TableAllocatorError::UnknownOwner)?
            .refcount;
        if refcount > 0 {
            return Err(TableAllocatorError::StillReferenced(refcount));
        }
        let allocation = self.allocated.remove(owner).expect("checked above");
        self.free_tables.push_front(allocation.table);
        self.free_priorities.push_front(allocation.priority);
        Ok(allocation.table)
    }
}

impl<K: Clone + Eq + Hash> Default for TableAllocator<K> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_matches_upstream_ntk_conf() {
        let mut allocator: TableAllocator<&str> = TableAllocator::new();
        assert_eq!(allocator.main_table(), 251);
        assert!(allocator.owns_table(251));
        assert!(allocator.owns_table(200));
        assert!(allocator.owns_table(250));
        assert!(!allocator.owns_table(199));
        assert!(!allocator.owns_table(RT_TABLE_MAIN));

        let (table, priority) = allocator.acquire("aa:bb:cc:dd:ee:ff").unwrap();
        assert_eq!(table, 200);
        assert!(priority < allocator.main_rule_priority());
    }

    #[test]
    fn acquire_is_idempotent_per_owner() {
        let mut allocator: TableAllocator<&str> = TableAllocator::new();
        let first = allocator.acquire("peer-a").unwrap();
        let second = allocator.acquire("peer-a").unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn distinct_owners_get_distinct_tables() {
        let mut allocator: TableAllocator<&str> = TableAllocator::new();
        let (t1, p1) = allocator.acquire("peer-a").unwrap();
        let (t2, p2) = allocator.acquire("peer-b").unwrap();
        assert_ne!(t1, t2);
        assert_ne!(p1, p2);
    }

    #[test]
    fn exhausts_after_every_slot_taken() {
        let mut allocator = TableAllocator::with_range(200..=201, 251, 10_000).unwrap();
        allocator.acquire("a").unwrap();
        allocator.acquire("b").unwrap();
        assert_eq!(allocator.acquire("c"), Err(TableAllocatorError::Exhausted));
    }

    #[test]
    fn release_refuses_while_referenced() {
        let mut allocator: TableAllocator<&str> = TableAllocator::new();
        allocator.acquire("peer-a").unwrap();
        allocator.incref(&"peer-a").unwrap();
        assert_eq!(
            allocator.release(&"peer-a"),
            Err(TableAllocatorError::StillReferenced(1))
        );
        assert_eq!(allocator.decref(&"peer-a").unwrap(), 0);
        assert_eq!(allocator.release(&"peer-a").unwrap(), 200);
    }

    #[test]
    fn released_table_is_reused() {
        let mut allocator: TableAllocator<&str> = TableAllocator::new();
        let (table, _) = allocator.acquire("peer-a").unwrap();
        allocator.release(&"peer-a").unwrap();
        let (reused, _) = allocator.acquire("peer-b").unwrap();
        assert_eq!(table, reused);
    }

    #[test]
    fn operations_on_unknown_owner_fail() {
        let mut allocator: TableAllocator<&str> = TableAllocator::new();
        assert_eq!(
            allocator.incref(&"nobody"),
            Err(TableAllocatorError::UnknownOwner)
        );
        assert_eq!(
            allocator.decref(&"nobody"),
            Err(TableAllocatorError::UnknownOwner)
        );
        assert_eq!(
            allocator.release(&"nobody"),
            Err(TableAllocatorError::UnknownOwner)
        );
    }

    #[test]
    fn rejects_ranges_overlapping_kernel_reserved_tables() {
        assert_eq!(
            TableAllocator::<&str>::with_range(250..=254, 260, 1_000).unwrap_err(),
            TableAllocatorError::ReservedTable(RT_TABLE_DEFAULT)
        );
        assert_eq!(
            TableAllocator::<&str>::with_range(200..=210, RT_TABLE_MAIN, 1_000).unwrap_err(),
            TableAllocatorError::ReservedTable(RT_TABLE_MAIN)
        );
    }

    #[test]
    fn rejects_main_table_inside_peer_range() {
        assert_eq!(
            TableAllocator::<&str>::with_range(200..=210, 205, 1_000).unwrap_err(),
            TableAllocatorError::InvalidRange { main: 205 }
        );
    }

    #[test]
    fn owned_tables_covers_main_and_peer_range() {
        let allocator = TableAllocator::<&str>::with_range(200..=201, 251, 1_000).unwrap();
        let owned: Vec<u32> = allocator.owned_tables().collect();
        assert_eq!(owned, vec![251, 200, 201]);
    }
}

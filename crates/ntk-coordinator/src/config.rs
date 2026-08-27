//! Injectable timing and redundancy constants, transcribed from
//! `research/impl/vala/coordinator/peer_service.vala:27-30` and `:93-113`
//! (`timeout_exec_for_request`) and `research/notes/01-vala-core-routing.md` §7's constants
//! table, rather than hard-coded at their use sites.

use std::time::Duration;

/// Tuning knobs for the fixed-keys database's booking/cache TTLs, the propagation anti-replay
/// window, and DHT round-trip timeouts. Construct via [`Config::default`] for upstream's own
/// values, or override individual fields for tests/deployments that need different pacing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// TTL of a `Booking` (real or virtual position reservation) before it is purged as
    /// expired (`CoordService.msec_new_reservation`,
    /// `research/impl/vala/coordinator/peer_service.vala:28`).
    pub booking_ttl: Duration,
    /// TTL of the cached `get_n_nodes` answer (`CoordService.msec_n_nodes`,
    /// `research/impl/vala/coordinator/peer_service.vala:29`).
    pub n_nodes_cache_ttl: Duration,
    /// How long an executed propagation's `propagation_id` is remembered to reject a replay
    /// (`CoordinatorManager.propagation_cleanup`,
    /// `research/impl/vala/coordinator/coord.vala:238-259`).
    pub propagation_retention: Duration,
    /// Replica fanout for the fixed-keys writes the reserve protocol makes
    /// (`CoordService.q_replica_new_reservation`,
    /// `research/impl/vala/coordinator/peer_service.vala:30`).
    pub replica_fanout: u32,
    /// DHT round-trip timeout for a write operation: `reserve`/`delete_reserve`/
    /// `set_hooking_memory`/`get_n_nodes` (`timeout_write_operation`,
    /// `research/impl/vala/coordinator/peer_service.vala:95-98,102-109`).
    pub write_timeout: Duration,
    /// DHT round-trip timeout for the Hooking-facing `evaluate_enter`/`begin_enter`/
    /// `completed_enter`/`abort_enter` proxy calls (`timeout_hooking_operation`,
    /// `research/impl/vala/coordinator/peer_service.vala:99-105`).
    pub hooking_timeout: Duration,
    /// DHT round-trip timeout for `get_hooking_memory`, a read-only lookup
    /// (`research/impl/vala/coordinator/peer_service.vala:110`).
    pub read_timeout: Duration,
    /// Timeout for one outbound `ReplicaRequest` fanout call
    /// (`research/impl/vala/coordinator/peer_service.vala:111`).
    pub replica_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            booking_ttl: Duration::from_millis(60_000),
            n_nodes_cache_ttl: Duration::from_millis(20_000),
            propagation_retention: Duration::from_millis(200_000),
            replica_fanout: 15,
            write_timeout: Duration::from_millis(8_000),
            hooking_timeout: Duration::from_millis(8_000),
            read_timeout: Duration::from_millis(1_000),
            replica_timeout: Duration::from_millis(1_000),
        }
    }
}

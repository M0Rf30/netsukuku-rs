//! Injectable configuration: every QSPN timer/threshold constant from
//! `research/notes/01-vala-core-routing.md` §3 "Constants", collected in one
//! struct so none of it is scattered as a magic number through the logic.
//!
//! `max_paths`/`max_common_hops_ratio`/`arc_timeout` have no built-in
//! upstream default — `QspnManager.init` takes them as deployment-chosen ctor
//! arguments (`qspn.vala:68-86`). [`QspnConfig::default`] uses the one
//! shipped reference deployment's actual values
//! (`research/impl/vala/ntkd/ntkd.vala:41-43`: `max_paths=5`,
//! `max_common_hops_ratio=0.6`, `arc_timeout=10000ms`) rather than inventing
//! new numbers. Every other field is a literal upstream constant.

use std::time::Duration;

use crate::path::RoutePath;

/// `get_mch_ratio`'s two lookup ladders
/// (`research/impl/vala/qspn/qspn.vala:1888-1909`).
#[derive(Clone, Debug, PartialEq)]
pub struct MchRatioTable {
    /// `l` by gateway count, index 0 = 1 gateway ... index 6 = 7 gateways
    /// (`qspn.vala:1891-1897`).
    pub gateway_ratio: [f64; 7],
    /// `l` when more than 7 gateways are available (`qspn.vala:1898`).
    pub gateway_ratio_overflow: f64,
    /// `g` bands: `(exclusive upper bound on destination size, ratio)`,
    /// ascending (`qspn.vala:1901-1906`).
    pub size_ratio_bands: [(u32, f64); 6],
    /// `g` once destination size reaches/exceeds every band above
    /// (`qspn.vala:1907`).
    pub size_ratio_overflow: f64,
}

impl Default for MchRatioTable {
    fn default() -> Self {
        Self {
            gateway_ratio: [0.45, 0.35, 0.27, 0.20, 0.15, 0.12, 0.10],
            gateway_ratio_overflow: 0.08,
            size_ratio_bands: [
                (10, 1.0),
                (25, 0.9),
                (75, 0.8),
                (250, 0.6),
                (750, 0.3),
                (3000, 0.1),
            ],
            size_ratio_overflow: 0.0001,
        }
    }
}

/// Hop-overlap weighting coefficients used by disjoint-path admission
/// (`qspn.vala:1575,1594-1595`): `floor(intermediate_coeff * sqrt(n))` per
/// shared intermediate hop, `floor(destination_coeff * sqrt(n)) -
/// destination_offset` for the destination hop itself.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct OverlapWeights {
    pub intermediate_coeff: f64,
    pub destination_coeff: f64,
    pub destination_offset: f64,
}

impl Default for OverlapWeights {
    fn default() -> Self {
        Self {
            intermediate_coeff: 1.5,
            destination_coeff: 0.75,
            destination_offset: 1.0,
        }
    }
}

/// Every injectable QSPN timer/threshold, upstream defaults preserved.
///
/// [`Self::prepare_destroy_wait`] remains "reserved": its owning mechanism
/// (`prepare_destroy`/`destroy`'s broadcast identity-teardown,
/// `qspn.vala:2450-2505`) is out of scope for this crate (see
/// [`crate::manager::QspnHandle::check_connectivity`]'s docs for why). The
/// constant is still named here, with its upstream default, so a future
/// composition layer that adds that mechanism can reuse this same config
/// type without a breaking change.
#[derive(Clone, Debug, PartialEq)]
pub struct QspnConfig {
    /// Per-destination cap on admitted disjoint paths (`qspn.vala:70,93`).
    pub max_paths: usize,
    /// Base overlap-tolerance ratio fed into [`crate::mch_ratio::mch_ratio`]
    /// (`qspn.vala:71,94`).
    pub max_common_hops_ratio: f64,
    /// Bound on how long the RPC layer polls for an inbound caller to resolve
    /// to a known arc (`qspn.vala:72,95`, poll loops at `qspn.vala:2551-2565`).
    pub arc_timeout: Duration,
    /// Delay between actor start and the first `BootstrapComplete` event for
    /// a `create_net` identity, letting the constructor return before the
    /// signal fires (`qspn.vala:206-219`).
    pub bootstrap_signal_delay: Duration,
    /// Fallback max wait during an `enter_net` identity's bootstrap phase
    /// before forcing exit even without a qualifying ETP
    /// (`qspn.vala:556-565`).
    pub bootstrap_fallback_max_wait: Duration,
    /// Pause after bootstrap exits before `PresenceNotified`
    /// (`qspn.vala:625-626`).
    pub presence_notified_delay: Duration,
    /// Interval between unconditional full-ETP re-publishes while at least
    /// one arc is up (`qspn.vala:678-683`).
    pub periodic_full_etp_interval: Duration,
    /// Delay before re-flooding on first detection of a g-node split
    /// (`qspn.vala:1932`).
    pub first_detection_split_delay: Duration,
    /// Delay before `publish_connectivity` after `make_connectivity`
    /// (`qspn.vala:2259`).
    pub publish_connectivity_delay: Duration,
    /// Reserved: wait after `got_prepare_destroy` before self-removal
    /// (`qspn.vala:2761`) — part of `prepare_destroy`/`destroy`'s broadcast
    /// teardown, out of scope for this crate (see this struct's own docs).
    pub prepare_destroy_wait: Duration,
    /// Poll interval while resolving an inbound caller to one of this node's
    /// arcs (`qspn.vala:2564,2627,2785`).
    pub caller_arc_poll_interval: Duration,
    /// `nodes_inside` change-noise tolerance band, e.g. `0.10` for ±10%
    /// (`qspn.vala:1433`).
    pub nodes_inside_tolerance: f64,
    /// Hop-overlap weighting coefficients (`qspn.vala:1575,1594-1595`).
    pub overlap_weights: OverlapWeights,
    /// `get_mch_ratio`'s lookup tables (`qspn.vala:1888-1909`).
    pub mch_ratio_table: MchRatioTable,
    /// Minimum interval between successive arc-flap-triggered gathers
    /// (`manager::Actor::request_arc_gather`). **Deliberate deviation, no
    /// upstream equivalent**: `qspn.vala:821-859`'s `arc_is_changed` kicks off
    /// `gather_full_etp_set` unconditionally on every call, fine under
    /// upstream's own assumption that `IQspnArc` cost changes are already
    /// hysteresis-gated by whatever `Cost` implementation a deployment
    /// plugs in. A genuinely flapping physical link bypasses that gate
    /// entirely (link-down/link-up is not a "small cost jitter", it is a
    /// distinct event each time), so an unbounded flap drove one full-ETP
    /// gather — and therefore one outbound RPC per arc — per flap. This
    /// bounds that fan-out to at most one gather per window: the first
    /// change in a quiet period still gathers immediately (no added
    /// latency for the ordinary single-change case), and any further
    /// changes inside the same window collapse into exactly one trailing
    /// catch-up gather once it elapses, rather than one per change.
    pub arc_gather_debounce: Duration,
}

impl Default for QspnConfig {
    fn default() -> Self {
        Self {
            max_paths: 5,
            max_common_hops_ratio: 0.6,
            arc_timeout: Duration::from_millis(10_000),
            bootstrap_signal_delay: Duration::from_millis(1),
            bootstrap_fallback_max_wait: Duration::from_millis(10_000),
            presence_notified_delay: Duration::from_millis(1_000),
            periodic_full_etp_interval: Duration::from_millis(600_000),
            first_detection_split_delay: Duration::from_millis(500),
            publish_connectivity_delay: Duration::from_millis(50),
            prepare_destroy_wait: Duration::from_millis(10_000),
            caller_arc_poll_interval: Duration::from_millis(10),
            nodes_inside_tolerance: 0.10,
            overlap_weights: OverlapWeights::default(),
            mch_ratio_table: MchRatioTable::default(),
            arc_gather_debounce: Duration::from_millis(250),
        }
    }
}

/// Pluggable split debounce, matching `IQspnThresholdCalculator`
/// (`research/impl/vala/qspn/api.vala:136-139`): how long to wait, once a
/// destination shows more than one live fingerprint, before actually
/// signaling the split — so a transient fork does not trigger migration
/// (`research/notes/01-vala-core-routing.md` §3, "debounced split-signal").
pub trait ThresholdCalculator: Send + Sync {
    fn calculate_threshold(&self, eldest: &RoutePath, other: &RoutePath) -> Duration;
}

/// The one reference implementation upstream ships: a flat threshold
/// regardless of the two paths (`research/impl/vala/ntkd/qspn_helpers.vala:94-100`).
#[derive(Clone, Copy, Debug)]
pub struct FixedThreshold(pub Duration);

impl Default for FixedThreshold {
    fn default() -> Self {
        Self(Duration::from_millis(10_000))
    }
}

impl ThresholdCalculator for FixedThreshold {
    fn calculate_threshold(&self, _eldest: &RoutePath, _other: &RoutePath) -> Duration {
        self.0
    }
}

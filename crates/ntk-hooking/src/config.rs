//! Injectable configuration: every timer/backoff constant from
//! `research/impl/vala/hooking/arc_handler.vala` and `hooking.vala`,
//! collected in one struct so none of it is a scattered magic number.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

/// `get_global_timeout(size)` (`hooking.vala:46-57`): a ladder from network
/// size to a "how long is reasonable to wait" duration.
///
/// **Not a protocol invariant.** Upstream flags this in-source as
/// placeholder/debug-only tuning ("*these are really just for scripted
/// debugging* ... *For real cases I don't know what's suited*",
/// `hooking.vala:48-55`; see also `research/notes/01-vala-core-routing.md`,
/// "Open questions" — "threshold/backoff constants are explicitly flagged
/// provisional"). [`HookingConfig::global_timeout`] therefore keeps this
/// fully pluggable rather than hardcoding the ladder as if it were
/// normative; [`default_global_timeout`] reproduces the exact upstream
/// numbers only as a documented, replaceable starting point.
pub type GlobalTimeoutFn = Arc<dyn Fn(u64) -> Duration + Send + Sync>;

/// Upstream's own ladder (`hooking.vala:51-56`), reproduced verbatim as the
/// default — see [`GlobalTimeoutFn`]'s docs for why this is not normative.
#[must_use]
pub fn default_global_timeout(size: u64) -> Duration {
    let ms = if size < 5 {
        1000
    } else if size < 15 {
        2000
    } else if size < 25 {
        3000
    } else if size < 100 {
        5000
    } else {
        10000
    };
    Duration::from_millis(ms)
}

/// Every injectable Hooking timer/backoff, upstream defaults preserved
/// (except [`HookingConfig::global_timeout`] itself — see its docs).
#[derive(Clone)]
pub struct HookingConfig {
    /// Retry wait after a peer's `retrieve_network_data` throws
    /// `NotBootstrappedError` (it is still hooking itself)
    /// (`arc_handler.vala:110-113`).
    pub not_bootstrapped_retry: Duration,

    /// Wait before redoing the whole arc-handler loop from start after a
    /// merge decision resolves to "wait" (my network is decisively larger,
    /// or a tiebreak favored me) (`arc_handler.vala:209-214`).
    pub merge_reject_wait: Duration,

    /// `get_global_timeout(n)` — see [`GlobalTimeoutFn`]'s docs. Defaults to
    /// [`default_global_timeout`].
    pub global_timeout: GlobalTimeoutFn,

    /// Divisor applied to `global_timeout(n)` for the `AskAgainError` retry
    /// wait on `evaluate_enter` (`arc_handler.vala:236-239`: `wait
    /// global_timeout(n) / 4`).
    pub ask_again_divisor: u32,

    /// Multiplier applied to `global_timeout(n)` for the
    /// `IgnoreNetworkError`/`AlreadyEnteringError`/no-migration-at-level-0
    /// restart-from-start wait (`arc_handler.vala:240-244,265-270,306-311`:
    /// `wait global_timeout(n) * 20`).
    pub restart_multiplier: u32,

    /// How long a migration-path BFS step (`send_search_request`/
    /// `send_explore_request`/`send_mig_request`) waits for its correlated
    /// response before treating the hop as unreachable
    /// (`message_routing.vala:145,534,849`: `int timeout = 100000; //
    /// TODO`, itself flagged as a placeholder upstream).
    pub routing_response_timeout: Duration,
}

impl fmt::Debug for HookingConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HookingConfig")
            .field("not_bootstrapped_retry", &self.not_bootstrapped_retry)
            .field("merge_reject_wait", &self.merge_reject_wait)
            .field("global_timeout", &"<fn>")
            .field("ask_again_divisor", &self.ask_again_divisor)
            .field("restart_multiplier", &self.restart_multiplier)
            .field("routing_response_timeout", &self.routing_response_timeout)
            .finish()
    }
}

impl Default for HookingConfig {
    fn default() -> Self {
        Self {
            not_bootstrapped_retry: Duration::from_millis(1000),
            merge_reject_wait: Duration::from_millis(600_000),
            global_timeout: Arc::new(default_global_timeout),
            ask_again_divisor: 4,
            restart_multiplier: 20,
            routing_response_timeout: Duration::from_millis(100_000),
        }
    }
}

impl HookingConfig {
    /// `global_timeout(n) / ask_again_divisor`.
    #[must_use]
    pub fn ask_again_wait(&self, n_nodes: u64) -> Duration {
        (self.global_timeout)(n_nodes) / self.ask_again_divisor
    }

    /// `global_timeout(n) * restart_multiplier`.
    #[must_use]
    pub fn restart_wait(&self, n_nodes: u64) -> Duration {
        (self.global_timeout)(n_nodes) * self.restart_multiplier
    }
}

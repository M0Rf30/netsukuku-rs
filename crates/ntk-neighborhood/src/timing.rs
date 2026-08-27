//! [`NeighborhoodTiming`]: every wall-clock interval this crate waits on,
//! collected into one injectable struct so tests never sleep upstream's
//! real 28-30s/60s constants (`research/notes/01-vala-core-routing.md` §4).

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

/// Injectable timing parameters. [`NeighborhoodTiming::default`] reproduces
/// upstream's real constants; tests should build their own instance with
/// millisecond-scale durations instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeighborhoodTiming {
    /// Interval between `here_i_am` radar broadcasts on a monitored NIC
    /// (`MonitorRunTasklet`, `neighborhood.vala:187`: `ms_wait(60000)`).
    pub radar_interval: Duration,
    /// Inclusive `[min, max]` range an exported arc's monitor waits between
    /// `nop`/cost-measurement ticks, picked anew each tick
    /// (`ArcMonitorRunTasklet`, `neighborhood.vala:290`:
    /// `Random.int_range(28000, 30000)`).
    pub arc_monitor_interval: (Duration, Duration),
}

impl Default for NeighborhoodTiming {
    fn default() -> Self {
        Self {
            radar_interval: Duration::from_millis(60_000),
            arc_monitor_interval: (Duration::from_millis(28_000), Duration::from_millis(30_000)),
        }
    }
}

impl NeighborhoodTiming {
    /// Picks a random duration in [`Self::arc_monitor_interval`]. Uses
    /// `std::collections::hash_map::RandomState` as an OS-randomness
    /// source, matching [`crate::NodeId::generate`]'s rationale for not
    /// depending on a `rand`-family crate absent from
    /// `[workspace.dependencies]`.
    pub(crate) fn next_arc_monitor_wait(&self) -> Duration {
        let (min, max) = self.arc_monitor_interval;
        if max <= min {
            return min;
        }
        let span_ms = (max - min).as_millis() as u64;
        let raw = RandomState::new().build_hasher().finish();
        min + Duration::from_millis(raw % (span_ms + 1))
    }
}

#[cfg(test)]
mod tests {
    use super::NeighborhoodTiming;
    use std::time::Duration;

    #[test]
    fn next_arc_monitor_wait_stays_in_range() {
        let timing = NeighborhoodTiming {
            radar_interval: Duration::from_millis(1),
            arc_monitor_interval: (Duration::from_millis(5), Duration::from_millis(9)),
        };
        for _ in 0..64 {
            let wait = timing.next_arc_monitor_wait();
            assert!(wait >= Duration::from_millis(5) && wait <= Duration::from_millis(9));
        }
    }

    #[test]
    fn next_arc_monitor_wait_handles_degenerate_range() {
        let timing = NeighborhoodTiming {
            radar_interval: Duration::from_millis(1),
            arc_monitor_interval: (Duration::from_millis(5), Duration::from_millis(5)),
        };
        assert_eq!(timing.next_arc_monitor_wait(), Duration::from_millis(5));
    }
}

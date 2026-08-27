//! Pure link-cost math: EMA smoothing and the hysteresis publication gate
//! (`ArcMonitorRunTasklet`, `research/impl/vala/neighborhood/neighborhood.vala:262-286`,
//! `research/notes/01-vala-core-routing.md` §4 point 5).
//!
//! Two *different* accumulators are involved and must not be conflated:
//! - `smoothed` — the EMA accumulator (upstream's `last_rtt`), updated on
//!   every successful RTT measurement via [`ema_step`], regardless of
//!   whether it gets published.
//! - the *published* [`ntk_common::Cost`] on the [`crate::Arc`] — updated
//!   only when [`exceeds_hysteresis`] fires against the smoothed value.
//!
//! This split is what creates the hysteresis: `smoothed` can drift up to
//! 2x away from the published value with zero externally-visible effect.

/// One EMA smoothing step over a raw RTT sample, using upstream's
/// asymmetric convergence rates (`neighborhood.vala:274-278`):
///
/// ```text
/// delta = sample - last
/// delta /= 10   if delta > 0   (sample is worse / higher than last)
/// delta /= 3    if delta < 0   (sample is better / lower than last)
/// last + delta
/// ```
///
/// Note this makes the smoothed cost rise *slowly* (damped by 10x) and fall
/// *quickly* (damped by only 3x) — the opposite asymmetry from the prose in
/// `research/notes/01-vala-core-routing.md` §4 ("converge up faster than
/// down"), which appears to describe the code backwards. The code
/// (`research/impl/vala/`, NORMATIVE per this crate's assignment) is what
/// is implemented here: a transient latency spike is absorbed gradually,
/// while an improvement is trusted almost immediately.
///
/// # Deliberate deviation: round away from zero instead of upstream's plain `/`
/// Upstream's `delta /= 10`/`delta /= 3` is C-style integer division, which
/// truncates toward zero. Ported literally, any `0 < |delta| < 10` (rise) or
/// `0 < |delta| < 3` (fall) truncates the damped step to exactly `0` —
/// `last` never moves, `sample` is silently dropped, and every subsequent
/// tick recomputes the *same* truncated-to-zero delta forever: the EMA
/// stalls short of the true value and never closes the last few units of
/// gap. Combined with [`exceeds_hysteresis`]'s boundary (see its own doc),
/// this is what makes a link that degrades to exactly 2x its published cost
/// permanently invisible. This crate instead floors the damped magnitude at
/// `1` whenever `delta != 0` (`.max(1)` rising, `.min(-1)` falling) — the
/// smallest possible step that is guaranteed to still make progress toward
/// `sample` every tick, so `last` provably converges to `sample` in a
/// bounded number of ticks instead of stalling. For `|delta|` at or above
/// the damping factor this is a no-op (the plain division already exceeds
/// magnitude 1), so the documented 10x/3x asymmetry above is unchanged for
/// every case upstream's own math already handled correctly.
#[must_use]
pub fn ema_step(last: u64, sample: u64) -> u64 {
    let delta = sample as i64 - last as i64;
    let damped = match delta {
        0 => 0,
        d if d > 0 => (d / 10).max(1),
        d => (d / 3).min(-1),
    };
    (last as i64 + damped).max(0) as u64
}

/// The 2x hysteresis gate (`neighborhood.vala:279-286`): a smoothed value
/// is only published (and only then does upstream fire `arc_changed`) once
/// it falls outside `(0.5x, 2x)` of the last *published* value.
///
/// # Deliberate deviation: the upper 2x boundary is now inclusive
/// Upstream's `last_rtt < arc.cost * 0.5 || last_rtt > arc.cost * 2` treats
/// exactly `2x` as still *inside* the suppressed band. Combined with
/// [`ema_step`]'s pre-fix stall (see that function's doc), a link that
/// degrades to *exactly* double its published cost could never republish:
/// the smoothed value asymptotically approaches `2x` from below, stalls
/// short of it forever, and the strict `>` never fires even after
/// [`ema_step`]'s own fix lets it *reach* `2x` exactly. This crate closes
/// only that one boundary (`>=` instead of `>`) and leaves the lower `0.5x`
/// boundary strictly as upstream defined it — an under-published cost
/// decrease is a missed optimization, not the stale-route correctness risk
/// an under-published cost *increase* is, so only the reported failure mode
/// is fixed, per this batch's "preserve upstream's semantics otherwise".
///
/// # Why `smoothed != published` guards the whole expression
/// Naively changing only `>` to `>=` makes `published == 0` degenerate:
/// `published.saturating_mul(2)` is `0`, so `smoothed >= 0` is trivially
/// true for *every* `u64`, including `smoothed == published == 0` — i.e. no
/// change at all would spuriously "exceed" hysteresis. Multiplying a zero
/// baseline by 2 is not a meaningful "double" in the first place; the
/// explicit no-change guard sidesteps that degenerate case without
/// affecting any `published > 0` comparison (there, `smoothed >=
/// published*2` already implies `smoothed != published`).
#[must_use]
pub fn exceeds_hysteresis(published: u64, smoothed: u64) -> bool {
    smoothed != published
        && (smoothed.saturating_mul(2) < published || smoothed >= published.saturating_mul(2))
}

#[cfg(test)]
mod tests {
    use super::{ema_step, exceeds_hysteresis};

    /// Table-driven: `(last, sample, expected_smoothed)`, one row per
    /// branch of the asymmetric damping rule.
    #[test]
    fn ema_step_table() {
        let cases: &[(u64, u64, u64)] = &[
            // No change.
            (1000, 1000, 1000),
            // Rise: delta=100, damped by /10 -> +10.
            (1000, 1100, 1010),
            // Fall: delta=-300, damped by /3 -> -100.
            (1000, 700, 900),
            // Small rise below the /10 integer-division floor now rounds
            // away from zero to +1 instead of stalling at 0 (the fixed
            // defect -- see `ema_step`'s own doc).
            (1000, 1005, 1001),
            // Small fall similarly rounds away from zero to -1.
            (1000, 998, 999),
            // Large rise from zero.
            (0, 500, 50),
        ];
        for &(last, sample, expected) in cases {
            assert_eq!(
                ema_step(last, sample),
                expected,
                "ema_step({last}, {sample})"
            );
        }
    }

    #[test]
    fn ema_step_stays_within_sample_and_last() {
        // A rise never overshoots the sample; a fall never undershoots it
        // (damping only ever shrinks `|delta|`, it cannot flip its sign) —
        // in particular this means the result can never go negative for a
        // non-negative `sample`.
        for &(last, sample) in &[(10u64, 0u64), (1, 1_000_000), (500, 0), (0, 0)] {
            let result = ema_step(last, sample);
            if sample >= last {
                assert!(
                    result >= last && result <= sample,
                    "rise {last}->{sample} = {result}"
                );
            } else {
                assert!(
                    result <= last && result >= sample,
                    "fall {last}->{sample} = {result}"
                );
            }
        }
    }

    /// Pins the fixed defect directly: repeatedly sampling the *same* true
    /// RTT used to stall forever once `|delta| < 10` (rise) with the old
    /// plain-integer-division damping -- `last` would sit 1-9 units short
    /// of `sample` indefinitely. It must now converge exactly, in a bounded
    /// number of ticks.
    #[test]
    fn ema_step_converges_instead_of_stalling_on_a_sub_floor_delta() {
        for &(start, target) in &[(1000u64, 1009u64), (1000, 991), (1000, 1001), (1000, 999)] {
            let mut smoothed = start;
            for _ in 0..20 {
                if smoothed == target {
                    break;
                }
                smoothed = ema_step(smoothed, target);
            }
            assert_eq!(
                smoothed, target,
                "ema_step({start}, {target}) repeated must converge, stalled at {smoothed}"
            );
        }
    }

    /// Table-driven hysteresis gate: `(published, smoothed, expect_publish)`.
    #[test]
    fn hysteresis_table() {
        let cases: &[(u64, u64, bool)] = &[
            (1000, 500, false), // smoothed*2 == published -> not below (lower bound stays strict)
            (1000, 2000, true), // smoothed == published*2 -> now exceeds (the fixed defect)
            (1000, 499, true),  // just below half
            (1000, 2001, true), // just above double
            (1000, 1000, false), // no change at all, regardless of boundary math
            (1000, 900, false),
            (0, 0, false), // degenerate zero baseline: no change must never exceed
        ];
        for &(published, smoothed, expected) in cases {
            assert_eq!(
                exceeds_hysteresis(published, smoothed),
                expected,
                "exceeds_hysteresis({published}, {smoothed})"
            );
        }
    }

    /// Pins the fixed defect directly: a link that degrades to *exactly*
    /// 2x its published cost must be published, not permanently invisible.
    #[test]
    fn hysteresis_publishes_at_exactly_double_the_published_cost() {
        assert!(exceeds_hysteresis(1000, 2000));
        assert!(exceeds_hysteresis(1, 2));
    }

    /// The gate genuinely suppresses sub-threshold drift: a sequence of
    /// small rises that individually never cross 2x the *original*
    /// published value must never trigger a republish.
    #[test]
    fn hysteresis_suppresses_sub_threshold_drift() {
        let published: u64 = 1000;
        let mut smoothed = published;
        for sample in [1010, 1020, 1500, 1999] {
            smoothed = ema_step(smoothed, sample);
            assert!(
                !exceeds_hysteresis(published, smoothed),
                "smoothed={smoothed} unexpectedly exceeded hysteresis around published={published}"
            );
        }
        // But a genuine jump past 2x does trigger it.
        smoothed = ema_step(smoothed, 10_000);
        assert!(exceeds_hysteresis(published, smoothed));
    }
}

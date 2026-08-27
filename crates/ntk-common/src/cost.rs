//! `Cost`: the ETP path cost/metric type.

use std::fmt;

/// A QSPN path cost. Mirrors the three-way split found in `qspn/api.vala`
/// between the `IQspnCost` interface and its two built-in sentinels:
///
/// - [`Cost::Null`] — the zero/identity cost of a trivial path (`NullCost`,
///   `research/impl/vala/qspn/api.vala:52-81`): used e.g. for the intrinsic
///   zero-cost path to a direct sender synthesized in `revise_etp`
///   (`research/notes/01-vala-core-routing.md` §3 rule 3).
/// - [`Cost::Finite`] — an ordinary, additive path cost. Upstream leaves the
///   concrete metric to the deployment (`IQspnCost`, `research/impl/vala/qspn/api.vala:43-50`);
///   the one committed reference implementation measures microsecond RTT
///   (`research/impl/vala/qspn/testsuites/system_peer/serializables.vala:299-350`).
///   This crate keeps the unit opaque to callers (a plain magnitude) rather than
///   hard-coding "microseconds", since `research/notes/03-specs-and-rfcs.md`'s
///   RFC 0002 entry notes cost is meant to be a pluggable metric at the QSPN
///   boundary. Committing to one concrete `Cost` type (rather than a bare trait)
///   is this crate's own choice, made because the assignment calls for "the ETP
///   path cost metric type" as a concrete base type other crates can share;
///   deployments needing a different unit convert into/out of it at their
///   boundary.
/// - [`Cost::Dead`] — the absorbing "unreachable" cost (`DeadCost`,
///   `research/impl/vala/qspn/api.vala:83-112`).
///
/// The derived [`Ord`] gives exactly the upstream total order: `Null < Finite(_)
/// < Dead` for cross-variant comparisons (any real cost sorts strictly between
/// the two sentinels, `qspn/testsuites/system_peer/serializables.vala:308-317`),
/// and numeric order between two `Finite` costs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Cost {
    /// Zero/identity cost.
    Null,
    /// A finite, additive cost magnitude.
    Finite(u64),
    /// Absorbing "unreachable" cost.
    Dead,
}

impl Cost {
    /// True if this is the absorbing "unreachable" cost (`i_qspn_is_dead`,
    /// `research/impl/vala/qspn/api.vala:48`).
    pub fn is_dead(self) -> bool {
        matches!(self, Cost::Dead)
    }

    /// True if this is the zero/identity cost (`i_qspn_is_null`,
    /// `research/impl/vala/qspn/api.vala:49`).
    pub fn is_null(self) -> bool {
        matches!(self, Cost::Null)
    }

    /// Concatenates two path segments' costs (`i_qspn_add_segment`,
    /// `research/impl/vala/qspn/api.vala:46`; reference semantics
    /// `research/impl/vala/qspn/testsuites/system_peer/serializables.vala:319-326`
    /// plus the `NullCost`/`DeadCost` built-ins, `qspn/api.vala:61-64,92-95`):
    /// `Dead` absorbs unconditionally, `Null` is the identity, and two `Finite`
    /// costs add with saturation so a path's total cost can never wrap around
    /// instead of correctly reporting "very expensive".
    pub fn saturating_add(self, other: Cost) -> Cost {
        match (self, other) {
            (Cost::Dead, _) | (_, Cost::Dead) => Cost::Dead,
            (Cost::Null, x) | (x, Cost::Null) => x,
            (Cost::Finite(a), Cost::Finite(b)) => Cost::Finite(a.saturating_add(b)),
        }
    }
}

impl fmt::Display for Cost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cost::Null => write!(f, "null"),
            Cost::Finite(v) => write!(f, "{v}"),
            Cost::Dead => write!(f, "dead"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_order_is_null_lt_finite_lt_dead() {
        assert!(Cost::Null < Cost::Finite(0));
        assert!(Cost::Finite(u64::MAX) < Cost::Dead);
        assert!(Cost::Null < Cost::Dead);
        assert!(Cost::Finite(3) < Cost::Finite(5));
    }

    #[test]
    fn null_is_the_additive_identity() {
        assert_eq!(Cost::Null.saturating_add(Cost::Finite(7)), Cost::Finite(7));
        assert_eq!(Cost::Finite(7).saturating_add(Cost::Null), Cost::Finite(7));
        assert_eq!(Cost::Null.saturating_add(Cost::Null), Cost::Null);
    }

    #[test]
    fn dead_absorbs_unconditionally() {
        assert_eq!(Cost::Dead.saturating_add(Cost::Null), Cost::Dead);
        assert_eq!(Cost::Null.saturating_add(Cost::Dead), Cost::Dead);
        assert_eq!(Cost::Dead.saturating_add(Cost::Finite(1)), Cost::Dead);
        assert_eq!(Cost::Finite(1).saturating_add(Cost::Dead), Cost::Dead);
    }

    #[test]
    fn finite_addition_saturates_instead_of_wrapping() {
        assert_eq!(
            Cost::Finite(u64::MAX).saturating_add(Cost::Finite(1)),
            Cost::Finite(u64::MAX)
        );
    }
}

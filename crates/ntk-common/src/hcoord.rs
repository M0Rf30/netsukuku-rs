//! Hierarchical coordinate: a (level, position) pair naming one g-node.

use std::fmt;

/// A hierarchical coordinate: g-node position `pos` at hierarchy `level`.
///
/// Direct port of the Vala `HCoord` shared type (`ntkd-common/ntkd_common.vala:21-35`),
/// used throughout QSPN/Hooking to name "the g-node reached via this hop" or "the
/// g-node a destination lives in relative to me" (`IQspnHop.i_qspn_get_hcoord`,
/// `research/impl/vala/qspn/api.vala:124`; see [`crate::Naddr::hcoord`]).
///
/// Upstream only ever compares `HCoord` values for equality
/// (`ntkd_common.vala:31-34`); the total order derived here (by `level` then by
/// `pos`) has no upstream equivalent and is this crate's own addition, useful for
/// putting coordinates in sorted collections.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct HCoord {
    /// The hierarchy level of the named g-node.
    pub level: usize,
    /// The g-node's position among its `gsize(level)` siblings.
    pub pos: u32,
}

impl HCoord {
    /// Builds a coordinate naming the g-node at `pos` within `level`.
    pub fn new(level: usize, pos: u32) -> Self {
        Self { level, pos }
    }
}

impl fmt::Display for HCoord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.pos, self.level)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn orders_by_level_then_position() {
        assert!(HCoord::new(0, 9) < HCoord::new(1, 0));
        assert!(HCoord::new(2, 1) < HCoord::new(2, 5));
        assert_eq!(HCoord::new(2, 5), HCoord::new(2, 5));
    }

    #[test]
    fn displays_as_pos_at_level() {
        assert_eq!(HCoord::new(2, 5).to_string(), "5@2");
    }
}

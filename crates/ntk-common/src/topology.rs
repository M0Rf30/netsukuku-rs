//! Network shape: number of hierarchy levels and each level's g-node size.

use std::sync::Arc;

use crate::error::Error;

/// The shape of a Netsukuku address hierarchy: how many levels it has and how
/// many children (`gsize(i)`) a g-node has at each level.
///
/// The legacy Alpt-era spec fixed this at a single constant, `MAXGROUPNODE = 256`,
/// applied uniformly to every level (`research/impl/c/netsukuku/doc` monolithic
/// draft, cited in `research/notes/03-specs-and-rfcs.md` §Topology/addressing
/// parameters). The current (vala-era) spec generalizes this to an arbitrary
/// per-level size, `gsize(i)` (`IQspnNaddr.i_qspn_get_gsize`,
/// `research/impl/vala/qspn/api.vala:26`; `vala-doc--ita-ModuloQspn-AnalisiFunzionale.md:136-142,355-356`).
/// This type models the generalized, current form; nothing in this crate assumes
/// a uniform or power-of-two size.
///
/// Cloning a `Topology` is cheap (`O(1)`, an `Arc` bump): many [`crate::Naddr`]
/// values in the same network share one topology.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Topology {
    gsizes: Arc<[u32]>,
}

impl Topology {
    /// Builds a topology from a per-level g-node size sequence, index 0 being the
    /// innermost (most local) level and the last index the outermost level this
    /// deployment tracks explicitly.
    ///
    /// # Errors
    /// [`Error::EmptyTopology`] if `gsizes` is empty (a network has at least one
    /// level); [`Error::ZeroGsize`] if any level's size is zero (a g-node with no
    /// possible members is not representable, and legacy/current specs alike
    /// never allow it even though current relaxes the *value* to be arbitrary).
    pub fn new(gsizes: impl IntoIterator<Item = u32>) -> Result<Self, Error> {
        let gsizes: Vec<u32> = gsizes.into_iter().collect();
        if gsizes.is_empty() {
            return Err(Error::EmptyTopology);
        }
        if let Some(level) = gsizes.iter().position(|&g| g == 0) {
            return Err(Error::ZeroGsize { level });
        }
        Ok(Self {
            gsizes: gsizes.into(),
        })
    }

    /// Number of levels in the hierarchy (`i_qspn_get_levels`,
    /// `research/impl/vala/qspn/api.vala:25`).
    pub fn levels(&self) -> usize {
        self.gsizes.len()
    }

    /// The g-node size at `level`, or `None` if `level` is out of range
    /// (`i_qspn_get_gsize`, `research/impl/vala/qspn/api.vala:26`).
    pub fn gsize(&self, level: usize) -> Option<u32> {
        self.gsizes.get(level).copied()
    }

    /// All per-level g-node sizes, index 0 first.
    pub fn gsizes(&self) -> &[u32] {
        &self.gsizes
    }

    /// The total number of distinct leaf addresses this topology can represent
    /// (the product of every level's g-node size), or `None` if that product
    /// overflows a `u128`. This generalizes the legacy fixed-fanout capacity
    /// (`256^levels`, `research/notes/03-specs-and-rfcs.md` §Topology/addressing
    /// parameters, "max-nodes math" row) to arbitrary per-level sizes.
    pub fn max_nodes(&self) -> Option<u128> {
        self.gsizes
            .iter()
            .try_fold(1u128, |acc, &g| acc.checked_mul(u128::from(g)))
    }
}

#[cfg(feature = "serde")]
impl serde::Serialize for Topology {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.gsizes.serialize(serializer)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for Topology {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let gsizes = Vec::<u32>::deserialize(deserializer)?;
        Topology::new(gsizes).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_topology() {
        assert_eq!(Topology::new([]), Err(Error::EmptyTopology));
    }

    #[test]
    fn rejects_zero_gsize() {
        assert_eq!(Topology::new([8, 0, 4]), Err(Error::ZeroGsize { level: 1 }));
    }

    #[test]
    fn accessors_report_shape() {
        let t = Topology::new([8, 16, 4]).unwrap();
        assert_eq!(t.levels(), 3);
        assert_eq!(t.gsize(0), Some(8));
        assert_eq!(t.gsize(2), Some(4));
        assert_eq!(t.gsize(3), None);
        assert_eq!(t.gsizes(), &[8, 16, 4]);
    }

    #[test]
    fn max_nodes_is_the_product_of_gsizes() {
        let t = Topology::new([8, 16, 4]).unwrap();
        assert_eq!(t.max_nodes(), Some(8 * 16 * 4));
    }

    #[test]
    fn max_nodes_saturates_to_none_on_overflow() {
        let t = Topology::new([u32::MAX, u32::MAX, u32::MAX, u32::MAX, u32::MAX]).unwrap();
        assert_eq!(t.max_nodes(), None);
    }

    #[test]
    fn clone_is_a_cheap_handle_to_the_same_data() {
        let t = Topology::new([8, 16]).unwrap();
        let u = t.clone();
        assert_eq!(t, u);
    }
}

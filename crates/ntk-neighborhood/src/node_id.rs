//! [`NodeId`]: the per-identity random discovery id
//! (`NeighborhoodNodeID`, `research/impl/vala/neighborhood/serializables.vala:23-35`).

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

use ntk_proto::domain::{from_typed_value, typed_value};
use ntk_proto::v1::TypedValue;

use crate::error::NeighborhoodError;
use crate::v1;

/// `type_tag` this crate uses for [`NodeId`]'s `TypedValue` payload.
pub const NODE_ID_TAG: &str = "neighborhood.NodeId";

/// A neighbor-discovery node id: a positive, nonzero 32-bit integer chosen
/// at random per identity, used solely to disambiguate neighbors sharing a
/// MAC-collision-prone medium (`NeighborhoodNodeID`,
/// `research/impl/vala/neighborhood/serializables.vala:23-35`). Never
/// interpreted as, or convertible to, a `qspn`/`identities` identity — this
/// module's scope stops at discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(i32);

impl NodeId {
    /// Generates a fresh random id in `[1, i32::MAX)`, matching upstream's
    /// `PRNGen.int_range(1, int.MAX)` (`serializables.vala:27`). Uses
    /// `std::collections::hash_map::RandomState` as an OS-randomness
    /// source rather than pulling in a `rand`-family crate — `RandomState`
    /// is not in `[workspace.dependencies]`'s dependency list either, but
    /// unlike a missing crate this is a `std` facility already guaranteed
    /// present, so no escalation applies.
    #[must_use]
    pub fn generate() -> Self {
        loop {
            let raw = RandomState::new().build_hasher().finish();
            #[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
            let candidate = (raw as u32 & 0x7FFF_FFFF) as i32;
            if candidate != 0 {
                return Self(candidate);
            }
        }
    }

    /// Validates a raw id against upstream's invariant (positive, nonzero).
    ///
    /// # Errors
    /// Returns [`NeighborhoodError::MalformedWire`] if `id <= 0`.
    pub fn from_raw(id: i32) -> Result<Self, NeighborhoodError> {
        if id <= 0 {
            return Err(NeighborhoodError::MalformedWire(format!(
                "NodeId must be positive, got {id}"
            )));
        }
        Ok(Self(id))
    }

    /// The raw id value.
    #[must_use]
    pub fn get(self) -> i32 {
        self.0
    }

    pub(crate) fn to_typed_value(self) -> TypedValue {
        typed_value(NODE_ID_TAG, &v1::NodeId { id: self.0 })
    }

    /// Decodes and re-validates a peer-supplied `TypedValue` — never trusts
    /// the peer's id is positive just because it decoded.
    pub(crate) fn from_typed_value(tv: &TypedValue) -> Result<Self, NeighborhoodError> {
        let wire: v1::NodeId = from_typed_value(tv, NODE_ID_TAG)?;
        Self::from_raw(wire.id)
    }
}

#[cfg(test)]
mod tests {
    use super::NodeId;

    #[test]
    fn generate_is_positive_and_nonzero() {
        for _ in 0..64 {
            assert!(NodeId::generate().get() > 0);
        }
    }

    #[test]
    fn from_raw_rejects_non_positive() {
        assert!(NodeId::from_raw(0).is_err());
        assert!(NodeId::from_raw(-1).is_err());
        assert!(NodeId::from_raw(1).is_ok());
    }

    #[test]
    fn wire_round_trip() {
        let id = NodeId::from_raw(42).unwrap();
        let tv = id.to_typed_value();
        assert_eq!(NodeId::from_typed_value(&tv).unwrap(), id);
    }
}

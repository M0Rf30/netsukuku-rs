//! Round-trip and hostile-input coverage for `ntk_proto::domain` — the
//! shared `ntk-common` <-> wire codec every phase-2 protocol module codes
//! against.

use ntk_common::{Cost, Fingerprint, FingerprintParts, HCoord, Naddr, Topology};
use ntk_proto::domain::{DomainDecodeError, from_typed_value, typed_value, v1};
use proptest::prelude::*;

fn topology_strategy() -> impl Strategy<Value = Topology> {
    prop::collection::vec(1u32..=32, 1..=6).prop_map(|gsizes| Topology::new(gsizes).unwrap())
}

fn naddr_strategy() -> impl Strategy<Value = Naddr> {
    topology_strategy().prop_flat_map(|topology| {
        let levels = topology.levels();
        prop::collection::vec(any::<u32>(), levels).prop_map(move |raw| {
            let pos: Vec<u32> = raw
                .iter()
                .zip(topology.gsizes())
                .map(|(&r, &g)| r % g)
                .collect();
            Naddr::new(topology.clone(), pos).unwrap()
        })
    })
}

fn cost_strategy() -> impl Strategy<Value = Cost> {
    prop_oneof![
        Just(Cost::Null),
        any::<u64>().prop_map(Cost::Finite),
        Just(Cost::Dead),
    ]
}

/// Arbitrary *valid* `FingerprintParts`: `elderships_seed` is generated with
/// exactly `level` entries (the one structural invariant `from_parts`
/// enforces), each independently possibly virtual (`None`) — this exercises
/// both the plain "some entries virtual" case and, via `prop::option::of`,
/// the level-0 "own eldership virtual" case directly.
fn fingerprint_parts_strategy() -> impl Strategy<Value = FingerprintParts<Vec<u8>>> {
    (0usize..=5).prop_flat_map(|level| {
        (
            prop::collection::vec(any::<u8>(), 0..8),
            prop::option::of(any::<u32>()),
            prop::collection::vec(any::<u32>(), 0..5),
            prop::collection::vec(prop::option::of(any::<u32>()), level),
        )
            .prop_map(
                move |(id, eldership, pending_elderships, elderships_seed)| FingerprintParts {
                    id,
                    level,
                    eldership,
                    pending_elderships,
                    elderships_seed,
                },
            )
    })
}

proptest! {
    #[test]
    fn topology_round_trips(topology in topology_strategy()) {
        let wire = v1::Topology::from(&topology);
        let back = Topology::try_from(&wire).unwrap();
        prop_assert_eq!(topology, back);
    }

    #[test]
    fn naddr_round_trips(naddr in naddr_strategy()) {
        let wire = v1::Naddr::from(&naddr);
        let back = Naddr::try_from(&wire).unwrap();
        prop_assert_eq!(naddr, back);
    }

    #[test]
    fn hcoord_round_trips(level in 0usize..10_000, pos in any::<u32>()) {
        let hcoord = HCoord::new(level, pos);
        let wire = v1::HCoord::from(hcoord);
        let back = HCoord::try_from(&wire).unwrap();
        prop_assert_eq!(hcoord, back);
    }

    #[test]
    fn cost_round_trips(cost in cost_strategy()) {
        let wire = v1::Cost::from(cost);
        let back = Cost::try_from(&wire).unwrap();
        prop_assert_eq!(cost, back);
    }

    #[test]
    fn fingerprint_round_trips(parts in fingerprint_parts_strategy()) {
        let fingerprint = Fingerprint::from_parts(parts).unwrap();
        let wire = v1::Fingerprint::from(&fingerprint);
        let back = Fingerprint::try_from(&wire).unwrap();
        prop_assert_eq!(fingerprint.to_parts(), back.to_parts());
    }
}

/// A `Fingerprint` whose own current-level eldership is virtual/null — only
/// reachable via [`Fingerprint::from_parts`] directly (neither `new` nor
/// `construct` ever store `None`/`-1` in the *own* eldership field, only in
/// `elderships_seed`; see the module docs on `ntk_common::FingerprintParts`).
/// Proves the sentinel survives an actual wire round trip rather than being
/// silently coerced to a real claim.
#[test]
fn fingerprint_round_trip_covers_virtual_own_eldership() {
    let parts = FingerprintParts {
        id: b"node-a".to_vec(),
        level: 0,
        eldership: None,
        pending_elderships: vec![7, 9],
        elderships_seed: vec![],
    };
    let fingerprint = Fingerprint::from_parts(parts.clone()).unwrap();

    let wire = v1::Fingerprint::from(&fingerprint);
    assert_eq!(
        wire.eldership, None,
        "virtual own eldership must not leak a raw sentinel onto the wire"
    );

    let back = Fingerprint::try_from(&wire).unwrap();
    assert_eq!(back.to_parts(), parts);
}

/// A two-level aggregation where the level-0 -> level-1 step is a virtual
/// win (`is_null_eldership = true`), then level-1 -> level-2 aggregates
/// again with a real claim. The resulting `elderships_seed` is
/// `[Some(10), None]`: index 0 is the freshly-pushed real claim, index 1 is
/// the *inherited* virtual entry from the first aggregation. Proves the
/// wire codec preserves an inherited virtual entry buried inside a
/// non-empty seed, not just a virtual entry at the head.
#[test]
fn fingerprint_round_trip_covers_inherited_virtual_seed_entry() {
    let f0 = Fingerprint::new(b"node-b".to_vec(), 5, vec![10, 20]);
    let f1 = f0.construct(&[], true).unwrap();
    assert_eq!(f1.to_parts().elderships_seed, vec![None]);

    let f2 = f1.construct(&[], false).unwrap();
    let parts2 = f2.to_parts();
    assert_eq!(parts2.level, 2);
    assert_eq!(parts2.elderships_seed, vec![Some(10), None]);

    let wire = v1::Fingerprint::from(&f2);
    assert_eq!(
        wire.elderships_seed
            .iter()
            .map(|e| e.value)
            .collect::<Vec<_>>(),
        vec![Some(10), None]
    );

    let back = Fingerprint::try_from(&wire).unwrap();
    assert_eq!(back.to_parts(), parts2);
}

// ---------------------------------------------------------------------------
// Hostile / corrupt wire input: every case below must be REJECTED, never
// silently coerced into a valid-looking domain value.
// ---------------------------------------------------------------------------

/// A position at or above its level's `gsize` is *virtual*, not malformed: it is
/// how a g-node mid-migration describes itself before its entry completes, and
/// `ntk-qspn`'s entering identities put exactly such an address on the wire. So
/// the codec must accept it — rejecting it here would make migration traffic
/// undecodable the moment it crossed a real socket, while still passing every
/// fake-transport test. (This test previously asserted the opposite, before
/// virtual addressing existed.)
#[test]
fn accepts_a_virtual_position_because_it_is_a_real_protocol_state() {
    let wire = v1::Naddr {
        topology: Some(v1::Topology { gsizes: vec![2, 3] }),
        pos: vec![1, 3], // 3 >= gsize(1) == 3, i.e. virtual at level 1
    };
    let decoded = Naddr::try_from(&wire).expect("a virtual position decodes");
    assert!(decoded.is_virtual());
    assert_eq!(decoded.is_virtual_at(1), Some(true));
    assert_eq!(decoded.is_virtual_at(0), Some(false));
    assert_eq!(decoded.positions(), [1, 3]);
}

/// The structural check is retained: a peer cannot send an address whose level
/// count disagrees with the topology it carries.
#[test]
fn rejects_a_position_vector_of_the_wrong_length() {
    let wire = v1::Naddr {
        topology: Some(v1::Topology { gsizes: vec![2, 3] }),
        pos: vec![1],
    };
    let err = Naddr::try_from(&wire).unwrap_err();
    assert!(matches!(
        err,
        DomainDecodeError::Invalid(ntk_common::Error::LevelCountMismatch { .. })
    ));
}

#[test]
fn rejects_zero_gsize() {
    let wire = v1::Topology { gsizes: vec![4, 0] };
    let err = Topology::try_from(&wire).unwrap_err();
    assert!(matches!(
        err,
        DomainDecodeError::Invalid(ntk_common::Error::ZeroGsize { level: 1 })
    ));
}

#[test]
fn rejects_empty_topology() {
    let wire = v1::Topology { gsizes: vec![] };
    let err = Topology::try_from(&wire).unwrap_err();
    assert!(matches!(
        err,
        DomainDecodeError::Invalid(ntk_common::Error::EmptyTopology)
    ));
}

#[test]
fn rejects_naddr_missing_topology() {
    let wire = v1::Naddr {
        topology: None,
        pos: vec![1, 2],
    };
    let err = Naddr::try_from(&wire).unwrap_err();
    assert_eq!(err, DomainDecodeError::MissingTopology);
}

#[test]
fn rejects_cost_with_absent_oneof() {
    let wire = v1::Cost { value: None };
    let err = Cost::try_from(&wire).unwrap_err();
    assert_eq!(err, DomainDecodeError::MissingCostValue);
}

/// `HCoord` carries no `Topology` of its own to revalidate a decoded `level`/`pos` against — by
/// design (see this crate's `domain.rs`, `TryFrom<&v1::HCoord>`'s doc comment): it names a bare
/// coordinate, and checking it against a g-node hierarchy is necessarily the job of whichever
/// topology-aware caller combines it with one (e.g. `ntk-peerservices`'s wire codec, which bounds
/// `PeerMessageForwarder.lvl`/`.pos` against its own `Topology` before ever building an `HCoord`
/// from them). An extreme `level`/`pos` therefore decodes successfully here — not because it is
/// meaningful, but because this layer has nothing to check it against.
#[test]
fn hcoord_decodes_an_extreme_level_because_it_has_no_topology_to_check_it_against() {
    let wire = v1::HCoord {
        level: u64::MAX,
        pos: u32::MAX,
    };
    let hcoord = HCoord::try_from(&wire).unwrap();
    assert_eq!(hcoord.level, u64::MAX as usize);
    assert_eq!(hcoord.pos, u32::MAX);
}

#[test]
fn rejects_fingerprint_with_seed_length_mismatch() {
    let wire = v1::Fingerprint {
        id: b"x".to_vec(),
        level: 2,
        eldership: Some(1),
        pending_elderships: vec![],
        elderships_seed: vec![], // level 2 requires exactly 2 entries
    };
    let err = Fingerprint::try_from(&wire).unwrap_err();
    assert!(matches!(
        err,
        DomainDecodeError::Invalid(ntk_common::Error::FingerprintSeedLength {
            level: 2,
            seed_len: 0
        })
    ));
}

#[test]
fn typed_value_round_trips_and_verifies_type_tag() {
    let topology = Topology::new([2, 3]).unwrap();
    let wire = v1::Topology::from(&topology);
    let tv = typed_value("domain.Topology", &wire);

    let decoded: v1::Topology = from_typed_value(&tv, "domain.Topology").unwrap();
    assert_eq!(decoded, wire);

    let err = from_typed_value::<v1::Topology>(&tv, "domain.WrongTag").unwrap_err();
    assert_eq!(
        err,
        DomainDecodeError::TypeTagMismatch {
            expected: "domain.WrongTag".to_owned(),
            actual: "domain.Topology".to_owned(),
        }
    );
}

#[test]
fn typed_value_rejects_truncated_payload() {
    let topology = Topology::new([2, 3, 4]).unwrap();
    let wire = v1::Topology::from(&topology);
    let mut tv = typed_value("domain.Topology", &wire);
    tv.payload.truncate(1); // corrupt: not a valid encoding anymore
    // A single leftover byte from a longer varint-prefixed field is an
    // incomplete/invalid encoding prost must reject, not silently accept.
    let err = from_typed_value::<v1::Topology>(&tv, "domain.Topology").unwrap_err();
    assert!(matches!(err, DomainDecodeError::PayloadDecode(_)));
}

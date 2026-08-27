//! Participation maps: which g-nodes are known to participate in which service, and the
//! flood-gossip fold/merge algorithm that keeps `retrieved_below_level` a monotonic freshness
//! marker (`research/impl/vala/peerservices/map_handler.vala`,
//! `research/notes/02-vala-services-daemon.md` §3 "Participation maps/gossip").

use std::collections::{BTreeMap, BTreeSet};

use ntk_common::{HCoord, Topology};

use crate::service::ServiceId;

/// One service's known-participant g-nodes (`PeerParticipantMap`,
/// `research/impl/vala/peerservices/serializables.vala:362-424`). A [`BTreeSet`] gives dedup and
/// deterministic iteration order for free — upstream's `ArrayList` with a hand-rolled
/// `HCoord.equals` comparator (`peers.vala:184`) has neither.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParticipantMap {
    participants: BTreeSet<HCoord>,
}

impl ParticipantMap {
    /// An empty participant map.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Every known participant g-node, in `(level, pos)` order.
    pub fn participants(&self) -> impl Iterator<Item = HCoord> + '_ {
        self.participants.iter().copied()
    }

    /// True if `h` is known to participate.
    #[must_use]
    pub fn contains(&self, h: HCoord) -> bool {
        self.participants.contains(&h)
    }

    /// True if no g-node is known to participate.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.participants.is_empty()
    }

    /// How many g-nodes are known to participate.
    #[must_use]
    pub fn len(&self) -> usize {
        self.participants.len()
    }

    /// Records `h` as a participant. Returns `true` if this was new information
    /// (`add_participant`, `research/impl/vala/peerservices/peers.vala:325-332`).
    pub fn insert(&mut self, h: HCoord) -> bool {
        self.participants.insert(h)
    }

    /// Forgets `h` as a participant. Returns `true` if it had been recorded
    /// (`remove_participant`, `peers.vala:333-342`).
    pub fn remove(&mut self, h: HCoord) -> bool {
        self.participants.remove(&h)
    }

    /// True if every recorded coordinate is a representable level/position in `topology`
    /// (`check_valid`, `research/impl/vala/peerservices/serializables.vala:413-423`).
    #[must_use]
    pub fn is_valid(&self, topology: &Topology) -> bool {
        self.participants
            .iter()
            .all(|hc| topology.gsize(hc.level).is_some_and(|gsize| hc.pos < gsize))
    }
}

impl FromIterator<HCoord> for ParticipantMap {
    fn from_iter<T: IntoIterator<Item = HCoord>>(iter: T) -> Self {
        Self {
            participants: iter.into_iter().collect(),
        }
    }
}

/// The full per-service participation snapshot exchanged between neighbors
/// (`PeerParticipantSet`, `research/impl/vala/peerservices/serializables.vala:426-519`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParticipantSet {
    /// Every level below this one is known to be fully accurate in this snapshot — the gossip
    /// protocol's monotonic freshness marker (`map_handler.vala:72,109-125`).
    pub retrieved_below_level: usize,
    /// The address of the node that produced this snapshot. May contain virtual (out-of-range)
    /// positions for a node mid-migration — deliberately not validated against `topology`,
    /// matching upstream's own `check_valid` (`serializables.vala:511-517`, "NOT MANDATORY...
    /// because may be a virtual node").
    pub my_pos: Vec<u32>,
    /// Known participants per service.
    pub participant_set: BTreeMap<ServiceId, ParticipantMap>,
}

impl ParticipantSet {
    /// True if `retrieved_below_level` is in range, `my_pos` has exactly `topology.levels()`
    /// entries, and every service's map is valid (`check_valid`, `serializables.vala:503-518`).
    #[must_use]
    pub fn is_valid(&self, topology: &Topology) -> bool {
        self.retrieved_below_level <= topology.levels()
            && self.my_pos.len() == topology.levels()
            && self.participant_set.values().all(|m| m.is_valid(topology))
    }
}

/// Filters `participant_set` down to entries below `below_level`, dropping any service left with
/// no participants (`produce_maps_below_level`,
/// `research/impl/vala/peerservices/map_handler.vala:106-125`). This is what a node sends when
/// asked for its participation knowledge, or forwards on to a neighbor after a gossip update.
#[must_use]
pub fn produce_below_level(
    participant_set: &BTreeMap<ServiceId, ParticipantMap>,
    my_pos: &[u32],
    below_level: usize,
) -> ParticipantSet {
    let mut out = BTreeMap::new();
    for (&p_id, map) in participant_set {
        let filtered: ParticipantMap = map
            .participants()
            .filter(|hc| hc.level < below_level)
            .collect();
        if !filtered.is_empty() {
            out.insert(p_id, filtered);
        }
    }
    ParticipantSet {
        retrieved_below_level: below_level,
        my_pos: my_pos.to_vec(),
        participant_set: out,
    }
}

/// Re-expresses a neighbor's `incoming` snapshot at the granularity of my own address: finds the
/// highest level at which my address and `incoming.my_pos` still agree (the "maximum distinct
/// g-node"), then folds every entry below that level into a single coordinate at that level —
/// the sender's detailed knowledge below our common ancestor is more precision than I need,
/// since from my point of view the whole thing is "that one g-node of mine"
/// (`copy_and_forward`'s folding half, `research/impl/vala/peerservices/map_handler.vala:244-269`).
///
/// The result still carries `incoming.retrieved_below_level` unchanged; applying it (merging new
/// levels into my own knowledge and bumping my own `retrieved_below_level`) is the caller's
/// job — this function only reshapes the data, it does not decide freshness.
///
/// # Panics
/// If `my_pos.len() != levels` or `incoming.my_pos.len() != levels`, or if `levels == 0`.
#[must_use]
pub fn fold_to_my_granularity(
    my_pos: &[u32],
    levels: usize,
    mut incoming: ParticipantSet,
) -> ParticipantSet {
    assert_eq!(my_pos.len(), levels);
    assert_eq!(incoming.my_pos.len(), levels);
    assert!(levels > 0);

    let mut mdg_lvl = levels - 1;
    while mdg_lvl > 0 && my_pos[mdg_lvl] == incoming.my_pos[mdg_lvl] {
        mdg_lvl -= 1;
    }
    let mdg_pos = incoming.my_pos[mdg_lvl];

    let mut mdg_services = BTreeSet::new();
    for (&p_id, map) in &incoming.participant_set {
        if map.participants().any(|hc| hc.level < mdg_lvl) {
            mdg_services.insert(p_id);
        }
    }
    for map in incoming.participant_set.values_mut() {
        *map = map
            .participants()
            .filter(|hc| hc.level >= mdg_lvl)
            .collect();
    }
    for p_id in mdg_services {
        incoming
            .participant_set
            .entry(p_id)
            .or_default()
            .insert(HCoord::new(mdg_lvl, mdg_pos));
    }
    incoming
}

/// True if `incoming` is worth merging at all: strictly fresher than what I already have
/// (`give_participant_maps`'s freshness gate, `research/impl/vala/peerservices/map_handler.vala:238-242`,
/// and `retrieve_participant_set`'s, `map_handler.vala:229-233`).
#[must_use]
pub fn is_fresher(my_retrieved_below_level: usize, incoming: &ParticipantSet) -> bool {
    incoming.retrieved_below_level > my_retrieved_below_level
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology(gsizes: &[u32]) -> Topology {
        Topology::new(gsizes.iter().copied()).unwrap()
    }

    #[test]
    fn produce_below_level_drops_deeper_and_empty_entries() {
        let sid = ServiceId::new(1);
        let mut map = ParticipantMap::new();
        map.insert(HCoord::new(0, 1));
        map.insert(HCoord::new(2, 1));
        let mut set = BTreeMap::new();
        set.insert(sid, map);
        let below = produce_below_level(&set, &[0, 0, 0], 1);
        assert_eq!(below.retrieved_below_level, 1);
        assert!(below.participant_set[&sid].contains(HCoord::new(0, 1)));
        assert!(!below.participant_set[&sid].contains(HCoord::new(2, 1)));
    }

    #[test]
    fn produce_below_level_drops_service_with_no_remaining_participants() {
        let sid = ServiceId::new(7);
        let mut map = ParticipantMap::new();
        map.insert(HCoord::new(3, 0));
        let mut set = BTreeMap::new();
        set.insert(sid, map);
        let below = produce_below_level(&set, &[0, 0, 0, 0], 2);
        assert!(!below.participant_set.contains_key(&sid));
    }

    #[test]
    fn fold_merges_divergent_levels_into_one_coordinate() {
        // I am at [1,1,1]; neighbor at [1,0,1] diverges from me first at level 1 -> mdg_lvl=1.
        let sid = ServiceId::new(3);
        let mut map = ParticipantMap::new();
        map.insert(HCoord::new(0, 2)); // below mdg_lvl(1) -> folded into (1, mdg_pos)
        let mut set = BTreeMap::new();
        set.insert(sid, map);
        let incoming = ParticipantSet {
            retrieved_below_level: 3,
            my_pos: vec![1, 0, 1],
            participant_set: set,
        };
        let folded = fold_to_my_granularity(&[1, 1, 1], 3, incoming);
        assert!(folded.participant_set[&sid].contains(HCoord::new(1, 0)));
        assert!(!folded.participant_set[&sid].contains(HCoord::new(0, 2)));
    }

    #[test]
    fn fold_is_idempotent_when_reapplied_to_its_own_output() {
        let sid = ServiceId::new(9);
        let mut map = ParticipantMap::new();
        map.insert(HCoord::new(1, 0));
        let mut set = BTreeMap::new();
        set.insert(sid, map);
        let incoming = ParticipantSet {
            retrieved_below_level: 4,
            my_pos: vec![0, 0, 1, 1],
            participant_set: set,
        };
        let once = fold_to_my_granularity(&[1, 1, 1, 1], 4, incoming.clone());
        let twice = fold_to_my_granularity(&[1, 1, 1, 1], 4, once.clone());
        assert_eq!(once, twice);
    }

    proptest::proptest! {
        /// Merging a set of participation facts twice (or in any order) yields the same map as
        /// merging it once — the property `add_participant`-driven gossip application relies on
        /// to be safe against duplicate/reordered flood delivery.
        #[test]
        fn participant_map_merge_is_idempotent_and_order_independent(
            facts in proptest::collection::vec((0usize..4, 0u32..6), 0..12),
        ) {
            let coords: Vec<HCoord> = facts.iter().map(|&(level, pos)| HCoord::new(level, pos)).collect();

            let once: ParticipantMap = coords.iter().copied().collect();

            let mut twice = ParticipantMap::new();
            for h in &coords {
                twice.insert(*h);
            }
            for h in &coords {
                twice.insert(*h);
            }

            let mut reversed = ParticipantMap::new();
            for h in coords.iter().rev() {
                reversed.insert(*h);
            }

            proptest::prop_assert_eq!(&once, &twice);
            proptest::prop_assert_eq!(&once, &reversed);
        }
    }

    #[test]
    fn is_valid_rejects_out_of_range_position() {
        let t = topology(&[2, 2]);
        let mut map = ParticipantMap::new();
        map.insert(HCoord::new(0, 5));
        assert!(!map.is_valid(&t));
    }

    #[test]
    fn freshness_rule_is_strictly_monotonic() {
        let set = ParticipantSet {
            retrieved_below_level: 2,
            my_pos: vec![0, 0],
            participant_set: BTreeMap::new(),
        };
        assert!(!is_fresher(2, &set));
        assert!(!is_fresher(3, &set));
        assert!(is_fresher(1, &set));
    }
}

//! `Fingerprint`: g-node identity and eldership, as used by QSPN split/merge
//! detection.

use crate::error::Error;

/// A g-node's identity fingerprint: an opaque origin `Id` plus the eldership
/// bookkeeping QSPN uses to decide which branch of a network split is
/// authoritative.
///
/// Faithful port of the reference `IQspnFingerprint` implementation
/// (`research/impl/vala/qspn/api.vala:35-41`,
/// `research/impl/vala/qspn/testsuites/system_peer/serializables.vala:110-296`).
/// Per this crate's assignment, the identity value itself is left generic over
/// `Id` rather than committing to a hash algorithm — QSPN (phase 2) picks the
/// concrete type (a random 64-bit value in the Vala reference, a content hash,
/// or anything else with the bounds below).
///
/// A level-0 `Fingerprint` names a single real node (built with [`Fingerprint::new`]).
/// [`Fingerprint::construct`] aggregates one level's worth of sibling fingerprints
/// into the next level's fingerprint, exactly mirroring how `qspn.vala`'s
/// `update_clusters` builds `my_fingerprints[i]` from `my_fingerprints[i-1]` and
/// the best-path fingerprints of every known level-`(i-1)` destination
/// (`research/impl/vala/qspn/qspn.vala:1954-2074`).
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Fingerprint<Id> {
    id: Id,
    level: usize,
    /// This fingerprint's own eldership claim at `level`; `-1` is upstream's
    /// `is_null_eldership` sentinel (virtual position, no claim). Never exposed
    /// directly — see [`Fingerprint::construct`]'s `is_null_eldership` parameter.
    eldership: i64,
    /// Not-yet-consumed eldership counters for levels above `level`, always real
    /// (only the *current* level's claim can ever be virtual).
    pending_elderships: Vec<u32>,
    /// Trail of the winning branch's own eldership claim recorded at each level
    /// aggregated so far, most recently aggregated level first. May contain `-1`
    /// entries inherited from a virtual win (see
    /// `virtual_eldership_wins_unconditionally_over_real_siblings` below).
    elderships_seed: Vec<i64>,
}

/// True if `candidate`'s own-level eldership claim outranks `current`'s in the
/// per-level aggregation race (private `elder`,
/// `research/impl/vala/qspn/testsuites/system_peer/serializables.vala:212-229`).
/// Lower claims are more senior ("eldest"); ties favor `candidate` — this is why
/// [`Fingerprint::construct`] is *not* associative/order-independent in general
/// (see the module tests). A virtual (`-1`) `candidate` never outranks anything;
/// symmetrically, once `current` is virtual it can never be outranked, even by
/// another virtual candidate — a literal, if counter-intuitive, translation of
/// the upstream comparison.
fn elder_claim_outranks(candidate: i64, current: i64) -> bool {
    if candidate == -1 {
        return false;
    }
    current >= candidate
}

/// Plain-data view of every field inside a [`Fingerprint`], for serialization
/// across a crate boundary that must not gain the ability to construct an
/// invalid `Fingerprint` (e.g. `ntk-proto`'s wire codec, decoding bytes from
/// an untrusted peer). Produced by [`Fingerprint::to_parts`], consumed (with
/// revalidation) by [`Fingerprint::from_parts`] — together these are the only
/// way to get a `Fingerprint`'s full state out of or into the type without
/// depending on this crate's private field layout.
///
/// The `-1` `is_null_eldership` sentinel documented on [`Fingerprint`]'s
/// private `eldership` field is never exposed here: both `eldership` and each
/// `elderships_seed` entry use `Option<u32>`, `None` meaning "virtual, no
/// claim". This keeps the sentinel purely an implementation detail of this
/// crate — nothing outside it ever sees a raw negative number, and there is
/// no way to misuse the type to encode an out-of-domain negative eldership.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FingerprintParts<Id> {
    /// See [`Fingerprint::id`].
    pub id: Id,
    /// See [`Fingerprint::level`].
    pub level: usize,
    /// This fingerprint's own eldership claim at `level`. `None` is the
    /// virtual/null-eldership case.
    pub eldership: Option<u32>,
    /// Not-yet-consumed eldership counters for levels above `level`, most
    /// local (next to be consumed) first. Always real: only the *current*
    /// level's own claim can ever be virtual.
    pub pending_elderships: Vec<u32>,
    /// Trail of the winning branch's own eldership claim recorded at each
    /// level aggregated so far, most recently aggregated level first. A
    /// `None` entry is an inherited virtual win (see
    /// [`Fingerprint::construct`]'s `is_null_eldership` parameter).
    pub elderships_seed: Vec<Option<u32>>,
}

/// `-1` sentinel <-> `None`, in either direction. The only two kinds of value
/// this crate ever stores in an `eldership`-shaped `i64` are `-1` and a
/// non-negative value that started life as a `u32`, so the conversion is
/// exact both ways.
fn eldership_to_option(raw: i64) -> Option<u32> {
    u32::try_from(raw).ok()
}

fn eldership_from_option(value: Option<u32>) -> i64 {
    value.map_or(-1, i64::from)
}

impl<Id: Clone + PartialEq> Fingerprint<Id> {
    /// A fresh level-0 fingerprint for a single real node: its own persistent
    /// `id`, its eldership claim at level 0, and one eldership counter per level
    /// above 0 that later [`Fingerprint::construct`] calls will consume one at a
    /// time.
    pub fn new(id: Id, eldership: u32, pending_elderships: impl Into<Vec<u32>>) -> Self {
        Self {
            id,
            level: 0,
            eldership: i64::from(eldership),
            pending_elderships: pending_elderships.into(),
            elderships_seed: Vec::new(),
        }
    }

    /// The hierarchy level this fingerprint aggregates (`i_qspn_get_level`,
    /// `research/impl/vala/qspn/api.vala:38`).
    pub fn level(&self) -> usize {
        self.level
    }

    /// The origin identity propagated from whichever member ultimately won every
    /// eldership race up to this level.
    pub fn id(&self) -> &Id {
        &self.id
    }

    /// `i_qspn_equals` (`research/impl/vala/qspn/api.vala:37`): same origin
    /// identity at the same level. Unlike the Vala reference, which asserts equal
    /// levels and panics otherwise
    /// (`research/impl/vala/qspn/testsuites/system_peer/serializables.vala:199-210`),
    /// this folds the level into the comparison so the function is total.
    pub fn identity_eq(&self, other: &Fingerprint<Id>) -> bool {
        self.level == other.level && self.id == other.id
    }

    /// Aggregates this fingerprint (my own fingerprint one level down) with the
    /// fingerprints of every other known destination at the same level into the
    /// next level's fingerprint (`i_qspn_construct`,
    /// `research/impl/vala/qspn/api.vala:39`). `siblings` must not include `self`.
    ///
    /// `is_null_eldership` overrides *this* fingerprint's own claim to virtual
    /// for this call only, matching `update_clusters`'s per-call recomputation
    /// from live topology state (`my_naddr.i_qspn_get_pos(i-1) >= gsizes[i-1]`,
    /// `research/impl/vala/qspn/qspn.vala:1962,2010`) rather than baking a fixed
    /// virtual/real flag into the fingerprint's stored state.
    ///
    /// # Errors
    /// [`Error::TopOfHierarchy`] if this fingerprint has no more levels to climb.
    pub fn construct(
        &self,
        siblings: &[Fingerprint<Id>],
        is_null_eldership: bool,
    ) -> Result<Fingerprint<Id>, Error> {
        let (&next_eldership, rest) = self
            .pending_elderships
            .split_first()
            .ok_or(Error::TopOfHierarchy)?;

        let my_claim: i64 = if is_null_eldership {
            -1
        } else {
            self.eldership
        };
        let mut champion_claim = my_claim;
        let mut champion_id = &self.id;
        let mut champion_seed: &[i64] = &self.elderships_seed;
        for f in siblings {
            if elder_claim_outranks(f.eldership, champion_claim) {
                champion_claim = f.eldership;
                champion_id = &f.id;
                champion_seed = &f.elderships_seed;
            }
        }

        let mut elderships_seed = Vec::with_capacity(champion_seed.len() + 1);
        elderships_seed.push(champion_claim);
        elderships_seed.extend_from_slice(champion_seed);

        Ok(Fingerprint {
            id: champion_id.clone(),
            level: self.level + 1,
            eldership: i64::from(next_eldership),
            pending_elderships: rest.to_vec(),
            elderships_seed,
        })
    }

    /// Total order used to pick the surviving branch when two fingerprints of the
    /// *same* destination disagree, i.e. a split (`i_qspn_elder_seed`,
    /// `research/impl/vala/qspn/api.vala:40`; reference semantics
    /// `research/impl/vala/qspn/testsuites/system_peer/serializables.vala:231-261`).
    /// `self.elder_seed(other)? == true` means `self` outranks `other` and should
    /// be treated as the eldest.
    ///
    /// Only meaningful above level 0 (a level-0 fingerprint names a single real
    /// node, not an aggregated g-node with competing branches) and between two
    /// fingerprints aggregated to the same level.
    ///
    /// # Errors
    /// [`Error::FingerprintBaseLevel`] at level 0;
    /// [`Error::FingerprintLevelMismatch`] if levels differ;
    /// [`Error::IndistinguishableFingerprints`] if every recorded eldership-seed
    /// entry is equal (upstream treats this as unreachable given well-behaved,
    /// non-colliding identities, `assert_not_reached()` at
    /// `qspn/testsuites/system_peer/serializables.vala:260`).
    pub fn elder_seed(&self, other: &Fingerprint<Id>) -> Result<bool, Error> {
        if self.level == 0 {
            return Err(Error::FingerprintBaseLevel);
        }
        if self.level != other.level {
            return Err(Error::FingerprintLevelMismatch {
                self_level: self.level,
                other_level: other.level,
            });
        }
        for (&mine, &theirs) in self
            .elderships_seed
            .iter()
            .zip(other.elderships_seed.iter())
        {
            match mine.cmp(&theirs) {
                std::cmp::Ordering::Less => return Ok(true),
                std::cmp::Ordering::Greater => return Ok(false),
                std::cmp::Ordering::Equal => continue,
            }
        }
        Err(Error::IndistinguishableFingerprints)
    }

    /// True if `self` and `other` should be treated as naming the *same*
    /// admitted branch of a destination for exposure purposes: either they
    /// share an identity ([`Self::identity_eq`]), or [`Self::elder_seed`]
    /// cannot order them at all *and* both sides currently report more than
    /// one member for the g-node they name.
    ///
    /// That second case is the ordinary — not anomalous — outcome between
    /// two real members of the very same g-node: [`Self::construct`]'s
    /// champion race starts from each member's own fingerprint as the
    /// initial "current" and only lets a *candidate* sibling depose it, so
    /// for exactly two members with tied eldership claims each one
    /// necessarily names the *other* champion (see [`Self::construct`]'s
    /// docs). The two resulting fingerprints then carry different `id`s yet
    /// an identical eldership seed — [`Error::IndistinguishableFingerprints`]
    /// — even though both genuinely describe the one g-node both members
    /// belong to. Callers deciding whether a path is still worth exposing
    /// alongside the destination's current winner use this instead of bare
    /// [`Self::identity_eq`] so that case doesn't wrongly look like a
    /// disagreement between two *different* destinations.
    ///
    /// Any other [`Self::elder_seed`] error (e.g. mismatched levels) is a
    /// genuine bug in the caller's input, not this ordinary case, and is
    /// reported as "not the same branch" rather than swallowed here.
    ///
    /// # `self_nodes_inside`/`other_nodes_inside` scope the tie to a *live* g-node
    /// The rationale above silently assumes a real sibling sat on the other
    /// end of that `construct` call. It does not have to: `construct`-ing
    /// with *no* siblings at all (`siblings = &[]`) also leaves a
    /// fingerprint's own claim and `id` untouched — at the value level,
    /// indistinguishable from the tied-with-a-sibling case above. That
    /// coincidence is exactly what a g-node split produces: after a sever,
    /// every former member independently `construct`s alone (no sibling
    /// left to contend with), and because every node in this codebase
    /// bootstraps eldership at `0` (matching upstream,
    /// `research/impl/vala/qspn/testsuites/system_peer/system_peer.vala:259-260`),
    /// two now-*disconnected* halves derive the same all-zero seed by pure
    /// coincidence, not by a live tie. Folding that into one branch here
    /// would mean a g-node whose members all started at eldership `0` — the
    /// ordinary case, not an edge case — could never be observed to split.
    ///
    /// `self_nodes_inside`/`other_nodes_inside` — each fingerprint's own
    /// reported member count for the g-node it names (the call site's
    /// `EtpPath::nodes_inside`) — resolve the ambiguity: a real, live
    /// 2-member tie has *both* sides reporting the shared g-node's full
    /// membership (`>1`), because both members still see each other; a
    /// solo, no-sibling `construct` reports only itself (`1`). The
    /// indistinguishable-seed fallback above therefore only fires when
    /// *both* sides currently claim more than one member — the literal
    /// "two members of one g-node" case the rest of this doc describes —
    /// never when either side is, numerically, alone.
    #[must_use]
    pub fn same_branch(
        &self,
        other: &Fingerprint<Id>,
        self_nodes_inside: u32,
        other_nodes_inside: u32,
    ) -> bool {
        self.identity_eq(other)
            || (self_nodes_inside > 1
                && other_nodes_inside > 1
                && matches!(
                    self.elder_seed(other),
                    Err(Error::IndistinguishableFingerprints)
                ))
    }

    /// Decomposes this fingerprint into its plain-data [`FingerprintParts`]
    /// for serialization. See that type's docs for why this exists instead
    /// of public field access or a `#[derive(Serialize)]`.
    pub fn to_parts(&self) -> FingerprintParts<Id> {
        FingerprintParts {
            id: self.id.clone(),
            level: self.level,
            eldership: eldership_to_option(self.eldership),
            pending_elderships: self.pending_elderships.clone(),
            elderships_seed: self
                .elderships_seed
                .iter()
                .copied()
                .map(eldership_to_option)
                .collect(),
        }
    }

    /// Rebuilds a fingerprint from [`FingerprintParts`], revalidating the one
    /// structural invariant a decoder cannot otherwise see: `elderships_seed`
    /// holds exactly one entry per level already aggregated, so its length
    /// must equal `level` (empty at level 0; each [`Fingerprint::construct`]
    /// call pushes exactly one entry).
    ///
    /// # Errors
    /// [`Error::FingerprintSeedLength`] if `elderships_seed.len() != level`.
    pub fn from_parts(parts: FingerprintParts<Id>) -> Result<Self, Error> {
        if parts.elderships_seed.len() != parts.level {
            return Err(Error::FingerprintSeedLength {
                level: parts.level,
                seed_len: parts.elderships_seed.len(),
            });
        }
        Ok(Self {
            id: parts.id,
            level: parts.level,
            eldership: eldership_from_option(parts.eldership),
            pending_elderships: parts.pending_elderships,
            elderships_seed: parts
                .elderships_seed
                .into_iter()
                .map(eldership_from_option)
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fp(id: u32, eldership: u32, pending: &[u32]) -> Fingerprint<u32> {
        Fingerprint::new(id, eldership, pending.to_vec())
    }

    #[test]
    fn construct_rejects_top_of_hierarchy() {
        let top = fp(1, 0, &[]);
        assert_eq!(
            top.construct(&[], false).unwrap_err(),
            Error::TopOfHierarchy
        );
    }

    #[test]
    fn construct_picks_the_lowest_eldership_claim() {
        // Lower eldership counter wins ("eldest").
        let me = fp(1, 5, &[0]);
        let sibling_a = fp(2, 3, &[0]);
        let sibling_b = fp(3, 9, &[0]);
        let next = me.construct(&[sibling_a, sibling_b], false).unwrap();
        assert_eq!(*next.id(), 2);
        assert_eq!(next.level(), 1);
    }

    #[test]
    fn construct_ties_favor_the_later_candidate() {
        // Two siblings tie with `me` at eldership 5; the *last* one evaluated
        // wins, an inherited order-dependence of the reference `elder()`.
        let me = fp(1, 5, &[0]);
        let a = fp(2, 5, &[0]);
        let b = fp(3, 5, &[0]);

        let forward = me.construct(&[a.clone(), b.clone()], false).unwrap();
        assert_eq!(*forward.id(), 3, "last candidate should win the tie");

        let backward = me.construct(&[b, a], false).unwrap();
        assert_eq!(
            *backward.id(),
            2,
            "reordering siblings changes the winner on a tie: construct is not order-independent"
        );
    }

    #[test]
    fn virtual_eldership_wins_unconditionally_over_real_siblings() {
        // `is_null_eldership` makes `me`'s own claim virtual (-1). Per the
        // literal upstream comparison, a virtual current champion can never be
        // dethroned by a real candidate, however senior — the champion race is a
        // foregone conclusion the moment the starting fingerprint is virtual.
        let me = fp(1, 5, &[0]);
        let real_sibling = fp(2, 0, &[0]);
        let next = me.construct(&[real_sibling], true).unwrap();
        assert_eq!(
            *next.id(),
            1,
            "virtual self stays eldest even against a lower (more senior) real claim"
        );
    }

    #[test]
    fn elder_seed_requires_aggregated_level() {
        let a = fp(1, 0, &[]);
        let b = fp(2, 0, &[]);
        assert_eq!(a.elder_seed(&b), Err(Error::FingerprintBaseLevel));
    }

    #[test]
    fn elder_seed_requires_matching_levels() {
        let l1 = fp(1, 5, &[0, 0])
            .construct(&[fp(2, 5, &[0, 0])], false)
            .unwrap();
        let l2 = l1.construct(&[], false).unwrap();
        assert_eq!(
            l1.elder_seed(&l2),
            Err(Error::FingerprintLevelMismatch {
                self_level: 1,
                other_level: 2
            })
        );
    }

    #[test]
    fn elder_seed_lower_seed_wins() {
        let base_a = fp(10, 1, &[0]);
        let base_b = fp(20, 2, &[0]);
        let gnode_a = base_a.construct(&[], false).unwrap(); // seed = [1]
        let gnode_b = base_b.construct(&[], false).unwrap(); // seed = [2]
        assert_eq!(gnode_a.elder_seed(&gnode_b), Ok(true));
        assert_eq!(gnode_b.elder_seed(&gnode_a), Ok(false));
    }

    #[test]
    fn elder_seed_indistinguishable_when_seeds_fully_match() {
        let base_a = fp(10, 1, &[0]);
        let base_b = fp(20, 1, &[0]);
        let gnode_a = base_a.construct(&[], false).unwrap();
        let gnode_b = base_b.construct(&[], false).unwrap();
        assert_eq!(
            gnode_a.elder_seed(&gnode_b),
            Err(Error::IndistinguishableFingerprints)
        );
    }

    #[test]
    fn identity_eq_is_total_across_levels() {
        let a = fp(1, 0, &[0]);
        let b = a.construct(&[], false).unwrap();
        assert!(!a.identity_eq(&b));
    }

    #[test]
    fn same_branch_folds_two_real_members_of_one_still_tied_gnode() {
        // b1/b2, mutually adjacent, both bootstrapped at the default
        // eldership 0: each one's own `construct` sees the other as a real
        // sibling and is dethroned by the tie, so they name each other.
        let b1_l0 = fp(1, 0, &[0]);
        let b2_l0 = fp(2, 0, &[0]);
        let b1 = b1_l0
            .construct(std::slice::from_ref(&b2_l0), false)
            .unwrap();
        let b2 = b2_l0.construct(&[b1_l0], false).unwrap();
        assert_ne!(*b1.id(), *b2.id(), "the tie dethrones each side's own id");
        assert_eq!(
            b1.elder_seed(&b2),
            Err(Error::IndistinguishableFingerprints)
        );
        assert!(
            b1.same_branch(&b2, 2, 2),
            "two members of one still-tied 2-member g-node must be the same branch"
        );
    }

    #[test]
    fn same_branch_does_not_fold_two_disconnected_gnodes_with_a_coincidental_seed_tie() {
        // b1/b2 after a sever: each `construct`s with no siblings at all, so
        // neither is dethroned, yet the shared eldership-0 bootstrap still
        // makes their seeds coincide.
        let b1 = fp(1, 0, &[0]).construct(&[], false).unwrap();
        let b2 = fp(2, 0, &[0]).construct(&[], false).unwrap();
        assert_eq!(
            b1.elder_seed(&b2),
            Err(Error::IndistinguishableFingerprints)
        );
        assert!(
            !b1.same_branch(&b2, 1, 1),
            "two disconnected, now-single-member g-nodes must not be folded \
             into one branch just because their seeds coincide"
        );
    }

    #[test]
    fn same_branch_requires_both_sides_to_report_more_than_one_member() {
        // Only one side is actually part of a live multi-member g-node; the
        // other is alone. A coincidental seed tie still must not fold them.
        let alone = fp(1, 0, &[0]).construct(&[], false).unwrap();
        let c1_l0 = fp(2, 0, &[0]);
        let c2_l0 = fp(3, 0, &[0]);
        let tied = c1_l0.construct(&[c2_l0], false).unwrap();
        assert_eq!(
            alone.elder_seed(&tied),
            Err(Error::IndistinguishableFingerprints)
        );
        assert!(!alone.same_branch(&tied, 1, 2));
        assert!(!tied.same_branch(&alone, 2, 1));
    }
}

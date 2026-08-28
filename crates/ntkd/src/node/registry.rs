//! [`LinkId`]: the one canonical arc identifier `ntkd` mints per physically-discovered
//! neighborhood link, and [`LinkRegistry`], the table mapping it to every module's own
//! opaque per-arc handle.
//!
//! Four different crates each carry their own opaque arc identifier
//! (`ntk_neighborhood::Arc::neighbour_mac`, `ntk_qspn::ArcId` — minted *by* qspn on
//! `add_arc` —, `ntk_hooking::ArcId`, `ntk_identities::ArcId` — both "minted and owned by
//! the daemon"). The composition root is exactly the place that must reconcile them: this
//! registry keys everything by a link's stable `neighbour_mac` and hands out one monotonic
//! [`LinkId`] per link, reused verbatim as the raw value of both `ntk_hooking::ArcId` and
//! `ntk_identities::ArcId` (both are bare `pub struct ArcId(pub u64)`, "opaque handle the
//! daemon assigns"), while separately remembering whatever [`ntk_qspn::ArcId`] `QspnHandle::add_arc`
//! returned for that same link.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// The one canonical per-link identifier this daemon mints, reused as the raw value of both
/// [`ntk_hooking::ArcId`] and [`ntk_identities::ArcId`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LinkId(pub u64);

/// `type_tag` for [`encode_caller_id`]'s `TypedValue` encoding.
const NEIGHBOUR_ID_TAG: &str = "ntkd.NeighbourId";

impl LinkId {
    #[must_use]
    pub fn hooking(self) -> ntk_hooking::ArcId {
        ntk_hooking::ArcId(self.0)
    }

    #[must_use]
    pub fn identities(self) -> ntk_identities::ArcId {
        ntk_identities::ArcId(self.0)
    }
}

/// Encodes `id` — an [`ntk_neighborhood::NodeId`] — as a `TypedValue`. This node's own
/// stable id (`NeighborhoodConfig::my_id`, constant for the process's whole lifetime) travels
/// this way in `CallerContext.src_nic`, so the peer's [`LinkRegistry::link_for_caller`] can
/// resolve it back to *its own* [`LinkId`] for the arc; a *destination* id travels the exact
/// same way inside `crate::node::dispatch`'s `UnicastId::WholeNode`/`IdentityAware` payload
/// (`crates/ntk-proto/proto/ntk.proto`'s `UnicastId` message) — this port's deliberate choice to
/// reuse one identity-id type everywhere on the wire rather than mint a second one for
/// addressing purposes (see this doc's own reasoning below for why `NodeId` is the right type;
/// three separate bugs in this codebase came from inventing a second per-process identity).
///
/// Neither [`LinkId`] nor a MAC is used for this. [`LinkId`]: each node mints its own `LinkId`s
/// from an independent local counter starting at 1, so two different nodes' `LinkId`s routinely
/// collide in raw value while naming two completely unrelated arcs (e.g. two different nodes'
/// very first discovered link both being `LinkId(1)`) — decoding a peer-minted `LinkId` against
/// this node's own registry, as an earlier version of this module did, silently resolves to
/// whichever *local* arc happens to share that number, corrupting inbound routing for any node
/// with more than one arc. A MAC would work in principle (every node already observes its
/// peers' MACs via Neighborhood discovery) but recomputing "my own MAC for this interface" here
/// would have to reproduce whatever value the caller of [`crate::node::transport::start`]
/// originally handed `Manager::start_monitor` — a value this module has no reliable way to
/// reconstruct independently. `NodeId` has neither problem: Neighborhood discovery already
/// relies on it being globally distinguishing (`ntk_neighborhood::manager`'s own arc-identity
/// checks key off it), and it is one constant this identity already owns for its whole
/// lifetime, not a per-interface value to recompute.
#[must_use]
pub fn encode_caller_id(id: ntk_neighborhood::NodeId) -> ntk_proto::v1::TypedValue {
    ntk_proto::v1::TypedValue::new(NEIGHBOUR_ID_TAG, id.get().to_be_bytes().to_vec())
}

/// Inverse of [`encode_caller_id`] — decodes a `TypedValue` tagged [`NEIGHBOUR_ID_TAG`] back to
/// the [`ntk_neighborhood::NodeId`] it names, revalidating through `NodeId::from_raw` rather
/// than trusting a peer-supplied value is already positive. `pub(crate)` (not just used via
/// [`LinkRegistry::link_for_caller`] below) so `crate::node::dispatch`'s `UnicastId::WholeNode`/
/// `IdentityAware` resolution can decode the same encoding.
pub(crate) fn decode_caller_id(tv: &ntk_proto::v1::TypedValue) -> Option<ntk_neighborhood::NodeId> {
    if tv.type_tag != NEIGHBOUR_ID_TAG {
        return None;
    }
    let raw = i32::from_be_bytes(tv.payload.as_slice().try_into().ok()?);
    ntk_neighborhood::NodeId::from_raw(raw).ok()
}

/// `type_tag` for [`encode_identity_id`]'s `TypedValue` encoding.
const IDENTITY_ID_TAG: &str = "ntkd.IdentityId";

/// Encodes an [`ntk_identities::IdentityId`] as a `TypedValue`, for `UnicastId::IdentityAware`'s
/// payload.
///
/// Distinct from [`encode_caller_id`] on purpose, and the distinction is the whole point.
/// [`ntk_neighborhood::NodeId`] names the *node* — `NeighborhoodConfig::my_id`, one value for the
/// process's whole life — which is exactly right for `CallerContext.src_nic`, where the peer must
/// resolve "which arc did this arrive on". It is exactly wrong for naming *which identity* should
/// handle a call: a connectivity fork and the successor it bridges for run in one process and
/// share that node id, so it cannot tell them apart. `IdentityId` can: the registry mints a fresh
/// one per identity and `ntk_identities::Handle::migrate` returns the successor's.
///
/// Upstream draws the same line — `IdentityAwareUnicastID` carries an identities-level `NodeID`,
/// and `get_identity_skeleton` matches it against each entry of `local_identities`
/// (`research/impl/vala/ntkd/rpc/skeleton_factory.vala:284-291`) — while its own `src_nic`
/// equivalent stays a per-arc MAC.
///
/// `#[cfg(test)]` for now: nothing *sends* an `IdentityAware` `unicast_id` yet — `crate::node::
/// stubs` explains why naming a destination identity waits for the connectivity fork — so a
/// production encoder would be dead code. The decoder below is live, because the dispatcher
/// already has to interpret whatever a peer sends.
#[cfg(test)]
#[must_use]
pub(crate) fn encode_identity_id(id: ntk_identities::IdentityId) -> ntk_proto::v1::TypedValue {
    ntk_proto::v1::TypedValue::new(IDENTITY_ID_TAG, id.into_raw().to_be_bytes().to_vec())
}

/// Inverse of [`encode_identity_id`]. Returns `None` on a wrong tag or a payload that is not
/// exactly eight bytes; unlike [`decode_caller_id`] there is no range to revalidate, since every
/// `u64` is a well-formed [`ntk_identities::IdentityId`] — whether this node *hosts* that
/// identity is `crate::node::dispatch`'s question, not this function's.
pub(crate) fn decode_identity_id(
    tv: &ntk_proto::v1::TypedValue,
) -> Option<ntk_identities::IdentityId> {
    if tv.type_tag != IDENTITY_ID_TAG {
        return None;
    }
    let raw = u64::from_be_bytes(tv.payload.as_slice().try_into().ok()?);
    Some(ntk_identities::IdentityId::from_raw(raw))
}

/// Everything the daemon knows about one discovered link, indexed both by its stable
/// `neighbour_mac` key and by [`LinkId`].
#[derive(Debug, Clone)]
pub struct LinkEntry {
    pub id: LinkId,
    pub mac: String,
    pub dev: String,
    /// This arc's peer's own stable Neighborhood discovery id
    /// (`ntk_neighborhood::Arc::neighbour_id`) — see [`encode_caller_id`]'s doc for why inbound
    /// calls are resolved through this, not through a peer-minted [`LinkId`].
    pub neighbour_id: ntk_neighborhood::NodeId,
    pub qspn_arc: Option<ntk_qspn::ArcId>,
}

/// Single-owner map from a neighborhood arc's stable key (`neighbour_mac`) to the canonical
/// [`LinkId`] the rest of the daemon uses, plus the reverse lookups each adapter needs.
///
/// A plain `Mutex`-guarded table, not an actor: every access is a short, synchronous
/// map lookup/insert, never an await point, so a `Mutex` never risks holding a lock across
/// an outbound RPC (`research/notes/06-rust-stack.md` §Concurrency's actual concern).
#[derive(Debug, Default)]
pub struct LinkRegistry {
    next: AtomicU64,
    by_mac: Mutex<HashMap<String, LinkEntry>>,
    by_id: Mutex<HashMap<LinkId, String>>,
    /// Last time `crate::node::lifecycle::retry_removed_arc` actually retried each link, since
    /// that function's own bad-link retry has no natural stopping point otherwise: a peer whose
    /// connection is permanently gone (e.g. its whole generation torn down at shutdown) fails
    /// every fresh `add_arc` the exact same way, forever, with no backoff of its own —
    /// real-kernel confirmation: `two_star_groups_merge_into_one_network` teardown produced
    /// hundreds of `retried a bad-link arc` retries within a single millisecond once a peer's
    /// connection closed for good. Debounced by
    /// [`crate::node::lifecycle::BAD_LINK_RETRY_MIN_INTERVAL`] rather than capped by a fixed
    /// attempt count: a genuine, eventually-resolving collision (the coincidental
    /// `derive_initial_position` clash this retry exists to recover from) can legitimately need
    /// more attempts than any small count would allow, spread over the real tens-of-seconds
    /// this daemon's own merge negotiation already budgets — throttling by elapsed time bounds
    /// CPU/log volume without ever giving up while the underlying link might still recover.
    bad_link_last_retry: Mutex<HashMap<LinkId, std::time::Instant>>,
}

impl LinkRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the existing [`LinkId`] for `mac`, or mints and records a fresh one for the arc
    /// to `neighbour_id`.
    pub fn link_for_neighbour(
        &self,
        neighbour_id: ntk_neighborhood::NodeId,
        mac: &str,
        dev: &str,
    ) -> LinkId {
        let mut by_mac = self.by_mac.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = by_mac.get(mac) {
            return entry.id;
        }
        let id = LinkId(self.next.fetch_add(1, Ordering::Relaxed));
        by_mac.insert(
            mac.to_owned(),
            LinkEntry {
                id,
                mac: mac.to_owned(),
                dev: dev.to_owned(),
                neighbour_id,
                qspn_arc: None,
            },
        );
        self.by_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id, mac.to_owned());
        id
    }

    /// Resolves an inbound `CallerContext.src_nic` (see [`encode_caller_id`]'s doc) to this
    /// node's own [`LinkId`] for the arc it names, if known.
    #[must_use]
    pub fn link_for_caller(&self, tv: &ntk_proto::v1::TypedValue) -> Option<LinkId> {
        let id = decode_caller_id(tv)?;
        self.by_mac
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .find(|e| e.neighbour_id == id)
            .map(|e| e.id)
    }

    /// Records the [`ntk_qspn::ArcId`] `QspnHandle::add_arc` returned for `link`.
    ///
    /// # Invariant this relies on: at most one live `ArcId` per `LinkId`
    /// This unconditionally *overwrites* `link`'s previous `qspn_arc`, never calling
    /// `qspn.remove_arc` on whatever was there before — correct only because
    /// `crate::node::lifecycle::on_neighborhood_event`'s `ArcAdded` arm (this method's sole
    /// caller, alongside `crate::node::lifecycle::reattach_known_arcs`) is only ever driven
    /// by `ntk_neighborhood::Event::ArcAdded`, and that event is now guaranteed to fire at
    /// most once per arc's established lifetime (`ntk_neighborhood::manager::Manager::export_arc`'s
    /// own doc: its two callers — the peer's inbound negotiation and this node's outbound
    /// one — legitimately race, and the fix there is exactly to make a second racing call a
    /// no-op instead of a second export). Before that fix, a duplicate `ArcAdded` for one
    /// physical arc silently orphaned the previous `qspn_arc` here — never removed from
    /// qspn, just forgotten by this registry — leaving qspn with two live `ArcId`s for one
    /// neighbour (confirmed: a single-neighbour leaf's own route ending up a two-nexthop
    /// `Multipath` naming the identical gateway twice, and a real triangle topology's node
    /// admitting four `ArcId`s for two physical neighbours).
    pub fn set_qspn_arc(&self, link: LinkId, arc: ntk_qspn::ArcId) {
        if let Some(mac) = self
            .by_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&link)
            && let Some(entry) = self
                .by_mac
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get_mut(mac)
        {
            entry.qspn_arc = Some(arc);
        }
    }

    #[must_use]
    pub fn qspn_arc_of(&self, link: LinkId) -> Option<ntk_qspn::ArcId> {
        let mac = self
            .by_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&link)?
            .clone();
        self.by_mac
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&mac)
            .and_then(|e| e.qspn_arc)
    }

    #[must_use]
    pub fn link_of_qspn_arc(&self, arc: ntk_qspn::ArcId) -> Option<LinkId> {
        self.by_mac
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .find(|e| e.qspn_arc == Some(arc))
            .map(|e| e.id)
    }

    #[must_use]
    pub fn entry(&self, link: LinkId) -> Option<LinkEntry> {
        let mac = self
            .by_id
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&link)?
            .clone();
        self.by_mac
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&mac)
            .cloned()
    }

    #[must_use]
    pub fn link_for_dev_and_mac(&self, mac: &str) -> Option<LinkId> {
        self.by_mac
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(mac)
            .map(|e| e.id)
    }

    pub fn remove(&self, mac: &str) -> Option<LinkEntry> {
        let entry = self
            .by_mac
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(mac);
        if let Some(entry) = &entry {
            self.by_id
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&entry.id);
        }
        entry
    }

    #[must_use]
    pub fn all(&self) -> Vec<LinkEntry> {
        self.by_mac
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect()
    }

    /// Debounces `crate::node::lifecycle::retry_removed_arc` per link — see
    /// `Self::bad_link_last_retry` (private)'s own doc for why this exists. Returns `true` (and
    /// records `now`) only if `min_interval` has elapsed since this link's last retry, or it
    /// has never retried before.
    pub fn should_retry_bad_link(&self, link: LinkId, min_interval: std::time::Duration) -> bool {
        let mut last = self
            .bad_link_last_retry
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let now = std::time::Instant::now();
        match last.get(&link) {
            Some(prev) if now.duration_since(*prev) < min_interval => false,
            _ => {
                last.insert(link, now);
                true
            }
        }
    }
}

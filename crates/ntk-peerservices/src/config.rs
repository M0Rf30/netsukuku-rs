//! Injectable timing and redundancy constants, transcribed from
//! `research/notes/02-vala-services-daemon.md` §3 and RFC 0014 §2.2, rather than hard-coded at
//! their use sites.

use std::time::Duration;

/// Tuning knobs for routing timeouts, gossip pacing, and the RFC 0014 redundancy rule. Every
/// field has a documented upstream source; construct via [`Config::default`] for upstream's own
/// values, or override individual fields for tests/deployments that need different pacing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// Floor under every routing timeout, regardless of group size — upstream's rationale is
    /// that even a reply that *should* be fast can be delayed by a routing rule not yet
    /// installed a hop or two away (`min_timeout`,
    /// `research/impl/vala/peerservices/message_routing.vala:243`).
    pub min_timeout: Duration,
    /// Baseline routing timeout for a target g-node group of 100 nodes or fewer
    /// (`find_timeout_routing`, `message_routing.vala:244-254`).
    pub routing_timeout_small: Duration,
    /// Routing timeout once the target group exceeds 100 nodes.
    pub routing_timeout_medium: Duration,
    /// Routing timeout once the target group exceeds 1000 nodes.
    pub routing_timeout_large: Duration,
    /// Group size above which [`Config::routing_timeout_medium`] applies instead of
    /// [`Config::routing_timeout_small`].
    pub routing_timeout_medium_threshold: usize,
    /// Group size above which [`Config::routing_timeout_large`] applies instead of
    /// [`Config::routing_timeout_medium`].
    pub routing_timeout_large_threshold: usize,
    /// Poll interval while a client blocks on `wait_participation_maps`
    /// (`peers.vala:501-505`, `tasklet.ms_wait(10)`).
    pub participation_poll_interval: Duration,
    /// Backoff before retrying `approximate()` after a transient "no gateway available" result
    /// (`message_routing.vala:414-419`, `tasklet.ms_wait(20)`).
    pub gateway_retry_backoff: Duration,
    /// Bound on how many gateway candidates `Handle::relay`/`Handle::forward_msg`/
    /// `Handle::contact_peer` will try (each retry separated by [`Config::gateway_retry_backoff`])
    /// before giving up on the current target. Upstream carries no equivalent numeric cap at
    /// this layer: its own `get_gateway` (`research/impl/vala/ntkd/peers_helpers.vala:72-135`)
    /// treats `failed` by physically tearing down the underlying neighborhood arc and
    /// re-querying fresh paths, so a target with a single path converges to "no candidate left"
    /// after exactly one failure, and `message_routing.vala:934-955`'s own relay-equivalent
    /// loop (this crate's `Handle::relay`) has no bound because it never needed one. This port's
    /// own `RoutingEnv::gateway` implementations honour `failed` without the destructive arc
    /// teardown (see e.g. `ntkd`'s `RoutingEnvAdapter::gateway`), so the same convergence
    /// argument holds for a *correct* implementation — this field is this crate's own defensive
    /// backstop for any `RoutingEnv` that doesn't converge (many parallel paths, or one that
    /// never actually excludes a persistently-dead `failed` stub), so a dead gateway can never
    /// wedge the calling task's runtime regardless of the injected environment's own behavior.
    pub max_relay_attempts: usize,
    /// How many rotating refuse messages `contact_peer` keeps before collapsing older ones into
    /// an `"..."` placeholder for its final error (`message_routing.vala:340-347,494-500`).
    pub max_refuse_messages: usize,
    /// Default replica count for the RFC 0014 §2.2 step 5 redundancy rule ("send it to 31
    /// nodes, which have the closest IP to `m`"). Upstream's own port keeps this a per-call
    /// parameter rather than a global (`research/notes/02-vala-services-daemon.md` §3,
    /// "Replication factor `q` is a per-call param... not global") — this field is only the
    /// *default* a caller may use; [`crate::Handle::replicate`] still takes `q` explicitly.
    pub default_replication_factor: u32,
    /// How often the owning [`crate::actor::Manager`] re-floods its own optional-service
    /// participation facts as insurance against a lost delivery — a periodic repeat of exactly
    /// the flood [`crate::Handle::register`] already sends once, run in the `Manager`'s own
    /// actor task, never a second parallel gossip mechanism (`participate_tasklet`,
    /// `research/impl/vala/peerservices/map_handler.vala:331-362`). This crate models upstream's
    /// "5 times every 5 minutes, then randomly every 1-2 days" schedule as one fixed cadence
    /// instead. `None` (the default) disables the re-announce entirely — the behavior of every
    /// caller and test that predates this field.
    pub participation_reannounce_interval: Option<Duration>,
    /// Hard cap on how many routing hops (successive candidate attempts) a single
    /// [`crate::Handle::contact_peer`] call will take before giving up, independent of
    /// [`Config::routing_timeout`]. One hop is counted per candidate `approximate()` resolves —
    /// every self-loop retry, remote timeout, `Refuse`, `Failure`, or `NonParticipant` outcome
    /// that leads to another attempt — and the counter survives a `RedoFromStart`/
    /// `MissingOptionalMaps` restart rather than resetting with it, so a servant that keeps
    /// forcing restarts cannot bypass the bound.
    ///
    /// **Deviation, deliberate**: upstream (`research/impl/vala/peerservices/
    /// message_routing.vala:267-956`) has no hop counter at all — [`Config::routing_timeout`]
    /// is its only bound on one `contact_peer` call, so a routing pathology (a cycle of
    /// refusals/failures/restarts) can occupy the full timeout budget one hop at a time. This
    /// field is this crate's own defensive backstop, the same rationale as
    /// [`Config::max_relay_attempts`]'s own doc.
    ///
    /// **Default, justified**: 64. Realistic topologies span 4-16 levels; a healthy routing
    /// search resolves in roughly one hop per level plus the occasional retry, so 64 is
    /// generous headroom (4x the deepest realistic topology) for legitimate traffic while still
    /// stopping a pathological loop well short of exhausting [`Config::routing_timeout_large`]'s
    /// 20s budget one hop at a time.
    pub max_contact_peer_hops: usize,
    /// Hard cap on how many distinct participant g-nodes a single service's
    /// [`crate::ParticipantMap`] (inside the [`crate::actor::Manager`]'s `participant_set`)
    /// will track at once.
    ///
    /// **Why this exists**: `participant_set` is fed by inbound flood-gossip
    /// (`set_participant`/`give_participant_maps`) with no cap and no eviction, so it grows
    /// O(g-nodes known network-wide x registered services) — the binding memory constraint for
    /// a protocol that claims planetary scale.
    ///
    /// **Why refuse-new, not evict-existing**: routing correctness depends on this data —
    /// `non_participant_gnodes` (`actor.rs`) treats any g-node absent from the map as "not
    /// participating" and excludes it from `approximate`'s candidate pool (`routing.rs`), so
    /// evicting a *live* participant would make `contact_peer` silently route around a real
    /// servant. A naive LRU (or any other evict-existing policy) can therefore corrupt routing,
    /// not just memory. This field instead refuses only brand-new, never-before-seen facts once
    /// a service's map is full; every already-known participant is retained for the life of the
    /// process — the same refuse-new precedent as `ntk-andna`'s own
    /// `Config::max_counter_registrants`. `Manager` logs a `warn` the first time this engages
    /// for a service, since from that point on this node's routing view of that service is
    /// incomplete until restarted.
    ///
    /// **Default, justified**: 8192. This batch's own realistic-scale note bounds a topology at
    /// 4-16 levels with per-level g-node size up to 256, so one service's fully-populated view
    /// of the whole visible topology is at most `16 x 256 = 4096` distinct `HCoord` facts; 8192
    /// is double that worst-case realistic bound, so a legitimately complete view never trips
    /// the cap. Each entry is an `HCoord` (two integers) in a `BTreeSet`, so one maxed-out
    /// service costs on the order of a few hundred KiB.
    pub max_participants_per_service: usize,
    /// Multiplier applied to a [`crate::Handle::replicate`] call's own `timeout_exec` to derive
    /// its overall wall-clock deadline, independent of `q` or how many sequential
    /// [`crate::Handle::contact_peer`] attempts that takes.
    ///
    /// **Why this exists**: `replicate` walks the DHT `q` times *serially* (by necessity — each
    /// replica must exclude every node already collected, so replicas stay distinct), and each
    /// walk can itself take up to `timeout_exec`. `replicate` already tolerates returning fewer
    /// than `q` replicas (its own doc: "stops early ... if routing is exhausted"), so bounding
    /// the whole call's wall clock degrades a slow/partial network to fewer replicas instead of
    /// a `q x timeout_exec` worst-case stall — e.g. ANDNA's own `q = 31`,
    /// `timeout_exec = 5s` serialized to ~155s for one hostname registration before this field
    /// existed.
    ///
    /// **Default, justified**: 4. Comfortably covers a handful of sequential attempts —
    /// including retries against a partially degraded network — while still capping the
    /// worst-case stall at `4 x timeout_exec` regardless of how large a caller's own `q` is.
    pub replicate_deadline_multiplier: u32,
    /// Reject an inbound origin-request lacking a valid [`ntk_proto::v1::Auth`] once this node
    /// finally executes it as the elected servant (`crate::actor::Handle::verify_origin`,
    /// gated deep inside `Handle::forward_msg`'s self-loop, never at each relay hop).
    ///
    /// Defaults to `false` — the only setting byte-for-byte interoperable with an unmodified
    /// peer, since `PeerMessageForwarder.auth` is an additive, optional wire field: an old peer
    /// that has never heard of it simply never sets it, and this node must keep accepting that.
    /// `crate::actor::Handle::with_signing_key`'s own doc covers the originator-side half.
    pub require_auth: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            min_timeout: Duration::from_millis(500),
            routing_timeout_small: Duration::from_millis(200),
            routing_timeout_medium: Duration::from_millis(2000),
            routing_timeout_large: Duration::from_millis(20_000),
            routing_timeout_medium_threshold: 100,
            routing_timeout_large_threshold: 1000,
            participation_poll_interval: Duration::from_millis(10),
            gateway_retry_backoff: Duration::from_millis(20),
            max_relay_attempts: 16,
            max_refuse_messages: 10,
            default_replication_factor: 31,
            participation_reannounce_interval: None,
            max_contact_peer_hops: 64,
            max_participants_per_service: 8192,
            replicate_deadline_multiplier: 4,
            require_auth: false,
        }
    }
}

impl Config {
    /// The routing timeout for a target group of `nodes` nodes, including the
    /// [`Config::min_timeout`] floor (`find_timeout_routing`, `message_routing.vala:244-254`).
    #[must_use]
    pub fn routing_timeout(&self, nodes: usize) -> Duration {
        let base = if nodes > self.routing_timeout_large_threshold {
            self.routing_timeout_large
        } else if nodes > self.routing_timeout_medium_threshold {
            self.routing_timeout_medium
        } else {
            self.routing_timeout_small
        };
        base + self.min_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_timeout_bands_match_upstream() {
        let c = Config::default();
        assert_eq!(c.routing_timeout(1), Duration::from_millis(700));
        assert_eq!(c.routing_timeout(101), Duration::from_millis(2500));
        assert_eq!(c.routing_timeout(1001), Duration::from_millis(20_500));
    }
}

//! Shared test harness: spawns QSPN actors wired together through
//! [`FakeQspnStubFactory`] instances, using a deterministic sequential
//! [`ArcIdSource`] so a test can predict exactly which [`ArcId`] `add_arc`
//! will allocate and pre-register the peer mapping *before* calling
//! `add_arc` — avoiding a setup race against the actor's own
//! immediately-fires-a-fetch behavior (`arc_add`, `qspn.vala:718-798`: the
//! new arc must already be routable the instant it is registered).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use ntk_common::{Fingerprint, Naddr, Topology};
use ntk_qspn::{ArcId, ArcIdSource, FakeQspnStubFactory, FixedThreshold, QspnConfig, QspnHandle};
use tokio_util::sync::CancellationToken;

/// Allocates `1, 2, 3, ...` in order, matching [`Node`]'s own `next_id`
/// prediction counter one-for-one (both only ever advance on `add_arc`).
pub struct SequentialArcIdSource(AtomicU32);

impl SequentialArcIdSource {
    #[must_use]
    pub fn new() -> Self {
        Self(AtomicU32::new(1))
    }
}

impl ArcIdSource for SequentialArcIdSource {
    fn next(&self) -> u32 {
        self.0.fetch_add(1, Ordering::Relaxed)
    }
}

/// A config with every timer shortened to keep tests fast while still
/// exercising real (paused, injected) time — pair with
/// `#[tokio::test(start_paused = true)]` and `tokio::time::advance`.
#[must_use]
pub fn fast_config() -> QspnConfig {
    QspnConfig {
        bootstrap_signal_delay: Duration::from_millis(1),
        first_detection_split_delay: Duration::from_millis(50),
        periodic_full_etp_interval: Duration::from_secs(3600),
        ..QspnConfig::default()
    }
}

/// One simulated node: its handle, arc-id allocator, and stub factory.
pub struct Node {
    pub handle: QspnHandle,
    pub factory: Arc<FakeQspnStubFactory>,
    /// Mirrors the node's own [`SequentialArcIdSource`] call count — lets
    /// [`link`] predict the `ArcId` the next `add_arc` call will allocate
    /// without racing the actor.
    next_id: AtomicU32,
    _cancel: CancellationToken,
}

impl Node {
    /// Spawns a `create_net`-rooted node at `naddr`, with `config`.
    #[must_use]
    pub fn spawn(naddr: Naddr, id: u8, config: QspnConfig) -> Self {
        Self::spawn_with(naddr, id, 0, Duration::from_millis(20), config)
    }

    /// Like [`Self::spawn`], but with an explicit level-0 eldership claim
    /// (lower outranks higher, [`ntk_common::Fingerprint::elder_seed`]) and
    /// split-debounce threshold — needed to construct a deterministic
    /// fingerprint-split scenario (two isolated nodes independently
    /// claiming the same g-node position necessarily differ in `id`; giving
    /// them different `eldership` values too lets `elder_seed` pick a
    /// winner instead of erroring as indistinguishable).
    #[must_use]
    pub fn spawn_with(
        naddr: Naddr,
        id: u8,
        eldership: u32,
        split_threshold: Duration,
        config: QspnConfig,
    ) -> Self {
        let fingerprint =
            Fingerprint::new(vec![id], eldership, vec![0u32; naddr.topology().levels()]);
        let factory = Arc::new(FakeQspnStubFactory::new());
        let cancel = CancellationToken::new();
        let (handle, _join) = ntk_qspn::spawn(
            naddr,
            fingerprint,
            config,
            factory.clone(),
            Arc::new(FixedThreshold(split_threshold)),
            Arc::new(SequentialArcIdSource::new()),
            cancel.clone(),
        );
        Self {
            handle,
            factory,
            next_id: AtomicU32::new(1),
            _cancel: cancel,
        }
    }

    /// Spawns an `enter_net`-rooted node (see [`ntk_qspn::spawn_entering`])
    /// with no arcs yet — arcs are added afterward via [`link`], exercising
    /// `arc_add` during bootstrap (`qspn.vala:737-742`).
    #[must_use]
    #[allow(
        dead_code,
        reason = "used by some but not all test binaries sharing this module"
    )]
    pub fn spawn_entering(
        naddr: Naddr,
        id: u8,
        guest_gnode_level: usize,
        host_gnode_level: usize,
        config: QspnConfig,
    ) -> Self {
        let fingerprint = Fingerprint::new(vec![id], 0, vec![0u32; naddr.topology().levels()]);
        let factory = Arc::new(FakeQspnStubFactory::new());
        let cancel = CancellationToken::new();
        let (handle, _join) = ntk_qspn::spawn_entering(
            naddr,
            fingerprint,
            config,
            factory.clone(),
            Arc::new(FixedThreshold(Duration::from_millis(20))),
            Arc::new(SequentialArcIdSource::new()),
            Vec::new(),
            Vec::new(),
            guest_gnode_level,
            host_gnode_level,
            (0, 0),
            Vec::new(),
            cancel.clone(),
        )
        .expect("valid enter_net construction");
        Self {
            handle,
            factory,
            next_id: AtomicU32::new(1),
            _cancel: cancel,
        }
    }

    fn predict_next_arc(&self) -> ArcId {
        ArcId::from(self.next_id.fetch_add(1, Ordering::Relaxed))
    }
}

/// Yields to the executor repeatedly so concurrently-scheduled actor tasks
/// (talking to each other purely in-memory, with no real I/O or timers on
/// this path) get to drain their pending work — time-independent, so it
/// plays correctly with `start_paused = true` tests too.
pub async fn settle() {
    for _ in 0..256 {
        tokio::task::yield_now().await;
    }
}

/// Polls `check` (yielding via [`settle`] between attempts) until it
/// returns `true` or `max_rounds` attempts are exhausted. Used to wait for
/// multi-hop convergence (an ETP forwarded B -> A after C connects to B,
/// etc.) without any wall-clock sleep.
#[allow(
    dead_code,
    reason = "used by some but not all test binaries sharing this module"
)]
pub async fn wait_for(mut check: impl FnMut() -> bool, max_rounds: usize) -> bool {
    for _ in 0..max_rounds {
        if check() {
            return true;
        }
        settle().await;
    }
    check()
}

/// Connects `a` and `b` as a bidirectional arc at `cost`, pre-registering
/// both factory mappings before either side calls `add_arc` (see module
/// docs). The two sides' `add_arc` calls race each other's automatic
/// full-ETP fetch against the peer's own `add_arc` registration; on the rare
/// loss retries with fresh ids (upstream never reuses an `arc_id` either).
/// Returns `(a's ArcId for this link, b's ArcId for this link)`.
pub async fn link(a: &Node, b: &Node, cost: ntk_common::Cost) -> (ArcId, ArcId) {
    loop {
        let a_next = a.predict_next_arc();
        let b_next = b.predict_next_arc();
        a.factory.connect(a_next, b.handle.clone(), b_next);
        b.factory.connect(b_next, a.handle.clone(), a_next);
        let (ra, rb) = tokio::join!(a.handle.add_arc(cost), b.handle.add_arc(cost));
        let (got_a, got_b) = (ra.unwrap(), rb.unwrap());
        assert_eq!(
            got_a, a_next,
            "SequentialArcIdSource prediction drifted on a"
        );
        assert_eq!(
            got_b, b_next,
            "SequentialArcIdSource prediction drifted on b"
        );
        settle().await;
        let a_alive = a.handle.current_arcs().await.unwrap().contains(&got_a);
        let b_alive = b.handle.current_arcs().await.unwrap().contains(&got_b);
        if a_alive && b_alive {
            return (got_a, got_b);
        }
    }
}

#[must_use]
#[allow(
    dead_code,
    reason = "used by some but not all test binaries sharing this module"
)]
pub fn topology() -> Topology {
    Topology::new([4, 4]).expect("valid topology")
}

#[must_use]
pub fn naddr(topo: &Topology, pos: [u32; 2]) -> Naddr {
    Naddr::new(topo.clone(), pos).expect("valid address")
}

//! Multi-node integration test: a real `ntk-peerservices` network (the same 4-node, 2-level,
//! full-mesh harness `ntk-peerservices/tests/routing.rs` uses over `ntk_rpc::FakeRpcClient`) with
//! this crate's `AndnaService`/`CounterService` registered on every node, proving a hostname
//! registered at one node resolves from a different node, and that two nodes racing the same
//! name produce exactly one winner.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use ed25519_dalek::SigningKey;
use ntk_andna::{AndnaError, Config, Event, Hostname, Manager, RegisterOutcome, RegisterRequest};
use ntk_common::{HCoord, Naddr, Topology};
use ntk_peerservices::{
    Config as PeersConfig, Handle as PeersHandle, Manager as PeersManager, PeersRpcHandler,
    PeersStub, RoutingEnv, RpcPeersStub, TupleNode,
};
use ntk_rpc::FakeRpcClient;
use tokio_util::sync::CancellationToken;

const A: usize = 0;
const B: usize = 1;
const C: usize = 2;
const D: usize = 3;

/// A full-mesh, position-scoped-lookup routing environment — structurally identical to
/// `ntk-peerservices/tests/routing.rs`'s own `FakeEnv` (test-only code can't be shared across
/// crates, so this is a deliberate, minimal duplication of an already-proven harness, not a
/// second implementation of anything this crate owns).
struct FakeEnv {
    positions: Vec<[u32; 2]>,
    stubs: Arc<OnceLock<Vec<Arc<dyn PeersStub>>>>,
    my_index: usize,
}

impl FakeEnv {
    fn stubs(&self) -> &[Arc<dyn PeersStub>] {
        self.stubs
            .get()
            .expect("stubs installed before any routing call")
    }

    fn my_full(&self) -> &[u32; 2] {
        &self.positions[self.my_index]
    }
}

impl RoutingEnv for FakeEnv {
    fn gnode_exists(&self, hc: HCoord) -> bool {
        self.positions.iter().any(|p| p[hc.level] == hc.pos)
    }

    fn gateway(
        &self,
        hc: HCoord,
        failed: Option<&Arc<dyn PeersStub>>,
    ) -> Option<Arc<dyn PeersStub>> {
        let my_full = *self.my_full();
        self.positions
            .iter()
            .enumerate()
            .filter(|&(i, p)| {
                i != self.my_index
                    && p[hc.level] == hc.pos
                    && p[hc.level + 1..] == my_full[hc.level + 1..]
            })
            .map(|(i, _)| self.stubs()[i].clone())
            .find(|s| failed.is_none_or(|f| !Arc::ptr_eq(f, s)))
    }

    fn dial(&self, n: &TupleNode) -> Option<Arc<dyn PeersStub>> {
        let mut full_target = n.positions().to_vec();
        full_target.extend_from_slice(&self.my_full()[n.top()..]);
        self.positions
            .iter()
            .enumerate()
            .find(|(_, p)| p.as_slice() == full_target)
            .map(|(i, _)| self.stubs()[i].clone())
    }

    fn nodes_in_my_group(&self, _level: usize) -> usize {
        self.positions.len()
    }

    fn neighbors(&self) -> Vec<Arc<dyn PeersStub>> {
        (0..self.positions.len())
            .filter(|&i| i != self.my_index)
            .map(|i| self.stubs()[i].clone())
            .collect()
    }
}

fn build_peers_network() -> (Vec<PeersHandle>, CancellationToken) {
    let topology = Topology::new([2, 2]).unwrap();
    let positions: Vec<[u32; 2]> = vec![[0, 0], [1, 0], [0, 1], [1, 1]];
    let cancel = CancellationToken::new();
    let stub_cell: Arc<OnceLock<Vec<Arc<dyn PeersStub>>>> = Arc::new(OnceLock::new());

    let mut handles = Vec::new();
    for (i, pos) in positions.iter().enumerate() {
        let my_addr = Naddr::new(topology.clone(), pos.to_vec()).unwrap();
        let env = Arc::new(FakeEnv {
            positions: positions.clone(),
            stubs: stub_cell.clone(),
            my_index: i,
        });
        let (manager, handle) = PeersManager::new(
            topology.clone(),
            my_addr,
            env,
            PeersConfig::default(),
            topology.levels(),
        );
        tokio::spawn(manager.run(cancel.child_token()));
        handles.push(handle);
    }

    let stubs: Vec<Arc<dyn PeersStub>> = handles
        .iter()
        .map(|h| {
            let handler = Arc::new(PeersRpcHandler::new(h.clone()));
            let client = Arc::new(FakeRpcClient::new(handler));
            Arc::new(RpcPeersStub::new(client, topology.clone())) as Arc<dyn PeersStub>
        })
        .collect();
    assert!(
        stub_cell.set(stubs).is_ok(),
        "stub_cell set exactly once, before any routing call"
    );

    (handles, cancel)
}

/// Boots the 4-node network and registers this crate's two services on every node.
///
/// Every node registers *both* services on itself, so `contact_peer`'s optimistic routing (try
/// the closest candidate; if it turns out not to actually hold the service locally, exclude it
/// and retry) always converges without needing to wait for participation gossip first — unlike
/// `ntk-peerservices/tests/routing.rs`'s own harness, which registers a given service on only
/// *one* node and so does wait for gossip. Waiting here would also be substantially slower: two
/// services registered back-to-back from the same node collide in `ntk-peerservices`' gossip
/// dedup (`recent_published: BTreeSet<HCoord>` is keyed by position only, not `(p_id, HCoord)`),
/// so the second registration's participation fact is silently suppressed for 60 real seconds.
async fn build_andna_network(config: Config) -> (Vec<ntk_andna::Handle>, CancellationToken) {
    let (peers_handles, cancel) = build_peers_network();

    let mut andna_handles = Vec::new();
    for peers_handle in &peers_handles {
        let substrate: Arc<dyn ntk_andna::AndnaSubstrate> = Arc::new(peers_handle.clone());
        let (manager, handle) = Manager::new(substrate, config);
        tokio::spawn(manager.run(cancel.child_token()));
        handle.register_services().await;
        andna_handles.push(handle);
    }
    (andna_handles, cancel)
}

fn signed_request(
    key: &SigningKey,
    name: &str,
    owner: &Naddr,
    sequence: u64,
    now: u64,
) -> RegisterRequest {
    RegisterRequest::sign(
        key,
        Hostname::new(name).unwrap(),
        owner.clone(),
        sequence,
        now,
        16,
        1,
        Vec::new(),
    )
    .unwrap()
}

fn naddr(topology: &Topology, pos: [u32; 2]) -> Naddr {
    Naddr::new(topology.clone(), pos.to_vec()).unwrap()
}

#[tokio::test]
async fn name_registered_at_one_node_resolves_from_another() {
    let (handles, cancel) = build_andna_network(Config::default()).await;
    let topology = handles[A].topology().clone();
    let owner_key = SigningKey::from_bytes(&[7u8; 32]);
    let owner = naddr(&topology, [0, 0]);

    let req = signed_request(&owner_key, "angelica", &owner, 1, 1_000);
    let outcome = handles[A]
        .register(req)
        .await
        .expect("registration succeeds");
    assert!(matches!(outcome, RegisterOutcome::Registered { .. }));

    for &resolver in &[A, B, C, D] {
        let records = handles[resolver]
            .resolve(&Hostname::new("angelica").unwrap(), 0)
            .await
            .unwrap_or_else(|e| panic!("resolve from node {resolver} failed: {e}"));
        assert_eq!(records.len(), 1, "resolver {resolver}");
        match &records[0].target {
            ntk_andna::SnsdTarget::Address(a) => assert_eq!(a, &owner),
            other => panic!("expected an address record, got {other:?}"),
        }
    }

    cancel.cancel();
}

#[tokio::test]
async fn renewal_from_a_different_node_is_visible_everywhere() {
    // `AndnaService`/`CounterService` (server-side) always time-check against real wall-clock
    // time (`crate::actor::unix_now()`), never a client-supplied `timestamp_unix` — trusting a
    // client's own claimed time for TTL/rate-limit accounting would let it manipulate both. So a
    // fast in-process test racing two calls needs `min_renewal_interval` relaxed; the interval
    // itself is already covered at the pure `record::tests` level with explicit `now` values.
    let config = Config {
        min_renewal_interval: std::time::Duration::ZERO,
        ..Config::default()
    };
    let (handles, cancel) = build_andna_network(config).await;
    let topology = handles[A].topology().clone();
    let owner_key = SigningKey::from_bytes(&[9u8; 32]);
    let owner = naddr(&topology, [1, 1]);

    handles[B]
        .register(signed_request(&owner_key, "frenzu", &owner, 1, 1_000))
        .await
        .unwrap();
    let outcome = handles[C]
        .renew(signed_request(&owner_key, "frenzu", &owner, 2, 5_000))
        .await
        .unwrap();
    assert!(matches!(outcome, RegisterOutcome::Renewed { .. }));

    let records = handles[D]
        .resolve(&Hostname::new("frenzu").unwrap(), 0)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);

    cancel.cancel();
}

#[tokio::test]
async fn two_nodes_racing_the_same_name_produce_exactly_one_winner() {
    let (handles, cancel) = build_andna_network(Config::default()).await;
    let topology = handles[A].topology().clone();
    let key_a = SigningKey::from_bytes(&[1u8; 32]);
    let key_b = SigningKey::from_bytes(&[2u8; 32]);
    let owner_a = naddr(&topology, [0, 0]);
    let owner_b = naddr(&topology, [1, 1]);

    let req_a = signed_request(&key_a, "depausceve", &owner_a, 1, 1_000);
    let req_b = signed_request(&key_b, "depausceve", &owner_b, 1, 1_000);

    let (result_a, result_b) = tokio::join!(handles[A].register(req_a), handles[D].register(req_b));

    let outcomes = [result_a, result_b];
    let wins = outcomes
        .iter()
        .filter(|r| matches!(r, Ok(RegisterOutcome::Registered { .. })))
        .count();
    let losses = outcomes
        .iter()
        .filter(|r| matches!(r, Err(AndnaError::Rejected(_))))
        .count();
    assert_eq!(wins, 1, "exactly one racer must win: {outcomes:?}");
    assert_eq!(losses, 1, "exactly one racer must lose: {outcomes:?}");

    let records = handles[B]
        .resolve(&Hostname::new("depausceve").unwrap(), 0)
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    let winner = match (&outcomes[0], &records[0].target) {
        (Ok(_), ntk_andna::SnsdTarget::Address(a)) if a == &owner_a => true,
        (Err(_), ntk_andna::SnsdTarget::Address(a)) if a == &owner_b => true,
        _ => false,
    };
    assert!(
        winner,
        "resolved address must match whichever racer actually won"
    );

    cancel.cancel();
}

#[tokio::test]
async fn the_nth_plus_one_hostname_from_one_registrant_is_denied() {
    // A small cap here (the pure `counter::tests` cover the real default of 256) keeps this
    // network-wide integration test fast while still proving the Counter *service* — not just
    // the pure `CounterCache` — enforces the cap across the substrate.
    let cap = 3;
    let config = Config {
        max_hostnames_per_registrant: cap,
        ..Config::default()
    };
    let (handles, cancel) = build_andna_network(config).await;
    let topology = handles[A].topology().clone();
    let key = SigningKey::from_bytes(&[3u8; 32]);
    let owner = naddr(&topology, [0, 1]);

    for i in 0..cap as u32 {
        let req = signed_request(&key, &format!("h{i}"), &owner, u64::from(i) + 1, 1_000);
        handles[A]
            .register(req)
            .await
            .unwrap_or_else(|e| panic!("registration {i} should succeed: {e}"));
    }

    let req = signed_request(&key, "onemore", &owner, 300, 1_000);
    let err = handles[A].register(req).await.unwrap_err();
    assert!(
        matches!(err, AndnaError::CounterDenied(_)),
        "expected CounterDenied, got {err:?}"
    );

    cancel.cancel();
}

/// Sub-defect A regression: before `AndnaService::exec`'s inbound path gated on
/// `Config::max_hosted_records`, a `RegisterRequest` a hash-node receives straight off the wire
/// went to `Cache::register` with no capacity check at all — only a *self*-issued registration's
/// outbound `Handle::register` ever consulted a cap, and only the per-registrant one. Every
/// registration below varies `owner_naddr`, so the Counter service's per-registrant quota (256,
/// keyed by whoever actually dials it — always this test's one calling node, but spread thin
/// across as many different `counter_route_key(owner_naddr)` targets as distinct owners) never
/// comes close to biting at this volume; whatever eventually refuses one of these must be the
/// per-node *inbound* host-capacity cap, not the outbound per-registrant check.
#[tokio::test]
async fn a_flood_of_distinct_registrants_is_bounded_by_the_inbound_host_capacity_cap() {
    // 4 nodes x cap 2 = 8 hostname-slots total across the whole network, however replication
    // happens to spread across nodes for any given hostname's hash target. Registering 20
    // distinct (fresh registrant, fresh hostname) pairs guarantees at least one lands on an
    // already-full node, regardless of exactly how routing distributed the earlier ones — no
    // network internals needed, just pigeonhole counting.
    let cap = 2;
    let attempts = 20u32;
    let config = Config {
        max_hosted_records: cap,
        ..Config::default()
    };
    let (handles, cancel) = build_andna_network(config).await;
    let topology = handles[A].topology().clone();

    let mut host_capacity_rejections = 0;
    for i in 0..attempts {
        // A fresh keypair *and* position per attempt keeps every routing/collision check out of
        // the way; see this test's own doc comment for why the per-registrant Counter cap can't
        // be what eventually refuses one of these.
        let key = SigningKey::from_bytes(&[(10 + i) as u8; 32]);
        let owner = naddr(&topology, [i % 2, (i / 2) % 2]);
        let req = signed_request(&key, &format!("host{i}"), &owner, 1, 1_000);
        match handles[A].register(req).await {
            Ok(_) => {}
            Err(AndnaError::Rejected(reason)) if reason.contains("already hosts the maximum") => {
                host_capacity_rejections += 1;
            }
            Err(other) => panic!("registration {i} failed for an unexpected reason: {other}"),
        }
    }

    assert!(
        host_capacity_rejections > 0,
        "expected at least one of {attempts} registrations, from {attempts} distinct \
         registrants, to be refused by the inbound host-capacity cap (network total capacity is \
         only 4 nodes x {cap}); none were"
    );

    cancel.cancel();
}

/// Sub-defect B regression: before `ntk_andna::run_expiry_reclaimer` existed, nothing in a
/// running daemon ever called `Handle::purge_expired` — an expired hostname stayed in
/// `Cache::records` forever (`run_steady_state`'s select loop had no timer arm at all). Drives
/// the reclaimer's own interval with `tokio::time::pause`/`advance`, never a real sleep.
#[tokio::test(start_paused = true)]
async fn run_expiry_reclaimer_actually_reclaims_an_expired_hostname_on_its_own_cadence() {
    let config = Config {
        // Expires the instant *any* later real-clock `now` is read (`run_expiry_reclaimer` reads
        // the real clock, which `tokio::time::pause` never affects) — no real sleep needed.
        name_ttl: Duration::ZERO,
        expiry_purge_interval: Duration::from_secs(60),
        ..Config::default()
    };
    let (handles, cancel) = build_andna_network(config).await;
    let topology = handles[A].topology().clone();
    let mut events = handles[A].events();

    let key = SigningKey::from_bytes(&[42u8; 32]);
    let owner = naddr(&topology, [0, 0]);
    let req = signed_request(&key, "ephemeral", &owner, 1, 1_000);
    handles[A]
        .register(req)
        .await
        .expect("registration succeeds");

    tokio::spawn(ntk_andna::run_expiry_reclaimer(
        handles[A].clone(),
        cancel.child_token(),
    ));
    tokio::time::advance(Duration::from_secs(61)).await;

    let expired = loop {
        match events
            .recv()
            .await
            .expect("reclaimer publishes an Event::Expired")
        {
            Event::Expired { hostname } => break hostname,
            other => {
                // Replication can publish more than one `Event::Registered` for a single
                // registration on this node's own channel; only the eventual `Expired` matters.
                assert!(
                    matches!(other, Event::Registered { .. }),
                    "unexpected event before reclamation: {other:?}"
                );
            }
        }
    };
    assert_eq!(expired, Hostname::new("ephemeral").unwrap());

    cancel.cancel();
}

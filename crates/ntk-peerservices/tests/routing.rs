//! Multi-node routing tests over the in-memory fake transport (`ntk_rpc::FakeRpcClient` +
//! [`PeersRpcHandler`]): a small 4-node, 2-level network (`gsizes = [2, 2]`), each node running
//! its own [`Manager`] actor, wired full-mesh via [`RpcPeersStub`] so every node can reach every
//! other node's [`PeersRpcHandler`] directly — exercising the real wire encoding
//! (`crate::wire`) end to end, not a shortcut.
//!
//! Nodes: `A = [0,0]`, `B = [1,0]`, `C = [0,1]`, `D = [1,1]` (level-0 position first). `A`/`B`
//! share the level-1 group `0`; `C`/`D` share group `1`.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use futures::future::BoxFuture;
use ntk_common::{HCoord, Naddr, Topology};
use ntk_peerservices::{
    Config, Event, ExecError, Handle, Manager, PeerService, PeersRpcHandler, PeersStub, Refusal,
    RoutingEnv, RpcPeersStub, ServiceId, TupleNode,
};
use ntk_proto::v1::TypedValue;
use ntk_rpc::FakeRpcClient;
use tokio_util::sync::CancellationToken;

const A: usize = 0;
const B: usize = 1;
const C: usize = 2;
const D: usize = 3;

/// A full-mesh, position-scoped-lookup routing environment: every node can reach every other
/// node in one hop, but `gateway`/`dial` still respect g-node scoping (a lookup for a target at
/// `level` only matches candidates sharing my own ancestry *above* that level — otherwise, e.g.,
/// a level-0 gateway lookup could return a same-numbered sibling from a wholly different g-node).
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
        // `n` is a prefix of the target's real address; the missing (higher) levels are shared
        // with whoever is asking (routing only ever reaches you if you're inside that scope).
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

/// Boots the 4-node full-mesh network and returns each node's [`Handle`], plus the
/// [`CancellationToken`] governing every spawned actor.
fn build_network() -> (Vec<Handle>, CancellationToken) {
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
        let (manager, handle) = Manager::new(
            topology.clone(),
            my_addr,
            env,
            Config::default(),
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

fn exact(topology: &Topology, pos: [u32; 2]) -> TupleNode {
    TupleNode::new(topology.clone(), pos.to_vec()).unwrap()
}

async fn register_and_wait_for_gossip(
    handles: &[Handle],
    at: usize,
    service: Arc<dyn PeerService>,
) {
    let sid = service.service_id();
    let mut rxs: Vec<_> = handles
        .iter()
        .enumerate()
        .filter(|&(i, _)| i != at)
        .map(|(_, h)| h.events())
        .collect();
    handles[at].register(service).await;
    for rx in &mut rxs {
        loop {
            let evt = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("gossip propagated within 2s")
                .expect("event channel stayed open");
            if matches!(evt, Event::ParticipantAdded { p_id, .. } if p_id == sid) {
                break;
            }
        }
    }
}

/// Always answers with the request echoed back unchanged.
struct EchoService {
    id: ServiceId,
}

impl PeerService for EchoService {
    fn service_id(&self) -> ServiceId {
        self.id
    }
    fn is_optional(&self) -> bool {
        true
    }
    fn exec<'a>(
        &'a self,
        request: TypedValue,
        _client_tuple: &'a [u32],
    ) -> BoxFuture<'a, Result<TypedValue, ExecError>> {
        Box::pin(async move { Ok(request) })
    }
}

/// Always refuses at a fixed level.
struct RefusingService {
    id: ServiceId,
    level: usize,
}

impl PeerService for RefusingService {
    fn service_id(&self) -> ServiceId {
        self.id
    }
    fn is_optional(&self) -> bool {
        true
    }
    fn exec<'a>(
        &'a self,
        _request: TypedValue,
        _client_tuple: &'a [u32],
    ) -> BoxFuture<'a, Result<TypedValue, ExecError>> {
        Box::pin(async move {
            Err(ExecError::Refuse(Refusal {
                level: self.level,
                message: "busy".to_owned(),
            }))
        })
    }
}

/// Answers a pre-scripted sequence of outcomes, one per call, recording how many times it ran.
struct ScriptedService {
    id: ServiceId,
    script: Mutex<VecDeque<Result<TypedValue, ExecError>>>,
    calls: AtomicUsize,
}

impl PeerService for ScriptedService {
    fn service_id(&self) -> ServiceId {
        self.id
    }
    fn is_optional(&self) -> bool {
        true
    }
    fn exec<'a>(
        &'a self,
        _request: TypedValue,
        _client_tuple: &'a [u32],
    ) -> BoxFuture<'a, Result<TypedValue, ExecError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            self.script
                .lock()
                .unwrap()
                .pop_front()
                .expect("script has an entry for this call")
        })
    }
}

#[tokio::test]
async fn key_resolves_to_the_same_hash_node_from_every_starting_point() {
    let (handles, cancel) = build_network();
    let topology = handles[A].topology().clone();
    let sid = ServiceId::new(1);

    register_and_wait_for_gossip(&handles, B, Arc::new(EchoService { id: sid })).await;

    let target = exact(&topology, [1, 0]); // B's exact address
    let request = TypedValue::new("test.echo", b"ping".to_vec());

    for &start in &[A, B, C, D] {
        let (response, respondant) = handles[start]
            .contact_peer(
                sid,
                Some(target.clone()),
                request.clone(),
                Duration::from_secs(1),
                None,
                Vec::new(),
            )
            .await
            .unwrap_or_else(|e| panic!("contact_peer from node {start} failed: {e}"));
        assert_eq!(
            respondant.positions(),
            &[1, 0],
            "node {start} resolved to a different hash node"
        );
        assert_eq!(response.payload, request.payload);
    }

    cancel.cancel();
}

#[tokio::test]
async fn refuse_excludes_the_refusing_gnode_at_the_correct_level() {
    let (handles, cancel) = build_network();
    let topology = handles[A].topology().clone();
    let sid = ServiceId::new(2);

    // A refuses (excluding its whole level-1 group, level=1); C accepts.
    register_and_wait_for_gossip(&handles, A, Arc::new(RefusingService { id: sid, level: 1 }))
        .await;
    register_and_wait_for_gossip(&handles, C, Arc::new(EchoService { id: sid })).await;

    let target = exact(&topology, [0, 0]); // A's own exact address
    let request = TypedValue::new("test.echo", b"refuse-me".to_vec());

    let (response, respondant) = handles[A]
        .contact_peer(
            sid,
            Some(target),
            request.clone(),
            Duration::from_secs(1),
            None,
            Vec::new(),
        )
        .await
        .expect("routing recovers from the refusal by excluding A's whole group");

    assert_eq!(
        respondant.positions(),
        &[0, 1],
        "should have failed over to C, not A's own (refusing) group"
    );
    assert_eq!(response.payload, request.payload);

    cancel.cancel();
}

#[tokio::test]
async fn redo_from_start_restarts_instead_of_returning_a_stale_result() {
    let (handles, cancel) = build_network();
    let topology = handles[A].topology().clone();
    let sid = ServiceId::new(3);

    let service = Arc::new(ScriptedService {
        id: sid,
        script: Mutex::new(VecDeque::from([
            Err(ExecError::RedoFromStart),
            Ok(TypedValue::new("test.scripted", b"second-attempt".to_vec())),
        ])),
        calls: AtomicUsize::new(0),
    });
    register_and_wait_for_gossip(&handles, D, service.clone()).await;

    let target = exact(&topology, [1, 1]); // D's exact address
    let request = TypedValue::new("test.echo", b"redo-me".to_vec());

    let (response, respondant) = handles[A]
        .contact_peer(
            sid,
            Some(target),
            request,
            Duration::from_secs(1),
            None,
            Vec::new(),
        )
        .await
        .expect("contact_peer succeeds after the servant asks for a restart");

    assert_eq!(respondant.positions(), &[1, 1]);
    assert_eq!(response.payload, b"second-attempt");
    assert_eq!(
        service.calls.load(Ordering::SeqCst),
        2,
        "redo_from_start must trigger exactly one real restart, not a silent single pass"
    );

    cancel.cancel();
}

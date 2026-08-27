//! Full-triangle topology: three single-NIC nodes on one flat segment,
//! `gsizes = [2, 2]`, where `a` sits alone in level-1 slot 0 and `b1`/`b2`
//! are the two level-0 siblings of slot 1 — every pair directly adjacent.
//! This is the shape that first surfaced `IndistinguishableFingerprints`
//! and a spurious `Acyclic` message drop against the real kernel (three
//! mutually-direct neighbours never occurred in any earlier topology).

mod support;

use std::sync::{Arc, Mutex};

use ntk_common::{Cost, HCoord, Topology};
use support::{Node, fast_config, link, naddr, wait_for};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

/// Captures every `WARN`-level event's message, ignoring everything else —
/// just enough to assert on the two log lines the real-kernel repro showed
/// (`update_map failed` / `revise_etp: cyclic ETP dropped`) without pulling
/// in a tracing-subscriber dev-dependency.
#[derive(Default)]
struct WarnCapture(Mutex<Vec<String>>);

struct MessageVisitor(String);

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

impl Subscriber for WarnCapture {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        *metadata.level() == tracing::Level::WARN
    }
    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }
    fn record(&self, _span: &Id, _values: &Record<'_>) {}
    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}
    fn event(&self, event: &Event<'_>) {
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(visitor.0);
    }
    fn enter(&self, _span: &Id) {}
    fn exit(&self, _span: &Id) {}
}

fn cost_to(snapshot: &ntk_qspn::RouteSnapshot, level: usize, pos: u32) -> Vec<Cost> {
    snapshot
        .levels
        .get(level)
        .into_iter()
        .flatten()
        .find(|e| e.destination == HCoord::new(level, pos))
        .map(|e| e.paths.iter().map(|p| p.cost).collect())
        .unwrap_or_default()
}

/// A full triangle converges: `a` learns two admitted paths (via `b1` and
/// via `b2`) to the shared level-1 g-node, and each of `b1`/`b2` learns the
/// other as a direct level-0 sibling plus two admitted paths to `a`'s g-node
/// — the direct arc and the legitimate, more expensive path routed through
/// its sibling (a real triangle gives every node two genuinely distinct
/// first hops to the far side, so this multipath is expected, not a bug).
///
/// Before the fix: `a`'s `update_map` fails outright with
/// `Common(IndistinguishableFingerprints)`. `b1` and `b2`, tied at the
/// default eldership 0, each independently compute their *own* shared
/// g-node's level-1 fingerprint via `Fingerprint::construct`; that fold
/// always starts from the computer's own fingerprint as champion and only
/// lets a *candidate* sibling depose it on a tie, so for exactly these two
/// tied members each one names the *other* champion. `a` then holds two
/// differently-identified but numerically indistinguishable fingerprints
/// for the very same destination, and ordering them is fatal, aborting
/// `update_map` for the whole batch — so `a` never learns the level-1
/// destination at all. (A `revise_etp: cyclic ETP dropped` WARN also fires
/// in this topology, independently: an internal-arc forward of "this is a
/// fact about a fellow g-node member" legitimately loops back into that same
/// member's own g-node — the literal, upstream-faithful message-header
/// acyclic rule, `qspn.vala:1096-1104` — and is harmless here since the
/// dropped message never carried anything not already known directly.) So
/// the route-set assertion below fails.
///
/// After the fix: an indistinguishable-fingerprint comparison degrades to
/// "keep the current answer" instead of erroring, and a path whose
/// fingerprint is merely indistinguishable-from (not just identical to) the
/// destination's chosen winner is still exposed alongside it, so the
/// topology converges as the triangle implies.
#[tokio::test]
async fn triangle_all_pairs_direct_converges_to_expected_routes() {
    let capture = Arc::new(WarnCapture::default());
    let _guard = tracing::subscriber::set_default(capture.clone());

    let topo = Topology::new([2, 2]).expect("valid topology");
    let a = Node::spawn(naddr(&topo, [0, 0]), 1, fast_config());
    let b1 = Node::spawn(naddr(&topo, [0, 1]), 2, fast_config());
    let b2 = Node::spawn(naddr(&topo, [1, 1]), 3, fast_config());

    link(&a, &b1, Cost::Finite(10)).await;
    link(&a, &b2, Cost::Finite(10)).await;
    link(&b1, &b2, Cost::Finite(10)).await;

    let ok = wait_for(
        || {
            cost_to(&a.handle.snapshot(), 1, 1) == vec![Cost::Finite(10), Cost::Finite(10)]
                && cost_to(&b1.handle.snapshot(), 0, 1) == vec![Cost::Finite(10)]
                && cost_to(&b1.handle.snapshot(), 1, 0) == vec![Cost::Finite(10), Cost::Finite(20)]
                && cost_to(&b2.handle.snapshot(), 0, 0) == vec![Cost::Finite(10)]
                && cost_to(&b2.handle.snapshot(), 1, 0) == vec![Cost::Finite(10), Cost::Finite(20)]
        },
        200,
    )
    .await;

    let warnings = capture.0.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert!(
        ok,
        "triangle did not converge: a={:?} b1={:?} b2={:?}\nwarnings seen: {warnings:#?}",
        a.handle.snapshot(),
        b1.handle.snapshot(),
        b2.handle.snapshot()
    );
    assert!(
        warnings.iter().all(|w| !w.contains("update_map failed")),
        "update_map failed at least once: {warnings:#?}"
    );
    // `revise_etp: cyclic ETP dropped` legitimately still fires in this
    // topology (see the message-header-loop case in the doc comment above)
    // and is not asserted against — it costs nothing here since the
    // direct arc already delivers the same fact.
}

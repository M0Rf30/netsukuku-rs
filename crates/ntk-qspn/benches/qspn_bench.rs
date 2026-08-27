//! Benchmarks for the two hot paths the audit flagged as unmeasured:
//! [`QspnState::snapshot`] (rebuilds the whole exported route set) and
//! [`QspnState::update_map`] (the per-ETP admission workhorse,
//! `O(dests * od_set * rd * hops)`). Both are reachable through this
//! crate's ordinary public API (`QspnState::new` + `add_arc` +
//! `update_map` + `update_clusters`), so no `bench`-gated internals are
//! needed here — the `bench` feature exists solely to gate this
//! `[[bench]]` target itself off the default build, per the workspace's
//! "don't widen the public API just to be measurable" policy.
//!
//! Inputs are parameterized over two points at Netsukuku's realistic
//! scale (`research/notes/*`: 4-16 g-node levels, single-digit arcs per
//! node, `max_paths=5`, per-level gsize up to 256) rather than one
//! arbitrary size, so the slope with map size is visible. `Cost::Finite`
//! is microsecond RTT (`ntk_common::Cost`'s doc); every synthetic arc/path
//! cost below is in that unit.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use ntk_common::{Cost, Fingerprint, HCoord, Naddr, Topology};
use ntk_qspn::{ArcId, EtpPath, NodePath, QspnConfig, QspnState};

/// A handful of registered local arcs — "single-digit arcs per node".
const LOCAL_ARC_COUNT: u32 = 6;

/// How large a synthetic map to build: `dests_per_level` destinations at
/// each of `levels` levels, each offered `candidates_per_dest` disjoint
/// paths (admission caps this at `QspnConfig::max_paths`, 5 by default).
struct MapShape {
    levels: usize,
    dests_per_level: usize,
    candidates_per_dest: usize,
}

/// A level-`level` fingerprint: `QspnState::update_map`/`snapshot` require
/// a path's fingerprint to sit at the same level as its destination, and
/// [`Fingerprint::construct`] only climbs one level per call.
fn fingerprint_at_level(id: [u8; 2], level: usize) -> Fingerprint<Vec<u8>> {
    let mut fp = Fingerprint::new(id.to_vec(), 0, vec![0u32; level.max(1)]);
    for _ in 0..level {
        fp = fp.construct(&[], false).expect("valid champion climb");
    }
    fp
}

/// Builds a populated [`QspnState`] of the given shape entirely through the
/// public API — one `update_map` call per level, each climbing through the
/// previous level's already-admitted hops so every candidate is a genuine
/// multi-hop path (exercising the `hops` factor in `update_map`'s own
/// `O(dests * od_set * rd * hops)`), not the degenerate single-hop case.
/// Returns the state plus the deepest level's own offered batch, for
/// [`bench_update_map`] to re-drive as a representative incoming ETP.
fn build_state(shape: &MapShape) -> (QspnState, Vec<NodePath>) {
    let topology = Topology::new(vec![64u32; shape.levels]).expect("valid topology");
    let my_naddr = Naddr::new(topology, vec![0u32; shape.levels]).expect("valid address");
    let my_fp = Fingerprint::new(vec![0u8, 0u8], 0, vec![0u32; shape.levels]);
    let mut state = QspnState::new(my_naddr, my_fp, QspnConfig::default());

    let local_arcs: Vec<ArcId> = (1..=LOCAL_ARC_COUNT).map(ArcId::from).collect();
    for (i, &arc) in local_arcs.iter().enumerate() {
        // Realistic LAN/WAN RTTs, staggered so paths sort distinctly.
        state.add_arc(arc, Cost::Finite(200 + 150 * i as u64));
    }

    // canonical[level][d] = the first (guaranteed-admitted, via the
    // first-occurrence-of-a-fingerprint admission bypass) candidate's own
    // hop/arc chain for that level's d-th destination — what the next
    // level up climbs through to build a genuinely deepening path.
    let mut canonical: Vec<Vec<(Vec<HCoord>, Vec<ArcId>)>> = Vec::with_capacity(shape.levels);
    let mut last_level_batch = Vec::new();

    for level in 0..shape.levels {
        let mut q_set = Vec::with_capacity(shape.dests_per_level * shape.candidates_per_dest);
        let mut this_level_canonical = Vec::with_capacity(shape.dests_per_level);
        for d in 0..shape.dests_per_level {
            // Position 0 at every level is this node's own; keep clear of it.
            let pos = (d as u32 % 63) + 1;
            let dest = HCoord::new(level, pos);
            let fp = fingerprint_at_level([level as u8, (d % 256) as u8], level);
            for c in 0..shape.candidates_per_dest {
                let local_arc = local_arcs[c % local_arcs.len()];
                let (hops, arcs) = if level == 0 {
                    (vec![dest], vec![local_arc])
                } else {
                    let prev = &canonical[level - 1];
                    let (base_hops, base_arcs) = &prev[(d + c) % prev.len()];
                    let mut hops = base_hops.clone();
                    let mut arcs = base_arcs.clone();
                    hops.push(dest);
                    arcs.push(local_arc);
                    (hops, arcs)
                };
                let path = EtpPath {
                    hops,
                    arcs,
                    cost: Cost::Finite(300 + 60 * c as u64),
                    fingerprint: fp.clone(),
                    nodes_inside: 1,
                    ignore_outside: vec![false; shape.levels],
                };
                let node_path = NodePath::new(local_arc, path);
                if c == 0 {
                    this_level_canonical
                        .push((node_path.path.hops.clone(), node_path.path.arcs.clone()));
                }
                q_set.push(node_path);
            }
        }
        state
            .update_map(&q_set, None)
            .expect("synthetic bench input must be admitted");
        canonical.push(this_level_canonical);
        last_level_batch = q_set;
    }
    state
        .update_clusters()
        .expect("synthetic bench input must climb cleanly");
    (state, last_level_batch)
}

/// `(levels, destinations per level, candidate paths per destination)` —
/// one modest point and one larger one, so the benchmark output shows the
/// slope rather than a single opaque number. The first lands close to the
/// audit's own "~40 destinations / ~100 paths" measurement; the second
/// scales both dimensions up.
const SHAPES: [(usize, usize, usize); 2] = [(4, 10, 3), (4, 40, 5)];

fn bench_snapshot(c: &mut Criterion) {
    let mut group = c.benchmark_group("qspn_snapshot");
    for &(levels, dests_per_level, candidates_per_dest) in &SHAPES {
        let shape = MapShape {
            levels,
            dests_per_level,
            candidates_per_dest,
        };
        let (state, _) = build_state(&shape);
        let name = format!(
            "levels={levels}_dests_per_level={dests_per_level}_candidates={candidates_per_dest}"
        );
        group.bench_function(name, |b| {
            b.iter(|| black_box(state.snapshot().expect("bench state must snapshot cleanly")));
        });
    }
    group.finish();
}

fn bench_update_map(c: &mut Criterion) {
    let mut group = c.benchmark_group("qspn_update_map");
    for &(levels, dests_per_level, candidates_per_dest) in &SHAPES {
        let shape = MapShape {
            levels,
            dests_per_level,
            candidates_per_dest,
        };
        let (mut state, top_level_batch) = build_state(&shape);
        let name = format!(
            "levels={levels}_dests_per_level={dests_per_level}_candidates={candidates_per_dest}"
        );
        group.bench_function(name, |b| {
            b.iter(|| {
                state
                    .update_map(black_box(&top_level_batch), None)
                    .expect("bench state must re-admit cleanly")
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_snapshot, bench_update_map);
criterion_main!(benches);

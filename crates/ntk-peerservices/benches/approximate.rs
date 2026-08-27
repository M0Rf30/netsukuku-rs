//! Benchmarks [`ntk_peerservices::approximate`] — the DHT key->position function
//! (RFC 0014 §2, Definition 2.3) called once per `Handle::contact_peer` hop
//! (`crates/ntk-peerservices/src/routing.rs`).
//!
//! `approximate` is `O(sum(gsize(level)) for level in 0..valid_levels)`: it scans every
//! candidate position at every level below the target's scope, and each candidate costs an
//! `O(levels)` `dist()` call. Per-level `gsize` is **not** bounded by this crate's own `Config`
//! — it comes straight from the caller's [`ntk_common::Topology`] — so the two topologies below
//! bracket this batch's own realistic-scale note (4-16 levels, per-level gsize up to 256): a
//! small deployment (`[4,4,4,4]`) and the worst realistic case (`[256,256,256,256]`), to show the
//! slope in gsize directly rather than picking one arbitrary size.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use ntk_common::{HCoord, Topology};
use ntk_peerservices::{TupleNode, approximate};

/// Every g-node "exists" and every candidate is a live participant — the worst case for
/// `approximate`'s own cost, since nothing is ever skipped via `exclude_list`/`gnode_exists`.
fn always_exists(_h: HCoord) -> bool {
    true
}

fn bench_approximate(c: &mut Criterion) {
    let mut group = c.benchmark_group("approximate");
    for gsizes in [[4u32, 4, 4, 4], [256, 256, 256, 256]] {
        let topology = Topology::new(gsizes).unwrap();
        let my_pos = vec![0u32; topology.levels()];
        // A fully-resolved key at the deepest scope this topology can express, the case that
        // walks every level `approximate` is willing to scan.
        let x_macron = TupleNode::new(
            topology.clone(),
            gsizes.iter().map(|&g| g / 2).collect::<Vec<_>>(),
        )
        .unwrap();
        let label = format!("{}x{}", gsizes[0], gsizes.len());
        group.bench_function(label, |b| {
            b.iter(|| {
                approximate(
                    black_box(&topology),
                    black_box(&my_pos),
                    black_box(Some(&x_macron)),
                    black_box(&[]),
                    always_exists,
                )
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_approximate);
criterion_main!(benches);

//! The key→g-node mapping's hashing half: turning an already-hashed key into a target
//! [`TupleNode`] in this topology's address space. Pairing this with [`crate::tuple::approximate`]
//! (which then finds the closest *existing* participant to that target) implements RFC 0014 §2,
//! Definition 2.3's `h: KEY -> IP` / `H: IP -> IP*` composition — upstream calls the composed
//! result `h(k) = H(h'(k))`, "the hash node of `k`".

use ntk_common::Topology;

use crate::tuple::TupleNode;

/// Maps `hash` — a caller-computed hash of a service key — to a target [`TupleNode`] by
/// successive `(hash % gsize, hash /= gsize)` per level, level 0 first (`perfect_tuple`,
/// `research/impl/vala/peerservices/peers.vala:804-818`).
///
/// Upstream's own `hash_from_key` first reduces the hash into `0..capacity` (the product of
/// every g-node size) before this decomposition. This function skips that step deliberately:
/// mixed-radix decomposition via repeated `(hash % gsize, hash /= gsize)` is mathematically
/// equivalent to first reducing `hash` modulo the product of every `gsize` (each step only
/// consumes `hash`'s low-order remainder, so any pre-reduction the caller could have done is
/// redone here as a side effect) — so `hash` may be an arbitrary-width hash of the key (e.g. the
/// low 128 bits of an MD5/SHA output, RFC 0014 §2 Definition 2.3's own example), with no
/// overflow/pre-bounding step for the caller to get right.
///
/// The concrete `KEY -> hash` function (`h`, RFC 0014 §2, Definition 2.3) is deliberately not
/// this crate's concern: each service built on this substrate defines its own key space and
/// hash.
#[must_use]
pub fn hash_to_tuple(topology: &Topology, hash: u128) -> TupleNode {
    let mut h = hash;
    let mut pos = vec![0u32; topology.levels()];
    for (level, slot) in pos.iter_mut().enumerate() {
        let gsize = u128::from(topology.gsize(level).expect("level < topology.levels()"));
        *slot = (h % gsize) as u32;
        h /= gsize;
    }
    TupleNode::new(topology.clone(), pos).expect("modulo reduction keeps every position in range")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology(gsizes: &[u32]) -> Topology {
        Topology::new(gsizes.iter().copied()).unwrap()
    }

    #[test]
    fn decomposes_low_order_digits_first() {
        let t = topology(&[5, 5, 5]);
        // hash = 0*1 + 3*5 + 2*25 = 65
        let tuple = hash_to_tuple(&t, 65);
        assert_eq!(tuple.positions(), &[0, 3, 2]);
    }

    #[test]
    fn is_deterministic() {
        let t = topology(&[7, 3, 11, 2]);
        assert_eq!(hash_to_tuple(&t, 123_456), hash_to_tuple(&t, 123_456));
    }

    proptest::proptest! {
        #[test]
        fn every_position_is_in_range(
            gsizes in proptest::collection::vec(1u32..12, 1..6),
            hash: u128,
        ) {
            let t = topology(&gsizes);
            let tuple = hash_to_tuple(&t, hash);
            for (level, &p) in tuple.positions().iter().enumerate() {
                proptest::prop_assert!(p < gsizes[level]);
            }
        }
    }
}

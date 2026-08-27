//! ETP shape validation — `check_incoming_message`/`check_outgoing_message`/
//! `check_any_message`/`check_tplist`
//! (`research/impl/vala/qspn/etp_message.vala:125-191`). Every ETP this actor
//! accepts or emits passes through here first; [`crate::revise_etp`] and
//! `update_map` assume validated input (e.g. that a path's fingerprint level
//! always matches its own destination hop's level).

use ntk_common::{HCoord, Naddr, Topology};

use crate::path::EtpMessage;

/// `check_tplist` (`etp_message.vala:180-191`): hop levels must be
/// non-decreasing and in range.
///
/// Upstream's own check here is `c.pos < 0` (`etp_message.vala:188`) — a bare
/// non-negativity check, meaningful only because Vala's `HCoord.pos` is a
/// signed `int`. This port's `HCoord::pos` is `u32`, so that literal
/// translation is vacuously true and checks nothing; a prior version of this
/// comment noted the type difference but drew the wrong conclusion from it,
/// treating "cannot be negative" as "must be in range" and skipping the
/// upper-bound check entirely. What is actually guaranteed by `pos: u32` is
/// only non-negativity; what must be *checked* is that `pos` is strictly less
/// than `gsize(level)` — the only bound that makes it a real position among
/// that level's siblings rather than a value nothing in this topology names.
/// Upstream never checks that upper bound either (see the `PositionOutOfRange`
/// history in `crates/ntkd/src/kernel/addressing.rs` for how far an
/// unvalidated position can travel before it corrupts something): this is a
/// deliberate divergence from upstream, not a faithfulness gap. Upstream's
/// own `ip_global_gnode` (`ipv4_compute.vala:73-105`) packs a peer-supplied
/// position into a fixed-width bitfield exactly as this port's
/// `ntkd::kernel::addressing::pack` does, with no range check of its own
/// either — the omission is upstream's, this port closes it here instead of
/// reproducing it, because an out-of-range position packed into a bitfield
/// silently overflows into an adjacent level's bits rather than failing
/// loudly (`crates/ntkd/src/kernel/addressing.rs`'s `PositionOutOfRange`
/// doc), and rejecting it at the boundary that actually knows the topology
/// (here) is cheaper and more legible than trusting every downstream
/// consumer to re-derive the same bound.
fn check_hop_list(hops: &[HCoord], topology: &Topology) -> bool {
    let levels = topology.levels();
    let mut curlvl = 0usize;
    for c in hops {
        if c.level < curlvl || c.level >= levels {
            return false;
        }
        let gsize = topology
            .gsize(c.level)
            .expect("c.level < levels checked above");
        if c.pos >= gsize {
            return false;
        }
        curlvl = c.level;
    }
    true
}

/// `check_any_message` (`etp_message.vala:167-179`): shape checks that apply
/// to an ETP regardless of direction.
fn check_any_message(m: &EtpMessage, topology: &Topology) -> bool {
    if !check_hop_list(&m.hops, topology) {
        return false;
    }
    let levels = topology.levels();
    for p in &m.paths {
        let Some(last) = p.hops.last() else {
            return false;
        };
        if p.ignore_outside.len() != levels {
            return false;
        }
        if last.level >= levels {
            return false;
        }
        if p.ignore_outside[last.level + 1..]
            .iter()
            .any(|&ignore| !ignore)
        {
            return false;
        }
        if p.fingerprint.level() != last.level {
            return false;
        }
        if p.hops.len() != p.arcs.len() {
            return false;
        }
        if !check_hop_list(&p.hops, topology) {
            return false;
        }
    }
    true
}

/// `check_incoming_message` (`etp_message.vala:128-139`): the address MUST
/// share this node's topology and MUST NOT be this node's own address.
#[must_use]
pub fn check_incoming_message(m: &EtpMessage, my_naddr: &Naddr) -> bool {
    let levels = my_naddr.topology().levels();
    if m.node_address.topology() != my_naddr.topology() {
        return false;
    }
    let same = (0..levels).all(|l| m.node_address.pos(l) == my_naddr.pos(l));
    if same {
        return false;
    }
    check_any_message(m, my_naddr.topology())
}

/// `check_outgoing_message` (`etp_message.vala:142-153`): the address MUST be
/// this node's own.
#[must_use]
pub fn check_outgoing_message(m: &EtpMessage, my_naddr: &Naddr) -> bool {
    let levels = my_naddr.topology().levels();
    if m.node_address.topology() != my_naddr.topology() {
        return false;
    }
    let same = (0..levels).all(|l| m.node_address.pos(l) == my_naddr.pos(l));
    if !same {
        return false;
    }
    check_any_message(m, my_naddr.topology())
}

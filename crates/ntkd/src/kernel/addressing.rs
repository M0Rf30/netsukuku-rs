//! NIP↔IPv4 address computation: translates a hierarchical [`Naddr`]/[`HCoord`] into the
//! `10.0.0.0/8` IPv4 representation the kernel actually routes on.
//!
//! Ported fresh from `research/impl/vala/ntkd/ipv4_compute.vala:23-105` (`ip_global_node`,
//! `ip_global_gnode`) — per `research/notes/02-vala-services-daemon.md` open question 5, no
//! existing Rust type in `ntk-common`/`ntk-netlink` performs this conversion (`ntk_common::Naddr`
//! is deliberately topology-position-only, `ntk_netlink` is deliberately address-space-agnostic),
//! so the composition root owns it, exactly as upstream's own `ntkd` binary does.
//!
//! Only **kind 0 (global/public)** is implemented. Upstream's kind 1 (internal, embeds
//! `inside_level`) and kind 2 (anonymizing) both exist solely to support the NAT/subnet-boundary
//! feature (`subnetlevel`, `identity_ip_commands.vala`'s NETMAP/SNAT rules) — deliberately out of
//! scope for this daemon (no approved native NAT crate, `research/notes/06-rust-stack.md` open
//! question 5), so their address kinds are correspondingly absent here, not stubbed.
//!
//! # Bit layout
//! `10.i2.i1.i0` — the fixed `10` octet, then a 24-bit accumulator built one level at a time,
//! outermost level first: `acc = (acc << bits(level)) | pos(level)`, seeded with the 2-bit kind
//! tag before the first shift so it ends up above every position bit
//! (`ipv4_compute.vala:33-38`). `bits(gsize) = ceil(log2(gsize))` generalizes upstream's `g_exp`
//! (upstream requires power-of-two `gsize = 1 << g_exp`; `ntk_common::Topology` allows arbitrary
//! sizes, so this crate computes the bit width a level's positions actually need instead of
//! requiring a stored `g_exp` table).

use ntk_common::{HCoord, Naddr, Topology};
use ntk_netlink::Ipv4Net;
use std::net::Ipv4Addr;

/// The kind tag for a global/public address (`ip_global_node`/`ip_global_gnode`,
/// `ipv4_compute.vala:23,73`). The only kind this module implements — see module docs.
const KIND_GLOBAL: u32 = 0;

/// A topology cannot be encoded into the 24 usable bits of `10.0.0.0/8`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AddressingError {
    /// `hc.level` is not one of `naddr`'s levels.
    #[error("level {level} is out of range for a {levels}-level topology")]
    LevelOutOfRange { level: usize, levels: usize },
    /// The topology's positions plus the 2-bit kind tag do not fit in 24 bits.
    #[error(
        "topology needs {needed} address bits (+2 kind bits), only 24 are available in 10.0.0.0/8"
    )]
    TopologyTooWide { needed: u32 },
    /// A position is not strictly less than its level's g-node size, so it needs more bits than
    /// that level's field has — packing it would silently overflow into the adjacent level.
    #[error("position {pos} at level {level} is out of range: g-node size is {gsize}")]
    PositionOutOfRange { level: usize, pos: u32, gsize: u32 },
}

/// Bits needed to represent every value in `0..gsize` (`ceil(log2(gsize))`; `0` for `gsize <= 1`).
fn level_bits(gsize: u32) -> u32 {
    32 - gsize.saturating_sub(1).leading_zeros()
}

/// Total address bits `topology` needs, or [`AddressingError::TopologyTooWide`] if that plus the
/// 2 kind bits would not fit in the 24 bits available after the fixed `10` octet.
fn total_bits(topology: &Topology) -> Result<u32, AddressingError> {
    let needed: u32 = topology.gsizes().iter().copied().map(level_bits).sum();
    if needed + 2 > 24 {
        return Err(AddressingError::TopologyTooWide { needed });
    }
    Ok(needed)
}

/// Packs `kind` plus one position per level (`positions(level)`, outermost level first) into the
/// low 24 bits of a `10.x.x.x` address, and returns the CIDR prefix length that would leave every
/// level below `zero_below` as a wildcard (`32` if `zero_below == 0`, i.e. nothing is wildcarded).
///
/// # Errors
/// [`AddressingError::TopologyTooWide`] if `topology` does not fit in 24 bits.
/// [`AddressingError::PositionOutOfRange`] if a non-wildcarded `positions(level)` is not
/// strictly less than that level's g-node size — packing it as-is would need more bits than the
/// level's field has, overflowing into the adjacent (already-shifted) level's bits.
fn pack(
    topology: &Topology,
    kind: u32,
    zero_below: usize,
    positions: impl Fn(usize) -> u32,
) -> Result<(Ipv4Addr, u8), AddressingError> {
    let levels = topology.levels();
    // Only the `Err` side effect matters here: `pack` never needs the address' total bit count
    // itself, only `total_bits`'s "does this topology fit in 24 bits" guard. Not binding the
    // count at all (rather than binding it to `needed` and then bare-discarding it) keeps that
    // intentional — this crate has had three real defects start from a computed-but-unused
    // parameter that looked like it should matter.
    total_bits(topology)?;
    let mut acc: u32 = kind;
    let mut zeroed_bits: u32 = 0;
    for level in (0..levels).rev() {
        let gsize = topology
            .gsize(level)
            .expect("level < topology.levels() by loop range");
        let bits = level_bits(gsize);
        let value = if level < zero_below {
            0
        } else {
            let value = positions(level);
            if value >= gsize {
                return Err(AddressingError::PositionOutOfRange {
                    level,
                    pos: value,
                    gsize,
                });
            }
            value
        };
        acc = (acc << bits) | value;
        if level < zero_below {
            zeroed_bits += bits;
        }
    }
    let address = Ipv4Addr::new(10, (acc >> 16) as u8, (acc >> 8) as u8, acc as u8);
    let prefix = u8::try_from(32 - zeroed_bits).expect("zeroed_bits <= 24 < 32");
    Ok((address, prefix))
}

/// The full host address (`/32`, kind global) for `naddr` — `ip_global_node`
/// (`ipv4_compute.vala:23-46`), used to configure this identity's own address.
///
/// # Errors
/// [`AddressingError::TopologyTooWide`] if `naddr`'s topology does not fit in 24 bits.
/// [`AddressingError::PositionOutOfRange`] cannot actually occur here: every position `pack`
/// reads comes from `naddr.pos`, and [`Naddr::new`] already rejects any position that is not
/// strictly less than its level's g-node size at construction time.
pub fn host_address(naddr: &Naddr) -> Result<Ipv4Net, AddressingError> {
    let (address, _) = pack(naddr.topology(), KIND_GLOBAL, 0, |level| {
        naddr.pos(level).expect("level < topology.levels()")
    })?;
    Ok(Ipv4Net::host(address))
}

/// The CIDR block routing to the g-node named by `hc`, as seen from a node whose own address is
/// `my_naddr` — `ip_global_gnode` (`ipv4_compute.vala:73-105`). Levels above `hc.level` are taken
/// from `my_naddr` (by [`Naddr::hcoord`]'s own contract, `dest`'s positions above the divergence
/// level equal `my_naddr`'s), `hc.level` itself is `hc.pos`, and every level below is wildcarded
/// (prefix ends there) — "any address inside this g-node".
///
/// # Errors
/// [`AddressingError::LevelOutOfRange`] if `hc.level >= my_naddr.topology().levels()`.
/// [`AddressingError::PositionOutOfRange`] if `hc.pos` is not strictly less than
/// `hc.level`'s g-node size — unlike [`Naddr`], [`HCoord`] carries no such invariant of its own,
/// so this is the only place that catches an out-of-range `hc.pos` before it would otherwise
/// silently overflow into the adjacent level's packed bits.
pub fn gnode_destination(my_naddr: &Naddr, hc: HCoord) -> Result<Ipv4Net, AddressingError> {
    let levels = my_naddr.topology().levels();
    if hc.level >= levels {
        return Err(AddressingError::LevelOutOfRange {
            level: hc.level,
            levels,
        });
    }
    let (address, prefix) = pack(my_naddr.topology(), KIND_GLOBAL, hc.level, |level| {
        if level == hc.level {
            hc.pos
        } else {
            my_naddr.pos(level).expect("level < topology.levels()")
        }
    })?;
    Ok(Ipv4Net::new(address, prefix).expect("prefix computed in-range by pack()"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology() -> Topology {
        Topology::new([4, 2, 2, 2]).unwrap() // g_exp = [2,1,1,1], matching upstream's default.
    }

    #[test]
    fn host_address_matches_upstream_bit_packing() {
        // pos = [1,0,1,0] (level 0 innermost, gsize 4/2 bits; levels 1-3 gsize 2/1 bit each),
        // packed outermost level first (level 3 down to level 0):
        // acc = ((((0<<1|0)<<1|1)<<1|0)<<2|1) = ((0<<1|1)<<1|0)<<2|1 = (1<<1|0)<<2|1 = 2<<2|1 = 9.
        let naddr = Naddr::new(topology(), [1, 0, 1, 0]).unwrap();
        let net = host_address(&naddr).unwrap();
        assert_eq!(net.address(), Ipv4Addr::new(10, 0, 0, 9));
        assert_eq!(net.prefix_len(), 32);
        // Boundary-valid: gsize 4 means the max in-range level-0 position is 3 (0b11, exactly
        // filling its 2-bit field) — pins that the position-range guard added alongside this
        // test does not reject, or otherwise alter, a valid boundary value.
        // acc = ((((0<<1|0)<<1|1)<<1|0)<<2|3) = 2<<2|3 = 11.
        let boundary_naddr = Naddr::new(topology(), [3, 0, 1, 0]).unwrap();
        let boundary_net = host_address(&boundary_naddr).unwrap();
        assert_eq!(boundary_net.address(), Ipv4Addr::new(10, 0, 0, 11));
        assert_eq!(boundary_net.prefix_len(), 32);
    }

    #[test]
    fn gnode_destination_zeroes_below_and_shrinks_prefix() {
        let naddr = Naddr::new(topology(), [1, 0, 1, 0]).unwrap();
        // Destination g-node at level 1, pos 1 — a *sibling* of naddr's own level-1 gnode
        // (naddr.pos(1) == 0): keeps levels above 1 from naddr (level2=1, level3=0), fixes
        // level1=1, wildcards level0.
        let net = gnode_destination(&naddr, HCoord::new(1, 1)).unwrap();
        assert_eq!(net.prefix_len(), 32 - 2); // level 0 alone is 2 bits wide.
        // Any address sharing levels 1-3 with this coordinate (any level-0 position) lives
        // inside the resulting gnode, regardless of its own level-0 position.
        let inside = Naddr::new(topology(), [3, 1, 1, 0]).unwrap();
        assert!(net.contains(host_address(&inside).unwrap().address()));
        // naddr itself belongs to the *other* level-1 sibling (pos 0), not this one.
        assert!(!net.contains(host_address(&naddr).unwrap().address()));
    }

    #[test]
    fn gnode_destination_same_level_as_self_is_own_gnode() {
        let naddr = Naddr::new(topology(), [1, 0, 1, 0]).unwrap();
        let net = gnode_destination(&naddr, HCoord::new(0, naddr.pos(0).unwrap())).unwrap();
        assert_eq!(net.prefix_len(), 32);
        assert_eq!(net.address(), host_address(&naddr).unwrap().address());
    }

    #[test]
    fn rejects_out_of_range_level() {
        let naddr = Naddr::new(topology(), [1, 0, 1, 0]).unwrap();
        assert!(matches!(
            gnode_destination(&naddr, HCoord::new(4, 0)),
            Err(AddressingError::LevelOutOfRange {
                level: 4,
                levels: 4
            })
        ));
    }

    #[test]
    fn rejects_topology_too_wide_for_10_8() {
        // 22 levels of gsize 2 = 22 bits + 2 kind bits = 24, fits exactly.
        let wide = Topology::new(std::iter::repeat_n(2u32, 22)).unwrap();
        let naddr = Naddr::new(wide, vec![0u32; 22]).unwrap();
        assert!(host_address(&naddr).is_ok());
        // One more level tips it over.
        let too_wide = Topology::new(std::iter::repeat_n(2u32, 23)).unwrap();
        let naddr = Naddr::new(too_wide, vec![0u32; 23]).unwrap();
        assert!(matches!(
            host_address(&naddr),
            Err(AddressingError::TopologyTooWide { .. })
        ));
    }

    #[test]
    fn level_bits_matches_power_of_two_g_exp() {
        assert_eq!(level_bits(1), 0);
        assert_eq!(level_bits(2), 1);
        assert_eq!(level_bits(4), 2);
        assert_eq!(level_bits(8), 3);
        // Non-power-of-two generalizes to the ceiling.
        assert_eq!(level_bits(3), 2);
        assert_eq!(level_bits(5), 3);
    }

    #[test]
    fn rejects_out_of_range_position_at_every_level() {
        // Topology [4,2,2,2] -> gsizes [4,2,2,2]. One past the maximum valid position at each
        // level must be refused, not silently packed and left to overflow into the level above.
        let naddr = Naddr::new(topology(), [1, 0, 1, 0]).unwrap();
        for (level, gsize) in [(0u32, 4u32), (1, 2), (2, 2), (3, 2)] {
            let level = level as usize;
            assert!(
                matches!(
                    gnode_destination(&naddr, HCoord::new(level, gsize)),
                    Err(AddressingError::PositionOutOfRange {
                        level: err_level,
                        pos,
                        gsize: err_gsize,
                    }) if err_level == level && pos == gsize && err_gsize == gsize
                ),
                "level {level} (gsize {gsize}) accepted an out-of-range position"
            );
        }
    }

    #[test]
    fn out_of_range_position_no_longer_corrupts_the_adjacent_level() {
        // Before the `pack` position-range guard, this exact case (level 0, gsize 4, pos 7 —
        // 3 bits crammed into a 2-bit field) silently returned `Ok` with address 10.0.0.15: the
        // invalid value's high bit OR'd straight into level 1's already-shifted field, flipping
        // its decoded position from 0 to 1 (level 1's correct packing, pos 3, is 10.0.0.11 —
        // see `host_address_matches_upstream_bit_packing`'s boundary assertion). This test
        // failed (no `Err`) before the guard and passes now that it returns one.
        let naddr = Naddr::new(topology(), [1, 0, 1, 0]).unwrap();
        assert!(matches!(
            gnode_destination(&naddr, HCoord::new(0, 7)),
            Err(AddressingError::PositionOutOfRange {
                level: 0,
                pos: 7,
                gsize: 4
            })
        ));
    }
}

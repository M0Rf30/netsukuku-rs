//! DHT route-key derivation: which target [`ntk_peerservices::hash_to_tuple`] should place a
//! hostname's `Andna` record at, and which target it should place a registrant's `Counter`
//! record at.

use ntk_common::Naddr;

/// The Counter service's routing key for `registrant`: `blake3` of the registrant's own address
/// positions.
///
/// NTK_RFC 0007 ("ANDNA counter system based on public key"): upstream originally hashed the
/// register_node's **public key** to find its `counter_gnode`, which let one physical node
/// mint unlimited keypairs to bypass the 256-hostname cap; the fix hashes the register_node's
/// **address** instead, so evading the cap requires actually moving in the address space, not
/// just generating a new key (`research/specs/vala-doc--rfc-Ntk_andna_counter_pubk`).
#[must_use]
pub fn counter_route_key(registrant: &Naddr) -> u128 {
    let mut hasher = blake3::Hasher::new();
    for pos in registrant.positions() {
        hasher.update(&pos.to_le_bytes());
    }
    let hash = hasher.finalize();
    let mut low16 = [0u8; 16];
    low16.copy_from_slice(&hash.as_bytes()[..16]);
    u128::from_le_bytes(low16)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ntk_common::Topology;

    fn naddr(pos: [u32; 2]) -> Naddr {
        Naddr::new(Topology::new([4, 4]).unwrap(), pos).unwrap()
    }

    #[test]
    fn deterministic_and_position_sensitive() {
        assert_eq!(
            counter_route_key(&naddr([1, 2])),
            counter_route_key(&naddr([1, 2]))
        );
        assert_ne!(
            counter_route_key(&naddr([1, 2])),
            counter_route_key(&naddr([2, 1]))
        );
    }
}

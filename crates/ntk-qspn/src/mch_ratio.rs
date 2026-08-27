//! `get_mch_ratio` (`research/impl/vala/qspn/qspn.vala:1888-1909`): the
//! size/gateway-adaptive overlap-tolerance ratio disjoint-path admission
//! checks candidate paths against.

use crate::config::MchRatioTable;

/// Computes the admissible hop-overlap ratio for a destination of `size`
/// nodes reachable through `numgw` distinct gateways, given the deployment's
/// base `max_common_hops_ratio` and lookup `table`. Verbatim port of
/// `qspn.vala:1888-1909`: more gateways or a bigger destination tightens
/// (shrinks) the tolerance.
#[must_use]
pub fn mch_ratio(max_common_hops_ratio: f64, table: &MchRatioTable, size: u32, numgw: u32) -> f64 {
    let l = match numgw {
        1..=7 => table.gateway_ratio[(numgw - 1) as usize],
        _ => table.gateway_ratio_overflow,
    };
    let e = max_common_hops_ratio * l;
    let g = table
        .size_ratio_bands
        .iter()
        .find(|&&(bound, _)| size < bound)
        .map_or(table.size_ratio_overflow, |&(_, ratio)| ratio);
    (max_common_hops_ratio - e) * g + e
}

#[cfg(test)]
mod tests {
    use super::*;

    // Verbatim against the upstream table (qspn.vala:1888-1909), computed by
    // hand from the same formula the function implements, for a grid of
    // (numgw, size) pairs spanning every band of both ladders.
    fn expected(base: f64, numgw: u32, size: u32) -> f64 {
        let l = match numgw {
            1 => 0.45,
            2 => 0.35,
            3 => 0.27,
            4 => 0.20,
            5 => 0.15,
            6 => 0.12,
            7 => 0.10,
            _ => 0.08,
        };
        let e = base * l;
        let g = if size < 10 {
            1.0
        } else if size < 25 {
            0.9
        } else if size < 75 {
            0.8
        } else if size < 250 {
            0.6
        } else if size < 750 {
            0.3
        } else if size < 3000 {
            0.1
        } else {
            0.0001
        };
        (base - e) * g + e
    }

    #[test]
    fn matches_upstream_table_across_gateway_and_size_bands() {
        let table = MchRatioTable::default();
        let base = 0.6; // the reference deployment's max_common_hops_ratio
        for numgw in [0u32, 1, 2, 3, 4, 5, 6, 7, 8, 20] {
            for size in [
                0u32, 9, 10, 24, 25, 74, 75, 249, 250, 749, 750, 2999, 3000, 100_000,
            ] {
                let got = mch_ratio(base, &table, size, numgw);
                let want = expected(base, numgw, size);
                assert!(
                    (got - want).abs() < 1e-12,
                    "numgw={numgw} size={size}: got {got}, want {want}"
                );
            }
        }
    }

    #[test]
    fn more_gateways_never_widens_tolerance() {
        let table = MchRatioTable::default();
        let base = 0.6;
        let mut prev = mch_ratio(base, &table, 100, 1);
        for numgw in 2..=10 {
            let cur = mch_ratio(base, &table, 100, numgw);
            assert!(cur <= prev + 1e-12, "numgw={numgw}: {cur} > {prev}");
            prev = cur;
        }
    }

    #[test]
    fn bigger_destination_never_widens_tolerance() {
        let table = MchRatioTable::default();
        let base = 0.6;
        let sizes = [1u32, 15, 50, 100, 500, 1000, 5000];
        let mut prev = mch_ratio(base, &table, sizes[0], 3);
        for &size in &sizes[1..] {
            let cur = mch_ratio(base, &table, size, 3);
            assert!(cur <= prev + 1e-12, "size={size}: {cur} > {prev}");
            prev = cur;
        }
    }
}

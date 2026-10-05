// SPDX-FileCopyrightText: 2026 Gianluca Boiano
// SPDX-License-Identifier: GPL-3.0-or-later

//! Crash-recovery: the `ntkclean` behavior, ported from
//! `ntkd/cleaning/cleaning.vala`, but done right.
//!
//! Upstream's `ntkclean` regex-scrapes the *text output* of `ip route
//! list`/`ip address show`/`iptables -L ... -n` to guess which lines its own
//! daemon could plausibly have produced, then issues matching `del` commands
//! (`research/notes/02-vala-services-daemon.md` "Crash-recovery tool"). That
//! is fragile by construction — it can only recognize shapes the author
//! anticipated, and a false match silently deletes state that belongs to
//! something else on the host.
//!
//! This module enumerates the *structured* kernel state via the same
//! netlink trait every other Netsukuku operation uses
//! ([`AddressTable`]/[`RouteTable`]/[`RuleTable`]), and applies one
//! ownership predicate per object kind:
//!
//! - **Route ownership**: a route belongs to Netsukuku *iff* its `table` id
//!   is one [`TableAllocator::owned_tables`] enumerates — the fixed main
//!   table plus the whole configured peer-table range. We do not
//!   additionally inspect the destination: Netsukuku is the only writer to
//!   these table ids by construction (nothing else on a normal host has a
//!   reason to write routes into table 200-251), so table membership alone
//!   is sufficient and exact. We never touch `main`/`default`/`local` (ids
//!   254/253/255) or `unspec` (0), even if a route there happens to have a
//!   `10.0.0.0/8` destination that looks like ours — see design decision 1,
//!   this crate never manipulates the host's own routing table.
//! - **Rule ownership**: identical predicate, applied to
//!   [`RuleSpec::table`] — this covers both the main identity's plain
//!   `table <main>` rule and every per-peer `fwmark <tid> table <tid>` rule.
//! - **Address ownership**: an address belongs to Netsukuku *iff* it is a
//!   `/32` inside [`NETSUKUKU_ADDRESS_SPACE`] (`10.0.0.0/8`,
//!   `ipv4_compute.vala:23-168`) **on loopback** — the only place the daemon
//!   installs its identity address. Unlike table ids, `10.0.0.0/8` is not
//!   exclusively Netsukuku's, so operator LAN/VIP addresses (other prefix
//!   lengths, or any address on a managed NIC) are never deleted.
//!
//! **Explicitly out of scope** (unlike upstream's `ntkclean`): stale
//! `ntkv*` network namespaces, `macvlan` pseudo-devices, and
//! `iptables`/`NAT` rules. This crate's own [`AddressTable`]/[`RouteTable`]/
//! [`RuleTable`] never create namespaces, links, or NAT rules — only the
//! identities/neighborhood crates (phase 2) and the anonymizing-address
//! feature (explicitly deferred, `research/notes/06-rust-stack.md` open
//! question 5) will. Cleaning up state this crate cannot itself create would
//! be scope creep with no way to state an ownership rule for it here.

use crate::error::NetlinkError;
use crate::table::TableAllocator;
use crate::traits::{AddressTable, RouteTable, RuleTable, TopologyQuery};
use crate::types::{AddressEntry, Interface, NETSUKUKU_ADDRESS_SPACE, RouteKey, RuleSpec};

/// What [`cleanup`] found and removed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CleanupReport {
    /// Addresses removed, in removal order.
    pub addresses_removed: Vec<AddressEntry>,
    /// Routes removed, in removal order.
    pub routes_removed: Vec<RouteKey>,
    /// Rules removed, in removal order.
    pub rules_removed: Vec<RuleSpec>,
}

impl CleanupReport {
    /// Whether anything was removed at all.
    pub fn is_empty(&self) -> bool {
        self.addresses_removed.is_empty()
            && self.routes_removed.is_empty()
            && self.rules_removed.is_empty()
    }
}

/// Removes every piece of kernel state this crate can determine belongs to
/// Netsukuku — see the module documentation for the exact, per-object-kind
/// ownership predicate. `_managed_interfaces` is accepted for API compatibility but no longer
/// widens the address sweep: only the identity `/32` on `lo` is removed.
pub async fn cleanup<T, K>(
    kernel: &T,
    table_allocator: &TableAllocator<K>,
    _managed_interfaces: &[Interface],
) -> Result<CleanupReport, NetlinkError>
where
    T: AddressTable + RouteTable + RuleTable + TopologyQuery,
{
    let mut report = CleanupReport::default();

    // Only what the daemon itself installs is swept: its identity address, a /32 inside
    // 10.0.0.0/8 on loopback. Any other 10/8 address — an operator's LAN or VIP address on a
    // managed NIC or on `lo` with a shorter prefix — is not ours to delete.
    let lo = Interface::name("lo");
    for entry in kernel.list_addresses(Some(&lo)).await? {
        if entry.network.prefix_len() == 32
            && NETSUKUKU_ADDRESS_SPACE.contains(entry.network.address())
        {
            match kernel.remove_address(&lo, entry.network).await {
                Ok(()) => report.addresses_removed.push(entry),
                Err(error) if error.is_not_found() => {}
                Err(error) => return Err(error),
            }
        }
    }

    // One unfiltered dump, filtered by table ownership here, instead of one dump per table.
    for route in kernel.list_routes(None).await? {
        if !table_allocator.owns_table(route.table) {
            continue;
        }
        let key = RouteKey {
            destination: route.destination,
            table: route.table,
        };
        match kernel.remove_route(key).await {
            Ok(()) => report.routes_removed.push(key),
            Err(error) if error.is_not_found() => {}
            Err(error) => return Err(error),
        }
    }

    for rule in kernel.list_rules().await? {
        if table_allocator.owns_table(rule.table) {
            match kernel.remove_rule(&rule).await {
                Ok(()) => report.rules_removed.push(rule),
                Err(error) if error.is_not_found() => {}
                Err(error) => return Err(error),
            }
        }
    }

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeNetlink;
    use crate::types::{Ipv4Net, LinkInfo, RouteSpec, RouteTarget, RuleSelector};
    use std::net::Ipv4Addr;

    fn managed_fake() -> FakeNetlink {
        FakeNetlink::with_links(vec![
            LinkInfo {
                index: 1,
                name: "lo".into(),
                is_up: true,
            },
            LinkInfo {
                index: 2,
                name: "eth0".into(),
                is_up: true,
            },
        ])
    }

    #[tokio::test]
    async fn removes_only_owned_addresses_routes_and_rules() {
        let fake = managed_fake();
        let eth0 = Interface::name("eth0");
        let allocator: TableAllocator<&str> = TableAllocator::new();

        let lo = Interface::name("lo");
        // Owned: the identity /32 on loopback.
        let owned_addr = Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 5), 32).unwrap();
        fake.add_address(&lo, owned_addr).await.unwrap();
        // Foreign: not in 10.0.0.0/8, must survive.
        let foreign_addr = Ipv4Net::new(Ipv4Addr::new(192, 168, 1, 5), 32).unwrap();
        fake.add_address(&eth0, foreign_addr).await.unwrap();

        // Owned: a route in an allocator-owned table.
        let owned_route = RouteSpec {
            destination: Ipv4Net::new(Ipv4Addr::new(10, 1, 0, 0), 16).unwrap(),
            table: allocator.main_table(),
            target: RouteTarget::Unreachable,
        };
        fake.add_route(&owned_route).await.unwrap();
        // Foreign: a route in a table the allocator does not own, must survive.
        let foreign_route = RouteSpec {
            destination: Ipv4Net::new(Ipv4Addr::new(172, 16, 0, 0), 16).unwrap(),
            table: 900,
            target: RouteTarget::Unreachable,
        };
        fake.add_route(&foreign_route).await.unwrap();

        // Owned: the main identity's catch-all rule.
        let owned_rule = RuleSpec {
            table: allocator.main_table(),
            priority: allocator.main_rule_priority(),
            selector: RuleSelector::Any,
        };
        fake.add_rule(&owned_rule).await.unwrap();
        // Foreign: a rule pointing at an unrelated table, must survive.
        let foreign_rule = RuleSpec {
            table: 900,
            priority: 50,
            selector: RuleSelector::Any,
        };
        fake.add_rule(&foreign_rule).await.unwrap();

        let report = cleanup(&fake, &allocator, std::slice::from_ref(&eth0))
            .await
            .unwrap();

        assert_eq!(
            report.addresses_removed,
            vec![AddressEntry {
                interface_index: 1,
                network: owned_addr
            }]
        );
        assert_eq!(
            report.routes_removed,
            vec![RouteKey {
                destination: owned_route.destination,
                table: owned_route.table
            }]
        );
        assert_eq!(report.rules_removed, vec![owned_rule]);

        // Foreign state survives.
        assert_eq!(
            fake.list_addresses(Some(&eth0)).await.unwrap(),
            vec![AddressEntry {
                interface_index: 2,
                network: foreign_addr
            }]
        );
        assert_eq!(
            fake.list_routes(Some(900)).await.unwrap(),
            vec![foreign_route]
        );
        assert_eq!(fake.list_rules().await.unwrap(), vec![foreign_rule]);
    }

    #[tokio::test]
    async fn operator_ten_slash_eight_addresses_are_never_deleted() {
        let fake = managed_fake();
        let eth0 = Interface::name("eth0");
        let lo = Interface::name("lo");
        let lan = Ipv4Net::new(Ipv4Addr::new(10, 1, 2, 3), 24).unwrap();
        let vip_on_nic = Ipv4Net::new(Ipv4Addr::new(10, 9, 9, 9), 32).unwrap();
        let wide_on_lo = Ipv4Net::new(Ipv4Addr::new(10, 8, 0, 1), 16).unwrap();
        fake.add_address(&eth0, lan).await.unwrap();
        fake.add_address(&eth0, vip_on_nic).await.unwrap();
        fake.add_address(&lo, wide_on_lo).await.unwrap();

        let allocator: TableAllocator<&str> = TableAllocator::new();
        let report = cleanup(&fake, &allocator, std::slice::from_ref(&eth0))
            .await
            .unwrap();

        assert!(report.is_empty(), "nothing of ours was present: {report:?}");
        assert_eq!(fake.list_addresses(None).await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn identity_address_on_loopback_is_swept_without_managed_interfaces() {
        let fake = managed_fake();
        let lo = Interface::name("lo");
        let owned_addr = Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 1), 32).unwrap();
        fake.add_address(&lo, owned_addr).await.unwrap();

        let allocator: TableAllocator<&str> = TableAllocator::new();
        let report = cleanup(&fake, &allocator, &[]).await.unwrap();

        assert_eq!(
            report.addresses_removed,
            vec![AddressEntry {
                interface_index: 1,
                network: owned_addr
            }]
        );
    }

    #[tokio::test]
    async fn routes_in_every_owned_table_are_swept_from_one_dump() {
        let fake = managed_fake();
        let allocator: TableAllocator<&str> = TableAllocator::new();
        let tables = [200, 233, allocator.main_table()];
        for table in tables {
            fake.add_route(&RouteSpec {
                destination: Ipv4Net::new(Ipv4Addr::new(10, 5, 0, 0), 16).unwrap(),
                table,
                target: RouteTarget::Unreachable,
            })
            .await
            .unwrap();
        }
        let report = cleanup(&fake, &allocator, &[]).await.unwrap();
        assert_eq!(report.routes_removed.len(), 3);
        assert!(fake.list_routes(None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn empty_kernel_state_yields_empty_report() {
        let fake = managed_fake();
        let allocator: TableAllocator<&str> = TableAllocator::new();
        let report = cleanup(&fake, &allocator, &[Interface::name("eth0")])
            .await
            .unwrap();
        assert!(report.is_empty());
    }
}

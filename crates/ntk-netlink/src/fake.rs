//! An in-memory, non-privileged stand-in for [`crate::RealNetlink`]. Records
//! every mutation in invocation order and answers queries from its own
//! model, so upper-layer crates (and their `turmoil`-based simulations, per
//! `research/notes/06-rust-stack.md` "Trait boundary is load-bearing for
//! simulation coverage") never need real `CAP_NET_ADMIN` to exercise their
//! netlink-facing logic.

use std::collections::HashMap;
use std::sync::Mutex;

use crate::error::NetlinkError;
use crate::table::guard_table;
use crate::traits::{AddressTable, RouteTable, RuleTable, TopologyQuery, resolve_interface};
use crate::types::{
    AddressEntry, Interface, Ipv4Net, LinkInfo, NeighbourInfo, Operation, RouteKey, RouteSpec,
    RuleSpec,
};

#[derive(Debug)]
struct FakeState {
    operations: Vec<Operation>,
    links: Vec<LinkInfo>,
    neighbours: Vec<NeighbourInfo>,
    addresses: Vec<AddressEntry>,
    routes: HashMap<(u32, Ipv4Net), RouteSpec>,
    rules: Vec<RuleSpec>,
}

/// A recording, in-memory implementation of the full [`crate::Netlink`]
/// surface. Every mutating call is appended to an ordered operation log
/// (see [`FakeNetlink::operations`]) and applied to a small in-memory model
/// that answers the corresponding query methods, so a test can both assert
/// "exactly these operations happened, in this order" and "the resulting
/// state looks like this" without ever touching a real kernel.
///
/// Interface resolution goes through [`resolve_interface`] exactly like
/// [`crate::RealNetlink`] does — an [`Interface::Name`] or
/// [`Interface::Index`] that isn't in the seeded link table (see
/// [`FakeNetlink::with_links`]) fails with [`NetlinkError::InterfaceNotFound`],
/// matching `ip address add ... dev nonexistent`'s real failure mode.
#[derive(Debug)]
pub struct FakeNetlink {
    state: Mutex<FakeState>,
}

impl FakeNetlink {
    /// An empty fake: no links, no addresses, no routes, no rules.
    pub fn new() -> Self {
        Self {
            state: Mutex::new(FakeState {
                operations: Vec::new(),
                links: Vec::new(),
                neighbours: Vec::new(),
                addresses: Vec::new(),
                routes: HashMap::new(),
                rules: Vec::new(),
            }),
        }
    }

    /// An empty fake seeded with a link table, so [`Interface`] resolution
    /// succeeds for address operations without a real kernel.
    pub fn with_links(links: Vec<LinkInfo>) -> Self {
        let fake = Self::new();
        fake.lock().links = links;
        fake
    }

    /// Replaces the neighbour-cache model. There is no `add_neighbour`
    /// mutation in [`crate::TopologyQuery`] (upstream never installs static
    /// ARP entries), so tests inject neighbour-discovery fixtures directly.
    pub fn seed_neighbours(&self, neighbours: Vec<NeighbourInfo>) {
        self.lock().neighbours = neighbours;
    }

    /// The ordered log of every mutating call made so far.
    pub fn operations(&self) -> Vec<Operation> {
        self.lock().operations.clone()
    }

    /// Empties the operation log without touching the address/route/rule
    /// model — useful to isolate "operations caused by the next step" in a
    /// multi-phase test.
    pub fn clear_operations(&self) {
        self.lock().operations.clear();
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.state.lock().expect("FakeNetlink mutex poisoned")
    }
}

impl Default for FakeNetlink {
    fn default() -> Self {
        Self::new()
    }
}

impl AddressTable for FakeNetlink {
    async fn add_address(
        &self,
        interface: &Interface,
        network: Ipv4Net,
    ) -> Result<(), NetlinkError> {
        let link = resolve_interface(self, interface).await?;
        let mut state = self.lock();
        if state
            .addresses
            .iter()
            .any(|a| a.interface_index == link.index && a.network == network)
        {
            return Err(NetlinkError::AlreadyExists(format!(
                "address {network} on {interface}"
            )));
        }
        state.addresses.push(AddressEntry {
            interface_index: link.index,
            network,
        });
        state.operations.push(Operation::AddAddress {
            interface: interface.clone(),
            network,
        });
        Ok(())
    }

    async fn remove_address(
        &self,
        interface: &Interface,
        network: Ipv4Net,
    ) -> Result<(), NetlinkError> {
        let link = resolve_interface(self, interface).await?;
        let mut state = self.lock();
        let position = state
            .addresses
            .iter()
            .position(|a| a.interface_index == link.index && a.network == network)
            .ok_or_else(|| NetlinkError::NotFound(format!("address {network} on {interface}")))?;
        state.addresses.remove(position);
        state.operations.push(Operation::RemoveAddress {
            interface: interface.clone(),
            network,
        });
        Ok(())
    }

    async fn list_addresses(
        &self,
        interface: Option<&Interface>,
    ) -> Result<Vec<AddressEntry>, NetlinkError> {
        match interface {
            None => Ok(self.lock().addresses.clone()),
            Some(iface) => {
                let link = resolve_interface(self, iface).await?;
                Ok(self
                    .lock()
                    .addresses
                    .iter()
                    .filter(|a| a.interface_index == link.index)
                    .cloned()
                    .collect())
            }
        }
    }
}

impl RouteTable for FakeNetlink {
    async fn add_route(&self, route: &RouteSpec) -> Result<(), NetlinkError> {
        guard_table(route.table)?;
        let mut state = self.lock();
        let key = (route.table, route.destination);
        if state.routes.contains_key(&key) {
            return Err(NetlinkError::AlreadyExists(format!(
                "route {} table {}",
                route.destination, route.table
            )));
        }
        state.routes.insert(key, route.clone());
        state.operations.push(Operation::AddRoute(route.clone()));
        Ok(())
    }

    async fn change_route(&self, route: &RouteSpec) -> Result<(), NetlinkError> {
        guard_table(route.table)?;
        let mut state = self.lock();
        state
            .routes
            .insert((route.table, route.destination), route.clone());
        state.operations.push(Operation::ChangeRoute(route.clone()));
        Ok(())
    }

    async fn remove_route(&self, route: RouteKey) -> Result<(), NetlinkError> {
        guard_table(route.table)?;
        let mut state = self.lock();
        state
            .routes
            .remove(&(route.table, route.destination))
            .ok_or_else(|| {
                NetlinkError::NotFound(format!("route {} table {}", route.destination, route.table))
            })?;
        state.operations.push(Operation::RemoveRoute(route));
        Ok(())
    }

    async fn list_routes(&self, table: Option<u32>) -> Result<Vec<RouteSpec>, NetlinkError> {
        let state = self.lock();
        Ok(state
            .routes
            .values()
            .filter(|r| table.is_none_or(|t| r.table == t))
            .cloned()
            .collect())
    }
}

impl RuleTable for FakeNetlink {
    async fn add_rule(&self, rule: &RuleSpec) -> Result<(), NetlinkError> {
        guard_table(rule.table)?;
        let mut state = self.lock();
        if state.rules.contains(rule) {
            return Err(NetlinkError::AlreadyExists(format!("rule {rule:?}")));
        }
        state.rules.push(*rule);
        state.operations.push(Operation::AddRule(*rule));
        Ok(())
    }

    async fn remove_rule(&self, rule: &RuleSpec) -> Result<(), NetlinkError> {
        guard_table(rule.table)?;
        let mut state = self.lock();
        let position = state
            .rules
            .iter()
            .position(|r| r == rule)
            .ok_or_else(|| NetlinkError::NotFound(format!("rule {rule:?}")))?;
        state.rules.remove(position);
        state.operations.push(Operation::RemoveRule(*rule));
        Ok(())
    }

    async fn list_rules(&self) -> Result<Vec<RuleSpec>, NetlinkError> {
        Ok(self.lock().rules.clone())
    }
}

impl TopologyQuery for FakeNetlink {
    async fn list_links(&self) -> Result<Vec<LinkInfo>, NetlinkError> {
        Ok(self.lock().links.clone())
    }

    async fn list_neighbours(
        &self,
        interface: Option<&Interface>,
    ) -> Result<Vec<NeighbourInfo>, NetlinkError> {
        match interface {
            None => Ok(self.lock().neighbours.clone()),
            Some(iface) => {
                let link = resolve_interface(self, iface).await?;
                Ok(self
                    .lock()
                    .neighbours
                    .iter()
                    .filter(|n| n.interface_index == link.index)
                    .cloned()
                    .collect())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{NeighbourState, RouteTarget, RuleSelector};
    use std::net::Ipv4Addr;

    fn eth0() -> Interface {
        Interface::name("eth0")
    }

    fn fake_with_eth0() -> FakeNetlink {
        FakeNetlink::with_links(vec![LinkInfo {
            index: 2,
            name: "eth0".into(),
            is_up: true,
        }])
    }

    #[tokio::test]
    async fn add_address_records_operation_and_is_queryable() {
        let fake = fake_with_eth0();
        let network = Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 1), 32).unwrap();
        fake.add_address(&eth0(), network).await.unwrap();

        assert_eq!(
            fake.operations(),
            vec![Operation::AddAddress {
                interface: eth0(),
                network
            }]
        );
        assert_eq!(
            fake.list_addresses(Some(&eth0())).await.unwrap(),
            vec![AddressEntry {
                interface_index: 2,
                network
            }]
        );
    }

    #[tokio::test]
    async fn add_address_on_unknown_interface_fails() {
        let fake = FakeNetlink::new();
        let network = Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 1), 32).unwrap();
        let err = fake.add_address(&eth0(), network).await.unwrap_err();
        assert!(matches!(err, NetlinkError::InterfaceNotFound(_)));
    }

    #[tokio::test]
    async fn duplicate_address_is_rejected() {
        let fake = fake_with_eth0();
        let network = Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 1), 32).unwrap();
        fake.add_address(&eth0(), network).await.unwrap();
        let err = fake.add_address(&eth0(), network).await.unwrap_err();
        assert!(matches!(err, NetlinkError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn remove_address_requires_prior_add() {
        let fake = fake_with_eth0();
        let network = Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 1), 32).unwrap();
        let err = fake.remove_address(&eth0(), network).await.unwrap_err();
        assert!(matches!(err, NetlinkError::NotFound(_)));

        fake.add_address(&eth0(), network).await.unwrap();
        fake.remove_address(&eth0(), network).await.unwrap();
        assert!(fake.list_addresses(Some(&eth0())).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn route_lifecycle_is_recorded_in_order() {
        let fake = FakeNetlink::new();
        let destination = Ipv4Net::new(Ipv4Addr::new(10, 1, 0, 0), 16).unwrap();
        let spec = RouteSpec {
            destination,
            table: 200,
            target: RouteTarget::Unreachable,
        };
        fake.add_route(&spec).await.unwrap();

        let changed = RouteSpec {
            destination,
            table: 200,
            target: RouteTarget::Gateway {
                via: Ipv4Addr::new(10, 0, 0, 2),
                dev: eth0(),
                src: None,
            },
        };
        fake.change_route(&changed).await.unwrap();
        fake.remove_route(RouteKey {
            destination,
            table: 200,
        })
        .await
        .unwrap();

        assert_eq!(
            fake.operations(),
            vec![
                Operation::AddRoute(spec),
                Operation::ChangeRoute(changed),
                Operation::RemoveRoute(RouteKey {
                    destination,
                    table: 200
                }),
            ]
        );
        assert!(fake.list_routes(Some(200)).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn duplicate_route_add_is_rejected_but_change_upserts() {
        let fake = FakeNetlink::new();
        let destination = Ipv4Net::new(Ipv4Addr::new(10, 2, 0, 0), 16).unwrap();
        let spec = RouteSpec {
            destination,
            table: 200,
            target: RouteTarget::Unreachable,
        };
        fake.add_route(&spec).await.unwrap();
        assert!(matches!(
            fake.add_route(&spec).await,
            Err(NetlinkError::AlreadyExists(_))
        ));
        fake.change_route(&spec).await.unwrap();
        assert_eq!(fake.list_routes(Some(200)).await.unwrap(), vec![spec]);
    }

    #[tokio::test]
    async fn onlink_route_is_recorded_like_any_other_route() {
        let fake = fake_with_eth0();
        let destination = Ipv4Net::host(Ipv4Addr::new(169, 254, 1, 2));
        let spec = RouteSpec {
            destination,
            table: 200,
            target: RouteTarget::OnLink { dev: eth0() },
        };
        fake.add_route(&spec).await.unwrap();
        assert_eq!(fake.operations(), vec![Operation::AddRoute(spec.clone())]);
        assert_eq!(fake.list_routes(Some(200)).await.unwrap(), vec![spec]);

        fake.remove_route(RouteKey {
            destination,
            table: 200,
        })
        .await
        .unwrap();
        assert!(fake.list_routes(Some(200)).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn route_and_rule_ops_reject_kernel_reserved_tables() {
        let fake = FakeNetlink::new();
        let destination = Ipv4Net::new(Ipv4Addr::UNSPECIFIED, 0).unwrap();
        let spec = RouteSpec {
            destination,
            table: 254,
            target: RouteTarget::Unreachable,
        };
        assert!(matches!(
            fake.add_route(&spec).await,
            Err(NetlinkError::ReservedTable(254))
        ));

        let rule = RuleSpec {
            table: 255,
            priority: 100,
            selector: RuleSelector::Any,
        };
        assert!(matches!(
            fake.add_rule(&rule).await,
            Err(NetlinkError::ReservedTable(255))
        ));
    }

    #[tokio::test]
    async fn rule_lifecycle_is_recorded() {
        let fake = FakeNetlink::new();
        let rule = RuleSpec {
            table: 251,
            priority: 10_000,
            selector: RuleSelector::Any,
        };
        fake.add_rule(&rule).await.unwrap();
        assert_eq!(fake.list_rules().await.unwrap(), vec![rule]);
        fake.remove_rule(&rule).await.unwrap();
        assert!(fake.list_rules().await.unwrap().is_empty());
        assert_eq!(
            fake.operations(),
            vec![Operation::AddRule(rule), Operation::RemoveRule(rule)]
        );
    }

    #[tokio::test]
    async fn neighbours_are_seeded_not_recorded() {
        let fake = fake_with_eth0();
        fake.seed_neighbours(vec![NeighbourInfo {
            interface_index: 2,
            address: Ipv4Addr::new(10, 0, 0, 5),
            link_layer_address: Some([0, 1, 2, 3, 4, 5]),
            state: NeighbourState::Reachable,
        }]);
        assert_eq!(fake.list_neighbours(Some(&eth0())).await.unwrap().len(), 1);
        assert_eq!(fake.list_neighbours(None).await.unwrap().len(), 1);
        assert!(fake.operations().is_empty());
    }

    #[tokio::test]
    async fn clear_operations_does_not_touch_model() {
        let fake = fake_with_eth0();
        let network = Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 1), 32).unwrap();
        fake.add_address(&eth0(), network).await.unwrap();
        fake.clear_operations();
        assert!(fake.operations().is_empty());
        assert_eq!(fake.list_addresses(None).await.unwrap().len(), 1);
    }
}

//! The real backend: `rtnetlink`/`netlink-packet-route` over a genuine
//! `NETLINK_ROUTE` socket, replacing every `ip`(8) subprocess call in
//! `ntkd/identity_ip_commands.vala` with a native netlink request.

use std::net::{IpAddr, Ipv4Addr};

use futures::TryStreamExt;
use netlink_packet_route::AddressFamily;
use netlink_packet_route::address::{AddressAttribute, AddressMessage};
use netlink_packet_route::link::{LinkAttribute, LinkFlags, LinkMessage};
use netlink_packet_route::neighbour::{
    NeighbourAddress, NeighbourAttribute, NeighbourMessage, NeighbourState as KernelNeighbourState,
};
use netlink_packet_route::route::{
    RouteAddress, RouteAttribute, RouteMessage, RouteNextHop, RouteScope, RouteType,
};
use netlink_packet_route::rule::{RuleAction, RuleAttribute, RuleMessage};
use rtnetlink::{AddressMessageBuilder, IpVersion, RouteMessageBuilder, RouteNextHopBuilder};

use crate::error::NetlinkError;
use crate::table::guard_table;
use crate::traits::{AddressTable, RouteTable, RuleTable, TopologyQuery, resolve_interface};
use crate::types::{
    AddressEntry, Interface, Ipv4Net, LinkInfo, NeighbourInfo, NeighbourState, Nexthop, RouteKey,
    RouteSpec, RouteTarget, RuleSelector, RuleSpec,
};

/// A live netlink connection. Read-only calls (every `list_*` method) work
/// unprivileged; every mutating call requires `CAP_NET_ADMIN`.
#[derive(Debug)]
pub struct RealNetlink {
    handle: rtnetlink::Handle,
}

impl RealNetlink {
    /// Opens a `NETLINK_ROUTE` socket and spawns its background I/O driver
    /// onto the current Tokio runtime (must be called from within one).
    pub fn new() -> Result<Self, NetlinkError> {
        let (connection, handle, _messages) =
            rtnetlink::new_connection().map_err(NetlinkError::Connect)?;
        tokio::spawn(connection);
        Ok(Self { handle })
    }

    async fn build_route_message(&self, route: &RouteSpec) -> Result<RouteMessage, NetlinkError> {
        let mut builder = RouteMessageBuilder::<Ipv4Addr>::new()
            .destination_prefix(route.destination.address(), route.destination.prefix_len())
            .table_id(route.table);
        builder = match &route.target {
            RouteTarget::Unreachable => builder.kind(RouteType::Unreachable),
            RouteTarget::Gateway { via, dev, src } => {
                let link = resolve_interface(self, dev).await?;
                let mut builder = builder.gateway(*via).output_interface(link.index);
                if let Some(src) = src {
                    builder = builder.pref_source(*src);
                }
                builder
            }
            RouteTarget::OnLink { dev } => {
                let link = resolve_interface(self, dev).await?;
                builder.output_interface(link.index).scope(RouteScope::Link)
            }
            RouteTarget::Multipath(nexthops) => {
                let mut hops = Vec::with_capacity(nexthops.len());
                for nexthop in nexthops {
                    let link = resolve_interface(self, &nexthop.dev).await?;
                    let hop = RouteNextHopBuilder::new_ipv4()
                        .interface(link.index)
                        .weight(nexthop.weight)
                        .via(IpAddr::V4(nexthop.via))
                        .expect("an IPv4 nexthop builder always accepts an IPv4 via-address")
                        .build();
                    hops.push(hop);
                }
                builder.multipath(hops)
            }
        };
        Ok(builder.build())
    }
}

impl AddressTable for RealNetlink {
    async fn add_address(
        &self,
        interface: &Interface,
        network: Ipv4Net,
    ) -> Result<(), NetlinkError> {
        let link = resolve_interface(self, interface).await?;
        self.handle
            .address()
            .add(
                link.index,
                IpAddr::V4(network.address()),
                network.prefix_len(),
            )
            .execute()
            .await?;
        Ok(())
    }

    async fn remove_address(
        &self,
        interface: &Interface,
        network: Ipv4Net,
    ) -> Result<(), NetlinkError> {
        let link = resolve_interface(self, interface).await?;
        let message = AddressMessageBuilder::<Ipv4Addr>::new()
            .index(link.index)
            .address(network.address(), network.prefix_len())
            .build();
        self.handle.address().del(message).execute().await?;
        Ok(())
    }

    async fn list_addresses(
        &self,
        interface: Option<&Interface>,
    ) -> Result<Vec<AddressEntry>, NetlinkError> {
        let mut request = self.handle.address().get();
        if let Some(interface) = interface {
            let link = resolve_interface(self, interface).await?;
            request = request.set_link_index_filter(link.index);
        }
        let mut stream = request.execute();
        let mut entries = Vec::new();
        while let Some(message) = stream.try_next().await? {
            if let Some(entry) = parse_address(&message) {
                entries.push(entry);
            }
        }
        Ok(entries)
    }
}

impl RouteTable for RealNetlink {
    async fn add_route(&self, route: &RouteSpec) -> Result<(), NetlinkError> {
        guard_table(route.table)?;
        let message = self.build_route_message(route).await?;
        self.handle.route().add(message).execute().await?;
        Ok(())
    }

    async fn change_route(&self, route: &RouteSpec) -> Result<(), NetlinkError> {
        guard_table(route.table)?;
        let message = self.build_route_message(route).await?;
        self.handle.route().add(message).replace().execute().await?;
        Ok(())
    }

    async fn remove_route(&self, route: RouteKey) -> Result<(), NetlinkError> {
        guard_table(route.table)?;
        // `RouteMessageBuilder::new` defaults `kind` to `Unicast` and `scope` to `Universe`,
        // but the kernel's `RTM_DELROUTE` handler matches on both `rtm_type` and `rtm_scope`
        // when they are not the wildcard values (`Unspec`/`NoWhere` respectively): a stored
        // `Unreachable`/`Multipath` route would not match a delete request that (implicitly)
        // asks for `Unicast`, and a stored `OnLink` route (`scope: Link`, confirmed empirically
        // against a real kernel — a plain `Universe`-scope delete request fails it with
        // `ESRCH` even though the route exists) would not match one that (implicitly) asks for
        // `Universe` — either way failing with `ESRCH` even though a route to this destination
        // exists. Reset both to their wildcard so deletion matches by `(table, destination)`
        // alone, regardless of route shape — exactly what plain `ip route del <dst> table <t>`
        // does.
        let message = RouteMessageBuilder::<Ipv4Addr>::new()
            .destination_prefix(route.destination.address(), route.destination.prefix_len())
            .table_id(route.table)
            .kind(RouteType::Unspec)
            .scope(RouteScope::NoWhere)
            .build();
        self.handle.route().del(message).execute().await?;
        Ok(())
    }

    async fn list_routes(&self, table: Option<u32>) -> Result<Vec<RouteSpec>, NetlinkError> {
        let query = RouteMessageBuilder::<Ipv4Addr>::new().build();
        let mut stream = self.handle.route().get(query).execute();
        let mut specs = Vec::new();
        while let Some(message) = stream.try_next().await? {
            if let Some(spec) = parse_route(&message)
                && table.is_none_or(|wanted| spec.table == wanted)
            {
                specs.push(spec);
            }
        }
        Ok(specs)
    }
}

impl RuleTable for RealNetlink {
    async fn add_rule(&self, rule: &RuleSpec) -> Result<(), NetlinkError> {
        guard_table(rule.table)?;
        let mut request = self
            .handle
            .rule()
            .add()
            .v4()
            .table_id(rule.table)
            .priority(rule.priority)
            .action(RuleAction::ToTable);
        if let RuleSelector::FwMark(mark) = rule.selector {
            request = request.fw_mark(mark);
        }
        request.execute().await?;
        Ok(())
    }

    async fn remove_rule(&self, rule: &RuleSpec) -> Result<(), NetlinkError> {
        guard_table(rule.table)?;
        let mut message = RuleMessage::default();
        message.header.family = AddressFamily::Inet;
        message.header.action = RuleAction::ToTable;
        if rule.table > 255 {
            message.attributes.push(RuleAttribute::Table(rule.table));
        } else {
            message.header.table = rule.table as u8;
        }
        message
            .attributes
            .push(RuleAttribute::Priority(rule.priority));
        if let RuleSelector::FwMark(mark) = rule.selector {
            message.attributes.push(RuleAttribute::FwMark(mark));
        }
        self.handle.rule().del(message).execute().await?;
        Ok(())
    }

    async fn list_rules(&self) -> Result<Vec<RuleSpec>, NetlinkError> {
        let mut stream = self.handle.rule().get(IpVersion::V4).execute();
        let mut specs = Vec::new();
        while let Some(message) = stream.try_next().await? {
            if let Some(spec) = parse_rule(&message) {
                specs.push(spec);
            }
        }
        Ok(specs)
    }
}

impl TopologyQuery for RealNetlink {
    async fn list_links(&self) -> Result<Vec<LinkInfo>, NetlinkError> {
        let mut stream = self.handle.link().get().execute();
        let mut links = Vec::new();
        while let Some(message) = stream.try_next().await? {
            if let Some(link) = parse_link(&message) {
                links.push(link);
            }
        }
        Ok(links)
    }

    async fn list_neighbours(
        &self,
        interface: Option<&Interface>,
    ) -> Result<Vec<NeighbourInfo>, NetlinkError> {
        let mut stream = self
            .handle
            .neighbours()
            .get()
            .set_address_family(AddressFamily::Inet)
            .execute();
        let mut neighbours = Vec::new();
        while let Some(message) = stream.try_next().await? {
            if let Some(neighbour) = parse_neighbour(&message) {
                neighbours.push(neighbour);
            }
        }
        if let Some(interface) = interface {
            let link = resolve_interface(self, interface).await?;
            neighbours.retain(|neighbour| neighbour.interface_index == link.index);
        }
        Ok(neighbours)
    }
}

fn parse_address(message: &AddressMessage) -> Option<AddressEntry> {
    if message.header.family != AddressFamily::Inet {
        return None;
    }
    let address = message
        .attributes
        .iter()
        .find_map(|attribute| match attribute {
            AddressAttribute::Address(IpAddr::V4(address)) => Some(*address),
            _ => None,
        })?;
    let network = Ipv4Net::new(address, message.header.prefix_len).ok()?;
    Some(AddressEntry {
        interface_index: message.header.index,
        network,
    })
}

fn parse_route(message: &RouteMessage) -> Option<RouteSpec> {
    if message.header.address_family != AddressFamily::Inet {
        return None;
    }
    let destination_address = message
        .attributes
        .iter()
        .find_map(|attribute| match attribute {
            RouteAttribute::Destination(RouteAddress::Inet(address)) => Some(*address),
            _ => None,
        })
        .unwrap_or(Ipv4Addr::UNSPECIFIED);
    let destination = Ipv4Net::new(
        destination_address,
        message.header.destination_prefix_length,
    )
    .ok()?;
    let table = message
        .attributes
        .iter()
        .find_map(|attribute| {
            if let RouteAttribute::Table(table) = attribute {
                Some(*table)
            } else {
                None
            }
        })
        .unwrap_or(message.header.table as u32);
    let target = if message.header.kind == RouteType::Unreachable {
        RouteTarget::Unreachable
    } else if let Some(nexthops) = message.attributes.iter().find_map(|attribute| {
        if let RouteAttribute::MultiPath(nexthops) = attribute {
            Some(nexthops)
        } else {
            None
        }
    }) {
        RouteTarget::Multipath(nexthops.iter().filter_map(parse_nexthop).collect())
    } else {
        let via = message
            .attributes
            .iter()
            .find_map(|attribute| match attribute {
                RouteAttribute::Gateway(RouteAddress::Inet(address)) => Some(*address),
                _ => None,
            });
        let dev = message.attributes.iter().find_map(|attribute| {
            if let RouteAttribute::Oif(index) = attribute {
                Some(Interface::Index(*index))
            } else {
                None
            }
        })?;
        match via {
            Some(via) => {
                let src = message
                    .attributes
                    .iter()
                    .find_map(|attribute| match attribute {
                        RouteAttribute::PrefSource(RouteAddress::Inet(address)) => Some(*address),
                        _ => None,
                    });
                RouteTarget::Gateway { via, dev, src }
            }
            None => RouteTarget::OnLink { dev },
        }
    };
    Some(RouteSpec {
        destination,
        table,
        target,
    })
}

fn parse_nexthop(nexthop: &RouteNextHop) -> Option<Nexthop> {
    let via = nexthop
        .attributes
        .iter()
        .find_map(|attribute| match attribute {
            RouteAttribute::Gateway(RouteAddress::Inet(address)) => Some(*address),
            _ => None,
        })?;
    Some(Nexthop {
        via,
        dev: Interface::Index(nexthop.interface_index),
        weight: nexthop.hops,
    })
}

fn parse_rule(message: &RuleMessage) -> Option<RuleSpec> {
    if message.header.family != AddressFamily::Inet {
        return None;
    }
    let table = message
        .attributes
        .iter()
        .find_map(|attribute| {
            if let RuleAttribute::Table(table) = attribute {
                Some(*table)
            } else {
                None
            }
        })
        .unwrap_or(message.header.table as u32);
    // Rules this crate did not create (e.g. the kernel's own built-in
    // `local`/`main`/`default` rules) never carry an explicit priority
    // attribute; skip them rather than guess one, since a guessed value
    // could never round-trip back to `remove_rule`.
    let priority = message.attributes.iter().find_map(|attribute| {
        if let RuleAttribute::Priority(priority) = attribute {
            Some(*priority)
        } else {
            None
        }
    })?;
    let selector = message
        .attributes
        .iter()
        .find_map(|attribute| {
            if let RuleAttribute::FwMark(mark) = attribute {
                Some(RuleSelector::FwMark(*mark))
            } else {
                None
            }
        })
        .unwrap_or(RuleSelector::Any);
    Some(RuleSpec {
        table,
        priority,
        selector,
    })
}

fn parse_link(message: &LinkMessage) -> Option<LinkInfo> {
    let name = message.attributes.iter().find_map(|attribute| {
        if let LinkAttribute::IfName(name) = attribute {
            Some(name.clone())
        } else {
            None
        }
    })?;
    Some(LinkInfo {
        index: message.header.index,
        name,
        is_up: message.header.flags.contains(LinkFlags::Up),
    })
}

fn parse_neighbour(message: &NeighbourMessage) -> Option<NeighbourInfo> {
    if message.header.family != AddressFamily::Inet {
        return None;
    }
    let address = message
        .attributes
        .iter()
        .find_map(|attribute| match attribute {
            NeighbourAttribute::Destination(NeighbourAddress::Inet(address)) => Some(*address),
            _ => None,
        })?;
    let link_layer_address = message.attributes.iter().find_map(|attribute| {
        if let NeighbourAttribute::LinkLayerAddress(bytes) = attribute {
            <[u8; 6]>::try_from(bytes.as_slice()).ok()
        } else {
            None
        }
    });
    Some(NeighbourInfo {
        interface_index: message.header.ifindex,
        address,
        link_layer_address,
        state: map_neighbour_state(message.header.state),
    })
}

fn map_neighbour_state(state: KernelNeighbourState) -> NeighbourState {
    match state {
        KernelNeighbourState::Reachable | KernelNeighbourState::Permanent => {
            NeighbourState::Reachable
        }
        KernelNeighbourState::Stale | KernelNeighbourState::Delay | KernelNeighbourState::Probe => {
            NeighbourState::Stale
        }
        KernelNeighbourState::Incomplete => NeighbourState::Incomplete,
        _ => NeighbourState::Failed,
    }
}

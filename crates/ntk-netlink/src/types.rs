//! Domain types at the crate's API boundary: std `Ipv4Addr`, `u8` prefix
//! lengths and `u32` table ids only — no `ipnet`/`macaddr`/third-party
//! address crates, per the phase-1 contract.

use std::fmt;
use std::net::Ipv4Addr;

use crate::error::NetlinkError;

/// A reference to a network interface, by name or by kernel `ifindex`.
///
/// Both forms round-trip through [`crate::TopologyQuery::list_links`]
/// (`research/impl/vala/ntkd/identity_ip_commands.vala` addresses interfaces
/// by name throughout, e.g. `ip address add ... dev eth1`; the kernel's own
/// netlink messages carry the numeric index). Recorded [`crate::Operation`]s
/// preserve whichever form the caller passed — no implicit resolution
/// happens when comparing two `Interface` values.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Interface {
    /// Interface name, e.g. `"eth0"`, `"lo"`.
    Name(String),
    /// Kernel `ifindex`.
    Index(u32),
}

impl Interface {
    /// Builds an [`Interface::Name`].
    pub fn name(name: impl Into<String>) -> Self {
        Interface::Name(name.into())
    }

    /// Builds an [`Interface::Index`].
    pub fn index(index: u32) -> Self {
        Interface::Index(index)
    }
}

impl From<&str> for Interface {
    fn from(name: &str) -> Self {
        Interface::Name(name.to_owned())
    }
}

impl From<u32> for Interface {
    fn from(index: u32) -> Self {
        Interface::Index(index)
    }
}

impl fmt::Display for Interface {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Interface::Name(name) => f.write_str(name),
            Interface::Index(index) => write!(f, "#{index}"),
        }
    }
}

/// An IPv4 network in CIDR notation (address + prefix length).
///
/// Every Netsukuku address upstream is IPv4-only, `10.0.0.0/8`
/// (`research/notes/02-vala-services-daemon.md` "NIP↔IPv4",
/// `ipv4_compute.vala:23-168`) — this type is the crate-wide representation
/// for both host addresses (`prefix_len == 32`) and g-node ranges.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Ipv4Net {
    address: Ipv4Addr,
    prefix_len: u8,
}

impl Ipv4Net {
    /// Builds a network, rejecting a prefix length outside `0..=32`.
    pub fn new(address: Ipv4Addr, prefix_len: u8) -> Result<Self, NetlinkError> {
        if prefix_len > 32 {
            return Err(NetlinkError::InvalidPrefixLength(prefix_len));
        }
        Ok(Self {
            address,
            prefix_len,
        })
    }

    /// Builds a network from a compile-time-valid prefix length. Used only
    /// for `const` definitions such as [`NETSUKUKU_ADDRESS_SPACE`].
    const fn new_unchecked(address: Ipv4Addr, prefix_len: u8) -> Self {
        Self {
            address,
            prefix_len,
        }
    }

    /// A single host address (`/32`).
    pub fn host(address: Ipv4Addr) -> Self {
        Self {
            address,
            prefix_len: 32,
        }
    }

    /// The network's address as given (not necessarily the zeroed network
    /// base — callers that built this via [`Ipv4Net::new`] may have passed a
    /// host address with a shorter prefix on purpose, mirroring how upstream
    /// stores individual `/32` identity addresses and wider `/N` g-node
    /// ranges through the same field).
    pub fn address(&self) -> Ipv4Addr {
        self.address
    }

    /// The prefix length, `0..=32`.
    pub fn prefix_len(&self) -> u8 {
        self.prefix_len
    }

    fn mask(&self) -> u32 {
        if self.prefix_len == 0 {
            0
        } else {
            u32::MAX << (32 - self.prefix_len)
        }
    }

    /// Whether `addr` falls inside this network.
    pub fn contains(&self, addr: Ipv4Addr) -> bool {
        let mask = self.mask();
        (u32::from(self.address) & mask) == (u32::from(addr) & mask)
    }
}

impl fmt::Debug for Ipv4Net {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.address, self.prefix_len)
    }
}

impl fmt::Display for Ipv4Net {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// The entire Netsukuku address space: `10.0.0.0/8`
/// (`research/impl/vala/ntkd/ipv4_compute.vala:23-168`). Used by
/// [`crate::cleanup`] to decide whether an address on a managed interface is
/// ours to remove.
pub const NETSUKUKU_ADDRESS_SPACE: Ipv4Net = Ipv4Net::new_unchecked(Ipv4Addr::new(10, 0, 0, 0), 8);

/// A single weighted next hop of a multipath route
/// (`ip route ... nexthop via <via> dev <dev> weight <weight>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Nexthop {
    /// Gateway address for this next hop.
    pub via: Ipv4Addr,
    /// Outgoing interface for this next hop.
    pub dev: Interface,
    /// Kernel next-hop weight (0..=255; `ip route` displays it as `weight+1`,
    /// see [`rtnetlink::RouteNextHopBuilder::weight`]).
    pub weight: u8,
}

/// What a route does once matched, mirroring the three shapes
/// `identity_ip_commands.vala` installs plus multipath (real netlink
/// capability this crate exposes fully even though no literal upstream
/// command produces it yet — QSPN's disjoint-path admission is the intended
/// future consumer; `research/notes/03-specs-and-rfcs.md` RFC 0013).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteTarget {
    /// `ip route add unreachable <dst> table <t>` — no known path
    /// (`identity_ip_commands.vala:95-98`).
    Unreachable,
    /// `ip route change <dst> via <via> dev <dev> [src <src>] table <t>` —
    /// path exists (`identity_ip_commands.vala:533-560` region per
    /// `research/notes/02`).
    Gateway {
        /// Gateway (next-hop) address.
        via: Ipv4Addr,
        /// Outgoing interface.
        dev: Interface,
        /// Preferred source address for packets sent via this route.
        src: Option<Ipv4Addr>,
    },
    /// `ip route add <dst> dev <dev> scope link table <t>` — `<dst>` is directly reachable on
    /// `<dev>`, no gateway. Netsukuku's own motivating use: a per-neighbor `/32` host route
    /// (`RouteKey::destination` a host address) that disambiguates longest-prefix-match when
    /// several monitored NICs each hold a connected route to the same shared prefix (e.g. RFC
    /// 3927's `169.254.0.0/16`) — the on-link `/32` always outranks the ambiguous `/16`
    /// regardless of which NIC's connected route the kernel installed first.
    OnLink {
        /// Outgoing interface — the only NIC this destination is actually reachable on.
        dev: Interface,
    },
    /// `ip route add <dst> table <t> nexthop via .. weight .. [nexthop ..]`.
    Multipath(Vec<Nexthop>),
}

/// A full route specification for [`crate::RouteTable::add_route`] /
/// [`crate::RouteTable::change_route`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteSpec {
    /// Destination network.
    pub destination: Ipv4Net,
    /// Numeric routing table id. Never a kernel-reserved id — enforced by
    /// every [`crate::RouteTable`] implementation.
    pub table: u32,
    /// What the route does.
    pub target: RouteTarget,
}

/// The identifying key of a route for [`crate::RouteTable::remove_route`]:
/// Netsukuku never installs two routes to the same destination in the same
/// table (confirmed by the `identity_ip_commands.vala` inventory — every
/// `ip route del` there names only `<dst> table <t>`, never a gateway).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RouteKey {
    /// Destination network.
    pub destination: Ipv4Net,
    /// Numeric routing table id.
    pub table: u32,
}

/// What an `ip rule` selects on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RuleSelector {
    /// Matches every packet not already matched by a higher-priority rule —
    /// `ip rule add table <t>` (`identity_ip_commands.vala:89-90`, the main
    /// identity's own rule).
    Any,
    /// Matches packets carrying `fwmark` — `ip rule add fwmark <mark> table
    /// <t>` (`identity_ip_commands.vala:157-158`, per-peer-MAC policy
    /// routing keyed by the mark `iptables -t mangle` stamps on ingress).
    FwMark(u32),
}

/// A full `ip rule` specification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RuleSpec {
    /// Numeric routing table the rule points to. Never a kernel-reserved id.
    pub table: u32,
    /// Rule evaluation priority (lower is evaluated first). See
    /// [`crate::TableAllocator`] for how Netsukuku assigns these
    /// deterministically — upstream leaves it to the kernel's default,
    /// undocumented ordering (`[INFERENCE]`, see `table.rs`).
    pub priority: u32,
    /// What the rule matches on.
    pub selector: RuleSelector,
}

/// One address on one interface, as returned by
/// [`crate::AddressTable::list_addresses`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AddressEntry {
    /// Kernel `ifindex` the address is configured on.
    pub interface_index: u32,
    /// The address and its prefix length.
    pub network: Ipv4Net,
}

/// One network interface, as returned by [`crate::TopologyQuery::list_links`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LinkInfo {
    /// Kernel `ifindex`.
    pub index: u32,
    /// Interface name.
    pub name: String,
    /// Whether `IFF_UP` is set (administratively up).
    pub is_up: bool,
}

/// Coarse neighbour-cache reachability, collapsing the kernel's `NUD_*`
/// states (`netlink_packet_route::neighbour::NeighbourState`) to the
/// distinctions a routing daemon actually acts on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NeighbourState {
    /// `NUD_REACHABLE` or `NUD_PERMANENT`: usable now.
    Reachable,
    /// `NUD_STALE`, `NUD_DELAY` or `NUD_PROBE`: usable, being re-verified.
    Stale,
    /// `NUD_INCOMPLETE`: resolution in progress, not yet usable.
    Incomplete,
    /// `NUD_FAILED`, `NUD_NOARP` or `NUD_NONE`: not usable.
    Failed,
}

/// One ARP/neighbour-cache entry, as returned by
/// [`crate::TopologyQuery::list_neighbours`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeighbourInfo {
    /// Kernel `ifindex` the entry was learned on.
    pub interface_index: u32,
    /// Neighbour's IPv4 address.
    pub address: Ipv4Addr,
    /// Neighbour's link-layer (MAC) address, if resolved.
    pub link_layer_address: Option<[u8; 6]>,
    /// Cache-entry state.
    pub state: NeighbourState,
}

/// One kernel-state mutation, as recorded by [`crate::FakeNetlink`] in
/// invocation order. Every field is exactly the argument the corresponding
/// [`crate::AddressTable`] / [`crate::RouteTable`] / [`crate::RuleTable`]
/// method was called with — assert on it directly with `assert_eq!` against
/// an expected `Vec<Operation>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    /// [`crate::AddressTable::add_address`].
    AddAddress {
        interface: Interface,
        network: Ipv4Net,
    },
    /// [`crate::AddressTable::remove_address`].
    RemoveAddress {
        interface: Interface,
        network: Ipv4Net,
    },
    /// [`crate::RouteTable::add_route`].
    AddRoute(RouteSpec),
    /// [`crate::RouteTable::change_route`].
    ChangeRoute(RouteSpec),
    /// [`crate::RouteTable::remove_route`].
    RemoveRoute(RouteKey),
    /// [`crate::RuleTable::add_rule`].
    AddRule(RuleSpec),
    /// [`crate::RuleTable::remove_rule`].
    RemoveRule(RuleSpec),
}

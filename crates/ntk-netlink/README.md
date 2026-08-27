# ntk-netlink

The kernel seam for [`netsukuku-rs`](https://github.com/M0Rf30/netsukuku-rs): the only place the
daemon touches Linux kernel routing state. Netsukuku is an **L3 routing protocol that owns real
kernel routing tables via netlink** — not a TUN overlay, not a VPN. This crate has no `ntk-*`
sibling dependency; it is used directly by `ntkd`, the daemon binary, to install the routes QSPN
computes and to manage addresses, policy rules, and link/neighbour introspection.

Upstream's daemon drives the kernel exclusively by shelling out to `ip`(8)/`iptables`(8)/`sysctl`(8)
and recovers from a crash by regex-scraping that same tooling's text output. This crate replaces
both with real netlink requests over [`rtnetlink`](https://docs.rs/rtnetlink)/
[`netlink-packet-route`](https://docs.rs/netlink-packet-route) — no subprocesses, no output
scraping, anywhere. **Linux-only.**

## What it provides

Four small traits, one per `ip` sub-command family, so a consumer that only needs address
management doesn't have to depend on route/rule mocking too:

- **`AddressTable`** — `add_address`/`remove_address`/`list_addresses` (`ip address add|del|show`).
- **`RouteTable`** — `add_route`/`change_route`/`remove_route`/`list_routes`
  (`ip route add|change|del|show table <t>`), including multipath/ECMP.
- **`RuleTable`** — `add_rule`/`remove_rule`/`list_rules` (`ip rule add|del|show`), requiring
  `CONFIG_IP_MULTIPLE_TABLES`.
- **`TopologyQuery`** — read-only `list_links`/`list_neighbours` (`ip link show`,
  `ip neighbour show`).

`Netlink` is a blanket trait over all four, for functions that need the whole surface. Two
implementations satisfy it:

- **`RealNetlink`** — the production backend, a live `NETLINK_ROUTE` socket. Read-only calls work
  unprivileged; every mutating call needs `CAP_NET_ADMIN`.
- **`FakeNetlink`** — an in-memory, non-privileged recording implementation for upper-layer unit
  and simulation tests. Every mutating call is appended, in invocation order, to an **ordered
  operation log** (`FakeNetlink::operations`) and applied to a small in-memory model that answers
  the corresponding query methods — so a test can assert both "exactly these operations happened,
  in this order" and "the resulting state looks like this," without ever touching a real kernel.
  This log is the most broadly reusable piece here for anyone testing netlink-facing code: prove
  an unchanged snapshot issues *zero* operations, not merely few.

Rounding out the crate: `TableAllocator` (numbered routing-table/rule-priority allocation),
`detect_capabilities` (kernel-feature preflight), and `cleanup` (crash recovery scoped to exactly
what these traits can create).

Explicitly out of scope: TUN devices and `iptables`/NAT rule manipulation. Neither appears
anywhere in this crate's API.

## Example

```rust
use ntk_netlink::{AddressTable, FakeNetlink, Interface, Ipv4Net, LinkInfo, Operation};
use std::net::Ipv4Addr;

#[tokio::main]
async fn main() {
    let kernel = FakeNetlink::with_links(vec![LinkInfo {
        index: 2,
        name: "eth0".into(),
        is_up: true,
    }]);

    let network = Ipv4Net::new(Ipv4Addr::new(10, 0, 0, 1), 32).unwrap();
    kernel
        .add_address(&Interface::name("eth0"), network)
        .await
        .unwrap();

    assert_eq!(
        kernel.operations(),
        vec![Operation::AddAddress {
            interface: Interface::name("eth0"),
            network,
        }]
    );
}
```

Swap `FakeNetlink` for `RealNetlink::new()` (built from within a Tokio runtime) to issue the same
calls against a real kernel — the trait surface is identical either way.

## License

GPL-3.0-or-later. Part of the [netsukuku-rs](https://github.com/M0Rf30/netsukuku-rs) workspace.

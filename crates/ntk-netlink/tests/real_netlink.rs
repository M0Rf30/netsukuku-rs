//! Integration tests against a genuine `NETLINK_ROUTE` socket.
//!
//! These require `CAP_NET_ADMIN` and mutate real kernel state, so they are
//! `#[ignore]`d by default — `cargo test -p ntk-netlink` (no privileges) is
//! green without ever running this file's bodies. To actually run them,
//! do it inside a disposable network namespace so nothing on the host is
//! touched:
//!
//! ```sh
//! sudo unshare --net --map-root-user \
//!     cargo test -p ntk-netlink --test real_netlink -- --ignored --test-threads=1
//! ```

use std::net::Ipv4Addr;

use ntk_netlink::{
    AddressTable, Interface, Ipv4Net, RealNetlink, RouteKey, RouteSpec, RouteTable, RouteTarget,
    RuleSelector, RuleSpec, RuleTable, TopologyQuery,
};

#[tokio::test]
#[ignore = "requires CAP_NET_ADMIN; see module docs for the netns invocation"]
async fn loopback_address_add_list_remove_round_trips() {
    let netlink = RealNetlink::new().expect("open netlink socket");
    let lo = Interface::name("lo");
    let network = Ipv4Net::new(Ipv4Addr::new(10, 250, 0, 1), 32).unwrap();

    netlink
        .add_address(&lo, network)
        .await
        .expect("add address");
    let addresses = netlink
        .list_addresses(Some(&lo))
        .await
        .expect("list addresses");
    assert!(addresses.iter().any(|entry| entry.network == network));

    netlink
        .remove_address(&lo, network)
        .await
        .expect("remove address");
    let addresses = netlink
        .list_addresses(Some(&lo))
        .await
        .expect("list addresses after remove");
    assert!(!addresses.iter().any(|entry| entry.network == network));
}

#[tokio::test]
#[ignore = "requires CAP_NET_ADMIN; see module docs for the netns invocation"]
async fn unreachable_route_add_list_remove_round_trips() {
    let netlink = RealNetlink::new().expect("open netlink socket");
    let destination = Ipv4Net::new(Ipv4Addr::new(10, 251, 0, 0), 24).unwrap();
    let table = 249;
    let route = RouteSpec {
        destination,
        table,
        target: RouteTarget::Unreachable,
    };

    netlink.add_route(&route).await.expect("add route");
    let routes = netlink.list_routes(Some(table)).await.expect("list routes");
    assert!(routes.contains(&route));

    netlink
        .remove_route(RouteKey { destination, table })
        .await
        .expect("remove route");
    let routes = netlink
        .list_routes(Some(table))
        .await
        .expect("list routes after remove");
    assert!(routes.is_empty());
}

#[tokio::test]
#[ignore = "requires CAP_NET_ADMIN; see module docs for the netns invocation"]
async fn change_route_replaces_a_gateway_route() {
    let netlink = RealNetlink::new().expect("open netlink socket");
    let lo = netlink
        .list_links()
        .await
        .expect("list links")
        .into_iter()
        .find(|link| link.name == "lo")
        .expect("lo exists");
    let destination = Ipv4Net::new(Ipv4Addr::new(10, 252, 0, 0), 24).unwrap();
    let table = 247;

    let unreachable = RouteSpec {
        destination,
        table,
        target: RouteTarget::Unreachable,
    };
    netlink
        .add_route(&unreachable)
        .await
        .expect("add unreachable route");

    let gateway = RouteSpec {
        destination,
        table,
        target: RouteTarget::Gateway {
            via: Ipv4Addr::new(127, 0, 0, 1),
            dev: Interface::Index(lo.index),
            src: None,
        },
    };
    netlink
        .change_route(&gateway)
        .await
        .expect("change route to gateway");
    let routes = netlink.list_routes(Some(table)).await.expect("list routes");
    assert!(routes.contains(&gateway), "routes: {routes:?}");

    netlink
        .remove_route(RouteKey { destination, table })
        .await
        .expect("remove route");
}

#[tokio::test]
#[ignore = "requires CAP_NET_ADMIN; see module docs for the netns invocation"]
async fn onlink_route_add_list_remove_round_trips() {
    let netlink = RealNetlink::new().expect("open netlink socket");
    let lo = netlink
        .list_links()
        .await
        .expect("list links")
        .into_iter()
        .find(|link| link.name == "lo")
        .expect("lo exists");
    let destination = Ipv4Net::new(Ipv4Addr::new(10, 253, 0, 1), 32).unwrap();
    let table = 245;
    let route = RouteSpec {
        destination,
        table,
        target: RouteTarget::OnLink {
            dev: Interface::Index(lo.index),
        },
    };

    netlink.add_route(&route).await.expect("add on-link route");
    let routes = netlink.list_routes(Some(table)).await.expect("list routes");
    assert!(routes.contains(&route), "routes: {routes:?}");

    netlink
        .remove_route(RouteKey { destination, table })
        .await
        .expect("remove route");
    let routes = netlink
        .list_routes(Some(table))
        .await
        .expect("list routes after remove");
    assert!(routes.is_empty());
}

#[tokio::test]
#[ignore = "requires CAP_NET_ADMIN; see module docs for the netns invocation"]
async fn rule_add_list_remove_round_trips() {
    let netlink = RealNetlink::new().expect("open netlink socket");
    let rule = RuleSpec {
        table: 248,
        priority: 9_998,
        selector: RuleSelector::FwMark(0xfa),
    };

    netlink.add_rule(&rule).await.expect("add rule");
    let rules = netlink.list_rules().await.expect("list rules");
    assert!(rules.contains(&rule));

    netlink.remove_rule(&rule).await.expect("remove rule");
    let rules = netlink.list_rules().await.expect("list rules after remove");
    assert!(!rules.contains(&rule));
}

#[tokio::test]
#[ignore = "requires CAP_NET_ADMIN; see module docs for the netns invocation"]
async fn kernel_capabilities_are_detected_in_a_real_netns() {
    let netlink = RealNetlink::new().expect("open netlink socket");
    let capabilities = ntk_netlink::detect_capabilities(&netlink).await;
    assert!(
        capabilities.ensure_supported().is_ok(),
        "capabilities: {capabilities:?}"
    );
}

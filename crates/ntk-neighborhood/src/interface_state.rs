//! [`InterfaceState`]: a `Send`-provable adapter over
//! `ntk_netlink::TopologyQuery`, used for this crate's "which local
//! interfaces participate" responsibility (`crate::Manager::start_monitor`/
//! `sync_interfaces`).
//!
//! # Why this indirection exists
//! `TopologyQuery`'s methods are `async fn`s in a trait
//! (`#[allow(async_fn_in_trait)]`, `crates/ntk-netlink/src/traits.rs:28`);
//! that attribute suppresses the lint about the returned future not
//! provably implementing `Send`, on the stated assumption that
//! `ntk-netlink` consumers stay generic over the trait and never send its
//! futures across a `tokio::spawn` boundary. This crate's [`crate::Manager`]
//! *is* driven from inside one `tokio::spawn`ed actor task, so it needs a
//! `Send` future — but no bound on `TopologyQuery` itself can express that
//! generically (there is no named associated future type to constrain).
//! Concretely, though, both of `ntk-netlink`'s implementors are Send:
//! `RealNetlink` only holds an `rtnetlink::Handle` (`Clone`, `Send`,
//! `Sync`) across its awaits, and `FakeNetlink` never holds its `Mutex`
//! guard across one (`crates/ntk-netlink/src/fake.rs:248-249`). This trait
//! makes that provable to the compiler at the only two call sites where
//! `Self` is concrete, without touching `ntk-netlink` (out of this crate's
//! ownership) or reimplementing interface enumeration.
use futures::future::BoxFuture;
use ntk_netlink::{FakeNetlink, Interface, LinkInfo, NetlinkError, RealNetlink, TopologyQuery};

/// `Send`-provable link-state query, implemented for exactly the two
/// concrete `ntk_netlink` backends.
pub trait InterfaceState: Send + Sync + std::fmt::Debug {
    /// `ip link show`, via [`TopologyQuery::list_links`].
    fn list_links(&self) -> BoxFuture<'_, Result<Vec<LinkInfo>, NetlinkError>>;
}

/// Resolves `name` to its current [`LinkInfo`], the `Send`-provable
/// counterpart of [`ntk_netlink::resolve_interface`] for the
/// [`Interface::Name`] form — the only one this crate needs, since
/// [`crate::nic::LocalNic::dev`] is always a name.
pub async fn resolve_by_name(
    kernel: &dyn InterfaceState,
    name: &str,
) -> Result<LinkInfo, NetlinkError> {
    let links = kernel.list_links().await?;
    links
        .into_iter()
        .find(|link| link.name == name)
        .ok_or_else(|| NetlinkError::InterfaceNotFound(Interface::Name(name.to_owned())))
}

impl InterfaceState for RealNetlink {
    fn list_links(&self) -> BoxFuture<'_, Result<Vec<LinkInfo>, NetlinkError>> {
        Box::pin(TopologyQuery::list_links(self))
    }
}

impl InterfaceState for FakeNetlink {
    fn list_links(&self) -> BoxFuture<'_, Result<Vec<LinkInfo>, NetlinkError>> {
        Box::pin(TopologyQuery::list_links(self))
    }
}

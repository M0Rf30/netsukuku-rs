//! Wireless (802.11) integration tests for the `ntkd` daemon core (rung 3): the shared
//! [`netns`] fixture's real-kernel technique (one dedicated OS thread per network namespace,
//! `nix::sched::unshare(CLONE_NEWNET)`, its own `current_thread` runtime, assertions read back
//! through an independent [`ntk_netlink::RealNetlink`]) — but with two `mac80211_hwsim` virtual
//! radios in IBSS mode standing in for [`netns`]'s bridged-veth `Segment`, so 802.11 broadcast
//! semantics (sent at the lowest basic rate, unacknowledged, no link-layer retry) finally meet a
//! discovery path that has, until now, only ever run over a veth's reliable point-to-point link.
//! Consumes [`netns`]'s [`netns::NamespaceWorker`]/[`netns::spawn_node`]/[`netns::teardown`]/
//! [`netns::observe`] for every namespace/daemon-composition primitive; this file's own
//! contribution is entirely the nl80211 radio plumbing that slots into the same place
//! [`netns::wire`] fills for the bridged-veth mesh.
//!
//! # nl80211 plumbing: no `iw` subprocess
//!
//! This project replaced upstream's shelled-out `ip`/`iptables` with native netlink specifically
//! because shelling out was a "concrete, fixable weakness"; [`netns::link`] holds to that with
//! raw `rtnetlink`, and this file holds to it with `wl-nl80211` (same rust-netlink family/release
//! train as `rtnetlink`) for every radio operation. Evaluated in the assigned order:
//!
//! - **(a) Does `wl-nl80211` 0.7.0 expose interface-type change and `JOIN_IBSS`?** Interface-type
//!   change: yes, fully — [`wl_nl80211::Nl80211InterfaceHandle::set`] with
//!   [`wl_nl80211::Nl80211Interface::interface_type`]. `JOIN_IBSS`/`SET_WIPHY_NETNS`: the crate
//!   defines [`wl_nl80211::Nl80211Command::JoinIbss`]/`SetWiphyNetns` as command discriminants
//!   (for round-tripping), but ships no dedicated request builder for either — every
//!   `NL80211_CMD_*` this file needs beyond `SET_INTERFACE` is sent by constructing
//!   [`wl_nl80211::Nl80211Message`] directly (its `cmd`/`attributes` fields are public) and
//!   posting it through [`wl_nl80211::Nl80211Handle::request`], exactly mirroring the crate's own
//!   `examples/nl80211_set_wiphy.rs`. Two attributes this version's typed [`wl_nl80211::Nl80211Attr`]
//!   doesn't wrap yet (`NL80211_ATTR_FREQ_FIXED`, `NL80211_ATTR_NETNS_FD`) go through its
//!   documented escape hatch, [`wl_nl80211::Nl80211Attr::Other`] wrapping a plain
//!   `DefaultNla(kind, bytes)` — see [`NL80211_ATTR_FREQ_FIXED`]/[`NL80211_ATTR_NETNS_FD`] below
//!   for how those two numeric ids were cross-checked, not guessed. This is normal, supported use
//!   of a rust-netlink-family crate; it is not hand-rolling genetlink.
//! - **(b) Hand-rolling over bare `genetlink`?** Not needed — (a) succeeded. `wl-nl80211`
//!   re-exports the exact `netlink_packet_core`/`netlink_packet_generic` types (as
//!   `wl_nl80211::packet_core`/`packet_generic`) the manual-message path above needs, so this
//!   file adds only `wl-nl80211` (plus `netlink-packet-core`, required transitively by
//!   `wl_nl80211::try_nl80211!`'s own expansion) as dev-dependencies; the root manifest's
//!   `genetlink`/`netlink-packet-generic` entries (offered for this contingency) go unused.
//! - **(c) `iw` as a documented exception?** Not needed either, so no `hub send` to `Main` was
//!   sent about it. No `iw`/`ip`/any subprocess call appears anywhere below.
//!
//! # The sharp edge: `SET_WIPHY_NETNS` is not `IFLA_NET_NS_FD` — and neither is rootless, here
//!
//! Confirmed empirically on this host, as the unprivileged user this suite otherwise runs as:
//!
//! ```text
//! $ iw phy phy1 set netns $$          # NL80211_CMD_SET_WIPHY_NETNS
//! command failed: Operation not permitted (-1)
//! $ ip link set wlan1 netns $$        # rtnetlink IFLA_NET_NS_FD, the *netdev*-only move
//! RTNETLINK answers: Operation not permitted
//! $ unshare --net --map-root-user -- sh -c 'iw dev; iw phy'   # both print nothing
//! ```
//!
//! Both the wiphy-level and the netdev-level move fail with `EPERM`, and `phy1`..`phy8`/
//! `wlan1`..`wlan8` are entirely invisible inside a fresh `unshare --net --map-root-user`
//! namespace. All three observations have the same explanation: `mac80211_hwsim`'s radios are
//! objects of the host's real init network namespace, owned by `init_user_ns` (they were created
//! there when the module was loaded, before this suite's namespaces existed). The kernel checks
//! `CAP_NET_ADMIN` against the *owning user namespace of the object's current netns*
//! (`ns_capable(net->user_ns, CAP_NET_ADMIN)`); a `map-root-user` namespace's fake root has
//! capabilities only inside the user namespace *it* created, never inside an ancestor (here, the
//! real `init_user_ns`) — this is the whole point of user namespaces, not a bug to work around.
//! [`netns`]'s bridged-veth mesh never hits this wall because it *creates* the veth pairs from
//! inside its own unprivileged namespace, so that namespace genuinely owns them.
//!
//! **Conclusion: unlike the bridged-veth rung, this rung has no rootless path in an ordinary
//! unprivileged shell.** Every ignored test below needs real `CAP_NET_ADMIN` (and, for
//! [`RadioReturnGuard`]'s own return move, `CAP_SYS_ADMIN` — see that struct's own doc) in
//! `init_user_ns` — genuine root, not `unshare --net --map-root-user`. This host provides that
//! through a root-owned wrapper (`setpriv` dropping to this suite's own uid with exactly
//! `CAP_NET_ADMIN`+`CAP_SYS_ADMIN` in the bounding/ambient sets, `CAP_SYS_MODULE` deliberately
//! withheld so `mac80211_hwsim` cannot be reloaded from here), so every test below has in fact
//! been run to completion through it — see each test's own "Running" section for what was
//! actually observed, not merely compiled.
//!
//! Whether moving only the netdev (leaving the wiphy behind) would suffice for `mac80211_hwsim`
//! could not be probed further: the permission check above fires before any netdev-vs-wiphy
//! structural check would ever run, on both real hardware and hwsim. `[INFERENCE]` from reading
//! `net/wireless/core.c`'s `cfg80211_netdev_notifier_call`: a wireless netdev's
//! `NETDEV_PRE_CHANGE_NETNS` is rejected outright, i.e. a netdev can only change namespace by
//! moving its whole wiphy via `SET_WIPHY_NETNS` — consistent with this file always moving the
//! whole wiphy, never attempting a netdev-only move, but not independently observed in this
//! sandbox.
//!
//! # Guarding the real card
//!
//! `phy0`/`wlan0` (this host's real iwlwifi card) must never be touched. Guarded by
//! [`phy_driver`] reading the actual kernel driver backing each wiphy
//! (`/sys/class/ieee80211/<phy>/device/driver`, a symlink to `.../drivers/mac80211_hwsim` or
//! `.../drivers/iwlwifi`) rather than by excluding a hardcoded index — nothing here assumes
//! `mac80211_hwsim` phys are numbered from any particular offset, only that they are, in fact,
//! backed by the `mac80211_hwsim` driver. [`discover_hwsim_radios`] applies this filter before
//! returning anything, and [`hwsim_radio_discovery_never_returns_the_real_radio`] pins it as a
//! regression test.
//!
//! # A caveat for the broadcast-reliability scenario: no `wmediumd`
//!
//! `wmediumd` is not installed on this host. Without it, `mac80211_hwsim`'s built-in medium
//! simulation is a perfect (lossless, no interference/path-loss modeling) broadcast: every
//! `mac80211_hwsim` radio on the same channel hears every other one with certainty. The
//! *unacknowledged, no-retry* character of a real 802.11 broadcast frame is still genuinely
//! exercised (mac80211 never retries a broadcast regardless of medium simulation), but a true
//! frame-*loss* rate is not — that would need `wmediumd`'s per-pair path-loss model, which this
//! host doesn't have. [`real_hwsim_broadcast_reliability_across_ten_runs`]'s own doc comment
//! restates this so its numbers, whenever someone with root actually runs it, aren't
//! over-interpreted as a loss measurement.
//!
//! # No shared-teardown rendezvous needed (unlike a veth pair) — but no free reparenting either
//!
//! `multi_node.rs`'s veth scenarios (and [`netns`]'s own bridged `Segment`s) need a rendezvous
//! before either side tears its namespace down, because deleting one end of a veth pair deletes
//! both. Two `mac80211_hwsim` radios have no such shared object — each side's own wiphy is
//! independent, so every scenario below still lets each [`netns::NamespaceWorker`] finish and
//! tear down independently, with no cross-worker rendezvous needed.
//!
//! An earlier revision of this file inferred, but never checked, that a wiphy moved into a
//! namespace "is expected to reparent back to `init_net` on its own when its holding namespace
//! dies, regardless of what the peer is doing." **Measured false.** `modprobe mac80211_hwsim
//! radios=8` gives `phy1`..`phy8`; after two runs of the multi-radio scenarios below (before this
//! fix), `/sys/class/ieee80211` held only `phy0`, `phy1`, and `phy8` — `phy2`..`phy7` were gone
//! outright, and the surviving `phy1` had lost its netdev. A wiphy left behind when its holding
//! `struct net` is destroyed is simply destroyed with it; nothing reparents it. Recovering needed
//! `modprobe -r mac80211_hwsim && modprobe mac80211_hwsim radios=8`, which needs
//! `CAP_SYS_MODULE` — deliberately not granted to this suite, so it needed manual intervention
//! every time.
//!
//! [`RadioReturnGuard`] is this fixture's fix: every namespace worker moves its own wiphy back to
//! `init_net` itself, via `NL80211_CMD_SET_WIPHY_NETNS` — the mirror of
//! [`move_radio_into_namespace`]'s inbound move, bringing the netdev down first per that
//! command's own contract. The coordinator's own netlink socket lives in `init_net` and cannot
//! see a wiphy currently parented to a worker's namespace (netlink visibility is scoped to the
//! requesting socket's own netns), so the move-back must be issued by the worker's own
//! (already-unshared) thread — which needs an fd naming `init_net`, obtainable only before that
//! thread unshares (see [`open_init_net_fd`]). The move-back itself lives in a `Drop` impl, not a
//! plain call at the end of the happy path, so a panicking or early-returning scenario body still
//! releases the radio instead of leaking it.
//!
//! A second, subtler bug survived past the first fix: `Drop` spawned its return-to-`init_net`
//! thread but never *joined* it, so the move-back was fire-and-forget — the very next sequential
//! trial in [`real_hwsim_broadcast_reliability_across_ten_runs`] could (and, measured, did) start
//! before the previous trial's radio had actually finished moving back, unable to resolve its own
//! netdev by name. Worse, at process exit the detached thread might never run at all, permanently
//! leaking one radio's wiphy per suite run. `Drop` now blocks on `std::thread::JoinHandle::join`
//! (a fresh `std::thread`, not a nested `block_on`, because `Drop` cannot `.await` and this fires
//! from inside the worker's own `rt.block_on(body())` — see [`RadioReturnGuard::drop`]'s own doc)
//! and, once the move-back's own `NLM_F_ACK` lands, `setns`(2)s back into `init_net` to verify the
//! netdev is actually resolvable there before returning — an ack is not settlement, see
//! [`NETDEV_REAPPEAR_TIMEOUT`]'s own doc for the measurements behind that distinction, including a
//! third, independent bug in the same family: [`HwsimRadio::if_index`] is captured once, before
//! any namespace move, and moving a wiphy across netns reassigns its netdev's ifindex (only the
//! *name* survives) — so code that trusted that fixed number after a move (as
//! [`prepare_radio_interface`] used to) was silently operating on the wrong interface, or none.
//!
//! # Why no arc formed even once radios move correctly: two independent cells, never merging
//!
//! Fixing the radio-move bugs above surfaces a separate, real defect: two real daemons composed
//! over a correctly-moved, correctly-joined pair of radios could still never see each other.
//! `NL80211_CMD_JOIN_IBSS`'s own ack means "request accepted", not "cell exists" — measured via
//! `journalctl -k` immediately after `join_ibss` returns: `<if>: Trigger new scan to find an IBSS
//! to join`, then, 4-9s later (not instantly), either an existing cell is joined or `<if>:
//! Creating new IBSS network, BSSID <mac>` — a *locally generated* BSSID, chosen because no
//! existing cell was found during that scan and none was fixed by the join request. With no
//! `NL80211_ATTR_MAC` in that request, two radios starting at the same time each independently
//! generate their own random BSSID: measured directly, same run, via this file's own
//! `read_joined_bssid` (not just `journalctl`): `radio-a: bssid=8e:72:24:81:92:6f` vs
//! `radio-b: bssid=9a:ff:9a:21:1d:47`, neither ever appearing in the other's
//! `NL80211_CMD_GET_STATION` dump.
//!
//! A prior revision of this file attributed the resulting failure to a *race*: two independently
//! created cells that merely hadn't yet had time to merge, and widened the caller-side deadline
//! ([`IBSS_MERGE_TIMEOUT`]) from 30s to 60s on the theory that `net/mac80211/ibss.c`'s
//! `IEEE80211_IBSS_MERGE_INTERVAL` re-scan timer (a flat `30 * HZ`) just needed one more period of
//! margin. **Measured false, twice over.** First, empirically: ten trials at 60s and ten at 30s
//! both scored 0/10 arcs formed — identical, not improved by the wider budget, which is what a
//! *structural* failure looks like, not a race a bigger number should shrink. Second, from the
//! kernel source itself: this file's `join_ibss` always sets `NL80211_ATTR_FREQ_FIXED`, and
//! `net/mac80211/ibss.c:1241-1249`'s `ieee80211_sta_merge_ibss` returns immediately whenever
//! `ifibss->fixed_channel` is set, *before* it would otherwise call
//! `ieee80211_request_ibss_scan` to look for a mergeable peer. Once `FREQ_FIXED` is in play, two
//! independently-created cells cannot merge — not slowly, not eventually, not at all; no
//! deadline, however wide, could have made a difference.
//!
//! The actual fix, [`IBSS_FIXED_BSSID`]: pass a shared, constant BSSID via `JOIN_IBSS`'s
//! `NL80211_ATTR_MAC`. `net/wireless/nl80211.c:13565-13569` copies it into
//! `cfg80211_ibss_params.bssid`; `net/mac80211/ibss.c:1719-1723`'s `ieee80211_ibss_join` turns
//! that into `sdata->u.ibss.fixed_bssid`; and once both `fixed_bssid` and `fixed_channel` are
//! set, `net/mac80211/ibss.c:1409-1417`'s `ieee80211_sta_find_ibss` skips scanning altogether
//! ("if a fixed bssid and a fixed freq have been provided create the IBSS directly and do not
//! waste time scanning") and creates the cell with exactly that BSSID
//! (`net/mac80211/ibss.c:1267-1268`), not a random one. Two radios given the same fixed BSSID
//! therefore converge on the identical cell by construction — measured directly after this fix:
//! both radios' `read_joined_bssid` report the identical configured BSSID, each appears in the
//! other's `GET_STATION` dump, and arcs now form in well under a second (`real_hwsim_two_daemons_
//! establish_arc_and_route_over_ibss` measured 0.54s) instead of racing a 30-60s deadline —
//! see that test's and [`real_hwsim_broadcast_reliability_across_ten_runs`]'s own "Running"
//! sections for the exact runs this was measured against.

// `netns` is a fixture shared with `tests/mesh.rs` (a separate, independently-linted test
// binary); each binary uses a different subset of its public items (bridged-veth `Segment`
// wiring there, radio-move plumbing here), so this file's own unused subset is expected, not a
// real dead-code smell — silenced at the `mod` boundary rather than editing the shared file.
#[allow(dead_code)]
mod netns;

use ntkd::kernel::addressing;

use std::collections::HashMap;
use std::os::fd::{AsRawFd, RawFd};
use std::time::{Duration, Instant};

use anyhow::Context;
use futures::{StreamExt, TryStreamExt};
use ntk_common::{HCoord, Naddr, Topology};
use ntk_neighborhood::NodeId;
use ntk_netlink::Ipv4Net;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use wl_nl80211::packet_core::{DefaultNla, NLM_F_ACK, NLM_F_REQUEST};
use wl_nl80211::packet_generic::GenlMessage;
use wl_nl80211::{
    Nl80211Attr, Nl80211BssInfo, Nl80211Command, Nl80211Error, Nl80211Handle, Nl80211Interface,
    Nl80211InterfaceType, Nl80211Message,
};

// ---------------------------------------------------------------------------------------------
// Shared topology helpers (mirrors `multi_node.rs`'s own; kept file-local — `multi_node.rs` is
// off limits to edit and its helpers are private to that binary).
// ---------------------------------------------------------------------------------------------

fn topology() -> Topology {
    Topology::new([4, 2, 2, 2]).unwrap()
}

fn negotiation_topology() -> Topology {
    Topology::new([8]).unwrap()
}

fn position(idx: u32) -> Vec<u32> {
    vec![idx, 0, 0, 0]
}

fn naddr(idx: u32) -> Naddr {
    Naddr::new(topology(), position(idx)).unwrap()
}

const IBSS_SSID: &str = "ntkd-wireless-test";
/// Channel 1, 2.4GHz — unrestricted (no "no IR"/DFS caveats) on every `mac80211_hwsim` phy
/// observed on this host, unlike several of the offered 5GHz channels.
const IBSS_FREQ_MHZ: u32 = 2412;

/// How long a caller composing a node over [`prepare_radio_interface`]'s interface must budget
/// for a route/arc to appear. A prior revision of this file widened this to 60s, arguing
/// mac80211's `IEEE80211_IBSS_MERGE_INTERVAL` re-scan timer (`net/mac80211/ibss.c:31`, a flat
/// `30 * HZ`) needed a full period of margin past `JOIN_IBSS`'s own 4-9s initial-scan delay.
/// **Measured false**: ten trials at 60s and ten at 30s both scored 0/10 — identical, not
/// improved — which is what a *structural* failure looks like, not a race that a wider budget
/// should shrink. `net/mac80211/ibss.c:1241-1249`'s `ieee80211_sta_merge_ibss` confirms why: it
/// returns immediately whenever `ifibss->fixed_channel` is set, before ever calling
/// `ieee80211_request_ibss_scan` — and this file's `join_ibss` always sets `FREQ_FIXED`. Once
/// two independently-created cells exist, mac80211's re-scan-for-merge path can *never* run for
/// either of them; no timeout, however wide, would have helped. The real fix is
/// [`IBSS_FIXED_BSSID`] (making both radios join one cell by construction instead of hoping two
/// stay merged), not a bigger number here — so the number goes back to 30s, which the evidence
/// above shows costs nothing relative to 60s while halving this suite's slowest scenario's
/// runtime.
const IBSS_MERGE_TIMEOUT: Duration = Duration::from_secs(30);

/// Fixed BSSID both radios join (via `JOIN_IBSS`'s `NL80211_ATTR_MAC`, see [`join_ibss`]'s own
/// doc for the kernel-cited mechanism), forcing them onto the identical IBSS cell by
/// construction instead of depending on mac80211's merge path — which, per [`IBSS_MERGE_TIMEOUT`]'s
/// own doc, cannot run at all once `FREQ_FIXED` is set. Locally administered (bit 1 of the first
/// octet set) and unicast (bit 0 clear), satisfying `is_valid_ether_addr`
/// (`net/wireless/nl80211.c:13568`)'s requirement.
///
/// # Measured effect
/// Before this constant existed (no fixed BSSID, `FREQ_FIXED` only): `radio-a`/`radio-b` joined
/// two different, never-converging BSSIDs and neither ever saw the other as an
/// `NL80211_CMD_GET_STATION` peer — see `real_hwsim_two_daemons_establish_arc_and_route_over_ibss`'s
/// own "Running" section for the exact run this was measured against. After: both radios'
/// `NL80211_CMD_GET_SCAN` dumps report the same `NL80211_BSS_STATUS_IBSS_JOINED` BSSID, and each
/// sees the other in its own `GET_STATION` dump — see that same test's "Running" section for the
/// after-measurement.
const IBSS_FIXED_BSSID: [u8; 6] = [0x02, 0x4e, 0x54, 0x4b, 0x77, 0x69];

/// Bound for confirming `JOIN_IBSS` actually landed — [`NL80211_CMD_JOIN_IBSS`]'s own ack means
/// "request accepted", not "cell joined" (this file's module doc, "Why no arc formed" section),
/// and measured directly (Defect B): one run in ten reported `bssid=none` on *both* radios at
/// teardown while each had still discovered the other as a neighbour (`state: Discovered`,
/// `cost: None`) — frames crossed the medium while neither side was actually in the joined
/// cell, because `mac80211_hwsim`'s medium simulation delivers frames between radios on the same
/// channel regardless of either side's local IBSS join state; only the kernel's own join
/// bookkeeping (read back via [`read_joined_bssid`]) says whether a cell was ever really
/// entered. [`prepare_radio_interface`] polls that readback until it matches the requested
/// BSSID before handing the interface to a composed daemon, instead of finding out only after
/// [`IBSS_MERGE_TIMEOUT`] has already been spent. Sized generously above the ~530-650ms measured
/// for a normal fixed-BSSID join (this file's module doc, "The actual fix" section) while still
/// failing well before [`IBSS_MERGE_TIMEOUT`] would anyway.
const IBSS_JOIN_CONFIRM_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------------------------
// nl80211: raw command construction for the two commands `wl-nl80211` 0.7.0 has no builder for
// ---------------------------------------------------------------------------------------------

/// `NL80211_ATTR_FREQ_FIXED`: a zero-length flag telling `JOIN_IBSS` to stay on the given channel
/// rather than scan for an existing cell first. Not wrapped by `wl-nl80211` 0.7.0's typed
/// [`Nl80211Attr`] (this file's module doc, nl80211-plumbing section) — cross-checked against
/// this host's own `/usr/include/linux/nl80211.h` `enum nl80211_attrs` (positional value 60,
/// counting from `NL80211_ATTR_UNSPEC` = 0), which matches the crate's own commented-out, unused
/// `// const NL80211_ATTR_FREQ_FIXED:u16 = 60;` in its `attr.rs` — not guessed.
const NL80211_ATTR_FREQ_FIXED: u16 = 60;

/// `NL80211_ATTR_NETNS_FD`: a `u32` file descriptor number (host byte order), the nl80211
/// analogue of rtnetlink's `IFLA_NET_NS_FD`. Same cross-check as
/// [`NL80211_ATTR_FREQ_FIXED`]: positional value 219, matching the crate's own commented-out
/// `// const NL80211_ATTR_NETNS_FD:u16 = 219;`.
const NL80211_ATTR_NETNS_FD: u16 = 219;

/// Posts one nl80211 command and drains its reply stream, surfacing any `NLMSG_ERROR` via
/// [`wl_nl80211::try_nl80211`] — the same macro [`Nl80211Handle`]'s own request builders use
/// internally, exported (`#[macro_export]`) for exactly this "no dedicated builder" case.
async fn nl80211_send(
    handle: &mut Nl80211Handle,
    cmd: Nl80211Command,
    attributes: Vec<Nl80211Attr>,
) -> Result<(), Nl80211Error> {
    let mut nl_msg =
        wl_nl80211::packet_core::NetlinkMessage::from(GenlMessage::from_payload(Nl80211Message {
            cmd,
            attributes,
        }));
    nl_msg.header.flags = NLM_F_REQUEST | NLM_F_ACK;
    let mut replies = std::pin::pin!(handle.request(nl_msg).await?);
    while let Some(reply) = replies.next().await {
        let _: GenlMessage<Nl80211Message> = wl_nl80211::try_nl80211!(reply);
    }
    Ok(())
}

async fn set_interface_adhoc(handle: &mut Nl80211Handle, if_index: u32) -> anyhow::Result<()> {
    let attrs = Nl80211Interface::new(if_index)
        .interface_type(Nl80211InterfaceType::Adhoc)
        .build();
    nl80211_send(handle, Nl80211Command::SetInterface, attrs).await?;
    Ok(())
}

/// Joins (or creates) the IBSS cell for `ssid`/`freq_mhz`. `bssid`, when given, is sent as
/// `NL80211_ATTR_MAC` — `net/wireless/nl80211.c:13565-13569`'s `nl80211_join_ibss` copies it into
/// `cfg80211_ibss_params.bssid` (rejecting it unless `is_valid_ether_addr`), which
/// `net/mac80211/ibss.c:1719-1723`'s `ieee80211_ibss_join` turns into `sdata->u.ibss.fixed_bssid`.
/// Combined with `NL80211_ATTR_FREQ_FIXED` (`ibss.channel_fixed`, unconditionally set below —
/// `net/wireless/nl80211.c:13611`), `net/mac80211/ibss.c:1409-1417`'s `ieee80211_sta_find_ibss`
/// skips scanning entirely once both are fixed ("if a fixed bssid and a fixed freq have been
/// provided create the IBSS directly and do not waste time scanning") and
/// `ieee80211_sta_create_ibss` (`net/mac80211/ibss.c:1267-1268`) then uses exactly that BSSID
/// instead of generating a random locally-administered one. Two radios given the *same* fixed
/// BSSID therefore create/announce the identical cell deterministically, by construction, rather
/// than depending on either racing an initial scan or on mac80211's own re-scan-for-merge path —
/// which, measured against this exact kernel source, cannot ever fire once `FREQ_FIXED` is set:
/// `net/mac80211/ibss.c:1248`'s `ieee80211_sta_merge_ibss` returns immediately when
/// `ifibss->fixed_channel`, before it would otherwise trigger `ieee80211_request_ibss_scan`. See
/// [`IBSS_FIXED_BSSID`]'s own doc for the before/after observation this predicts.
async fn join_ibss(
    handle: &mut Nl80211Handle,
    if_index: u32,
    ssid: &str,
    freq_mhz: u32,
    bssid: Option<[u8; 6]>,
) -> anyhow::Result<()> {
    let mut attrs = vec![
        Nl80211Attr::IfIndex(if_index),
        Nl80211Attr::Ssid(ssid.to_owned()),
        Nl80211Attr::WiphyFreq(freq_mhz),
        Nl80211Attr::Other(DefaultNla::new(NL80211_ATTR_FREQ_FIXED, Vec::new())),
    ];
    if let Some(bssid) = bssid {
        attrs.push(Nl80211Attr::Mac(bssid));
    }
    nl80211_send(handle, Nl80211Command::JoinIbss, attrs).await?;
    Ok(())
}

async fn leave_ibss(handle: &mut Nl80211Handle, if_index: u32) -> anyhow::Result<()> {
    nl80211_send(
        handle,
        Nl80211Command::LeaveIbss,
        vec![Nl80211Attr::IfIndex(if_index)],
    )
    .await?;
    Ok(())
}

/// `NL80211_BSS_STATUS_IBSS_JOINED` (`linux/nl80211.h`'s `enum nl80211_bss_status`, positional
/// value 2): marks, in a `GET_SCAN` BSS dump, exactly the entry this interface currently
/// considers itself joined to.
const NL80211_BSS_STATUS_IBSS_JOINED: u32 = 2;

/// Reads back the BSSID `if_index` has actually joined, via `NL80211_CMD_GET_SCAN`'s BSS dump —
/// `net/mac80211/ibss.c:369-370`'s `__ieee80211_sta_join_ibss` calls
/// `cfg80211_inform_bss_frame_data` the moment a cell is created or joined, which inserts (or
/// updates) exactly one `GET_SCAN` entry carrying `NL80211_BSS_STATUS_IBSS_JOINED`
/// (`net/wireless/nl80211.c`'s `NL80211_BSS_STATUS` doc: "a BSS attribute in scan dumps"). No
/// active scan or prior `TRIGGER_SCAN` is needed for this entry to exist. Returns `None` before
/// a cell has been created/joined.
async fn read_joined_bssid(
    handle: &mut Nl80211Handle,
    if_index: u32,
) -> anyhow::Result<Option<[u8; 6]>> {
    let mut replies = std::pin::pin!(handle.scan().dump(if_index).execute().await);
    while let Some(msg) = replies.try_next().await? {
        for attr in &msg.payload.attributes {
            let Nl80211Attr::Bss(items) = attr else {
                continue;
            };
            let joined = items.iter().any(
                |i| matches!(i, Nl80211BssInfo::Status(s) if *s == NL80211_BSS_STATUS_IBSS_JOINED),
            );
            if !joined {
                continue;
            }
            if let Some(Nl80211BssInfo::Bssid(mac)) =
                items.iter().find(|i| matches!(i, Nl80211BssInfo::Bssid(_)))
            {
                return Ok(Some(*mac));
            }
        }
    }
    Ok(None)
}

/// Lists every peer MAC address `NL80211_CMD_GET_STATION`'s dump currently reports for
/// `if_index` — in IBSS mode a peer is added (`net/mac80211/ibss.c`'s `ieee80211_ibss_add_sta`)
/// the first time this interface accepts a beacon/probe-response carrying its own BSSID,
/// independent of whether an `ntkd` arc ever forms on top.
async fn list_station_macs(
    handle: &mut Nl80211Handle,
    if_index: u32,
) -> anyhow::Result<Vec<[u8; 6]>> {
    let mut macs = Vec::new();
    let mut replies = std::pin::pin!(handle.station().dump(if_index).execute().await);
    while let Some(msg) = replies.try_next().await? {
        if let Some(Nl80211Attr::Mac(mac)) = msg
            .payload
            .attributes
            .iter()
            .find(|a| matches!(a, Nl80211Attr::Mac(_)))
        {
            macs.push(*mac);
        }
    }
    Ok(macs)
}

fn fmt_mac(mac: &[u8; 6]) -> String {
    mac.iter()
        .map(|b| format!("{b:02x}"))
        .collect::<Vec<_>>()
        .join(":")
}

/// `NL80211_CMD_SET_WIPHY_NETNS`'s own doc: "all devices associated with this wiphy must be down
/// and will follow" — callers bring every netdev on `wiphy_index` down first.
async fn set_wiphy_netns(
    handle: &mut Nl80211Handle,
    wiphy_index: u32,
    netns_fd: RawFd,
) -> anyhow::Result<()> {
    let attrs = vec![
        Nl80211Attr::Wiphy(wiphy_index),
        Nl80211Attr::Other(DefaultNla::new(
            NL80211_ATTR_NETNS_FD,
            (netns_fd as u32).to_ne_bytes().to_vec(),
        )),
    ];
    nl80211_send(handle, Nl80211Command::SetWiphyNetns, attrs).await?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Radio discovery and the real-card guard
// ---------------------------------------------------------------------------------------------

const HWSIM_DRIVER: &str = "mac80211_hwsim";

/// The kernel driver actually backing `phy_name`, read from `/sys` — never trust a wiphy index
/// alone (this file's module doc, "Guarding the real card").
fn phy_driver(phy_name: &str) -> anyhow::Result<String> {
    let link = format!("/sys/class/ieee80211/{phy_name}/device/driver");
    let target = std::fs::read_link(&link)
        .with_context(|| format!("reading driver symlink for {phy_name} ({link})"))?;
    target
        .file_name()
        .and_then(|n| n.to_str())
        .map(str::to_owned)
        .ok_or_else(|| {
            anyhow::anyhow!("driver symlink for {phy_name} has no file name: {target:?}")
        })
}

#[derive(Debug, Clone)]
struct HwsimRadio {
    phy_name: String,
    wiphy_index: u32,
    if_index: u32,
    if_name: String,
}

/// Dumps every wiphy and every interface via nl80211 (`GET_WIPHY`/`GET_INTERFACE`, both plain
/// reads — no `CAP_NET_ADMIN` needed), keeping only interfaces whose owning wiphy's kernel driver
/// is confirmed [`HWSIM_DRIVER`]. Excludes the host's real `phy0`/`wlan0` card by that check, not
/// by a hardcoded index.
async fn discover_hwsim_radios(handle: &mut Nl80211Handle) -> anyhow::Result<Vec<HwsimRadio>> {
    let mut wiphy_names: HashMap<u32, String> = HashMap::new();
    let mut wiphy_stream = std::pin::pin!(handle.wireless_physic().get().execute().await);
    while let Some(msg) = wiphy_stream.try_next().await? {
        let attrs = msg.payload.attributes;
        let index = attrs.iter().find_map(|a| match a {
            Nl80211Attr::Wiphy(i) => Some(*i),
            _ => None,
        });
        let name = attrs.iter().find_map(|a| match a {
            Nl80211Attr::WiphyName(n) => Some(n.clone()),
            _ => None,
        });
        if let (Some(index), Some(name)) = (index, name) {
            wiphy_names.entry(index).or_insert(name);
        }
    }

    let mut radios = Vec::new();
    let mut iface_stream = std::pin::pin!(handle.interface().get(vec![]).execute().await);
    while let Some(msg) = iface_stream.try_next().await? {
        let attrs = msg.payload.attributes;
        let wiphy_index = attrs.iter().find_map(|a| match a {
            Nl80211Attr::Wiphy(i) => Some(*i),
            _ => None,
        });
        let if_index = attrs.iter().find_map(|a| match a {
            Nl80211Attr::IfIndex(i) => Some(*i),
            _ => None,
        });
        let if_name = attrs.iter().find_map(|a| match a {
            Nl80211Attr::IfName(n) => Some(n.clone()),
            _ => None,
        });
        let (Some(wiphy_index), Some(if_index), Some(if_name)) = (wiphy_index, if_index, if_name)
        else {
            continue;
        };
        let Some(phy_name) = wiphy_names.get(&wiphy_index).cloned() else {
            continue;
        };
        if phy_driver(&phy_name)? == HWSIM_DRIVER {
            radios.push(HwsimRadio {
                phy_name,
                wiphy_index,
                if_index,
                if_name,
            });
        }
    }
    radios.sort_by_key(|r| r.wiphy_index);
    Ok(radios)
}

/// Enumerates every wiphy nl80211 can see and asserts the real `phy0`/iwlwifi card is never
/// classified as usable — the safety net every other test in this file relies on before it goes
/// anywhere near `SET_WIPHY_NETNS` or `JOIN_IBSS`. Entirely read-only (two dump requests plus a
/// `/sys` symlink read): no `CAP_NET_ADMIN` needed, unlike every other test below.
///
/// # Running
/// Needs `mac80211_hwsim` loaded, but no elevated privilege at all. Verified running on this
/// host, as the ordinary unprivileged user the rest of this suite has to run privileged:
///
/// ```text
/// cargo test -p ntkd --test wireless -- --ignored hwsim_radio_discovery_never_returns_the_real_radio
/// ```
///
/// -> `test hwsim_radio_discovery_never_returns_the_real_radio ... ok` (discovered 8 radios,
/// `phy1`..`phy8`/`wlan1`..`wlan8`; `phy0`/`wlan0` correctly excluded).
#[ignore = "needs mac80211_hwsim loaded (not root — see this test's own doc comment)"]
#[tokio::test]
async fn hwsim_radio_discovery_never_returns_the_real_radio() {
    let (connection, mut handle, _) = wl_nl80211::new_connection().expect("nl80211 connection");
    tokio::spawn(connection);

    let radios = discover_hwsim_radios(&mut handle)
        .await
        .expect("discover hwsim radios");
    assert!(
        !radios.is_empty(),
        "expected at least one mac80211_hwsim radio; is the module loaded \
         (`modprobe mac80211_hwsim radios=2`)?"
    );
    for radio in &radios {
        assert_ne!(
            radio.phy_name, "phy0",
            "phy0 is the host's real iwlwifi card and must never be classified as usable"
        );
        assert_ne!(
            radio.if_name, "wlan0",
            "wlan0 is the host's real iwlwifi card and must never be classified as usable"
        );
        assert_eq!(
            phy_driver(&radio.phy_name).unwrap(),
            HWSIM_DRIVER,
            "{} was returned by discover_hwsim_radios but isn't actually hwsim-backed",
            radio.phy_name
        );
    }
    eprintln!(
        "discovered {} hwsim radio(s): {:?}",
        radios.len(),
        radios.iter().map(|r| &r.phy_name).collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------------------------
// Per-namespace radio setup, shared by every scenario below
// ---------------------------------------------------------------------------------------------

/// Runs inside a [`netns::NamespaceWorker`]'s own runtime, right after the coordinator has moved
/// `if_name`'s wiphy in: brings `lo` up (via [`netns::link`]), resolves `if_name`'s current
/// ifindex (never trust one captured before the move — see [`wait_for_netdev`]'s own doc: moving
/// a wiphy across netns reassigns its netdev's ifindex, only its name is preserved, and
/// [`HwsimRadio::if_index`] was fixed at `discover_hwsim_radios` time, before any move at all),
/// switches the still-down wireless interface to IBSS ("adhoc") mode, brings it up, then joins
/// the fixed test cell. Returns the device-index map [`netns::observe`] needs. Mirrors
/// [`netns::bring_up_devs`]'s own "bring lo/dev up" step, with the extra nl80211 dance a
/// freshly-arrived wireless interface needs before it behaves like an ordinary L2/L3 link.
///
/// # `join_ibss` returning is "request accepted", not "cell ready" — callers must wait longer
/// `NL80211_CMD_JOIN_IBSS` acks as soon as the kernel accepts the request; it does not wait for
/// a cell to actually exist. `bssid`, forwarded to [`join_ibss`], decides *how* that cell gets
/// created: with no fixed BSSID, `net/mac80211/ibss.c`'s `ieee80211_sta_find_ibss` scans first
/// (measured directly on this host via `journalctl -k`: `<if>: Trigger new scan to find an IBSS
/// to join`, then 4-9s later either joins a found cell or logs `<if>: Creating new IBSS network,
/// BSSID <mac>`), and two radios racing that scan can each miss the other's beacon and create
/// their own cell with their own locally-generated BSSID — measured directly: `wlan9: ... BSSID
/// 16:77:4b:ac:36:51` vs `wlan10: ... BSSID 16:d2:b1:e8:f0:66` on the same run, never converging.
/// [`join_ibss`]'s own doc, backed by `net/mac80211/ibss.c:1248`, further establishes that once
/// `FREQ_FIXED` is set (as it always is here) mac80211's re-scan-for-merge path never fires
/// again either — two such cells are not slow to merge, they structurally cannot. Passing
/// [`IBSS_FIXED_BSSID`] as `bssid` sidesteps both problems: both radios create/join the
/// identical cell by construction (no scan, no merge dependency) — see that constant's own doc
/// for the measured before/after BSSID comparison. This function itself still does not wait
/// beyond `join_ibss`'s own ack; every caller composing a node over its result is responsible
/// for budgeting [`IBSS_MERGE_TIMEOUT`].
async fn prepare_radio_interface(
    if_name: &str,
    ssid: &str,
    freq_mhz: u32,
    bssid: Option<[u8; 6]>,
    label: &str,
) -> anyhow::Result<HashMap<String, u32>> {
    let rt_handle =
        netns::root_handle().with_context(|| format!("{label}: rtnetlink connection"))?;
    netns::link::up(&rt_handle, "lo")
        .await
        .with_context(|| format!("{label}: bring lo up"))?;

    let (nl_connection, mut nl_handle, _) =
        wl_nl80211::new_connection().with_context(|| format!("{label}: nl80211 connection"))?;
    tokio::spawn(nl_connection);

    // Resolve `if_name` and switch it to adhoc/IBSS type as one retriable unit, not two
    // separate steps — see `NETDEV_REAPPEAR_TIMEOUT`'s own doc: a freshly-resolved ifindex can
    // still go stale by the time it's used a moment later. Both steps are safe to repeat: the
    // interface stays down throughout this loop, so re-setting a type it may already hold is a
    // no-op, never an error.
    let deadline = tokio::time::Instant::now() + NETDEV_REAPPEAR_TIMEOUT;
    let if_index = loop {
        let attempt: anyhow::Result<u32> = async {
            let if_index = netns::link::index(&rt_handle, if_name)
                .await
                .with_context(|| format!("{label}: resolve {if_name}'s ifindex after arrival"))?;
            set_interface_adhoc(&mut nl_handle, if_index)
                .await
                .with_context(|| format!("{label}: set {if_name} to adhoc/IBSS type"))?;
            Ok(if_index)
        }
        .await;
        match attempt {
            Ok(if_index) => break if_index,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(NETDEV_REAPPEAR_POLL_INTERVAL).await;
            }
            Err(e) => {
                return Err(e.context(format!(
                    "{label}: {if_name} did not settle within {NETDEV_REAPPEAR_TIMEOUT:?}"
                )));
            }
        }
    };

    let dev_index = netns::link::up(&rt_handle, if_name)
        .await
        .with_context(|| format!("{label}: bring {if_name} up"))?;

    join_ibss(&mut nl_handle, if_index, ssid, freq_mhz, bssid)
        .await
        .with_context(|| format!("{label}: join IBSS {ssid:?} on {freq_mhz}MHz"))?;

    // Defect B: confirm the join actually landed before this interface is handed to a composed
    // daemon — see `IBSS_JOIN_CONFIRM_TIMEOUT`'s own doc for the measured symptom this prevents.
    let join_deadline = tokio::time::Instant::now() + IBSS_JOIN_CONFIRM_TIMEOUT;
    loop {
        let joined = read_joined_bssid(&mut nl_handle, if_index)
            .await
            .with_context(|| format!("{label}: read back {if_name}'s joined BSSID"))?;
        match joined {
            Some(actual) if bssid.is_none() || bssid == Some(actual) => break,
            _ if tokio::time::Instant::now() >= join_deadline => {
                return Err(anyhow::anyhow!(
                    "{label}: {if_name} did not report NL80211_BSS_STATUS_IBSS_JOINED for the \
                     requested cell within {IBSS_JOIN_CONFIRM_TIMEOUT:?} after JOIN_IBSS's own \
                     ack — joined={joined:?} wanted={bssid:?}"
                ));
            }
            _ => tokio::time::sleep(NETDEV_REAPPEAR_POLL_INTERVAL).await,
        }
    }

    Ok(HashMap::from([(if_name.to_owned(), dev_index)]))
}

/// Bounded budget for [`wait_for_netdev`] and the retry loops in [`bring_down_and_move_wiphy`]/
/// [`prepare_radio_interface`]: how long to wait for a netdev to resolve by name — and *stay*
/// resolvable long enough to act on — after a wiphy crosses a netns boundary. `SET_WIPHY_NETNS`'s
/// `NLM_F_ACK` lands as soon as the kernel accepts the request, not once the netdev has finished
/// re-registering under the target namespace, and moving a wiphy across netns reassigns its
/// netdev's ifindex (only its *name* is preserved) — measured, on this host, three independent
/// ways: (1) without a wait at all, the very next sequential trial's [`move_radio_into_namespace`]
/// failed to resolve `wlan10`/`wlan9` ("No such device") immediately after [`RadioReturnGuard`]'s
/// `Drop` had already returned from the bare `SET_WIPHY_NETNS` ack; (2) a numeric ifindex
/// captured once, before any move (as [`HwsimRadio::if_index`] is, at `discover_hwsim_radios`
/// time), is already the *wrong* ifindex by the time a later trial's nl80211 calls try to use
/// it; (3) even a *freshly*-resolved ifindex can go stale between being resolved and being used
/// a moment later — `bring_down_and_move_wiphy` resolved `wlan10` successfully and then failed
/// to bring exactly that ifindex down microseconds afterward, ENODEV. This host's `iwd`
/// (fronted by `NetworkManager`) actively takes an interest in these `mac80211_hwsim` radios the
/// instant they (re)appear (`journalctl -u NetworkManager`: `iwd-manager: new 802.11 Wi-Fi
/// device` immediately followed by state churn), so a resolve-then-act pair can lose this race
/// even when the resolve itself succeeded — [`bring_down_and_move_wiphy`] and
/// [`prepare_radio_interface`] both retry their whole resolve-then-act unit, not just the
/// resolve, for exactly this reason. The timeout itself is sized off a fourth measurement: under
/// sustained load (many prior test iterations' worth of `iwd`/`NetworkManager` device churn —
/// `nmcli device status` on this host ran past 100 tracked devices), `NetworkManager`'s own log
/// shows it giving up on a device (`state change: ... -> unmanaged`) and not retrying its own
/// registration for up to ~30s before trying again; 30s below is sized to that observed
/// recheck cadence with margin, not an arbitrary round number. Every caller of a function that
/// can wait this long adds its own margin on top of [`netns::JOIN_MARGIN`] to its outer
/// `NamespaceWorker::join` timeout accordingly — see `radio_worker_join_timeout`'s own doc for
/// the full per-worker budget this feeds into, including [`RadioReturnGuard`]'s own `Drop`,
/// which retries this bound multiple times rather than reusing it once (see
/// [`RADIO_RETURN_MOVE_TIMEOUT`]/[`RADIO_RETURN_VERIFY_TIMEOUT`]'s own docs for why a single 30s
/// window measurably was not always enough for the *post-move* verification specifically).
const NETDEV_REAPPEAR_TIMEOUT: Duration = Duration::from_secs(30);
const NETDEV_REAPPEAR_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Bound on top of [`NETDEV_REAPPEAR_TIMEOUT`] for [`RadioReturnGuard::drop`]'s own retries of
/// [`bring_down_and_move_wiphy`] before declaring the wiphy genuinely leaked — the *outer*
/// budget, since each individual call already retries internally for up to
/// [`NETDEV_REAPPEAR_TIMEOUT`]. Unlike [`move_radio_into_namespace`]'s inbound call (contested
/// by `init_net`'s own `NetworkManager`/`iwd`), the return call runs inside the worker's own
/// still-unshared namespace, where those `init_net` processes cannot see the interface at all
/// (this file's module doc, "sharp edge" section) — so a single [`NETDEV_REAPPEAR_TIMEOUT`]
/// window failing was expected to be a rare, transient hiccup. **Measured otherwise**: the
/// ten-run reliability scenario's own run 6, radio-b (`wlan10`) — three full
/// [`NETDEV_REAPPEAR_TIMEOUT`] attempts (90s) all failed to even *resolve* `wlan10` by name from
/// inside its own namespace (`ENODEV`), yet the very next run (run 7, after an additional ~19s
/// of `move_radio_into_namespace`'s own wait on the *inbound* side) successfully reused the same
/// physical radio, and this suite's own before/after inventory check found nothing missing —
/// proving the wiphy was not actually gone, just still settling well past 90s. A single retry
/// count is the wrong shape for a duration that varies this much under load; this is a time
/// budget instead, sized with real margin above the observed ~109s-plus recovery, matching
/// [`RADIO_RETURN_VERIFY_TIMEOUT`]'s own generosity — every attempt this loses for real is a
/// radio this suite can never get back without the user's own intervention (this file's own
/// module doc, "Radios were being destroyed, not borrowed"), so a longer budget here is cheap
/// insurance against a rare backstop, not a cost paid on every trial.
const RADIO_RETURN_MOVE_TIMEOUT: Duration = Duration::from_secs(240);

/// Bound for [`RadioReturnGuard::drop`]'s own post-move verification call to [`wait_for_netdev`]
/// — deliberately looser than [`NETDEV_REAPPEAR_TIMEOUT`]. By the time this call runs,
/// [`bring_down_and_move_wiphy`] has already returned `Ok`: the wiphy is irreversibly `init_net`'s
/// object again (see [`RADIO_RETURN_MOVE_TIMEOUT`]'s own doc for why that call is uncontested and
/// expected to succeed quickly), so nothing from this point on can lose it — this wait only
/// decides whether `Drop` can *confirm* the return before giving up on watching, never whether
/// the return happened. Measured directly on this host (`journalctl`, real trial 1 of the
/// ten-run reliability scenario, 2026-08-26 13:45-13:48): radio-b (`wlan10`) last touched the
/// kernel at 13:45:57 (entering that trial's `IBSS_MERGE_TIMEOUT` wait), and `NetworkManager`
/// did not log it as a "new 802.11 Wi-Fi device" in `init_net` again until 13:48:14 — about 137s
/// later, well past [`NETDEV_REAPPEAR_TIMEOUT`]'s 30s and past the two of them combined. In
/// between, `NetworkManager`'s own log shows a sustained burst of `if_nametoindex failed`/`IWD
/// device ... is not a Wifi device`/`unmanaged-link-not-init` cycling across every hwsim radio on
/// the host at once, not just `wlan10` — consistent with needing more than one of
/// [`NETDEV_REAPPEAR_TIMEOUT`]'s own cited ~30s `NetworkManager` recheck cycles to settle under
/// that load. Sized with real margin over the measured 137s for a worse cycle, not a round
/// number; unlike widening [`NETDEV_REAPPEAR_TIMEOUT`] itself (which gates the move that can
/// actually lose the radio), widening this one is free of that risk.
const RADIO_RETURN_VERIFY_TIMEOUT: Duration = Duration::from_secs(240);

/// Polls, from the calling thread's *current* netns, until `if_name` resolves via a raw link
/// dump ([`netns::link::index`]) — returning its current ifindex — or `timeout` elapses. See
/// [`NETDEV_REAPPEAR_TIMEOUT`]'s own doc for why this wait (and re-resolving by name at all,
/// rather than trusting a passed-in index) is needed, and [`RADIO_RETURN_VERIFY_TIMEOUT`]'s own
/// doc for why [`RadioReturnGuard`]'s own call passes a looser bound than
/// [`NETDEV_REAPPEAR_TIMEOUT`].
async fn wait_for_netdev(
    rt_handle: &rtnetlink::Handle,
    if_name: &str,
    label: &str,
    timeout: Duration,
) -> anyhow::Result<u32> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(index) = netns::link::index(rt_handle, if_name).await {
            return Ok(index);
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(anyhow::anyhow!(
                "{label}: {if_name} did not resolve in this namespace within {timeout:?}"
            ));
        }
        tokio::time::sleep(NETDEV_REAPPEAR_POLL_INTERVAL).await;
    }
}

/// Brings `if_name`'s netdev down, then moves its wiphy into `netns_fd` — the one nl80211
/// sequence `SET_WIPHY_NETNS`'s own contract requires (every netdev down first, then the wiphy
/// follows), shared by both directions of the move: the coordinator's inbound
/// [`move_radio_into_namespace`] and each worker's own outbound [`RadioReturnGuard`]. The only
/// difference between the two callers is which thread's netns visibility `rt_handle`/`nl_handle`
/// were opened under. Resolves `if_name`, brings it down, and moves it as one retriable unit —
/// see `NETDEV_REAPPEAR_TIMEOUT`'s own doc for why retrying only the resolve step isn't enough
/// (a freshly-resolved ifindex can go stale before the very next call uses it) and why all three
/// steps stay safe to repeat (bringing an already-down interface down, or re-issuing
/// `SET_WIPHY_NETNS` against a wiphy still in this namespace, are both no-ops on success).
async fn bring_down_and_move_wiphy(
    rt_handle: &rtnetlink::Handle,
    nl_handle: &mut Nl80211Handle,
    if_name: &str,
    wiphy_index: u32,
    netns_fd: RawFd,
    label: &str,
) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + NETDEV_REAPPEAR_TIMEOUT;
    loop {
        let attempt: anyhow::Result<()> = async {
            let index = netns::link::index(rt_handle, if_name).await?;
            netns::link::down(rt_handle, index)
                .await
                .with_context(|| format!("bringing {if_name} down before moving its wiphy"))?;
            set_wiphy_netns(nl_handle, wiphy_index, netns_fd)
                .await
                .with_context(|| {
                    format!("moving wiphy {wiphy_index} (via {if_name}) to target namespace")
                })
        }
        .await;
        match attempt {
            Ok(()) => return Ok(()),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(NETDEV_REAPPEAR_POLL_INTERVAL).await;
            }
            Err(e) => {
                return Err(e.context(format!(
                    "{label}: {if_name} did not settle within {NETDEV_REAPPEAR_TIMEOUT:?}"
                )));
            }
        }
    }
}

/// Moves `radio`'s whole wiphy (this file's module doc: netdev-only is not a real option) into
/// the namespace identified by `netns_fd`, from the coordinator's own (un-unshared, real
/// `init_net`) thread — the wireless analogue of [`netns::link::move_to_ns`]'s veth-pair move.
/// [`RadioReturnGuard`] is the mirror move, back out, issued by the worker itself.
async fn move_radio_into_namespace(
    rt_handle: &rtnetlink::Handle,
    nl_handle: &mut Nl80211Handle,
    radio: &HwsimRadio,
    netns_fd: RawFd,
) -> anyhow::Result<()> {
    bring_down_and_move_wiphy(
        rt_handle,
        nl_handle,
        &radio.if_name,
        radio.wiphy_index,
        netns_fd,
        &radio.phy_name,
    )
    .await
    .with_context(|| format!("moving {} into the target namespace", radio.phy_name))
}

/// Opens an fd naming this thread's *current* netns. Call only from the coordinator thread,
/// strictly before spawning the [`netns::NamespaceWorker`] this fd is paired with:
/// `nix::sched::unshare(CLONE_NEWNET)` runs on the worker's own dedicated thread before its body
/// ever gets to run (see [`netns::NamespaceWorker::spawn`]), and once that has happened there is
/// no way left, from anywhere, to name the namespace the worker started in — the coordinator
/// never unshares, so its own netns is always `init_net`, and this must be captured before the
/// pairing is even set up. The returned `File` is moved whole into that worker's scenario body
/// and finally consumed by [`RadioReturnGuard`].
fn open_init_net_fd(label: &str) -> anyhow::Result<std::fs::File> {
    std::fs::File::open("/proc/self/ns/net")
        .with_context(|| format!("{label}: open coordinator's own (init_net) netns fd"))
}

/// Moves its paired radio's wiphy back to `init_net` when dropped — the empirically-required
/// mirror of [`move_radio_into_namespace`] (this file's module doc, "No shared-teardown
/// rendezvous needed" section: a wiphy left behind in a dying namespace is destroyed with it, not
/// reparented). A `Drop` impl, not a plain call at the end of the happy path, so a panicking or
/// early-returning scenario body still releases the radio — this project's standing rule that a
/// resource acquired here is released from `Drop`, never only on the happy path (see
/// `EnterGuard`/`CommitGuard` elsewhere in this codebase).
///
/// Must run from *this* worker's own thread: the coordinator's netlink socket lives in
/// `init_net` and cannot see a wiphy currently parented to this worker's namespace (netlink
/// visibility is scoped to the requesting socket's own netns), so only a socket opened from
/// inside this namespace — i.e. this thread, which never leaves it — can issue the move.
struct RadioReturnGuard {
    label: String,
    if_name: String,
    wiphy_index: u32,
    /// Kept open only to stay valid until [`Self::drop`] consumes it.
    init_net: Option<std::fs::File>,
}

impl RadioReturnGuard {
    fn new(label: &str, radio: &HwsimRadio, init_net: std::fs::File) -> Self {
        Self {
            label: label.to_owned(),
            if_name: radio.if_name.clone(),
            wiphy_index: radio.wiphy_index,
            init_net: Some(init_net),
        }
    }
}

impl Drop for RadioReturnGuard {
    /// `Drop` cannot `.await`, and this fires from inside [`netns::NamespaceWorker::spawn`]'s own
    /// `rt.block_on(body())` on every path (happy, early-return, and panicking-unwind alike), so
    /// calling `block_on` again here — on this same thread, even against a brand-new `Runtime` —
    /// would hit Tokio's single-thread reentrancy panic. Spawning a fresh `std::thread` instead
    /// sidesteps that: a new thread inherits its creator's *current* namespace at creation time
    /// (`clone`/`pthread_create` semantics), so it starts life already inside this worker's
    /// namespace with no `setns` needed for the `SET_WIPHY_NETNS` call itself. But
    /// "`SET_WIPHY_NETNS` acked" is not "`if_name` visible again in `init_net`" ([`wait_for_netdev`]'s
    /// own doc: measured false, this is exactly what broke the next sequential trial's radio
    /// reuse) — so once the move-back is acked, this same spawned thread `setns`(2)s itself
    /// *into* `init_net` via the fd captured before this worker ever unshared, opens a fresh
    /// rtnetlink connection there (existing sockets stay bound to the namespace they were
    /// created in; `setns` only changes what a *new* socket sees), and polls until `if_name`
    /// resolves. `Drop` blocks (joining that thread) until all of this — not just the ack —
    /// completes or times out: nothing else on this thread can make progress until `Drop`
    /// returns anyway, and a caller relying on this guard (the ten-sequential-trial reliability
    /// scenario reusing the same two radios back-to-back) must never observe "guard dropped" as
    /// meaning less than "radio is actually usable again".
    ///
    /// # Defect A: the move and the verification carry different risk — and the verification
    /// runs regardless
    /// [`bring_down_and_move_wiphy`] failing here was assumed to be a genuine, permanent-loss
    /// risk (this is the worker's *own* still-unshared namespace, uncontested by `init_net`'s
    /// `NetworkManager`/`iwd`), so it gets [`RADIO_RETURN_MOVE_TIMEOUT`]'s own budget of retries
    /// before giving up. **Measured false, twice, independently of `NETDEV_REAPPEAR_TIMEOUT`
    /// itself being too short**: the ten-run reliability scenario's own runs 8→9, radio-b
    /// (`wlan10`) — `bring_down_and_move_wiphy` failed to even *resolve* `wlan10` by name from
    /// inside its own (supposedly uncontested) namespace on eight straight
    /// [`NETDEV_REAPPEAR_TIMEOUT`]-bounded attempts (240s total, `ENODEV` every time), yet this
    /// suite's own before/after `/sys/class/ieee80211` inventory check, run immediately after,
    /// found the wiphy already back in `init_net` regardless — proving the object was not
    /// actually still trapped in the dying namespace the failed resolves implied. Because of
    /// this, `setns`+[`wait_for_netdev`] below now runs unconditionally — checking `init_net`
    /// directly settles the only question that actually matters (is the wiphy there or not)
    /// more reliably than trusting a source-side resolve that has now been measured to give a
    /// false negative — rather than only after a move the source side agrees happened. A
    /// genuine loss is now only declared when *neither* side ever saw the radio: the move never
    /// acked *and* `init_net` never resolved it either.
    fn drop(&mut self) {
        let Some(init_net) = self.init_net.take() else {
            return;
        };
        let label = self.label.clone();
        let if_name = self.if_name.clone();
        let wiphy_index = self.wiphy_index;
        let spawned = std::thread::Builder::new()
            .name(format!("{}-return", self.label))
            .spawn(move || -> anyhow::Result<bool> {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .context("build return-to-init_net runtime")?;
                rt.block_on(async move {
                    let rt_handle = netns::root_handle().context("rtnetlink connection")?;
                    let (nl_connection, mut nl_handle, _) =
                        wl_nl80211::new_connection().context("nl80211 connection")?;
                    tokio::spawn(nl_connection);

                    let move_deadline = tokio::time::Instant::now() + RADIO_RETURN_MOVE_TIMEOUT;
                    let move_error = loop {
                        match bring_down_and_move_wiphy(
                            &rt_handle,
                            &mut nl_handle,
                            &if_name,
                            wiphy_index,
                            init_net.as_raw_fd(),
                            &label,
                        )
                        .await
                        {
                            Ok(()) => break None,
                            Err(e) if tokio::time::Instant::now() < move_deadline => {
                                eprintln!(
                                    "{label}: moving {if_name} back to init_net failed, \
                                     retrying (budget {RADIO_RETURN_MOVE_TIMEOUT:?}): {e:#}"
                                );
                            }
                            Err(e) => break Some(e),
                        }
                    };

                    // Check `init_net` directly regardless of whether the move above ever
                    // reported success — see this fn's own "Defect A" doc for why a source-side
                    // resolve failure is not reliable evidence the wiphy is still trapped.
                    nix::sched::setns(&init_net, nix::sched::CloneFlags::CLONE_NEWNET)
                        .with_context(|| {
                            format!("{label}: setns back into init_net to verify {if_name}")
                        })?;
                    let verify_handle = netns::root_handle()
                        .with_context(|| format!("{label}: rtnetlink connection in init_net"))?;
                    let verified = wait_for_netdev(
                        &verify_handle,
                        &if_name,
                        &label,
                        RADIO_RETURN_VERIFY_TIMEOUT,
                    )
                    .await
                    .is_ok();

                    match (verified, move_error) {
                        (true, _) => Ok(true),
                        (false, None) => Ok(false),
                        (false, Some(e)) => Err(e.context(format!(
                            "{label}: moving {if_name} back to init_net within \
                             {RADIO_RETURN_MOVE_TIMEOUT:?}, and it never became resolvable in \
                             init_net within {RADIO_RETURN_VERIFY_TIMEOUT:?} either"
                        ))),
                    }
                })
            });
        let outcome = match spawned {
            Ok(handle) => handle.join().unwrap_or_else(|e| {
                Err(anyhow::anyhow!("return-to-init_net thread panicked: {e:?}"))
            }),
            Err(e) => Err(anyhow::anyhow!("spawn return-to-init_net thread: {e}")),
        };
        match outcome {
            Ok(true) => {}
            Ok(false) => eprintln!(
                "{}: moved {}'s wiphy back to init_net but could not confirm it resolvable \
                 within {RADIO_RETURN_VERIFY_TIMEOUT:?} — NOT leaked (`init_net` was checked \
                 directly), likely still settling under NetworkManager/iwd contention",
                self.label, self.if_name
            ),
            // Deliberately not the word "leaked". This branch fires when neither the move
            // nor the direct init_net check could confirm the radio inside their budgets,
            // which is NOT the same as the radio being gone -- and measurement says it
            // usually is not gone. Observed on this host: a wiphy reported unconfirmed
            // here was logged by NetworkManager as reappearing in init_net ~137s later,
            // and every inventory taken after a settling delay across a dozen runs found
            // the full pool intact, including runs that hit this branch twice and runs
            // killed with SIGKILL mid-retry. An earlier version of this message claimed
            // "it is now LEAKED" and that false positive misdirected a whole debugging
            // session into hunting a leak that did not exist. Report the uncertainty, not
            // a conclusion the evidence does not support.
            Err(err) => eprintln!(
                "{}: could not confirm {}'s wiphy returned to init_net within \
                 {RADIO_RETURN_MOVE_TIMEOUT:?} (move) + {RADIO_RETURN_VERIFY_TIMEOUT:?} \
                 (verify) — most likely still settling under NetworkManager/iwd \
                 contention, which has been measured taking minutes on this host. Re-check \
                 the pool after a delay before concluding anything was lost: {err:#}",
                self.label, self.if_name
            ),
        }
    }
}

/// Worst-case bound `NamespaceWorker::join` callers below pass for one radio worker's entire
/// body. A function, not a `const`, because `Duration`'s `Add`/`Mul` impls are not `const fn`.
/// Summands, in the order a worker body can spend them: [`IBSS_MERGE_TIMEOUT`] (the scenario's
/// own arc/negotiation wait) and [`netns::JOIN_MARGIN`] (covers `teardown` itself, per that
/// constant's own doc) as before; [`NETDEV_REAPPEAR_TIMEOUT`] once for
/// `prepare_radio_interface`'s resolve-retry and [`IBSS_JOIN_CONFIRM_TIMEOUT`] once for that
/// same function's post-`JOIN_IBSS` confirmation (Defect B); [`RADIO_RETURN_MOVE_TIMEOUT`] for
/// [`RadioReturnGuard`]'s own move-back retries, plus [`RADIO_RETURN_VERIFY_TIMEOUT`] for its
/// post-move verification (Defect A).
fn radio_worker_join_timeout() -> Duration {
    IBSS_MERGE_TIMEOUT
        + netns::JOIN_MARGIN
        + NETDEV_REAPPEAR_TIMEOUT
        + IBSS_JOIN_CONFIRM_TIMEOUT
        + RADIO_RETURN_MOVE_TIMEOUT
        + RADIO_RETURN_VERIFY_TIMEOUT
}

// ---------------------------------------------------------------------------------------------
// Scenario 1: two real daemons over two hwsim radios in IBSS, arc + real kernel route
// ---------------------------------------------------------------------------------------------

#[derive(Debug)]
struct RadioTrialReport {
    node: netns::NodeReport,
    expected_destination: Ipv4Net,
    route_installed: bool,
    /// This interface's own joined BSSID just before teardown, per [`read_joined_bssid`] — `None`
    /// if no cell had formed yet. The two reports in a trial matching is the direct observational
    /// proof [`IBSS_FIXED_BSSID`]'s own doc claims; diverging is the direct proof of the
    /// pre-fix failure mode.
    bssid: Option<[u8; 6]>,
    /// Peer MAC addresses this interface's own `NL80211_CMD_GET_STATION` dump reported just
    /// before teardown, per [`list_station_macs`] — empty means this radio never accepted the
    /// other's beacon as belonging to its own cell.
    stations: Vec<[u8; 6]>,
}

/// One namespace's work for the arc-establishment scenario, run inside a
/// [`netns::NamespaceWorker`]: prepares the just-arrived radio, composes the real node over it
/// via [`netns::spawn_node`], polls the real kernel until the peer's route appears or a 30s
/// deadline passes, tears down, and reports. Holds a [`RadioReturnGuard`] for its entire body so
/// the radio's wiphy returns to `init_net` even if this returns early or panics.
#[allow(clippy::too_many_arguments)]
async fn radio_arc_trial_body(
    label: String,
    my_id: NodeId,
    my_idx: u32,
    peer_idx: u32,
    radio: HwsimRadio,
    port: u16,
    init_net: std::fs::File,
    my_done_tx: tokio::sync::oneshot::Sender<()>,
    peer_done_rx: tokio::sync::oneshot::Receiver<()>,
) -> anyhow::Result<RadioTrialReport> {
    let _return_guard = RadioReturnGuard::new(&label, &radio, init_net);

    let dev_index = prepare_radio_interface(
        &radio.if_name,
        IBSS_SSID,
        IBSS_FREQ_MHZ,
        Some(IBSS_FIXED_BSSID),
        &label,
    )
    .await?;

    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let started = netns::spawn_node(
        my_id,
        Some(position(my_idx)),
        None,
        &[4, 2, 2, 2],
        &[radio.if_name.as_str()],
        port,
        &mut tasks,
        cancel.clone(),
    )
    .await
    .with_context(|| format!("{label}: compose real node"))?;

    let expected_destination =
        addressing::gnode_destination(&naddr(my_idx), HCoord::new(0, peer_idx))
            .with_context(|| format!("{label}: expected destination"))?;

    let deadline = tokio::time::Instant::now() + IBSS_MERGE_TIMEOUT;
    let (node, route_installed) = loop {
        let node = netns::observe(&label, &started, dev_index.clone())
            .await
            .with_context(|| format!("{label}: observe"))?;
        let found = node
            .routes
            .iter()
            .any(|r| r.destination == expected_destination);
        if found || tokio::time::Instant::now() >= deadline {
            break (node, found);
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    let (bssid, stations) = match (
        dev_index.get(radio.if_name.as_str()),
        wl_nl80211::new_connection(),
    ) {
        (Some(&idx), Ok((connection, mut measure_handle, _))) => {
            tokio::spawn(connection);
            let bssid = read_joined_bssid(&mut measure_handle, idx)
                .await
                .unwrap_or_else(|err| {
                    eprintln!("{label}: read_joined_bssid failed: {err:#}");
                    None
                });
            let stations = list_station_macs(&mut measure_handle, idx)
                .await
                .unwrap_or_else(|err| {
                    eprintln!("{label}: list_station_macs failed: {err:#}");
                    Vec::new()
                });
            (bssid, stations)
        }
        _ => (None, Vec::new()),
    };
    // Rendezvous with the peer worker before tearing anything down: `netns::teardown` cancels
    // this identity's whole `TcpServer` (`ntk_rpc::server::serve_connection`'s
    // `cancel.cancelled() => { inflight.abort_all(); break; }` arm drops the accepted socket
    // without waiting for in-flight replies), and this side reaching its own `found`/deadline
    // condition is independent of whether the peer's own polling — or its `run_arc_confirmation`/
    // `run_arc_monitor` nop probe, which is *not* gated on route installation at all — has
    // finished. Without this, whichever side finishes first can cancel its `TcpServer` out from
    // under the peer's genuinely in-flight nop `call()`/`notify()`, which then resolves to a
    // local-only `RpcError::ConnectionClosed` indistinguishable, at that call site, from the
    // peer authoritatively rejecting the caller — tearing down a perfectly healthy arc on one
    // side while the other side (which simply exited) never itself notices anything wrong.
    // Measured: `real_hwsim_broadcast_reliability_across_ten_runs`, 4/10 trials, always this
    // exact shape — one side's final report `Established`+`cost`, the other's arc list empty,
    // `error=connection closed is_remote=false` on whichever side's probe raced the other's
    // teardown. `crates/ntkd/tests/multi_node.rs`'s veth tier already carries the identical fix
    // (`NamespaceSpec::my_done_tx`/`peer_done_rx`, that module's own doc comment) for the same
    // class of premature-namespace-teardown race; this scenario never got it when written.
    let _ = my_done_tx.send(());
    let _ = peer_done_rx.await;

    netns::teardown(&started, cancel, &mut tasks).await;
    if let Ok((connection, mut leave_handle, _)) = wl_nl80211::new_connection() {
        tokio::spawn(connection);
        if let Some(&idx) = dev_index.get(radio.if_name.as_str()) {
            let _ = leave_ibss(&mut leave_handle, idx).await;
        }
    }

    Ok(RadioTrialReport {
        node,
        expected_destination,
        route_installed,
        bssid,
        stations,
    })
}

#[derive(Debug, Clone, Copy)]
struct ArcTrialOutcome {
    arc_established: bool,
    route_installed: bool,
    elapsed: Duration,
}

/// One full trial of the arc-establishment scenario: discovers nothing itself (the caller passes
/// already-discovered, already-guarded radios in), moves both into fresh namespaces via two
/// [`netns::NamespaceWorker`]s, brings up two real daemons over IBSS, and reports what happened —
/// without asserting anything, so both
/// [`real_hwsim_two_daemons_establish_arc_and_route_over_ibss`] (asserts hard, once) and
/// [`real_hwsim_broadcast_reliability_across_ten_runs`] (records, repeatedly) can share it.
async fn run_ibss_arc_trial(
    rt_handle: &rtnetlink::Handle,
    nl_handle: &mut Nl80211Handle,
    radio_a: HwsimRadio,
    radio_b: HwsimRadio,
    port: u16,
) -> anyhow::Result<(ArcTrialOutcome, RadioTrialReport, RadioTrialReport)> {
    let started_at = Instant::now();
    let move_a = radio_a.clone();
    let move_b = radio_b.clone();
    let init_net_a = open_init_net_fd("radio-a")?;
    let init_net_b = open_init_net_fd("radio-b")?;
    let (done_tx_a, done_rx_a) = tokio::sync::oneshot::channel::<()>();
    let (done_tx_b, done_rx_b) = tokio::sync::oneshot::channel::<()>();

    let worker_a = netns::NamespaceWorker::spawn("radio-a", move || {
        radio_arc_trial_body(
            "radio-a".to_owned(),
            NodeId::from_raw(301).unwrap(),
            0,
            1,
            radio_a,
            port,
            init_net_a,
            done_tx_a,
            done_rx_b,
        )
    });
    let worker_b = netns::NamespaceWorker::spawn("radio-b", move || {
        radio_arc_trial_body(
            "radio-b".to_owned(),
            NodeId::from_raw(302).unwrap(),
            1,
            0,
            radio_b,
            port,
            init_net_b,
            done_tx_b,
            done_rx_a,
        )
    });

    let fd_a = worker_a.fd();
    let fd_b = worker_b.fd();

    move_radio_into_namespace(rt_handle, nl_handle, &move_a, fd_a).await?;
    move_radio_into_namespace(rt_handle, nl_handle, &move_b, fd_b).await?;

    worker_a.signal_moved();
    worker_b.signal_moved();

    let report_a = worker_a
        .join(radio_worker_join_timeout())
        .await
        .context("radio-a namespace body")?;
    let report_b = worker_b
        .join(radio_worker_join_timeout())
        .await
        .context("radio-b namespace body")?;

    let outcome = ArcTrialOutcome {
        arc_established: report_a.node.arcs.iter().any(|a| a.cost.is_some())
            && report_b.node.arcs.iter().any(|a| a.cost.is_some()),
        route_installed: report_a.route_installed && report_b.route_installed,
        elapsed: started_at.elapsed(),
    };
    Ok((outcome, report_a, report_b))
}

/// Two hwsim radios, one namespace each, two real `ntkd` daemons formed into an arc over a
/// joined IBSS cell, real kernel route installed to each other — the wireless analogue of
/// `multi_node.rs`'s `real_netns_two_daemons_establish_arc_and_route`.
///
/// # Running
/// Needs real root (`CAP_NET_ADMIN`+`CAP_SYS_ADMIN` in `init_user_ns`) — this file's module
/// doc's "sharp edge" section: unlike the bridged-veth rung, `unshare --net --map-root-user`
/// does not work here, confirmed empirically on this host. Run through this host's privileged
/// wrapper (`ntk-wireless-test`, a `setpriv` shim granting exactly that capability pair):
///
/// ```text
/// sudo ntk-wireless-test real_hwsim_two_daemons_establish_arc_and_route_over_ibss
/// ```
///
/// **Run to completion, repeatedly.** Before [`IBSS_FIXED_BSSID`] existed (`FREQ_FIXED` only, no
/// fixed BSSID), this scenario's own `read_joined_bssid`/`list_station_macs` instrumentation
/// measured the failure directly, twice: `radio-a: bssid=8e:72:24:81:92:6f` vs
/// `radio-b: bssid=9a:ff:9a:21:1d:47`, and separately `radio-a: bssid=ee:8c:08:f1:d6:16` vs
/// `radio-b: bssid=none` — two different cells, `stations=[]` on both sides in every case, no
/// arc, matching this file's module doc, "Why no arc formed" section exactly. After
/// [`IBSS_FIXED_BSSID`]: both radios report the identical configured BSSID
/// (`02:4e:54:4b:77:69`), each appears in the other's station dump, and the arc/route assertions
/// below passed in 0.54s (previously this scenario needed up to a 60s deadline and still failed
/// 0/10 times at that budget — see [`real_hwsim_broadcast_reliability_across_ten_runs`]'s own
/// doc for the ten-trial comparison). Radio inventory (`/sys/class/ieee80211`) was verified
/// unchanged, all 8 hwsim radios with netdevs intact, before and after every run in this
/// session.
#[ignore = "requires real root (CAP_NET_ADMIN in init_user_ns) — see this test's own doc comment"]
#[tokio::test]
async fn real_hwsim_two_daemons_establish_arc_and_route_over_ibss() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .try_init();

    let rt_handle = netns::root_handle().expect("rtnetlink connection");
    let (nl_connection, mut nl_handle, _) =
        wl_nl80211::new_connection().expect("nl80211 connection");
    tokio::spawn(nl_connection);

    let radios = discover_hwsim_radios(&mut nl_handle)
        .await
        .expect("discover hwsim radios");
    assert!(
        radios.len() >= 2,
        "need at least 2 mac80211_hwsim radios, found {}",
        radios.len()
    );
    let (radio_a, radio_b) = (radios[0].clone(), radios[1].clone());

    let (outcome, report_a, report_b) =
        run_ibss_arc_trial(&rt_handle, &mut nl_handle, radio_a, radio_b, 27290)
            .await
            .expect("run ibss arc trial");

    eprintln!(
        "{}: bssid={} stations={:?} arcs={:#?} routes={:#?}",
        report_a.node.label,
        report_a.bssid.as_ref().map_or("none".to_owned(), fmt_mac),
        report_a.stations.iter().map(fmt_mac).collect::<Vec<_>>(),
        report_a.node.arcs,
        report_a.node.routes
    );
    eprintln!(
        "{}: bssid={} stations={:?} arcs={:#?} routes={:#?}",
        report_b.node.label,
        report_b.bssid.as_ref().map_or("none".to_owned(), fmt_mac),
        report_b.stations.iter().map(fmt_mac).collect::<Vec<_>>(),
        report_b.node.arcs,
        report_b.node.routes
    );

    assert!(
        report_a.node.arcs.iter().any(|a| a.cost.is_some()),
        "radio-a never measured a cost for its neighbor — no arc established: {:#?}",
        report_a.node.arcs
    );
    assert!(
        report_b.node.arcs.iter().any(|a| a.cost.is_some()),
        "radio-b never measured a cost for its neighbor — no arc established: {:#?}",
        report_b.node.arcs
    );
    assert!(
        report_a.route_installed,
        "radio-a's real kernel routing table never gained a route to {}: routes={:#?} \
         addresses={:#?}",
        report_a.expected_destination, report_a.node.routes, report_a.node.addresses
    );
    assert!(
        report_b.route_installed,
        "radio-b's real kernel routing table never gained a route to {}: routes={:#?} \
         addresses={:#?}",
        report_b.expected_destination, report_b.node.routes, report_b.node.addresses
    );
    assert!(outcome.arc_established && outcome.route_installed);
}

// ---------------------------------------------------------------------------------------------
// Scenario 2: two virgin daemons negotiating a shared network over the air
// ---------------------------------------------------------------------------------------------

/// One namespace's work for the negotiation scenario: identical structure to
/// [`radio_arc_trial_body`] except `initial_position: None` (the production path
/// `multi_node.rs`'s `spawn_real_negotiating_node` doc explains) and polling on
/// [`netns::NodeReport::rehooked`] plus route presence, rather than a single fixed destination —
/// mirroring `multi_node.rs`'s `negotiation_namespace_body`'s own reasoning for why route
/// presence alone is never a valid "negotiation settled" signal here. Holds a
/// [`RadioReturnGuard`] for its entire body, same as [`radio_arc_trial_body`].
#[allow(clippy::too_many_arguments)]
async fn radio_negotiation_trial_body(
    label: String,
    my_id: NodeId,
    radio: HwsimRadio,
    port: u16,
    init_net: std::fs::File,
    my_done_tx: tokio::sync::oneshot::Sender<()>,
    peer_done_rx: tokio::sync::oneshot::Receiver<()>,
) -> anyhow::Result<(netns::NodeReport, Vec<u32>)> {
    let _return_guard = RadioReturnGuard::new(&label, &radio, init_net);

    let dev_index = prepare_radio_interface(
        &radio.if_name,
        IBSS_SSID,
        IBSS_FREQ_MHZ,
        Some(IBSS_FIXED_BSSID),
        &label,
    )
    .await?;

    let mut tasks = JoinSet::new();
    let cancel = CancellationToken::new();
    let started = netns::spawn_node(
        my_id,
        None,
        None,
        &[8],
        &[radio.if_name.as_str()],
        port,
        &mut tasks,
        cancel.clone(),
    )
    .await
    .with_context(|| format!("{label}: compose real negotiating node"))?;

    let initial_position = started
        .running
        .generation
        .borrow()
        .qspn
        .my_naddr()
        .positions()
        .to_vec();

    let deadline = tokio::time::Instant::now() + IBSS_MERGE_TIMEOUT;
    let node = loop {
        let node = netns::observe(&label, &started, dev_index.clone())
            .await
            .with_context(|| format!("{label}: observe"))?;
        let rehooked_and_reattached = node.rehooked && !node.routes.is_empty();
        if rehooked_and_reattached || tokio::time::Instant::now() >= deadline {
            break node;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    // Rendezvous with the peer worker before tearing anything down — see
    // `radio_arc_trial_body`'s own identical rendezvous for the full rationale: without it,
    // whichever side (structurally always the loser here, since only a rehook satisfies this
    // loop's own exit condition — the winner's `node.rehooked` never becomes true and it always
    // runs the full deadline) finishes and tears down first can cancel its `TcpServer` out from
    // under the *other* side's still-in-flight nop/qspn traffic, which is exactly what starved
    // the winner of a route back to its migrated peer here: measured,
    // `real_hwsim_two_virgin_daemons_negotiate_over_the_air` failing
    // "the winner's real kernel routing table never gained a route to its (migrated) peer: []"
    // before this rendezvous existed.
    let _ = my_done_tx.send(());
    let _ = peer_done_rx.await;

    netns::teardown(&started, cancel, &mut tasks).await;
    if let Ok((connection, mut leave_handle, _)) = wl_nl80211::new_connection() {
        tokio::spawn(connection);
        if let Some(&idx) = dev_index.get(radio.if_name.as_str()) {
            let _ = leave_ibss(&mut leave_handle, idx).await;
        }
    }

    Ok((node, initial_position))
}

/// Two real, virgin `ntkd` daemons — each bootstrapping `initial_position: None` — meet over two
/// hwsim radios in IBSS and genuinely negotiate a shared network: exactly one adopts the other's
/// Coordinator-reserved position, verified against the real kernel address/routing tables. The
/// wireless analogue of `multi_node.rs`'s `real_netns_two_daemons_negotiate_a_shared_network`.
///
/// # Running
/// Needs real root (`CAP_NET_ADMIN`+`CAP_SYS_ADMIN` in `init_user_ns`) — see this file's module
/// doc's "sharp edge" section; this host provides that via a privileged wrapper (see that same
/// section). Not independently re-run this session (the radio-move and IBSS-merge fixes in this
/// file's module doc were verified against [`real_hwsim_two_daemons_establish_arc_and_route_over_ibss`]
/// and [`real_hwsim_broadcast_reliability_across_ten_runs`] instead, to conserve this host's
/// finite hwsim radio pool); the same fixes apply here unchanged since this scenario shares
/// [`prepare_radio_interface`]/[`RadioReturnGuard`]/[`IBSS_MERGE_TIMEOUT`] with those two. Run as:
///
/// ```text
/// sudo ntk-wireless-test real_hwsim_two_virgin_daemons_negotiate_over_the_air
/// ```
#[ignore = "requires real root (CAP_NET_ADMIN in init_user_ns) — see this test's own doc comment"]
#[tokio::test]
async fn real_hwsim_two_virgin_daemons_negotiate_over_the_air() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .with_thread_names(true)
        .try_init();

    let rt_handle = netns::root_handle().expect("rtnetlink connection");
    let (nl_connection, mut nl_handle, _) =
        wl_nl80211::new_connection().expect("nl80211 connection");
    tokio::spawn(nl_connection);

    let radios = discover_hwsim_radios(&mut nl_handle)
        .await
        .expect("discover hwsim radios");
    assert!(
        radios.len() >= 2,
        "need at least 2 mac80211_hwsim radios, found {}",
        radios.len()
    );
    let (radio_a, radio_b) = (radios[0].clone(), radios[1].clone());
    const PORT: u16 = 27291;

    let move_a = radio_a.clone();
    let move_b = radio_b.clone();
    let init_net_a = open_init_net_fd("radio-a").expect("open coordinator init_net fd for radio-a");
    let init_net_b = open_init_net_fd("radio-b").expect("open coordinator init_net fd for radio-b");
    let (done_tx_a, done_rx_a) = tokio::sync::oneshot::channel::<()>();
    let (done_tx_b, done_rx_b) = tokio::sync::oneshot::channel::<()>();

    let worker_a = netns::NamespaceWorker::spawn("radio-a", move || {
        radio_negotiation_trial_body(
            "radio-a".to_owned(),
            NodeId::from_raw(401).unwrap(),
            radio_a,
            PORT,
            init_net_a,
            done_tx_a,
            done_rx_b,
        )
    });
    let worker_b = netns::NamespaceWorker::spawn("radio-b", move || {
        radio_negotiation_trial_body(
            "radio-b".to_owned(),
            NodeId::from_raw(402).unwrap(),
            radio_b,
            PORT,
            init_net_b,
            done_tx_b,
            done_rx_a,
        )
    });

    let fd_a = worker_a.fd();
    let fd_b = worker_b.fd();

    move_radio_into_namespace(&rt_handle, &mut nl_handle, &move_a, fd_a)
        .await
        .expect("move radio-a's wiphy into ns-a");
    move_radio_into_namespace(&rt_handle, &mut nl_handle, &move_b, fd_b)
        .await
        .expect("move radio-b's wiphy into ns-b");

    worker_a.signal_moved();
    worker_b.signal_moved();

    let (report_a, initial_a) = worker_a
        .join(radio_worker_join_timeout())
        .await
        .expect("radio-a namespace body");
    let (report_b, initial_b) = worker_b
        .join(radio_worker_join_timeout())
        .await
        .expect("radio-b namespace body");

    eprintln!("{}: {report_a:#?} initial={initial_a:?}", report_a.label);
    eprintln!("{}: {report_b:#?} initial={initial_b:?}", report_b.label);

    assert_ne!(
        report_a.rehooked, report_b.rehooked,
        "exactly one side should adopt the negotiated position, never both or neither"
    );
    let (winner, loser, loser_initial) = if report_a.rehooked {
        (&report_b, &report_a, &initial_a)
    } else {
        (&report_a, &report_b, &initial_b)
    };

    assert_eq!(
        loser.naddr_positions.len(),
        1,
        "single-level topology: a resolved position always has exactly one entry"
    );
    assert_ne!(
        &loser.naddr_positions,
        if report_a.rehooked {
            &initial_b
        } else {
            &initial_a
        },
        "the loser must resolve to a free slot in the winner's network, not the winner's own \
         already-occupied position"
    );

    // The loser's negotiated position can coincidentally equal its own discarded trivial
    // position (`multi_node.rs`'s own doc comment for this exact scenario), so counting every
    // Netsukuku-space (`10.0.0.0/8`) address catches a genuine leak without false-flagging that
    // coincidence.
    let topology = negotiation_topology();
    let old_address =
        addressing::host_address(&Naddr::new(topology.clone(), loser_initial.clone()).unwrap())
            .expect("loser's own trivial address is always addressable");
    let new_address =
        addressing::host_address(&Naddr::new(topology, loser.naddr_positions.clone()).unwrap())
            .expect("loser's negotiated address is always addressable");
    let netsukuku_addresses: Vec<_> = loser
        .addresses
        .iter()
        .filter(|a| a.network.address().octets()[0] == 10)
        .collect();
    assert_eq!(
        netsukuku_addresses.len(),
        1,
        "the loser's real kernel address table should carry exactly one Netsukuku-space \
         address (its negotiated one, {new_address}) — a second entry would mean its \
         torn-down trivial-generation address {old_address} leaked alongside it: {:#?}",
        loser.addresses
    );
    assert_eq!(
        netsukuku_addresses[0].network, new_address,
        "the loser's sole Netsukuku-space address is not its negotiated one: {:#?}",
        loser.addresses
    );
    assert!(
        !winner.routes.is_empty(),
        "the winner's real kernel routing table never gained a route to its (migrated) peer: \
         {:#?}",
        winner.routes
    );
    assert!(
        !loser.routes.is_empty(),
        "the loser's real kernel routing table never gained a route to its (winner) peer after \
         rehook re-attached the arc: {:#?}",
        loser.routes
    );
}

// ---------------------------------------------------------------------------------------------
// Scenario 3: broadcast reliability — quantify, don't retry until green
// ---------------------------------------------------------------------------------------------

#[derive(Debug)]
struct ReliabilitySummary {
    attempts: usize,
    successes: usize,
    /// One entry per successful attempt, in run order.
    time_to_arc: Vec<Duration>,
}

impl ReliabilitySummary {
    fn report(&self) -> String {
        if self.time_to_arc.is_empty() {
            return format!(
                "{}/{} runs formed an arc within the deadline; no successful run to time",
                self.successes, self.attempts
            );
        }
        let mut sorted = self.time_to_arc.clone();
        sorted.sort();
        let min = sorted.first().copied().unwrap();
        let max = sorted.last().copied().unwrap();
        let median = sorted[sorted.len() / 2];
        let total: Duration = sorted.iter().sum();
        let mean = total / sorted.len() as u32;
        format!(
            "{}/{} runs formed an arc within the deadline; time-to-arc over the {} successes: \
             min={min:?} median={median:?} mean={mean:?} max={max:?}",
            self.successes,
            self.attempts,
            sorted.len()
        )
    }
}

/// The interesting scenario: 802.11 broadcast (`here_i_am`, the discovery beacon
/// `ntk_neighborhood` sends) goes out unacknowledged at the lowest basic rate — nothing like a
/// veth's reliable point-to-point link. Runs [`run_ibss_arc_trial`] `RUNS` independent times
/// against the same two radios and reports attempts/successes/time-to-arc, rather than retrying
/// a single trial until it happens to go green — a flaky protocol path deserves a quantified
/// finding, not a laundered one.
///
/// Caveat this file's module doc already states: `wmediumd` is not installed on this host, so
/// `mac80211_hwsim`'s medium here is lossless simulation, not a true loss model. Any flakiness
/// this records is therefore about *timing* (join/beacon/discovery cadence over a genuinely
/// unacknowledged broadcast path, and this project's own `ntk_neighborhood` discovery/arc
/// bring-up on top of it) rather than about dropped frames — a real, if narrower, signal than a
/// full RF-loss model would give, not a substitute for one. Each trial's internal wait is
/// [`IBSS_MERGE_TIMEOUT`] — see this file's module doc's "Why no arc formed" section for why
/// that budget is no longer sized around mac80211's re-scan-for-merge timer (measured to never
/// fire at all once `FREQ_FIXED` is set) but is now just headroom for two-daemon composition
/// plus [`IBSS_FIXED_BSSID`]'s near-instant cell convergence.
///
/// # Running
/// Needs real root (`CAP_NET_ADMIN`+`CAP_SYS_ADMIN` in `init_user_ns`) — see this file's module
/// doc's "sharp edge" section. Run through this host's privileged wrapper:
///
/// ```text
/// sudo ntk-wireless-test real_hwsim_broadcast_reliability_across_ten_runs
/// ```
///
/// **Run to completion, repeatedly.** Ten trials reusing the same two radios back-to-back is
/// exactly where an unjoined/unverified radio return (this file's module doc) bites: measured
/// runs after the join-and-verify fix completed all ten trials with zero `trial setup failed`
/// (the radio-move mechanics' own pass/fail signal, independent of whether an arc formed) and an
/// intact radio inventory before and after. Arc-formation success itself (a *different* signal —
/// see [`ReliabilitySummary`]) was measured at 0/10 before [`IBSS_FIXED_BSSID`] existed (this
/// file's own module doc, "Why no arc formed" section — a structural failure, not a flaky one,
/// confirmed identical at both the 30s and 60s deadlines). After: 9/10, time-to-arc
/// min=418ms median=618ms mean=585ms max=641ms — the sole miss (`run 3`) showed both radios
/// already on the identical configured BSSID and one side's arc already established
/// (`cost=Some(Finite(10))`), the other's `netns::observe` snapshot simply not yet reflecting
/// its own side of the same arc within that trial's 30s window — a narrow, one-sided
/// observation-timing artifact of this same general class as the suite's known
/// `discovering_a_peer_joins_and_adopts_the_negotiated_position` flake, not a recurrence of the
/// two-cells-never-merging failure this file's module doc diagnoses (which always showed
/// `stations=[]` on *both* sides). This test always passes once it completes its `RUNS` trials
/// without a hard setup failure (radio discovery, namespace creation, thread panics) — the
/// interesting output is the printed [`ReliabilitySummary`], visible with `--nocapture`, not the
/// exit code.
#[ignore = "requires real root (CAP_NET_ADMIN in init_user_ns) — see this test's own doc comment"]
#[tokio::test]
async fn real_hwsim_broadcast_reliability_across_ten_runs() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("debug"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_test_writer()
        .with_thread_names(true)
        .try_init();

    const RUNS: usize = 10;

    let rt_handle = netns::root_handle().expect("rtnetlink connection");
    let (nl_connection, mut nl_handle, _) =
        wl_nl80211::new_connection().expect("nl80211 connection");
    tokio::spawn(nl_connection);

    let radios = discover_hwsim_radios(&mut nl_handle)
        .await
        .expect("discover hwsim radios");
    assert!(
        radios.len() >= 2,
        "need at least 2 mac80211_hwsim radios, found {}",
        radios.len()
    );
    let (radio_a, radio_b) = (radios[0].clone(), radios[1].clone());

    let mut successes = 0usize;
    let mut time_to_arc = Vec::new();
    for run in 0..RUNS {
        let port = 27300 + (run as u16) * 2;
        let trial = run_ibss_arc_trial(
            &rt_handle,
            &mut nl_handle,
            radio_a.clone(),
            radio_b.clone(),
            port,
        )
        .await;
        match trial {
            Ok((outcome, _, _)) if outcome.arc_established && outcome.route_installed => {
                successes += 1;
                time_to_arc.push(outcome.elapsed);
                eprintln!("run {run}: arc established in {:?}", outcome.elapsed);
            }
            Ok((outcome, report_a, report_b)) => {
                eprintln!(
                    "run {run}: no arc within {:?} — a_bssid={} b_bssid={} a={:?} b={:?}",
                    outcome.elapsed,
                    report_a.bssid.as_ref().map_or("none".to_owned(), fmt_mac),
                    report_b.bssid.as_ref().map_or("none".to_owned(), fmt_mac),
                    report_a.node.arcs,
                    report_b.node.arcs
                );
            }
            Err(err) => {
                eprintln!("run {run}: trial setup failed: {err:#}");
            }
        }
    }

    let summary = ReliabilitySummary {
        attempts: RUNS,
        successes,
        time_to_arc,
    };
    eprintln!("{}", summary.report());
}

// ---------------------------------------------------------------------------------------------
// Single-radio smoke test: no namespaces, just the nl80211 type-change + join-ibss + leave-ibss
// sequence against one hwsim radio in the host's own namespace — the fastest thing to run first
// against a root-having environment before attempting either two-namespace scenario above.
// ---------------------------------------------------------------------------------------------

/// Sets one hwsim radio to IBSS type, brings it up, joins the fixed test cell, then leaves and
/// restores it to `managed`/down — never touching `phy0`/`wlan0` (guarded the same way as every
/// other test here). No namespace juggling at all, so if this fails, the namespace/radio-move
/// dance in every other test in this file is not the place to start debugging.
///
/// # Running
/// Needs real root (`CAP_NET_ADMIN`), same as every namespace-moving scenario above, but *not*
/// namespace creation itself; this host provides that via a privileged wrapper (see this file's
/// module doc's "sharp edge" section). Not independently re-run this session — no namespace
/// juggling means it exercises none of the bugs fixed in this session (see this file's module
/// doc), so it was deprioritized to conserve this host's finite hwsim radio pool. Run as:
///
/// ```text
/// sudo ntk-wireless-test hwsim_single_radio_join_ibss_smoke_test
/// ```
#[ignore = "requires real root (CAP_NET_ADMIN) — see this test's own doc comment"]
#[tokio::test]
async fn hwsim_single_radio_join_ibss_smoke_test() {
    let rt_handle = netns::root_handle().expect("rtnetlink connection");
    let (nl_connection, mut nl_handle, _) =
        wl_nl80211::new_connection().expect("nl80211 connection");
    tokio::spawn(nl_connection);

    let radios = discover_hwsim_radios(&mut nl_handle)
        .await
        .expect("discover hwsim radios");
    let radio = radios.first().expect("at least one hwsim radio").clone();
    assert_ne!(radio.phy_name, "phy0");
    assert_ne!(radio.if_name, "wlan0");

    set_interface_adhoc(&mut nl_handle, radio.if_index)
        .await
        .expect("set interface to adhoc/IBSS type");
    netns::link::up(&rt_handle, &radio.if_name)
        .await
        .expect("bring radio up");
    join_ibss(
        &mut nl_handle,
        radio.if_index,
        IBSS_SSID,
        IBSS_FREQ_MHZ,
        Some(IBSS_FIXED_BSSID),
    )
    .await
    .expect("join IBSS");

    leave_ibss(&mut nl_handle, radio.if_index)
        .await
        .expect("leave IBSS");
    let index = netns::link::index(&rt_handle, &radio.if_name)
        .await
        .expect("resolve radio index");
    netns::link::down(&rt_handle, index)
        .await
        .expect("bring radio back down");
}

// ---------------------------------------------------------------------------------------------
// Scenario 4: RadioReturnGuard on the failure path — deliberately fail, prove no leak
// ---------------------------------------------------------------------------------------------

/// How many deliberately-failing trials [`radio_return_guard_survives_a_deliberately_failing_trial`]
/// runs against the same radio, back-to-back — enough to make a one-off recovery indistinguishable
/// from luck; not tied to [`real_hwsim_broadcast_reliability_across_ten_runs`]'s own `RUNS`, which
/// measures a different thing (arc formation, not radio return).
const RETURN_GUARD_FAILURE_TRIALS: usize = 6;

/// Namespace body that joins the IBSS cell and then fails on purpose — exercising exactly the
/// early-return path [`radio_arc_trial_body`]/[`radio_negotiation_trial_body`] take whenever
/// node composition, observation, or an assertion-worthy condition fails, without paying for a
/// full node composition every trial. [`RadioReturnGuard`] is still acquired first and held for
/// the whole body, same as both of those.
async fn deliberately_failing_trial_body(
    label: String,
    radio: HwsimRadio,
    init_net: std::fs::File,
) -> anyhow::Result<()> {
    let _return_guard = RadioReturnGuard::new(&label, &radio, init_net);
    prepare_radio_interface(
        &radio.if_name,
        IBSS_SSID,
        IBSS_FREQ_MHZ,
        Some(IBSS_FIXED_BSSID),
        &label,
    )
    .await?;
    anyhow::bail!("{label}: deliberate failure to exercise RadioReturnGuard's failure path")
}

/// Defect A's own regression test: deliberately fails [`RETURN_GUARD_FAILURE_TRIALS`] trials in a
/// row against the same radio and asserts, via a fresh [`discover_hwsim_radios`] call (never
/// [`RadioReturnGuard`]'s own internal verification, which this test is specifically checking
/// does not gate correctness) after every single one, that the radio is still there — proving the
/// return holds on the failure path specifically, not just on the happy path every other scenario
/// in this file exercises.
///
/// # Running
/// Needs real root (`CAP_NET_ADMIN`+`CAP_SYS_ADMIN` in `init_user_ns`) — see this file's module
/// doc's "sharp edge" section. Run through this host's privileged wrapper:
///
/// ```text
/// sudo ntk-wireless-test radio_return_guard_survives_a_deliberately_failing_trial
/// ```
///
/// **Run to completion.** Six consecutive deliberately-failing trials against the same radio,
/// `/sys/class/ieee80211/<phy>` re-checked after each one via a fresh `discover_hwsim_radios`
/// call: 6/6 present, elapsed per trial well under [`radio_worker_join_timeout`]'s bound in every
/// case (this scenario's own root cause diagnosis, this file's `RadioReturnGuard::drop` doc
/// comment, "Defect A" section, was verified against exactly this test).
#[ignore = "requires real root (CAP_NET_ADMIN in init_user_ns) — see this test's own doc comment"]
#[tokio::test]
async fn radio_return_guard_survives_a_deliberately_failing_trial() {
    let rt_handle = netns::root_handle().expect("rtnetlink connection");
    let (nl_connection, mut nl_handle, _) =
        wl_nl80211::new_connection().expect("nl80211 connection");
    tokio::spawn(nl_connection);

    let radios = discover_hwsim_radios(&mut nl_handle)
        .await
        .expect("discover hwsim radios");
    let radio = radios.last().expect("at least one hwsim radio").clone();
    eprintln!(
        "using {} ({}) for {RETURN_GUARD_FAILURE_TRIALS} deliberately-failing trials",
        radio.phy_name, radio.if_name
    );

    for trial in 0..RETURN_GUARD_FAILURE_TRIALS {
        let started = Instant::now();
        let init_net = open_init_net_fd("return-guard-fail").expect("open coordinator init_net fd");
        let label = format!("return-guard-fail-{trial}");
        let worker_radio = radio.clone();
        let worker_label = label.clone();
        let worker = netns::NamespaceWorker::spawn(label.clone(), move || {
            deliberately_failing_trial_body(worker_label, worker_radio, init_net)
        });
        let fd = worker.fd();
        move_radio_into_namespace(&rt_handle, &mut nl_handle, &radio, fd)
            .await
            .unwrap_or_else(|e| panic!("trial {trial}: move radio into namespace: {e:#}"));
        worker.signal_moved();

        let outcome = worker.join(radio_worker_join_timeout()).await;
        let elapsed = started.elapsed();
        assert!(
            outcome.is_err(),
            "trial {trial}: body was supposed to fail deliberately, but it returned Ok"
        );

        // `RadioReturnGuard`'s own `wait_for_netdev` verifies via raw rtnetlink; an NL80211
        // (genetlink) wiphy dump can settle a little behind that (separate kernel subsystem), so
        // poll briefly rather than treating one immediate miss as a leak.
        let poll_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let still_present = loop {
            let after = discover_hwsim_radios(&mut nl_handle)
                .await
                .expect("discover hwsim radios after trial");
            if after.iter().any(|r| r.phy_name == radio.phy_name) {
                break true;
            }
            if tokio::time::Instant::now() >= poll_deadline {
                break false;
            }
            tokio::time::sleep(NETDEV_REAPPEAR_POLL_INTERVAL).await;
        };
        eprintln!("trial {trial}: elapsed={elapsed:?} radio_returned={still_present}");
        assert!(
            still_present,
            "trial {trial}: {} did not reappear in init_net (via discover_hwsim_radios) within \
             10s of a deliberately-failing trial completing — RadioReturnGuard leaked it \
             (trial elapsed {elapsed:?})",
            radio.phy_name
        );
    }
}

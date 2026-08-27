//! ANDNA: the distributed hostname service, built as two `PeerService` registrations on
//! `ntk-peerservices` (RFC 0014 §2: "a generic P2P service framework... ANDNA [is] two
//! instances of that service") rather than the bespoke hash-gnode/counter-gnode routing upstream
//! hand-rolled before PeerServices existed.
//!
//! **Upstream, genuinely absent**: `research/impl/vala/andna/andna.vala` is a 13-line stub and
//! `andna/serializables.vala` is 0 bytes (`research/notes/02-vala-services-daemon.md` §4) — this
//! crate is a reconstruction, not a port. Sources, in priority order: RFC 0014 (the generic DHT
//! layer ANDNA is *two instances of*, `research/papers/2007-lopumo-ntk-rfc-0014-p2p-over-netsukuku.pdf`),
//! the ANDNA design papers (`research/papers/2007-lopumo-andna-hostname-management.pdf`,
//! `2009-lopumo-andna-website-doc.pdf`), NTK_RFC 0009 (SNSD, `research/specs/vala-doc--rfc-Ntk_SNSD`)
//! and NTK_RFC 0007 (the counter-node public-key bypass fix,
//! `research/specs/vala-doc--rfc-Ntk_andna_counter_pubk`), and the only real implementation,
//! `research/impl/c/netsukuku/src/andna*.c`/`snsd_cache.c`. Every deviation from that C
//! implementation is called out at its point of use — see [`record`], [`counter`], [`config`],
//! and [`snsd`]'s module doc comments.
//!
//! - [`hostname`] — [`hostname::Hostname`] (validated, case-folded) and its `blake3` DHT hash.
//! - [`snsd`] — NTK_RFC 0009: [`snsd::SnsdRecord`]/[`snsd::SnsdTable`], the service/priority/
//!   weight record set under one hostname.
//! - [`record`] — [`record::RegisterRequest`] (ed25519-signed registration/renewal) and
//!   [`record::Cache`], the Andna service's collision/replay/TTL policy.
//! - [`counter`] — [`counter::CounterCache`], the Counter service's per-registrant 256-hostname
//!   cap (NTK_RFC 0007).
//! - [`route`] — `blake3`-based DHT route-key derivation for both services.
//! - [`config`] — [`config::Config`], every timing/capacity constant, injectable.
//! - [`substrate`] — [`substrate::AndnaSubstrate`], the outbound seam onto `ntk-peerservices`,
//!   plus an in-memory [`substrate::FakeSubstrate`].
//! - [`service`] — [`service::AndnaService`]/[`service::CounterService`], the two registered
//!   `PeerService`s (this crate's inbound path — see that module's doc comment for why there is
//!   no separate `ntk_rpc::RpcHandler`).
//! - [`actor`] — the single-owner [`actor::Manager`] and its [`actor::Handle`]
//!   (register/resolve/renew, `watch` snapshot, `broadcast` events).
//!
//! **SNSD scope**: implemented in full from NTK_RFC 0009's own text — service/priority/weight
//! semantics, the 16-per-service/256-total caps, the immutable-by-default zero record, and
//! weight-based selection. The RFC's *optional* pubkey liveness-challenge extension is
//! deliberately left out (see [`snsd`]'s module doc comment for why that is a documented scope
//! decision, not a stub).

mod actor;
mod config;
mod counter;
mod error;
mod hostname;
mod record;
mod route;
mod service;
mod snsd;
mod substrate;
mod wire;

/// Generated protobuf types for this module's own payloads (`proto/andna.proto`, package
/// `ntk.andna.v1`). These travel inside `ntk_proto::v1::TypedValue` payloads over the
/// `PeerService::exec` request path (see `proto/andna.proto`'s and [`service`]'s doc comments);
/// nothing outside [`wire`] constructs them directly.
#[allow(clippy::doc_markdown)]
pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/ntk.andna.v1.rs"));
}

pub use actor::{AndnaError, Event, Handle, Manager, Snapshot, run_expiry_reclaimer};
pub use config::Config;
pub use counter::CounterRejected;
pub use error::Error;
pub use hostname::{Hostname, HostnameHash};
pub use record::{Cache, HostedRecord, RegisterOutcome, RegisterRejected, RegisterRequest};
pub use route::counter_route_key;
pub use service::{AndnaService, CounterService, andna_service_id, counter_service_id};
pub use snsd::{
    MAX_WEIGHT, SnsdRecord, SnsdTable, SnsdTarget, ZERO_DEFAULT_PRIORITY, ZERO_DEFAULT_WEIGHT,
    ZERO_SERVICE,
};
pub use substrate::{AndnaSubstrate, FakeSubstrate};

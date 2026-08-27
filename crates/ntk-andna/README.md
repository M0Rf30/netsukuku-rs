# ntk-andna

ANDNA: the distributed hostname system for Netsukuku, built as two `PeerService` registrations
on `ntk-peerservices` rather than bespoke DHT routing.

## Where this sits

Part of the [netsukuku-rs](https://github.com/M0Rf30/netsukuku-rs) workspace. This crate depends
on `ntk-common`, `ntk-proto`, `ntk-rpc`, and `ntk-peerservices` — the DHT-over-hierarchy substrate
this crate's two services run on. It is consumed only by `ntkd`, the composition root; ANDNA has
no `MethodCall` RPC arms of its own (see below), so nothing else in the workspace depends on it.

## Provenance: this is a reconstruction, not a port

Worth being explicit about, because it's unusual for this workspace: upstream's Vala rewrite
(`research/impl/vala/andna/andna.vala`) is a 13-line stub, and its `serializables.vala` is empty.
There is no complete reference implementation to port from. This crate is instead reconstructed
from NTK_RFC 0014 (the generic DHT layer, describing ANDNA as "two instances of that service"),
NTK_RFC 0009 (SNSD — the per-hostname service/priority/weight record set), NTK_RFC 0007 (the
Counter-node public-key fix), and the older C daemon's `andna*.c`/`snsd_cache.c`, which is the
only implementation that actually runs end to end.

## What it provides

- [`Hostname`] — a validated, case-folded name and its `blake3` DHT route key.
- [`SnsdRecord`]/[`SnsdTable`] (NTK_RFC 0009) — the service/priority/weight record set a
  hostname can carry beyond its primary (service-0) address.
- [`RegisterRequest`] — a signed registration or renewal. Ownership of a hostname is pinned to
  an ed25519 key on first claim: whoever registers a name first owns it, and every subsequent
  registration or renewal must be signed by that same key. A `sequence` field, required to
  strictly increase on every accepted request, closes the replay hole the C implementation left
  open (its own check only rejects a *lower* sequence, so replaying the most recent valid
  request verbatim used to pass).
- [`Cache`] — the hash-node's collision/replay/TTL acceptance policy for `RegisterRequest`.
- [`AndnaService`]/[`CounterService`] — the two `PeerService`s actually registered on
  `ntk-peerservices`: `Andna` holds the hostname→record mapping, `Counter` caps how many
  hostnames one owner key can hold (NTK_RFC 0007). Because both run as generic PeerServices,
  ANDNA has no bespoke wire methods of its own — every request arrives as an opaque payload
  already routed by the substrate, dispatched inside `exec`.
- [`Handle::register`]/[`Handle::resolve`]/[`Handle::renew`] — this crate's own actor API for
  registering, looking up, and renewing a hostname.

Authentication of the *registrant* (ed25519 ownership) is always on; this is separate from
`ntk-proto`'s node-to-node `auth` module, whose `require_auth` defaults to `false`.

## Usage sketch

```rust,ignore
use ntk_andna::{Hostname, RegisterRequest};
use ed25519_dalek::SigningKey;

let signing_key = SigningKey::generate(&mut rand::rngs::OsRng);
let hostname = Hostname::new("myhost")?;
let req = RegisterRequest::sign(
    &signing_key,
    hostname,
    owner_naddr,      // ntk_common::Naddr this name should resolve to
    sequence,          // strictly increasing per owner key
    timestamp_unix,
    zero_priority,
    zero_weight,
    vec![],            // additional SNSD records, if any
)?;

// `handle: ntk_andna::Handle` comes from `Manager::new`, wired by `ntkd`.
let outcome = handle.register(req).await?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

## License

GPL-3.0-or-later. Part of [netsukuku-rs](https://github.com/M0Rf30/netsukuku-rs).

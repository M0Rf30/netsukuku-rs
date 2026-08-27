# ntk-proto

The wire schema and codec for the [`netsukuku-rs`](https://github.com/M0Rf30/netsukuku-rs)
inter-node RPC protocol. This is a foundation crate: it depends only on `ntk-common` and is
itself depended on by `ntk-rpc` (the transport) and every protocol-module crate — `ntk-qspn`,
`ntk-hooking`, `ntk-coordinator`, `ntk-peerservices`, `ntk-andna` — which import
`proto/domain.proto`'s shared types into their own module-specific `.proto` files.

## What it provides

- **`v1`** — `prost`-generated types compiled from `proto/ntk.proto`, the 39-method RPC surface
  (`Envelope`, `Request`/`Response`, `MethodCall`, `TypedValue`, and every method's argument
  message) grouped by owning module: neighborhood (5 methods), identity (3), QSPN (4), peers
  (12), coordinator (5), hooking (10).
- **`domain`** — `prost`-generated types compiled from `proto/domain.proto` (the wire form of
  `ntk-common`'s `Topology`, `Naddr`, `HCoord`, `Cost`, `Fingerprint`), plus the `From`/`TryFrom`
  conversions between them and their `ntk-common` counterparts. Encoding (domain -> wire) is
  infallible; decoding (wire -> domain) **revalidates** every value through `ntk-common`'s own
  constructors — a decoded value can never bypass the invariants those types enforce, since it
  may have come from an untrusted peer.
- **`auth`** — sender authentication for an `Envelope`: `sign`/`verify` over a domain-separated,
  length-framed canonical encoding of `(method, payload, sequence)`, built on ed25519 signatures
  over a BLAKE3 digest, plus `SequenceGuard` for replay rejection. This is a callable primitive,
  not something wired into every decode — `Envelope::auth` is optional, and *whether* to require
  it is a deployment policy decided by the layer above (`ntk-rpc`'s per-arc hop auth,
  `ntk-peerservices`/`ntk-coordinator`'s per-origin-request auth). **Authentication is not on by
  default**: nothing in this crate requires `auth` to be present.
- **`envelope`** — hand-written glue codegen cannot produce: `Envelope::request`/`broadcast`
  constructors and `ProtocolVersion::is_compatible_with` (a `major`-only compatibility check;
  proto3 field numbering makes additive `minor` changes safely decodable either direction).

## No system `protoc` required

Unlike most `prost`-based crates, code generation here uses [`protox`](https://docs.rs/protox), a
pure-Rust protobuf parser, instead of shelling out to a system `protoc` binary. Building this
crate — or anything depending on it — needs nothing beyond a normal `cargo build`.

`ntk-proto` also declares `links = "ntk_proto"`, so cargo propagates its `proto/` directory's
location to any directly-dependent crate's build script as the `DEP_NTK_PROTO_PROTO_INCLUDE`
environment variable. A sibling module crate compiling its own `.proto` file that
`import "domain.proto"` reads that variable instead of hardcoding a relative `../ntk-proto/proto`
path, which would break once the crate is packaged standalone for crates.io.

## Example

```rust
use ntk_common::Topology;
use ntk_proto::domain::v1;

fn main() -> Result<(), ntk_proto::domain::DomainDecodeError> {
    // Domain codec: encode a validated ntk-common value to its wire form and back.
    // Decoding revalidates through Topology's own constructor.
    let topology = Topology::new([16, 16, 256]).expect("valid topology");
    let wire: v1::Topology = (&topology).into();
    let round_tripped: Topology = (&wire).try_into()?;
    assert_eq!(topology, round_tripped);
    Ok(())
}
```

```rust
use ed25519_dalek::SigningKey;
use ntk_proto::auth::{sign, verify};

fn main() -> Result<(), ntk_proto::auth::AuthError> {
    // Auth: sign a (method, payload) pair under a keypair, then verify it.
    let signing_key = SigningKey::from_bytes(&[7u8; 32]);
    let auth = sign(&signing_key, /* sequence */ 1, "qspn.get_full_etp", b"payload");
    let verifying_key = verify(&auth, "qspn.get_full_etp", b"payload")?;
    assert_eq!(verifying_key, signing_key.verifying_key());
    Ok(())
}
```

`sign`/`verify` operate on `ed25519_dalek` types directly, so a caller adds `ed25519-dalek` as
its own dependency to construct a `SigningKey`.

## License

GPL-3.0-or-later. Part of the [netsukuku-rs](https://github.com/M0Rf30/netsukuku-rs) workspace.

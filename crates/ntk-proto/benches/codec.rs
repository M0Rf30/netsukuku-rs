//! Wire-codec benchmarks for `ntk-proto`.
//!
//! Four groups:
//!
//! - `domain_codec`: `From`/`TryFrom` conversions between `ntk-common`'s
//!   validated domain types and their `proto/domain.proto` wire form, for the
//!   three types the audit flagged as per-message allocators —
//!   [`ntk_proto::domain`] documents why `TryFrom` must revalidate rather
//!   than trust the wire: `Topology` (`gsizes().to_vec()`), `Naddr`
//!   (`positions().to_vec()` plus a nested `Topology`), and `Fingerprint`
//!   (`id.clone()` + `pending_elderships.clone()`). Level-dependent types are
//!   parameterized over 4 and 16 hierarchy levels — the realistic band this
//!   deployment targets — so the O(levels) allocation cost shows as a slope,
//!   not a single point.
//! - `envelope_roundtrip`: a full `Envelope` `prost` encode-to-bytes then
//!   decode-back for a small `Request(MethodCall::NeighborhoodHereIAm)` —
//!   the shape every RPC call takes on the wire today. **Pre-authentication
//!   baseline**, measured on the reference machine: encode 139.89 ns,
//!   decode 350.56 ns, `encode_decode_roundtrip` 503.11 ns.
//! - `auth`: [`ntk_proto::auth`]'s real marginal cost, measured against that
//!   same baseline: `sign` **11.31 µs**, `verify` **26.78 µs**,
//!   `authenticated_envelope_roundtrip` (decode + re-derive the signed
//!   payload + verify, the shape a receiver enforcing auth actually
//!   performs) **27.60 µs**. `verify` alone is **~53x** the pre-auth
//!   `encode_decode_roundtrip` baseline (26.78 µs / 503.11 ns) on this
//!   machine — smaller than the ~250x back-of-envelope figure this crate's
//!   design assumed (a general "ed25519 verify is ~100-150 µs" estimate),
//!   because this host's `curve25519-dalek` backend is faster than that
//!   estimate assumed; the conclusion the estimate was making is unchanged:
//!   authenticating one message costs one to two orders of magnitude more
//!   than the rest of the codec path combined. That is exactly why `sign`/
//!   `verify` are exposed as *callable primitives*, not wired into
//!   `Envelope` decode — callers invoke them per-arc (hop auth, amortized
//!   over a link's lifetime) or per-origin-request, never per relayed
//!   message.

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ed25519_dalek::SigningKey;
use ntk_common::{Fingerprint, FingerprintParts, Naddr, Topology};
use ntk_proto::auth;
use ntk_proto::domain;
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::{
    CallerContext, Envelope, MethodCall, NeighborhoodHereIAmArgs, ProtocolVersion, TypedValue,
};
use prost::Message;
use std::hint::black_box;

/// The realistic g-node hierarchy depths this deployment targets: shallow
/// (4) and deep (16) — see the module doc comment.
const LEVELS: [usize; 2] = [4, 16];

/// A topology with `levels` levels, g-node size 256 at every level (the
/// realistic per-level gsize ceiling stated in the assignment).
fn topology(levels: usize) -> Topology {
    Topology::new(std::iter::repeat_n(256u32, levels)).expect("valid topology")
}

fn naddr(levels: usize) -> Naddr {
    let topo = topology(levels);
    let pos = (0..levels).map(|i| (i as u32) % 256);
    Naddr::new(topo, pos).expect("valid naddr")
}

/// A fingerprint aggregated up to `levels`, with a full `elderships_seed`
/// trail — the shape a fingerprint has after climbing every level, so the
/// benchmark exercises the largest `pending_elderships`/`elderships_seed`
/// clone this type ever performs.
fn fingerprint(levels: usize) -> Fingerprint<Vec<u8>> {
    Fingerprint::from_parts(FingerprintParts {
        id: vec![0xABu8; 32],
        level: levels,
        eldership: Some(1),
        pending_elderships: vec![7u32; 3],
        elderships_seed: vec![Some(1u32); levels],
    })
    .expect("valid fingerprint")
}

fn bench_domain_codec(c: &mut Criterion) {
    let mut group = c.benchmark_group("domain_codec");

    for &levels in &LEVELS {
        let topo = topology(levels);
        let wire_topo = domain::v1::Topology::from(&topo);
        group.bench_with_input(
            BenchmarkId::new("topology_encode", levels),
            &topo,
            |b, topo| b.iter(|| black_box(domain::v1::Topology::from(black_box(topo)))),
        );
        group.bench_with_input(
            BenchmarkId::new("topology_decode", levels),
            &wire_topo,
            |b, wire| b.iter(|| black_box(Topology::try_from(black_box(wire))).unwrap()),
        );

        let addr = naddr(levels);
        let wire_addr = domain::v1::Naddr::from(&addr);
        group.bench_with_input(
            BenchmarkId::new("naddr_encode", levels),
            &addr,
            |b, addr| b.iter(|| black_box(domain::v1::Naddr::from(black_box(addr)))),
        );
        group.bench_with_input(
            BenchmarkId::new("naddr_decode", levels),
            &wire_addr,
            |b, wire| b.iter(|| black_box(Naddr::try_from(black_box(wire))).unwrap()),
        );

        let fp = fingerprint(levels);
        let wire_fp = domain::v1::Fingerprint::from(&fp);
        group.bench_with_input(
            BenchmarkId::new("fingerprint_encode", levels),
            &fp,
            |b, fp| b.iter(|| black_box(domain::v1::Fingerprint::from(black_box(fp)))),
        );
        group.bench_with_input(
            BenchmarkId::new("fingerprint_decode", levels),
            &wire_fp,
            |b, wire| {
                b.iter(|| black_box(Fingerprint::<Vec<u8>>::try_from(black_box(wire))).unwrap())
            },
        );
    }

    group.finish();
}

/// A small `Request` envelope carrying `MethodCall::NeighborhoodHereIAm` —
/// representative of the RPC path's per-message shape (one identity
/// `TypedValue`, two short strings, a `CallerContext`).
fn sample_envelope() -> Envelope {
    let caller = CallerContext {
        source_id: Some(TypedValue::new("ntk_identities::IdentityId", vec![1, 2, 3])),
        src_nic: Some(TypedValue::new("ntk_neighborhood::SrcNic", vec![4, 5])),
    };
    let args = NeighborhoodHereIAmArgs {
        my_id: Some(TypedValue::new("ntk_neighborhood::NodeId", vec![9, 9])),
        my_mac: "aa:bb:cc:dd:ee:ff".to_owned(),
        my_nic_addr: "10.0.0.1".to_owned(),
    };
    Envelope::request(
        ProtocolVersion::CURRENT,
        42,
        caller,
        TypedValue::new("ntk_identities::IdentityId", vec![7]),
        true,
        MethodCall {
            call: Some(Call::NeighborhoodHereIAm(args)),
        },
    )
}

fn bench_envelope_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("envelope_roundtrip");
    let envelope = sample_envelope();
    let bytes = envelope.encode_to_vec();

    // Pre-authentication baseline: the number the future per-message
    // ed25519-signing change must be measured against.
    group.bench_function("encode", |b| {
        b.iter(|| black_box(black_box(&envelope).encode_to_vec()))
    });
    group.bench_function("decode", |b| {
        b.iter(|| black_box(Envelope::decode(black_box(bytes.as_slice()))).unwrap())
    });
    group.bench_function("encode_decode_roundtrip", |b| {
        b.iter(|| {
            let buf = black_box(&envelope).encode_to_vec();
            black_box(Envelope::decode(buf.as_slice())).unwrap()
        })
    });

    group.finish();
}
/// The real marginal cost of sender authentication, benchmarked against the
/// pre-authentication `envelope_roundtrip` baseline recorded in this
/// module's doc comment.
///
/// `payload` is the sample envelope's encoded `MethodCall` — a realistic
/// per-request signing/verification target, not the whole envelope (an
/// envelope's `version`/`caller`/`correlation_id` framing is transport
/// bookkeeping, not part of what a signature needs to bind).
fn bench_auth(c: &mut Criterion) {
    let mut group = c.benchmark_group("auth");

    let signing_key = SigningKey::from_bytes(&[7u8; 32]);
    let method = "ntk.rpc.v1.NeighborhoodManager/HereIAm";
    let envelope = sample_envelope();
    let payload = envelope
        .as_request()
        .and_then(|r| r.call.as_ref())
        .expect("sample envelope carries a MethodCall")
        .encode_to_vec();

    group.bench_function("sign", |b| {
        b.iter(|| {
            black_box(auth::sign(
                black_box(&signing_key),
                1,
                method,
                black_box(&payload),
            ))
        })
    });

    let signed = auth::sign(&signing_key, 1, method, &payload);
    group.bench_function("verify", |b| {
        b.iter(|| {
            black_box(auth::verify(
                black_box(&signed),
                method,
                black_box(&payload),
            ))
            .unwrap()
        })
    });

    // Full authenticated envelope round trip: attach `Auth` to the sample
    // envelope, encode, decode, re-derive the payload the same way a real
    // receiver would (from the decoded `MethodCall`), then verify — the
    // shape a receiver that opts into per-origin-request enforcement
    // actually performs.
    let mut authenticated = sample_envelope();
    authenticated.auth = Some(auth::sign(&signing_key, 1, method, &payload));
    let wire_bytes = authenticated.encode_to_vec();

    group.bench_function("authenticated_envelope_roundtrip", |b| {
        b.iter(|| {
            let decoded = black_box(Envelope::decode(black_box(wire_bytes.as_slice()))).unwrap();
            let decoded_payload = decoded
                .as_request()
                .and_then(|r| r.call.as_ref())
                .expect("carries a MethodCall")
                .encode_to_vec();
            let received_auth = decoded.auth.as_ref().expect("carries Auth");
            black_box(auth::verify(
                black_box(received_auth),
                method,
                black_box(&decoded_payload),
            ))
            .unwrap()
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_domain_codec,
    bench_envelope_roundtrip,
    bench_auth
);
criterion_main!(benches);

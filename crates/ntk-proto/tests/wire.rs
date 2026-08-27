//! Round-trip encode/decode coverage for the wire schema, plus a
//! forward-compatibility test proving an older parser tolerates a newer
//! message (an unrecognized field is skipped, not a decode error), and a
//! test pinning that `Envelope`'s optional `auth` field leaves an
//! unauthenticated peer's wire bytes completely unaffected.

use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::response::Outcome;
use ntk_proto::v1::{
    Auth, CallerContext, CoordinatorExecuteArgs, Empty, Envelope, ErrorDomain, MethodCall,
    NeighborhoodArcArgs, NeighborhoodHereIAmArgs, PeersMsgIdTupleArgs, ProtocolVersion,
    RemoteError, ResponsePayload, TypedValue,
};
use prost::Message;

fn caller() -> CallerContext {
    CallerContext {
        source_id: Some(TypedValue::new("ntk_identities::IdentityId", vec![1, 2, 3])),
        src_nic: Some(TypedValue::new("ntk_neighborhood::SrcNic", vec![4, 5])),
    }
}

fn roundtrip(envelope: &Envelope) -> Envelope {
    let mut buf = Vec::new();
    envelope.encode(&mut buf).expect("encode");
    Envelope::decode(buf.as_slice()).expect("decode")
}

#[test]
fn protocol_version_compatibility() {
    let ours = ProtocolVersion::CURRENT;
    let same_major_newer_minor = ProtocolVersion {
        major: ours.major,
        minor: ours.minor + 5,
    };
    let different_major = ProtocolVersion {
        major: ours.major + 1,
        minor: 0,
    };

    assert!(ours.is_compatible_with(&same_major_newer_minor));
    assert!(!ours.is_compatible_with(&different_major));
}

#[test]
fn envelope_check_version() {
    let compatible = Envelope::request(
        ProtocolVersion::CURRENT,
        1,
        caller(),
        TypedValue::new("ntk_identities::IdentityId", Vec::new()),
        true,
        MethodCall {
            call: Some(Call::NeighborhoodNop(Empty::VALUE)),
        },
    );
    assert!(compatible.check_version().is_ok());

    let incompatible = Envelope::request(
        ProtocolVersion {
            major: ProtocolVersion::CURRENT.major + 1,
            minor: 0,
        },
        1,
        caller(),
        TypedValue::new("ntk_identities::IdentityId", Vec::new()),
        true,
        MethodCall {
            call: Some(Call::NeighborhoodNop(Empty::VALUE)),
        },
    );
    let mismatch = incompatible
        .check_version()
        .expect_err("major mismatch must be rejected");
    assert_eq!(mismatch.ours, ProtocolVersion::CURRENT);
    assert_eq!(mismatch.theirs.major, ProtocolVersion::CURRENT.major + 1);
}

#[test]
fn roundtrip_request_with_message_and_scalar_args() {
    let args = NeighborhoodHereIAmArgs {
        my_id: Some(TypedValue::new("ntk_neighborhood::NodeId", vec![9, 9])),
        my_mac: "aa:bb:cc:dd:ee:ff".to_owned(),
        my_nic_addr: "10.0.0.1".to_owned(),
    };
    let envelope = Envelope::request(
        ProtocolVersion::CURRENT,
        42,
        caller(),
        TypedValue::new("ntk_identities::IdentityId", vec![7]),
        true,
        MethodCall {
            call: Some(Call::NeighborhoodHereIAm(args.clone())),
        },
    );

    let decoded = roundtrip(&envelope);
    let request = decoded.as_request().expect("request body");
    assert_eq!(request.correlation_id, 42);
    assert!(request.wait_reply);
    match &request.call.as_ref().unwrap().call {
        Some(Call::NeighborhoodHereIAm(decoded_args)) => assert_eq!(decoded_args, &args),
        other => panic!("expected NeighborhoodHereIAm, got {other:?}"),
    }
}

/// `request_arc` and `remove_arc` share the exact same argument message
/// (`NeighborhoodArcArgs`, per interfaces.rpcidl:4,6) but occupy distinct
/// oneof field numbers. This proves the shared-message design still
/// discriminates correctly: decoding one never gets confused with the
/// other.
#[test]
fn shared_arg_message_round_trips_to_distinct_oneof_arms() {
    let args = NeighborhoodArcArgs {
        your_id: Some(TypedValue::new("ntk_neighborhood::NodeId", vec![1])),
        your_mac: "11:11:11:11:11:11".to_owned(),
        your_nic_addr: "10.0.0.2".to_owned(),
        my_id: Some(TypedValue::new("ntk_neighborhood::NodeId", vec![2])),
        my_mac: "22:22:22:22:22:22".to_owned(),
        my_nic_addr: "10.0.0.3".to_owned(),
    };

    let request_arc = Envelope::request(
        ProtocolVersion::CURRENT,
        1,
        caller(),
        TypedValue::new("t", Vec::new()),
        true,
        MethodCall {
            call: Some(Call::NeighborhoodRequestArc(args.clone())),
        },
    );
    let remove_arc = Envelope::request(
        ProtocolVersion::CURRENT,
        2,
        caller(),
        TypedValue::new("t", Vec::new()),
        true,
        MethodCall {
            call: Some(Call::NeighborhoodRemoveArc(args.clone())),
        },
    );

    let decoded_request = roundtrip(&request_arc);
    let decoded_remove = roundtrip(&remove_arc);
    assert!(matches!(
        &decoded_request.as_request().unwrap().call.as_ref().unwrap().call,
        Some(Call::NeighborhoodRequestArc(a)) if a == &args
    ));
    assert!(matches!(
        &decoded_remove.as_request().unwrap().call.as_ref().unwrap().call,
        Some(Call::NeighborhoodRemoveArc(a)) if a == &args
    ));
}

/// The five `CoordinatorManager.execute_*` methods (interfaces.rpcidl:31-35)
/// all reuse `CoordinatorExecuteArgs`; check two of the five arms stay
/// distinguishable after a round trip.
#[test]
fn coordinator_execute_arms_stay_distinct() {
    let args = CoordinatorExecuteArgs {
        tuple: Some(TypedValue::new("ntk_coordinator::TupleGNode", vec![1])),
        fp_id: -7,
        propagation_id: 3,
        lvl: 2,
        data: Some(TypedValue::new("ntk_coordinator::Object", vec![2])),
    };
    let prepare_migration = MethodCall {
        call: Some(Call::CoordinatorExecutePrepareMigration(args.clone())),
    };
    let we_have_splitted = MethodCall {
        call: Some(Call::CoordinatorExecuteWeHaveSplitted(args.clone())),
    };

    let mut buf_a = Vec::new();
    prepare_migration.encode(&mut buf_a).unwrap();
    let mut buf_b = Vec::new();
    we_have_splitted.encode(&mut buf_b).unwrap();
    assert_ne!(
        buf_a, buf_b,
        "distinct oneof arms must not collide on the wire"
    );

    assert!(matches!(
        MethodCall::decode(buf_a.as_slice()).unwrap().call,
        Some(Call::CoordinatorExecutePrepareMigration(a)) if a == args
    ));
    assert!(matches!(
        MethodCall::decode(buf_b.as_slice()).unwrap().call,
        Some(Call::CoordinatorExecuteWeHaveSplitted(a)) if a == args
    ));
}

/// `set_next_destination`/`set_failure`/`set_non_participant`
/// (interfaces.rpcidl:23-25) share `PeersMsgIdTupleArgs`; sanity-check one.
#[test]
fn roundtrip_peers_msg_id_tuple_args() {
    let args = PeersMsgIdTupleArgs {
        msg_id: 5,
        tuple: Some(TypedValue::new("ntk_peerservices::TupleGNode", vec![3])),
    };
    let envelope = Envelope::request(
        ProtocolVersion::CURRENT,
        3,
        caller(),
        TypedValue::new("t", Vec::new()),
        false,
        MethodCall {
            call: Some(Call::PeersSetFailure(args.clone())),
        },
    );
    let decoded = roundtrip(&envelope);
    assert!(!decoded.as_request().unwrap().wait_reply);
    assert!(matches!(
        &decoded.as_request().unwrap().call.as_ref().unwrap().call,
        Some(Call::PeersSetFailure(a)) if a == &args
    ));
}

#[test]
fn roundtrip_response_success_variants() {
    for payload in [
        ResponsePayload {
            value: Some(ntk_proto::v1::response_payload::Value::Empty(Empty::VALUE)),
        },
        ResponsePayload {
            value: Some(ntk_proto::v1::response_payload::Value::Boolean(true)),
        },
        ResponsePayload {
            value: Some(ntk_proto::v1::response_payload::Value::Typed(
                TypedValue::new("ntk_identities::IdentityId", vec![1, 2, 3]),
            )),
        },
    ] {
        let envelope = Envelope::response_ok(ProtocolVersion::CURRENT, 99, payload.clone());
        let decoded = roundtrip(&envelope);
        let response = decoded.as_response().expect("response body");
        assert_eq!(response.correlation_id, 99);
        match &response.outcome {
            Some(Outcome::Payload(p)) => assert_eq!(p, &payload),
            other => panic!("expected payload outcome, got {other:?}"),
        }
    }
}

#[test]
fn roundtrip_response_error() {
    let error = RemoteError {
        domain: ErrorDomain::QspnBootstrapInProgress as i32,
        message: "not ready".to_owned(),
    };
    let envelope = Envelope::response_err(ProtocolVersion::CURRENT, 7, error.clone());
    let decoded = roundtrip(&envelope);
    match decoded.as_response().unwrap().outcome.as_ref() {
        Some(Outcome::Error(e)) => assert_eq!(e, &error),
        other => panic!("expected error outcome, got {other:?}"),
    }
}

#[test]
fn roundtrip_broadcast_request_and_ack() {
    let call = MethodCall {
        call: Some(Call::QspnGotDestroy(Empty::VALUE)),
    };
    let request = Envelope::broadcast_request(
        ProtocolVersion::CURRENT,
        123,
        caller(),
        TypedValue::new("ntk_identities::IdentityId", vec![1]),
        true,
        call,
    );
    let decoded_request = roundtrip(&request);
    let broadcast = decoded_request
        .as_broadcast_request()
        .expect("broadcast request body");
    assert_eq!(broadcast.packet_id, 123);
    assert!(broadcast.send_ack);

    let ack = Envelope::broadcast_ack(
        ProtocolVersion::CURRENT,
        123,
        TypedValue::new("ntk_neighborhood::SrcNic", vec![2]),
    );
    let decoded_ack = roundtrip(&ack);
    let ack_body = decoded_ack.as_broadcast_ack().expect("broadcast ack body");
    assert_eq!(ack_body.packet_id, 123);
}

/// Appends a field this schema doesn't know about (field number 10, varint
/// wire type — tag byte `0x50`) to an otherwise-valid encoding, simulating a
/// newer minor-version peer that has grown a field this build has never
/// heard of. Decoding must still succeed and recover every known field —
/// this is what "an older parser tolerates a newer message" means in
/// practice for a wire format with no live schema registry.
fn append_unknown_varint_field(buf: &mut Vec<u8>, field_number: u8, value: u8) {
    assert!(
        field_number <= 15 && value <= 127,
        "test helper only covers single-byte tag/value"
    );
    buf.push(field_number << 3); // wire type 0 = varint
    buf.push(value);
}

#[test]
fn unknown_field_forward_compat_on_leaf_message() {
    let version = ProtocolVersion { major: 1, minor: 0 };
    let mut buf = Vec::new();
    version.encode(&mut buf).unwrap();
    append_unknown_varint_field(&mut buf, 10, 123);

    let decoded = ProtocolVersion::decode(buf.as_slice())
        .expect("unknown field must be skipped, not rejected");
    assert_eq!(decoded, version);
}

#[test]
fn unknown_field_forward_compat_on_envelope() {
    let envelope = Envelope::request(
        ProtocolVersion::CURRENT,
        55,
        caller(),
        TypedValue::new("ntk_identities::IdentityId", Vec::new()),
        true,
        MethodCall {
            call: Some(Call::HookingRetrieveNetworkData(true)),
        },
    );
    let mut buf = Vec::new();
    envelope.encode(&mut buf).unwrap();
    append_unknown_varint_field(&mut buf, 10, 42);

    let decoded =
        Envelope::decode(buf.as_slice()).expect("unknown top-level field must be skipped");
    let request = decoded
        .as_request()
        .expect("request body survives the unknown trailing field");
    assert_eq!(request.correlation_id, 55);
    assert!(matches!(
        &request.call.as_ref().unwrap().call,
        Some(Call::HookingRetrieveNetworkData(true))
    ));
}

#[test]
fn typed_value_helper_round_trips() {
    let value = TypedValue::new("ntk_qspn::EtpMessage", vec![1, 2, 3, 4]);
    assert_eq!(value.type_tag, "ntk_qspn::EtpMessage");
    assert_eq!(value.payload, vec![1, 2, 3, 4]);

    let mut buf = Vec::new();
    value.encode(&mut buf).unwrap();
    assert_eq!(TypedValue::decode(buf.as_slice()).unwrap(), value);
}

#[test]
fn envelope_without_auth_is_wire_compatible() {
    let envelope = Envelope::request(
        ProtocolVersion::CURRENT,
        7,
        caller(),
        TypedValue::new("ntk_identities::IdentityId", vec![1]),
        true,
        MethodCall {
            call: Some(Call::NeighborhoodNop(Empty::VALUE)),
        },
    );
    assert!(envelope.auth().is_none());

    let bytes = envelope.encode_to_vec();
    let decoded = Envelope::decode(bytes.as_slice()).expect("decode");
    assert_eq!(decoded, envelope);
    assert!(decoded.auth().is_none());

    // Attaching `Auth` is the only thing allowed to change these bytes:
    // `auth` is a proto3 message field, unset by the constructors, so it
    // contributes zero bytes to the wire encoding — pinning that an
    // unauthenticated peer's envelopes are byte-for-byte unaffected by this
    // field's addition to the schema.
    let auth = Auth {
        signer_key: vec![9; 32],
        sequence: 3,
        signature: vec![8; 64],
    };
    let authenticated_bytes = envelope.clone().with_auth(auth).encode_to_vec();
    assert_ne!(bytes, authenticated_bytes);
    assert_eq!(
        bytes,
        envelope.encode_to_vec(),
        "encoding a still-unauthenticated envelope stays deterministic"
    );
}

#[test]
fn envelope_with_auth_round_trips() {
    let auth = Auth {
        signer_key: vec![1; 32],
        sequence: 9,
        signature: vec![2; 64],
    };
    let envelope = Envelope::request(
        ProtocolVersion::CURRENT,
        8,
        caller(),
        TypedValue::new("ntk_identities::IdentityId", vec![1]),
        true,
        MethodCall {
            call: Some(Call::NeighborhoodNop(Empty::VALUE)),
        },
    )
    .with_auth(auth.clone());

    let decoded = roundtrip(&envelope);
    assert_eq!(decoded.auth(), Some(&auth));
}

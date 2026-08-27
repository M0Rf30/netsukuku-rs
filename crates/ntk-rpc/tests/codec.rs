//! Deterministic (no sockets) coverage for [`EnvelopeCodec`]'s frame-size
//! bound.

use bytes::BytesMut;
use ntk_proto::v1::method_call::Call;
use ntk_proto::v1::{CallerContext, MethodCall, ProtocolVersion, QspnSendEtpArgs, TypedValue};
use ntk_rpc::{EnvelopeCodec, RpcError};
use tokio_util::codec::Encoder;

#[test]
fn oversize_frame_is_rejected_on_encode() {
    let big_payload = vec![0u8; 10_000];
    let envelope = ntk_proto::v1::Envelope::request(
        ProtocolVersion::CURRENT,
        1,
        CallerContext {
            source_id: Some(TypedValue::new("t", Vec::new())),
            src_nic: Some(TypedValue::new("t", Vec::new())),
        },
        TypedValue::new("t", Vec::new()),
        true,
        MethodCall {
            call: Some(Call::QspnSendEtp(QspnSendEtpArgs {
                etp: Some(TypedValue::new("big", big_payload)),
                is_full: true,
            })),
        },
    );

    let mut codec = EnvelopeCodec::new(64);
    let mut dst = BytesMut::new();
    let error = codec
        .encode(envelope, &mut dst)
        .expect_err("a frame far over the 64-byte limit must be rejected");
    assert!(
        matches!(error, RpcError::Io(_)),
        "expected an io::Error carrying the frame-too-large rejection, got {error:?}"
    );
}

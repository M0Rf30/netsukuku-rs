//! Length-delimited framing for [`ntk_proto::v1::Envelope`], built on
//! `tokio_util`'s `LengthDelimitedCodec` (4-byte length prefix, mirroring
//! zcd's own stream framing — `research/impl/vala/zcd/connection_protocol.vala:34-141`)
//! plus a `prost`-based payload codec.

use bytes::BytesMut;
use ntk_proto::v1::Envelope;
use prost::Message;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::error::RpcError;

/// Codec for one `Envelope` per length-delimited frame. Frames larger than
/// `max_frame_length` are rejected on both decode (a malicious/buggy peer's
/// oversize length prefix) and encode (a caller trying to send an
/// oversize envelope).
#[derive(Debug)]
pub struct EnvelopeCodec {
    inner: LengthDelimitedCodec,
}

impl EnvelopeCodec {
    /// Builds a codec bounded to `max_frame_length` bytes per frame.
    #[must_use]
    pub fn new(max_frame_length: usize) -> Self {
        let inner = LengthDelimitedCodec::builder()
            .max_frame_length(max_frame_length)
            .new_codec();
        Self { inner }
    }
}

impl Decoder for EnvelopeCodec {
    type Item = Envelope;
    type Error = RpcError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Envelope>, RpcError> {
        let Some(frame) = self.inner.decode(src)? else {
            return Ok(None);
        };
        Ok(Some(Envelope::decode(frame)?))
    }
}

impl Encoder<Envelope> for EnvelopeCodec {
    type Error = RpcError;

    fn encode(&mut self, item: Envelope, dst: &mut BytesMut) -> Result<(), RpcError> {
        let mut buf = BytesMut::with_capacity(item.encoded_len());
        item.encode(&mut buf)?;
        self.inner.encode(buf.freeze(), dst)?;
        Ok(())
    }
}

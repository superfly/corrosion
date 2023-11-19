use std::io;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

#[derive(Debug, Default)]
pub struct Crc32LengthDelimitedCodec {
    inner: LengthDelimitedCodec,
}

impl Decoder for Crc32LengthDelimitedCodec {
    type Item = BytesMut;

    type Error = io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let decoded = self.inner.decode(src)?;
        match decoded {
            Some(mut decoded_with_csum) => {
                let decoded = decoded_with_csum.split_to(decoded_with_csum.len() - 4);
                let expected = decoded_with_csum.get_u32();
                let actual = crc32fast::hash(&decoded);
                if expected != actual {
                    return Err(io::Error::new(io::ErrorKind::Other, format!("corrupted decoded message, expected checksum {expected}, got: {actual}")));
                }
                Ok(Some(decoded))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<BytesMut> for Crc32LengthDelimitedCodec {
    type Error = io::Error;

    fn encode(&mut self, mut data: BytesMut, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let checksum = crc32fast::hash(&data);
        data.put_u32(checksum);
        self.inner.encode(data.freeze(), dst)
    }
}

impl Encoder<Bytes> for Crc32LengthDelimitedCodec {
    type Error = io::Error;

    fn encode(&mut self, data: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        self.encode(BytesMut::from(data.as_ref()), dst)
    }
}

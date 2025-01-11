use {
    prost::{
        bytes::{Buf, BufMut},
        encoding::{
            encode_key, encode_varint, encoded_len_varint, key_len, message, DecodeContext,
            WireType,
        },
        DecodeError, Message,
    },
    prost_types::Timestamp,
    yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof,
};

#[derive(Debug, Clone)]
pub struct SubscribeUpdateMessage<'a> {
    pub filters: &'a [&'a str],
    pub update: UpdateOneof,
    pub created_at: Timestamp,
}

impl<'a> Message for SubscribeUpdateMessage<'a> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        for filter in self.filters {
            bytes_encode(1, filter.as_bytes(), buf);
        }
        self.update.encode(buf);
        message::encode(11, &self.created_at, buf);
    }

    fn encoded_len(&self) -> usize {
        self.filters
            .iter()
            .map(|filter| bytes_encoded_len(1, filter.as_bytes()))
            .sum::<usize>()
            + self.update.encoded_len()
            + message::encoded_len(11, &self.created_at)
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

#[inline]
pub fn bytes_encode(tag: u32, value: &[u8], buf: &mut impl BufMut) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(value.len() as u64, buf);
    buf.put(value)
}

#[inline]
pub fn bytes_encoded_len(tag: u32, value: &[u8]) -> usize {
    key_len(tag) + encoded_len_varint(value.len() as u64) + value.len()
}

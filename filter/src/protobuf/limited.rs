use prost::{
    bytes::BufMut,
    encoding::{encode_key, encode_varint, WireType},
};

#[derive(Debug)]
pub enum UpdateOneofLimited<'a> {
    Slot(&'a [u8]),
}

impl<'a> UpdateOneofLimited<'a> {
    const fn tag(&self) -> u32 {
        match self {
            // Self::Account(_) => 2u32,
            Self::Slot(_) => 3u32,
            // Self::Transaction(_) => 4u32,
            // Self::TransactionStatus(_) => 10u32,
            // Self::Block(_) => 5u32,
            // Self::Ping(_) => 6u32,
            // Self::Pong(_) => 9u32,
            // Self::BlockMeta(_) => 7u32,
            // Self::Entry(_) => 8u32,
        }
    }

    const fn len(&self) -> usize {
        match self {
            // Self::Account(_) => 2u32,
            Self::Slot(slice) => slice.len(),
            // Self::Transaction(_) => 4u32,
            // Self::TransactionStatus(_) => 10u32,
            // Self::Block(_) => 5u32,
            // Self::Ping(_) => 6u32,
            // Self::Pong(_) => 9u32,
            // Self::BlockMeta(_) => 7u32,
            // Self::Entry(_) => 8u32,
        }
    }

    pub fn encode(&self, buf: &mut impl BufMut) {
        encode_key(self.tag(), WireType::LengthDelimited, buf);
        encode_varint(self.len() as u64, buf);
        match self {
            // Self::Account(_) => 2u32,
            Self::Slot(slice) => buf.put_slice(slice),
            // Self::Transaction(_) => 4u32,
            // Self::TransactionStatus(_) => 10u32,
            // Self::Block(_) => 5u32,
            // Self::Ping(_) => 6u32,
            // Self::Pong(_) => 9u32,
            // Self::BlockMeta(_) => 7u32,
            // Self::Entry(_) => 8u32,
        }
    }

    pub fn encoded_len(&self) -> usize {
        todo!()
    }
}

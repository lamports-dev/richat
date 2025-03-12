use prost::{
    bytes::BufMut,
    encoding::{encode_key, encode_varint, encoded_len_varint, key_len, WireType},
};

pub mod decode;
pub mod encode;

#[inline]
pub fn bytes_encode(tag: u32, value: &[u8], buf: &mut impl BufMut) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(value.len() as u64, buf);
    buf.put(value)
}

#[inline]
pub const fn bytes_encoded_len(tag: u32, value: &[u8]) -> usize {
    key_len(tag) + encoded_len_varint(value.len() as u64) + value.len()
}

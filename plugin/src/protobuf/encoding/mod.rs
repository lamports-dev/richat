use prost::{
    bytes::BufMut,
    encoding::{encode_key, encode_varint, encoded_len_varint, key_len, WireType},
};

pub mod account;
pub mod entry;
pub mod slot;
pub mod transaction;

pub use self::{account::Account, entry::Entry, slot::Slot, transaction::Transaction};

#[inline]
pub fn field_encoded_len(tag: u32, len: usize) -> usize {
    key_len(tag) + encoded_len_varint(len as u64) + len
}

#[inline]
fn bytes_encode_raw(tag: u32, value: &[u8], buf: &mut impl BufMut) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(value.len() as u64, buf);
    buf.put(value)
}

#[inline]
pub fn bytes_encoded_len(tag: u32, value: &[u8]) -> usize {
    field_encoded_len(tag, value.len())
}

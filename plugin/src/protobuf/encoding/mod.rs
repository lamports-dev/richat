use prost::{
    bytes::BufMut,
    encoding::{encode_key, encode_varint, encoded_len_varint, key_len, WireType},
};

pub use self::{
    account::Account, block_meta::BlockMeta, entry::Entry, slot::Slot, transaction::Transaction,
};

mod account;
mod block_meta;
mod entry;
mod slot;
mod transaction;

pub fn iter_encoded_len(tag: u32, iter: impl Iterator<Item = usize>, len: usize) -> usize {
    key_len(tag) * len
        + iter
            .map(|len| len + encoded_len_varint(len as u64))
            .sum::<usize>()
}

#[inline]
pub fn field_encoded_len(tag: u32, len: usize) -> usize {
    key_len(tag) + len + encoded_len_varint(len as u64)
}

#[inline]
fn bytes_encode(tag: u32, value: &[u8], buf: &mut impl BufMut) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(value.len() as u64, buf);
    buf.put(value)
}

#[inline]
pub fn bytes_encode_repeated(tag: u32, values: &[&[u8]], buf: &mut impl BufMut) {
    for value in values {
        bytes_encode(tag, value, buf)
    }
}

#[inline]
pub fn bytes_encoded_len(tag: u32, value: &[u8]) -> usize {
    field_encoded_len(tag, value.len())
}

#[inline]
pub fn bytes_encoded_len_repeated(tag: u32, values: &[&[u8]]) -> usize {
    key_len(tag) * values.len()
        + values
            .iter()
            .map(|value| value.len() + encoded_len_varint(value.len() as u64))
            .sum::<usize>()
}

#[inline]
pub fn str_encode_repeated(tag: u32, values: &[&str], buf: &mut impl BufMut) {
    for value in values {
        bytes_encode(tag, value.as_ref(), buf)
    }
}

#[inline]
pub fn str_encoded_len_repeated(tag: u32, values: &[&str]) -> usize {
    key_len(tag) * values.len()
        + values
            .iter()
            .map(|value| value.len() + encoded_len_varint(value.len() as u64))
            .sum::<usize>()
}

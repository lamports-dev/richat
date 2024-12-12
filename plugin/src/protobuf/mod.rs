pub mod encoding;
pub mod message;

pub use self::{
    encoding::{Account, Entry, Slot, Transaction},
    message::ProtobufMessage,
};

pub trait Message {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut);
    fn encoded_len(&self) -> usize;
}

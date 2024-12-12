pub mod encoding;
pub mod message;

pub use self::{
    encoding::{Account, Slot, Transaction},
    message::ProtobufMessage,
};

pub trait Message {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut);
    fn encoded_len(&self) -> usize;
}

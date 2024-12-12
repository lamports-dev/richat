use super::{Account, BlockMeta, Entry, Slot, Transaction};

#[derive(Debug)]
pub enum ProtobufMessage<'a> {
    Account(Account<'a>),
    Slot(Slot<'a>),
    Transaction(Transaction),
    Entry(Entry<'a>),
    BlockMeta(BlockMeta<'a>),
}

impl<'a> ProtobufMessage<'a> {
    pub fn encode(&self, buffer: &mut Vec<u8>) -> Vec<u8> {
        todo!()
    }
}

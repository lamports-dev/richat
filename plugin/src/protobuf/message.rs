use {
    super::{Account, Entry, Slot, Transaction},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoV4,
};

#[derive(Debug)]
pub enum ProtobufMessage<'a> {
    Account(Account<'a>),
    Slot(Slot<'a>),
    Transaction(Transaction),
    Entry(Entry<'a>),
    BlockMeta {
        blockinfo: &'a ReplicaBlockInfoV4<'a>,
    },
}

impl<'a> ProtobufMessage<'a> {
    pub fn encode(&self, buffer: &mut Vec<u8>) -> Vec<u8> {
        todo!()
    }
}

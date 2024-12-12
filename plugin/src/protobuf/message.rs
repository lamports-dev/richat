use {
    super::encoding::{Account, Slot, Transaction},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaBlockInfoV4, ReplicaEntryInfoV2,
    },
};

#[derive(Debug)]
pub enum ProtobufMessage<'a> {
    Account(Account<'a>),
    Slot(Slot<'a>),
    Transaction(Transaction<'a>),
    Entry {
        entry: &'a ReplicaEntryInfoV2<'a>,
    },
    BlockMeta {
        blockinfo: &'a ReplicaBlockInfoV4<'a>,
    },
}

impl<'a> ProtobufMessage<'a> {
    pub fn encode(&self, buffer: &mut Vec<u8>) -> Vec<u8> {
        todo!()
    }
}

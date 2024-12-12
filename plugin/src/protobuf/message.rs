use {
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaBlockInfoV4, ReplicaEntryInfoV2, ReplicaTransactionInfoV2, SlotStatus,
    },
    solana_sdk::clock::Slot,
};

use super::encoding::Account;

#[derive(Debug)]
pub enum ProtobufMessage<'a> {
    Account(Account<'a>),
    Slot {
        slot: Slot,
        parent: Option<u64>,
        status: &'a SlotStatus,
    },
    Transaction {
        slot: Slot,
        transaction: &'a ReplicaTransactionInfoV2<'a>,
    },
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

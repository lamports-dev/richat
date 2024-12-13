use {
    super::encoding::{self, Account, BlockMeta, Entry, Transaction},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaEntryInfoV2, ReplicaTransactionInfoV2,
        SlotStatus,
    },
    prost::encoding::message,
    solana_sdk::clock::Slot,
};

#[derive(Debug)]
pub enum ProtobufMessage<'a> {
    Account {
        slot: Slot,
        account: &'a ReplicaAccountInfoV3<'a>,
    },
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
        buffer.clear();
        match self {
            Self::Account { slot, account } => {
                let account = Account::new(*slot, account);
                message::encode(2, &account, buffer)
            }
            Self::Slot {
                slot,
                parent,
                status,
            } => {
                let slot = encoding::Slot::new(*slot, *parent, status);
                message::encode(3, &slot, buffer)
            }
            Self::Transaction { slot, transaction } => {
                let transaction = Transaction::new(*slot, transaction);
                message::encode(4, &transaction, buffer)
            }
            Self::Entry { entry } => {
                let entry = Entry::new(entry);
                message::encode(5, &entry, buffer)
            }
            Self::BlockMeta { blockinfo } => {
                let blockmeta = BlockMeta::new(blockinfo);
                message::encode(6, &blockmeta, buffer)
            }
        }
        buffer.to_owned()
    }
}

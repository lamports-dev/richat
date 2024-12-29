use {
    super::encoding,
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaEntryInfoV2, ReplicaTransactionInfoV2,
        SlotStatus,
    },
    prost::encoding::message,
    prost_types::Timestamp,
    solana_sdk::clock::Slot,
    std::time::SystemTime,
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
    pub const fn get_slot(&self) -> Slot {
        match self {
            Self::Account { slot, .. } => *slot,
            Self::Slot { slot, .. } => *slot,
            Self::Transaction { slot, .. } => *slot,
            Self::Entry { entry } => entry.slot,
            Self::BlockMeta { blockinfo } => blockinfo.slot,
        }
    }

    pub fn encode(&self, buffer: &mut Vec<u8>) -> Vec<u8> {
        self.encode_with_timestamp(buffer, SystemTime::now())
    }

    pub fn encode_with_timestamp(&self, buffer: &mut Vec<u8>, created_at: SystemTime) -> Vec<u8> {
        buffer.clear();
        match self {
            Self::Account { slot, account } => {
                let account = encoding::Account::new(*slot, account);
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
                let transaction = encoding::Transaction::new(*slot, transaction);
                message::encode(4, &transaction, buffer)
            }
            Self::BlockMeta { blockinfo } => {
                let blockmeta = encoding::BlockMeta::new(blockinfo);
                message::encode(7, &blockmeta, buffer)
            }
            Self::Entry { entry } => {
                let entry = encoding::Entry::new(entry);
                message::encode(8, &entry, buffer)
            }
        }
        let timestamp = Timestamp::from(created_at);
        message::encode(11, &timestamp, buffer);
        buffer.as_slice().to_vec()
    }
}

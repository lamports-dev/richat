use {
    super::encoding::{self, str_encode_repeated, Account, BlockMeta, Entry, Transaction},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaEntryInfoV2, ReplicaTransactionInfoV2,
        SlotStatus,
    },
    prost::encoding::message,
    prost_types::Timestamp,
    solana_sdk::clock::Slot,
    std::time::UNIX_EPOCH,
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
        str_encode_repeated(1, &[], buffer);
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
            Self::BlockMeta { blockinfo } => {
                let blockmeta = BlockMeta::new(blockinfo);
                message::encode(7, &blockmeta, buffer)
            }
            Self::Entry { entry } => {
                let entry = Entry::new(entry);
                message::encode(8, &entry, buffer)
            }
        }
        let now = std::time::SystemTime::now();
        let duration = now.duration_since(UNIX_EPOCH).unwrap_or_default();
        let timestamp = Timestamp {
            seconds: duration.as_secs() as i64,
            nanos: duration.as_nanos() as i32,
        };
        message::encode(11, &timestamp, buffer);
        buffer.to_owned()
    }
}

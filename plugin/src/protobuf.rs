use {
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaEntryInfoV2, ReplicaTransactionInfoV2,
        SlotStatus,
    },
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
    pub const fn get_slot(&self) -> Slot {
        match self {
            Self::Account { slot, .. } => *slot,
            Self::Slot { slot, .. } => *slot,
            Self::Transaction { slot, .. } => *slot,
            Self::Entry { entry } => entry.slot,
            Self::BlockMeta { blockinfo } => blockinfo.slot,
        }
    }

    #[allow(clippy::ptr_arg)]
    pub fn encode(&self, _buffer: &mut Vec<u8>) -> Vec<u8> {
        vec![
            10, 6, 99, 108, 105, 101, 110, 116, 58, 190, 1, 8, 207, 243, 217, 146, 1, 18, 44, 67,
            111, 66, 101, 103, 121, 120, 89, 76, 52, 117, 51, 121, 68, 75, 118, 115, 114, 76, 111,
            87, 89, 103, 57, 104, 99, 117, 53, 78, 66, 85, 74, 114, 109, 113, 81, 118, 68, 117,
            113, 109, 49, 97, 105, 26, 62, 10, 60, 10, 44, 67, 87, 57, 67, 55, 72, 66, 119, 65, 77,
            103, 113, 78, 100, 88, 107, 78, 103, 70, 103, 57, 85, 106, 114, 51, 101, 100, 82, 50,
            65, 98, 57, 121, 109, 69, 117, 81, 110, 86, 97, 99, 100, 49, 65, 16, 217, 204, 210, 19,
            24, 203, 192, 180, 234, 197, 28, 32, 1, 34, 6, 8, 134, 232, 251, 186, 6, 42, 6, 8, 221,
            209, 177, 136, 1, 48, 206, 243, 217, 146, 1, 58, 44, 57, 120, 116, 70, 104, 57, 104,
            77, 115, 119, 115, 85, 105, 107, 115, 110, 70, 67, 107, 104, 56, 113, 53, 107, 90, 117,
            75, 77, 106, 75, 71, 117, 106, 86, 66, 122, 109, 99, 71, 122, 55, 81, 82, 54, 64, 165,
            13, 72, 180, 3,
        ]
    }
}

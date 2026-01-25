mod encoding;
mod message;

pub use {
    encoding::{Account, BlockMeta, Entry, Slot, Transaction, bytes_encode, bytes_encoded_len},
    message::{ProtobufEncoder, ProtobufMessage},
};

#[cfg(test)]
mod tests {
    use {
        super::{ProtobufEncoder, ProtobufMessage},
        prost::Message,
        richat_benches::fixtures::{
            generate_accounts, generate_block_metas, generate_entries, generate_slots,
            generate_transactions,
        },
        richat_proto::geyser::{SubscribeUpdate, subscribe_update::UpdateOneof},
        std::time::SystemTime,
    };

    #[test]
    pub fn test_encode_account() {
        let created_at = SystemTime::now();
        for item in generate_accounts() {
            let (slot, replica) = item.to_replica();
            let msg_richat = ProtobufMessage::Account {
                slot,
                account: &replica,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "account: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Account(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "account: {item:?}");
        }
    }

    #[test]
    pub fn test_encode_block_meta() {
        let created_at = SystemTime::now();
        for item in generate_block_metas() {
            let replica = item.to_replica();
            let msg_richat = ProtobufMessage::BlockMeta {
                blockinfo: &replica,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "block meta: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::BlockMeta(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "block meta: {item:?}");
        }
    }

    #[test]
    pub fn test_encode_entry() {
        let created_at = SystemTime::now();
        for item in generate_entries() {
            let replica = item.to_replica();
            let msg_richat = ProtobufMessage::Entry { entry: &replica };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "entry: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Entry(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "entry: {item:?}");
        }
    }

    #[test]
    pub fn test_encode_slot() {
        let created_at = SystemTime::now();
        for item in generate_slots() {
            let (slot, parent, status) = item.to_replica();
            let msg_richat = ProtobufMessage::Slot {
                slot,
                parent,
                status,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "slot: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Slot(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "slot: {item:?}");
        }
    }

    #[test]
    pub fn test_encode_transaction() {
        let created_at = SystemTime::now();
        for item in generate_transactions() {
            let (slot, replica) = item.to_replica();
            let msg_richat = ProtobufMessage::Transaction {
                slot,
                transaction: &replica,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "transaction: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Transaction(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "transaction: {item:?}");
        }
    }
}

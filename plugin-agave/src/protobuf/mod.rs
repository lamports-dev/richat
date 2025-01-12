mod encoding;
mod message;

pub use {
    encoding::{Account, BlockMeta, Entry, Slot, Transaction},
    message::{ProtobufEncoder, ProtobufMessage},
};

#[cfg(any(test, feature = "fixtures"))]
pub mod fixtures {
    use {
        agave_geyser_plugin_interface::geyser_plugin_interface::{
            ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaEntryInfoV2, ReplicaTransactionInfoV2,
            SlotStatus,
        },
        prost_011::Message,
        solana_sdk::{
            clock::Slot,
            hash::{Hash, HASH_BYTES},
            message::{v0::LoadedAddresses, SimpleAddressLoader},
            pubkey::Pubkey,
            signature::Signature,
            transaction::{MessageHash, SanitizedTransaction},
        },
        solana_storage_proto::convert::generated,
        solana_transaction_status::{
            ConfirmedBlock, RewardsAndNumPartitions, TransactionStatusMeta,
        },
        std::{collections::HashSet, fs},
        yellowstone_grpc_proto::{
            convert_to,
            geyser::{
                CommitmentLevel, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
                SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdateSlot,
                SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
            },
        },
    };

    pub fn load_predefined_blocks() -> Vec<(Slot, ConfirmedBlock)> {
        fs::read_dir("./fixtures/blocks")
            .expect("failed to read `fixtures` directory")
            .map(|entry| {
                let entry = entry.expect("failed to read entry directory");
                let path = entry.path();

                let file_name = path.file_name().expect("failed to get fixture file name");
                let extension = path.extension().expect("failed to get fixture extension");
                let slot = file_name.to_str().expect("failed to stringify file name")
                    [0..extension.len()]
                    .parse::<u64>()
                    .expect("failed to parse file name");

                let data = fs::read(path).expect("failed to read fixture");
                let block = generated::ConfirmedBlock::decode(data.as_slice())
                    .expect("failed to decode fixture")
                    .try_into()
                    .expect("failed to parse block");

                (slot, block)
            })
            .collect::<Vec<_>>()
    }

    #[derive(Debug, Clone)]
    pub struct GeneratedAccount {
        pub pubkey: Pubkey,
        pub lamports: u64,
        pub owner: Pubkey,
        pub executable: bool,
        pub rent_epoch: u64,
        pub data: Vec<u8>,
        pub write_version: u64,
        pub transaction_signature: Option<SanitizedTransaction>,
        pub slot: Slot,
    }

    impl GeneratedAccount {
        pub fn to_replica(&self) -> (Slot, ReplicaAccountInfoV3) {
            let replica = ReplicaAccountInfoV3 {
                pubkey: self.pubkey.as_ref(),
                lamports: self.lamports,
                owner: self.owner.as_ref(),
                executable: self.executable,
                rent_epoch: self.rent_epoch,
                data: &self.data,
                write_version: self.write_version,
                txn: self.transaction_signature.as_ref(),
            };
            (self.slot, replica)
        }

        pub fn to_prost(&self) -> SubscribeUpdateAccount {
            SubscribeUpdateAccount {
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: self.pubkey.as_ref().to_vec(),
                    lamports: self.lamports,
                    owner: self.owner.as_ref().to_vec(),
                    executable: self.executable,
                    rent_epoch: self.rent_epoch,
                    data: self.data.clone(),
                    write_version: self.write_version,
                    txn_signature: self
                        .transaction_signature
                        .as_ref()
                        .map(|transaction| transaction.signature().as_ref().to_vec()),
                }),
                slot: self.slot,
                is_startup: false,
            }
        }
    }

    pub fn generate_accounts() -> Vec<GeneratedAccount> {
        const PUBKEY: Pubkey =
            Pubkey::from_str_const("28Dncoh8nmzXYEGLUcBA5SUw5WDwDBn15uUCwrWBbyuu");
        const OWNER: Pubkey =
            Pubkey::from_str_const("5jrPJWVGrFvQ2V9wRZC3kHEZhxo9pmMir15x73oHT6mn");

        let blocks = load_predefined_blocks();
        let block = blocks
            .first()
            .map(|(_slot, confirmed_block)| confirmed_block)
            .expect("failed to get first `ConfirmedBlock`");
        let versioned_transaction = block
            .transactions
            .first()
            .map(|transaction| transaction.get_transaction())
            .expect("failed to get first `VersionedTransaction`");
        let address_loader = match versioned_transaction.message.address_table_lookups() {
            Some(vec_atl) => SimpleAddressLoader::Enabled(LoadedAddresses {
                writable: vec_atl.iter().map(|atl| atl.account_key).collect(),
                readonly: vec_atl.iter().map(|atl| atl.account_key).collect(),
            }),
            None => SimpleAddressLoader::Disabled,
        };
        let sanitized_transaction = SanitizedTransaction::try_create(
            versioned_transaction,
            MessageHash::Compute, // message_hash
            None,                 // is_simple_vote_tx
            address_loader,
            &HashSet::new(), // reserved_account_keys
        )
        .expect("failed to create sanitized transaction");

        let mut accounts = Vec::new();
        for lamports in [0, 8123] {
            for executable in [true, false] {
                for rent_epoch in [0, 4242] {
                    for data in [
                        vec![],
                        vec![42; 165],
                        vec![42; 1024],
                        vec![42; 2 * 1024 * 1024],
                    ] {
                        for write_version in [0, 1] {
                            for transaction_signature in [None, Some(&sanitized_transaction)] {
                                for slot in [0, 310639056] {
                                    accounts.push(GeneratedAccount {
                                        pubkey: PUBKEY,
                                        lamports,
                                        owner: OWNER,
                                        executable,
                                        rent_epoch,
                                        data: data.to_owned(),
                                        write_version,
                                        transaction_signature: transaction_signature.cloned(),
                                        slot,
                                    })
                                }
                            }
                        }
                    }
                }
            }
        }

        accounts
    }

    #[derive(Debug, Clone)]
    pub struct GeneratedBlockMeta {
        slot: Slot,
        block: ConfirmedBlock,
        rewards: RewardsAndNumPartitions,
        entry_count: u64,
    }

    impl GeneratedBlockMeta {
        pub fn to_replica(&self) -> ReplicaBlockInfoV4 {
            ReplicaBlockInfoV4 {
                parent_slot: self.block.parent_slot,
                slot: self.slot,
                parent_blockhash: &self.block.previous_blockhash,
                blockhash: &self.block.blockhash,
                rewards: &self.rewards,
                block_time: self.block.block_time,
                block_height: self.block.block_height,
                executed_transaction_count: self.block.transactions.len() as u64,
                entry_count: self.entry_count,
            }
        }

        pub fn to_prost(&self) -> SubscribeUpdateBlockMeta {
            SubscribeUpdateBlockMeta {
                slot: self.slot,
                blockhash: self.block.blockhash.clone(),
                rewards: Some(convert_to::create_rewards_obj(
                    &self.rewards.rewards,
                    self.rewards.num_partitions,
                )),
                block_time: self.block.block_time.map(convert_to::create_timestamp),
                block_height: self.block.block_height.map(convert_to::create_block_height),
                parent_slot: self.block.parent_slot,
                parent_blockhash: self.block.previous_blockhash.clone(),
                executed_transaction_count: self.block.transactions.len() as u64,
                entries_count: self.entry_count,
            }
        }
    }

    pub fn generate_block_metas() -> Vec<GeneratedBlockMeta> {
        load_predefined_blocks()
            .into_iter()
            .flat_map(|(slot, block)| {
                let rewards = RewardsAndNumPartitions {
                    rewards: block.rewards.to_owned(),
                    num_partitions: block.num_partitions,
                };

                [
                    GeneratedBlockMeta {
                        slot,
                        block: block.clone(),
                        rewards: rewards.clone(),
                        entry_count: 0,
                    },
                    GeneratedBlockMeta {
                        slot,
                        block,
                        rewards,
                        entry_count: 42,
                    },
                ]
            })
            .collect::<Vec<_>>()
    }

    #[derive(Debug, Clone)]
    pub struct GeneratedEntry {
        pub slot: Slot,
        pub index: usize,
        pub num_hashes: u64,
        pub hash: Hash,
        pub executed_transaction_count: u64,
        pub starting_transaction_index: usize,
    }

    impl GeneratedEntry {
        pub fn to_replica(&self) -> ReplicaEntryInfoV2<'_> {
            ReplicaEntryInfoV2 {
                slot: self.slot,
                index: self.index,
                num_hashes: self.num_hashes,
                hash: self.hash.as_ref(),
                executed_transaction_count: self.executed_transaction_count,
                starting_transaction_index: self.starting_transaction_index,
            }
        }

        pub fn to_prost(&self) -> SubscribeUpdateEntry {
            SubscribeUpdateEntry {
                slot: self.slot,
                index: self.index as u64,
                num_hashes: self.num_hashes,
                hash: self.hash.as_ref().to_vec(),
                executed_transaction_count: self.executed_transaction_count,
                starting_transaction_index: self.starting_transaction_index as u64,
            }
        }
    }

    pub fn generate_entries() -> Vec<GeneratedEntry> {
        const ENTRY_HASHES: [Hash; 4] = [
            Hash::new_from_array([0; HASH_BYTES]),
            Hash::new_from_array([42; HASH_BYTES]),
            Hash::new_from_array([98; HASH_BYTES]),
            Hash::new_from_array([255; HASH_BYTES]),
        ];

        let mut entries = Vec::new();
        for slot in [0, 42, 310629080] {
            for index in [0, 42] {
                for num_hashes in [0, 128] {
                    for hash in &ENTRY_HASHES {
                        for executed_transaction_count in [0, 32] {
                            for starting_transaction_index in [0, 96, 1067] {
                                entries.push(GeneratedEntry {
                                    slot,
                                    index,
                                    num_hashes,
                                    hash: *hash,
                                    executed_transaction_count,
                                    starting_transaction_index,
                                });
                            }
                        }
                    }
                }
            }
        }
        entries
    }

    #[derive(Debug, Clone)]
    pub struct GeneratedSlot {
        pub slot: Slot,
        pub parent: Option<Slot>,
        pub status: SlotStatus,
    }

    impl GeneratedSlot {
        pub const fn to_replica(&self) -> (Slot, Option<Slot>, &SlotStatus) {
            (self.slot, self.parent, &self.status)
        }

        pub fn to_prost(&self) -> SubscribeUpdateSlot {
            SubscribeUpdateSlot {
                slot: self.slot,
                parent: self.parent,
                status: match &self.status {
                    SlotStatus::Processed => CommitmentLevel::Processed,
                    SlotStatus::Rooted => CommitmentLevel::Finalized,
                    SlotStatus::Confirmed => CommitmentLevel::Confirmed,
                    SlotStatus::FirstShredReceived => CommitmentLevel::FirstShredReceived,
                    SlotStatus::Completed => CommitmentLevel::Completed,
                    SlotStatus::CreatedBank => CommitmentLevel::CreatedBank,
                    SlotStatus::Dead(_error) => CommitmentLevel::Dead,
                } as i32,
                dead_error: if let SlotStatus::Dead(error) = &self.status {
                    Some(error.clone())
                } else {
                    None
                },
            }
        }
    }

    pub fn generate_slots() -> Vec<GeneratedSlot> {
        let mut slots = Vec::new();
        for slot in [0, 42, 310629080] {
            for parent in [None, Some(0), Some(42)] {
                for status in [
                    SlotStatus::Processed,
                    SlotStatus::Rooted,
                    SlotStatus::Confirmed,
                    SlotStatus::FirstShredReceived,
                    SlotStatus::Completed,
                    SlotStatus::CreatedBank,
                    SlotStatus::Dead("".to_owned()),
                    SlotStatus::Dead("42".to_owned()),
                ] {
                    slots.push(GeneratedSlot {
                        slot,
                        parent,
                        status,
                    })
                }
            }
        }
        slots
    }

    #[derive(Debug, Clone)]
    pub struct GeneratedTransaction {
        pub slot: Slot,
        pub signature: Signature,
        pub is_vote: bool,
        pub sanitized_transaction: SanitizedTransaction,
        pub transaction_status_meta: TransactionStatusMeta,
        pub index: usize,
    }

    impl GeneratedTransaction {
        pub const fn to_replica(&self) -> (Slot, ReplicaTransactionInfoV2<'_>) {
            let replica = ReplicaTransactionInfoV2 {
                signature: &self.signature,
                is_vote: self.is_vote,
                transaction: &self.sanitized_transaction,
                transaction_status_meta: &self.transaction_status_meta,
                index: self.index,
            };
            (self.slot, replica)
        }

        pub fn to_prost(&self) -> SubscribeUpdateTransaction {
            SubscribeUpdateTransaction {
                transaction: Some(SubscribeUpdateTransactionInfo {
                    signature: self.signature.as_ref().to_vec(),
                    is_vote: self.is_vote,
                    transaction: Some(convert_to::create_transaction(&self.sanitized_transaction)),
                    meta: Some(convert_to::create_transaction_meta(
                        &self.transaction_status_meta,
                    )),
                    index: self.index as u64,
                }),
                slot: self.slot,
            }
        }
    }

    pub fn generate_transactions() -> Vec<GeneratedTransaction> {
        load_predefined_blocks()
            .into_iter()
            .flat_map(|(slot, block)| {
                let mut transactions = block
                    .transactions
                    .into_iter()
                    .enumerate()
                    .map(|(index, transaction)| {
                        let versioned_transaction = transaction.get_transaction();
                        let address_loader =
                            match versioned_transaction.message.address_table_lookups() {
                                Some(vec_atl) => SimpleAddressLoader::Enabled(LoadedAddresses {
                                    writable: vec_atl.iter().map(|atl| atl.account_key).collect(),
                                    readonly: vec_atl.iter().map(|atl| atl.account_key).collect(),
                                }),
                                None => SimpleAddressLoader::Disabled,
                            };
                        let sanitized_transaction = SanitizedTransaction::try_create(
                            versioned_transaction,
                            MessageHash::Compute, // message_hash
                            None,                 // is_simple_vote_tx
                            address_loader,
                            &HashSet::new(), // reserved_account_keys
                        )
                        .expect("failed to create sanitized transaction");

                        GeneratedTransaction {
                            slot,
                            signature: *sanitized_transaction.signature(),
                            is_vote: sanitized_transaction.is_simple_vote_transaction(),
                            sanitized_transaction,
                            transaction_status_meta: transaction
                                .get_status_meta()
                                .expect("failed to get transaction status meta"),
                            index,
                        }
                    })
                    .collect::<Vec<_>>();

                if let Some(tx) = transactions.first() {
                    let mut tx = tx.clone();
                    tx.slot = 0;
                    transactions.push(tx);
                }

                transactions
            })
            .collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            fixtures::{
                generate_accounts, generate_block_metas, generate_entries, generate_slots,
                generate_transactions,
            },
            ProtobufEncoder, ProtobufMessage,
        },
        prost::Message,
        std::time::SystemTime,
        yellowstone_grpc_proto::geyser::{subscribe_update::UpdateOneof, SubscribeUpdate},
    };

    #[test]
    pub fn test_encode_account() {
        let created_at = SystemTime::now();
        for gen in generate_accounts() {
            let (slot, replica) = gen.to_replica();
            let msg_richat = ProtobufMessage::Account {
                slot,
                account: &replica,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "account: {gen:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Account(gen.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "account: {gen:?}");
        }
    }

    #[test]
    pub fn test_encode_block_meta() {
        let created_at = SystemTime::now();
        for gen in generate_block_metas() {
            let replica = gen.to_replica();
            let msg_richat = ProtobufMessage::BlockMeta {
                blockinfo: &replica,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "block meta: {gen:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::BlockMeta(gen.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "block meta: {gen:?}");
        }
    }

    #[test]
    pub fn test_encode_entry() {
        let created_at = SystemTime::now();
        for gen in generate_entries() {
            let replica = gen.to_replica();
            let msg_richat = ProtobufMessage::Entry { entry: &replica };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "entry: {gen:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Entry(gen.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "entry: {gen:?}");
        }
    }

    #[test]
    pub fn test_encode_slot() {
        let created_at = SystemTime::now();
        for gen in generate_slots() {
            let (slot, parent, status) = gen.to_replica();
            let msg_richat = ProtobufMessage::Slot {
                slot,
                parent,
                status,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "slot: {gen:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Slot(gen.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "slot: {gen:?}");
        }
    }

    #[test]
    pub fn test_encode_transaction() {
        let created_at = SystemTime::now();
        for gen in generate_transactions() {
            let (slot, replica) = gen.to_replica();
            let msg_richat = ProtobufMessage::Transaction {
                slot,
                transaction: &replica,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "transaction: {gen:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Transaction(gen.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "transaction: {gen:?}");

            // use prost::{bytes::Buf, encoding};
            // fn read(mut buffer: &[u8]) -> &[u8] {
            //     // read tx global object tag and total len
            //     assert_eq!(encoding::decode_varint(&mut buffer).unwrap() >> 3, 4);
            //     let _size = encoding::decode_varint(&mut buffer).unwrap();
            //     // read tx tag and total len
            //     assert_eq!(encoding::decode_varint(&mut buffer).unwrap() >> 3, 1);
            //     let _size = encoding::decode_varint(&mut buffer).unwrap();
            //     // read signature
            //     assert_eq!(encoding::decode_varint(&mut buffer).unwrap() >> 3, 1);
            //     assert_eq!(encoding::decode_varint(&mut buffer).unwrap(), 64);
            //     buffer.advance(64);
            //     // read vote
            //     let mut tag = encoding::decode_varint(&mut buffer).unwrap() >> 3;
            //     if tag == 2 {
            //         assert_eq!(encoding::decode_varint(&mut buffer).unwrap(), 1);
            //         tag = encoding::decode_varint(&mut buffer).unwrap() >> 3;
            //     }
            //     // read tx
            //     assert_eq!(tag, 3);
            //     let _size = encoding::decode_varint(&mut buffer).unwrap();
            //     // read signatures
            //     let tag = loop {
            //         let tag = encoding::decode_varint(&mut buffer).unwrap() >> 3;
            //         if tag != 1 {
            //             break tag;
            //         }
            //         assert_eq!(encoding::decode_varint(&mut buffer).unwrap(), 64);
            //         buffer.advance(64);
            //     };
            //     // read tx message
            //     assert_eq!(tag, 2);
            //     let size = encoding::decode_varint(&mut buffer).unwrap();

            //     &buffer[0..size as usize]
            // }

            // let slice_richat = read(vec_richat1.as_slice());
            // let slice_prost = read(vec_prost.as_slice());
            // assert_eq!(slice_richat, slice_prost, "transaction: {gen:?}");
        }
    }
}

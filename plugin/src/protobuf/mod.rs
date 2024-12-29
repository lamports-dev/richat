mod encoding;
mod message;

pub use message::ProtobufMessage;

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
            hash::Hash,
            message::SimpleAddressLoader,
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
    pub struct GenAccount {
        pub pubkey: Pubkey,
        pub lamports: u64,
        pub owner: Pubkey,
        pub executable: bool,
        pub rent_epoch: u64,
        pub data: Vec<u8>,
        pub write_version: u64,
        pub txn_signature: Option<SanitizedTransaction>,
        pub slot: Slot,
        pub is_startup: bool,
    }

    impl GenAccount {
        pub fn to_replica(&self) -> (Slot, ReplicaAccountInfoV3) {
            let replica = ReplicaAccountInfoV3 {
                pubkey: self.pubkey.as_ref(),
                lamports: self.lamports,
                owner: self.owner.as_ref(),
                executable: self.executable,
                rent_epoch: self.rent_epoch,
                data: &self.data,
                write_version: self.write_version,
                txn: self.txn_signature.as_ref(),
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
                        .txn_signature
                        .as_ref()
                        .map(|tx| tx.signature().as_ref().to_vec()),
                }),
                slot: self.slot,
                is_startup: self.is_startup,
            }
        }
    }

    pub fn generate_accounts() -> Vec<GenAccount> {
        const PUBKEY: Pubkey =
            Pubkey::from_str_const("28Dncoh8nmzXYEGLUcBA5SUw5WDwDBn15uUCwrWBbyuu");
        const OWNER: Pubkey =
            Pubkey::from_str_const("5jrPJWVGrFvQ2V9wRZC3kHEZhxo9pmMir15x73oHT6mn");

        let block = &load_predefined_blocks()[0].1;
        let tx_ver = block.transactions[0].get_transaction();
        let tx = SanitizedTransaction::try_create(
            tx_ver,
            MessageHash::Compute,          // message_hash
            None,                          // is_simple_vote_tx
            SimpleAddressLoader::Disabled, // address_loader
            &HashSet::new(),               // reserved_account_keys
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
                            for txn_signature in [None, Some(&tx)] {
                                for slot in [0, 310639056] {
                                    for is_startup in [true, false] {
                                        accounts.push(GenAccount {
                                            pubkey: PUBKEY,
                                            lamports,
                                            owner: OWNER,
                                            executable,
                                            rent_epoch,
                                            data: data.to_owned(),
                                            write_version,
                                            txn_signature: txn_signature.cloned(),
                                            slot,
                                            is_startup,
                                        })
                                    }
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
    pub struct GenBlockMeta {
        slot: Slot,
        block: ConfirmedBlock,
        rewards: RewardsAndNumPartitions,
        entry_count: u64,
    }

    impl GenBlockMeta {
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

    pub fn generate_blocks_meta() -> Vec<GenBlockMeta> {
        load_predefined_blocks()
            .into_iter()
            .flat_map(|(slot, block)| {
                let rewards = RewardsAndNumPartitions {
                    rewards: block.rewards.to_owned(),
                    num_partitions: block.num_partitions,
                };

                [
                    GenBlockMeta {
                        slot,
                        block: block.clone(),
                        rewards: rewards.clone(),
                        entry_count: 0,
                    },
                    GenBlockMeta {
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
    pub struct GenEntry {
        pub slot: Slot,
        pub index: usize,
        pub num_hashes: u64,
        pub hash: Hash,
        pub executed_transaction_count: u64,
        pub starting_transaction_index: usize,
    }

    impl GenEntry {
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

    pub fn generate_entries() -> Vec<GenEntry> {
        const ENTRY_HASHES: [Hash; 4] = [
            Hash::new_from_array([0; 32]),
            Hash::new_from_array([42; 32]),
            Hash::new_from_array([98; 32]),
            Hash::new_from_array([255; 32]),
        ];

        let mut entries = Vec::new();
        for slot in [0, 42, 310629080] {
            for index in [0, 42] {
                for num_hashes in [0, 128] {
                    for hash in &ENTRY_HASHES {
                        for executed_transaction_count in [0, 32] {
                            for starting_transaction_index in [0, 96, 1067] {
                                entries.push(GenEntry {
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
    pub struct GenSlot {
        pub slot: Slot,
        pub parent: Option<Slot>,
        pub status: SlotStatus,
    }

    impl GenSlot {
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

    pub fn generate_slots() -> Vec<GenSlot> {
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
                    slots.push(GenSlot {
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
    pub struct GenTransaction {
        pub slot: Slot,
        pub signature: Signature,
        pub is_vote: bool,
        pub tx: SanitizedTransaction,
        pub tx_status_meta: TransactionStatusMeta,
        pub index: usize,
    }

    impl GenTransaction {
        pub const fn to_replica(&self) -> (Slot, ReplicaTransactionInfoV2<'_>) {
            let replica = ReplicaTransactionInfoV2 {
                signature: &self.signature,
                is_vote: self.is_vote,
                transaction: &self.tx,
                transaction_status_meta: &self.tx_status_meta,
                index: self.index,
            };
            (self.slot, replica)
        }

        pub fn to_prost(&self) -> SubscribeUpdateTransaction {
            SubscribeUpdateTransaction {
                transaction: Some(SubscribeUpdateTransactionInfo {
                    signature: self.signature.as_ref().to_vec(),
                    is_vote: self.is_vote,
                    transaction: Some(convert_to::create_transaction(&self.tx)),
                    meta: Some(convert_to::create_transaction_meta(&self.tx_status_meta)),
                    index: self.index as u64,
                }),
                slot: self.slot,
            }
        }
    }

    pub fn generate_transactions() -> Vec<GenTransaction> {
        load_predefined_blocks()
            .into_iter()
            .flat_map(|(slot, block)| {
                let mut txs = block
                    .transactions
                    .into_iter()
                    .enumerate()
                    .map(|(index, tx)| {
                        let tx_ver = tx.get_transaction();
                        let tx_san = SanitizedTransaction::try_create(
                            tx_ver,
                            MessageHash::Compute,          // message_hash
                            None,                          // is_simple_vote_tx
                            SimpleAddressLoader::Disabled, // address_loader
                            &HashSet::new(),               // reserved_account_keys
                        )
                        .expect("failed to create sanitized transaction");

                        GenTransaction {
                            slot,
                            signature: *tx_san.signature(),
                            is_vote: tx_san.is_simple_vote_transaction(),
                            tx: tx_san,
                            tx_status_meta: tx
                                .get_status_meta()
                                .expect("failed to get transaction status meta"),
                            index,
                        }
                    })
                    .collect::<Vec<_>>();

                let mut tx = txs[0].clone();
                tx.slot = 0;
                txs.push(tx);

                txs
            })
            .collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            fixtures::{
                generate_accounts, generate_blocks_meta, generate_entries, generate_slots,
                generate_transactions,
            },
            ProtobufMessage,
        },
        prost::Message,
        std::time::SystemTime,
        yellowstone_grpc_proto::geyser::{subscribe_update::UpdateOneof, SubscribeUpdate},
    };

    fn cmp_vecs(richat: &[u8], prost: &[u8], message: &str) {
        // assert on len is useless because we check slices,
        // but error message would be better for future debug
        assert_eq!(richat.len(), prost.len(), "len failed for: {message}");
        assert_eq!(richat, prost, "vec failed for: {message}");
    }

    #[test]
    pub fn test_encode_account() {
        let accounts = generate_accounts();

        let mut buffer = Vec::new();
        let created_at = SystemTime::now();
        for account in accounts.iter() {
            let replica = account.to_replica();
            let message = ProtobufMessage::Account {
                slot: replica.0,
                account: &replica.1,
            };
            let vec_richat = message.encode_with_timestamp(&mut buffer, created_at);

            let message = SubscribeUpdate {
                filters: vec![],
                update_oneof: Some(UpdateOneof::Account(account.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = message.encode_to_vec();

            cmp_vecs(&vec_richat, &vec_prost, &format!("account {account:?}"));
        }
    }

    #[test]
    pub fn test_encode_block_meta() {
        let blocks_meta = generate_blocks_meta();

        let mut buffer = Vec::new();
        let created_at = SystemTime::now();
        for block_meta in blocks_meta {
            let replica = block_meta.to_replica();
            let message = ProtobufMessage::BlockMeta {
                blockinfo: &replica,
            };
            let vec_richat = message.encode_with_timestamp(&mut buffer, created_at);

            let message = SubscribeUpdate {
                filters: vec![],
                update_oneof: Some(UpdateOneof::BlockMeta(block_meta.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = message.encode_to_vec();

            cmp_vecs(
                &vec_richat,
                &vec_prost,
                &format!("block meta {block_meta:?}"),
            );
        }
    }

    #[test]
    pub fn test_encode_entry() {
        let entries = generate_entries();

        let mut buffer = Vec::new();
        let created_at = SystemTime::now();
        for entry in entries {
            let replica = entry.to_replica();
            let message = ProtobufMessage::Entry { entry: &replica };
            let vec_richat = message.encode_with_timestamp(&mut buffer, created_at);

            let message = SubscribeUpdate {
                filters: vec![],
                update_oneof: Some(UpdateOneof::Entry(entry.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = message.encode_to_vec();

            cmp_vecs(&vec_richat, &vec_prost, &format!("entry {entry:?}"));
        }
    }

    #[test]
    pub fn test_encode_slot() {
        let slots = generate_slots();

        let mut buffer = Vec::new();
        let created_at = SystemTime::now();
        for slot in slots {
            let replica = slot.to_replica();
            let message = ProtobufMessage::Slot {
                slot: replica.0,
                parent: replica.1,
                status: replica.2,
            };
            let vec_richat = message.encode_with_timestamp(&mut buffer, created_at);

            let message = SubscribeUpdate {
                filters: vec![],
                update_oneof: Some(UpdateOneof::Slot(slot.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = message.encode_to_vec();

            cmp_vecs(&vec_richat, &vec_prost, &format!("slot {slot:?}"));
        }
    }

    #[test]
    pub fn test_encode_transaction() {
        let transactions = generate_transactions();

        let mut buffer = Vec::new();
        let created_at = SystemTime::now();
        for transaction in transactions {
            let replica = transaction.to_replica();
            let message = ProtobufMessage::Transaction {
                slot: replica.0,
                transaction: &replica.1,
            };
            let vec_richat = message.encode_with_timestamp(&mut buffer, created_at);

            let message = SubscribeUpdate {
                filters: vec![],
                update_oneof: Some(UpdateOneof::Transaction(transaction.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = message.encode_to_vec();

            cmp_vecs(
                &vec_richat,
                &vec_prost,
                &format!("transaction {transaction:?}"),
            );
        }
    }
}

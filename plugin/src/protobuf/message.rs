use {
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaEntryInfoV2, ReplicaTransactionInfoV2,
        SlotStatus,
    },
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
        #[cfg(feature = "encode-prost")]
        self.encode_prost(buffer, created_at);
        #[cfg(not(feature = "encode-prost"))]
        self.encode_raw(buffer, created_at);
        buffer.as_slice().to_vec()
    }

    #[cfg(feature = "encode-prost")]
    fn encode_prost(&self, buffer: &mut Vec<u8>, created_at: SystemTime) {
        use {
            prost::Message,
            yellowstone_grpc_proto::{
                convert_to,
                geyser::{
                    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeUpdate,
                    SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateBlockMeta,
                    SubscribeUpdateEntry, SubscribeUpdateSlot, SubscribeUpdateTransaction,
                    SubscribeUpdateTransactionInfo,
                },
            },
        };

        SubscribeUpdate {
            filters: Vec::new(),
            update_oneof: Some(match self {
                Self::Account { slot, account } => UpdateOneof::Account(SubscribeUpdateAccount {
                    account: Some(SubscribeUpdateAccountInfo {
                        pubkey: account.pubkey.as_ref().to_vec(),
                        lamports: account.lamports,
                        owner: account.owner.as_ref().to_vec(),
                        executable: account.executable,
                        rent_epoch: account.rent_epoch,
                        data: account.data.to_vec(),
                        write_version: account.write_version,
                        txn_signature: account
                            .txn
                            .as_ref()
                            .map(|transaction| transaction.signature().as_ref().to_vec()),
                    }),
                    slot: *slot,
                    is_startup: false,
                }),
                Self::Slot {
                    slot,
                    parent,
                    status,
                } => UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: *slot,
                    parent: *parent,
                    status: match status {
                        SlotStatus::Processed => CommitmentLevel::Processed,
                        SlotStatus::Rooted => CommitmentLevel::Finalized,
                        SlotStatus::Confirmed => CommitmentLevel::Confirmed,
                    } as i32,
                }),
                Self::Transaction { slot, transaction } => {
                    UpdateOneof::Transaction(SubscribeUpdateTransaction {
                        transaction: Some(SubscribeUpdateTransactionInfo {
                            signature: transaction.signature.as_ref().to_vec(),
                            is_vote: transaction.is_vote,
                            transaction: Some(convert_to::create_transaction(
                                transaction.transaction,
                            )),
                            meta: Some(convert_to::create_transaction_meta(
                                transaction.transaction_status_meta,
                            )),
                            index: transaction.index as u64,
                        }),
                        slot: *slot,
                    })
                }
                Self::BlockMeta { blockinfo } => UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                    slot: blockinfo.slot,
                    blockhash: blockinfo.blockhash.to_string(),
                    rewards: Some(convert_to::create_rewards_obj(
                        &blockinfo.rewards.rewards,
                        blockinfo.rewards.num_partitions,
                    )),
                    block_time: blockinfo.block_time.map(convert_to::create_timestamp),
                    block_height: blockinfo.block_height.map(convert_to::create_block_height),
                    parent_slot: blockinfo.parent_slot,
                    parent_blockhash: blockinfo.parent_blockhash.to_string(),
                    executed_transaction_count: blockinfo.executed_transaction_count,
                    entries_count: blockinfo.entry_count,
                }),
                Self::Entry { entry } => UpdateOneof::Entry(SubscribeUpdateEntry {
                    slot: entry.slot,
                    index: entry.index as u64,
                    num_hashes: entry.num_hashes,
                    hash: entry.hash.as_ref().to_vec(),
                    executed_transaction_count: entry.executed_transaction_count,
                    starting_transaction_index: entry.starting_transaction_index as u64,
                }),
            }),
            created_at: Some(created_at.into()),
        }
        .encode(buffer)
        .expect("failed to encode")
    }

    #[cfg(not(feature = "encode-prost"))]
    fn encode_raw(&self, buffer: &mut Vec<u8>, created_at: SystemTime) {
        use {super::encoding, prost::encoding::message, prost_types::Timestamp};

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
    }
}

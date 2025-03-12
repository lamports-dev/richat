use {
    crate::{plugin::PluginNotification, protobuf::encoding},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaEntryInfoV2, ReplicaTransactionInfoV2,
        SlotStatus as GeyserSlotStatus,
    },
    prost::encoding::message,
    prost_types::Timestamp,
    solana_sdk::clock::Slot,
    std::time::SystemTime,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtobufEncoder {
    Prost,
    Raw,
}

#[derive(Debug)]
pub enum ProtobufMessage<'a> {
    Account {
        slot: Slot,
        account: &'a ReplicaAccountInfoV3<'a>,
    },
    Slot {
        slot: Slot,
        parent: Option<u64>,
        status: &'a GeyserSlotStatus,
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

impl ProtobufMessage<'_> {
    pub const fn get_plugin_notification(&self) -> PluginNotification {
        match self {
            Self::Account { .. } => PluginNotification::Account,
            Self::Slot { .. } => PluginNotification::Slot,
            Self::Transaction { .. } => PluginNotification::Transaction,
            Self::Entry { .. } => PluginNotification::Entry,
            Self::BlockMeta { .. } => PluginNotification::BlockMeta,
        }
    }

    pub const fn get_slot(&self) -> Slot {
        match self {
            Self::Account { slot, .. } => *slot,
            Self::Slot { slot, .. } => *slot,
            Self::Transaction { slot, .. } => *slot,
            Self::Entry { entry } => entry.slot,
            Self::BlockMeta { blockinfo } => blockinfo.slot,
        }
    }

    pub fn encode(&self, encoder: ProtobufEncoder) -> Vec<u8> {
        self.encode_with_timestamp(encoder, SystemTime::now())
    }

    pub fn encode_with_timestamp(
        &self,
        encoder: ProtobufEncoder,
        created_at: impl Into<Timestamp>,
    ) -> Vec<u8> {
        match encoder {
            ProtobufEncoder::Prost => self.encode_prost(created_at),
            ProtobufEncoder::Raw => self.encode_raw(created_at),
        }
    }

    pub fn encode_prost(&self, created_at: impl Into<Timestamp>) -> Vec<u8> {
        use {
            prost::Message,
            richat_proto::{
                convert_to,
                geyser::{
                    subscribe_update::UpdateOneof, SlotStatus, SubscribeUpdate,
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
                        GeyserSlotStatus::Processed => SlotStatus::SlotProcessed,
                        GeyserSlotStatus::Rooted => SlotStatus::SlotFinalized,
                        GeyserSlotStatus::Confirmed => SlotStatus::SlotConfirmed,
                        GeyserSlotStatus::FirstShredReceived => SlotStatus::SlotFirstShredReceived,
                        GeyserSlotStatus::Completed => SlotStatus::SlotCompleted,
                        GeyserSlotStatus::CreatedBank => SlotStatus::SlotCreatedBank,
                        GeyserSlotStatus::Dead(_) => SlotStatus::SlotDead,
                    } as i32,
                    dead_error: if let GeyserSlotStatus::Dead(error) = status {
                        Some(error.clone())
                    } else {
                        None
                    },
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
        .encode_to_vec()
    }

    pub fn encode_raw(&self, created_at: impl Into<Timestamp>) -> Vec<u8> {
        let created_at = created_at.into();

        let size = match self {
            Self::Account { slot, account } => {
                let account = encoding::Account::new(*slot, account);
                message::encoded_len(2, &account)
            }
            Self::Slot {
                slot,
                parent,
                status,
            } => {
                let slot = encoding::Slot::new(*slot, *parent, status);
                message::encoded_len(3, &slot)
            }
            Self::Transaction { slot, transaction } => {
                let transaction = encoding::Transaction::new(*slot, transaction);
                message::encoded_len(4, &transaction)
            }
            Self::BlockMeta { blockinfo } => {
                let blockmeta = encoding::BlockMeta::new(blockinfo);
                message::encoded_len(7, &blockmeta)
            }
            Self::Entry { entry } => {
                let entry = encoding::Entry::new(entry);
                message::encoded_len(8, &entry)
            }
        } + message::encoded_len(11, &created_at);

        let mut vec = Vec::with_capacity(size);
        let buffer = &mut vec;

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
        message::encode(11, &created_at, buffer);

        vec
    }
}

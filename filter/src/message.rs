use {
    prost::Message as _,
    prost_types::Timestamp,
    solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature},
    std::{collections::HashSet, sync::Arc},
    thiserror::Error,
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel as CommitmentLevelProto,
            SubscribeUpdate, SubscribeUpdateAccountInfo, SubscribeUpdateBlockMeta,
            SubscribeUpdateEntry, SubscribeUpdateTransactionInfo,
        },
        solana::storage::confirmed_block::{TransactionError, TransactionStatusMeta},
    },
};

#[derive(Debug, Error)]
pub enum MessageParseError {
    #[error(transparent)]
    Prost(#[from] prost::DecodeError),
    #[error("Field `{0}` should be defined")]
    FieldNotDefined(&'static str),
    #[error("Invalid enum value: {0}")]
    InvalidEnumValue(i32),
    #[error("Invalid pubkey length")]
    InvalidPubkey,
    #[error("Invalid signature length")]
    InvalidSignature,
    #[error("Invalid update: {0}")]
    InvalidUpdateMessage(&'static str),
    #[error("Incompatible encoding")]
    IncompatibleEncoding,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageParserEncoding {
    /// Use optimized parser to extract only required fields
    Limited,
    /// Parse full message with `prost`
    Prost,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Message {
    Slot(MessageSlot),
    Account(MessageAccount),
    Transaction(MessageTransaction),
    Entry(MessageEntry),
    BlockMeta(MessageBlockMeta),
    Block(MessageBlock),
}

impl Message {
    pub fn parse(data: Vec<u8>, parser: MessageParserEncoding) -> Result<Self, MessageParseError> {
        match parser {
            MessageParserEncoding::Limited => todo!(),
            MessageParserEncoding::Prost => {
                let update = SubscribeUpdate::decode(data.as_slice())?;
                MessageParserProst::parse(update)
            }
        }
    }

    pub fn create_block(
        accounts: Vec<Arc<MessageAccount>>,
        transactions: Vec<Arc<MessageTransaction>>,
        entries: Vec<Arc<MessageEntry>>,
        block_meta: Arc<MessageBlockMeta>,
        created_at: impl Into<MessageBlockCreatedAt>,
    ) -> Result<Self, MessageParseError> {
        let created_at = created_at.into();
        let created_at_encoding = created_at.encoding();

        for encoding in std::iter::once(block_meta.encoding())
            .chain(accounts.iter().map(|x| x.encoding()))
            .chain(transactions.iter().map(|x| x.encoding()))
            .chain(entries.iter().map(|x| x.encoding()))
        {
            if encoding != created_at_encoding {
                return Err(MessageParseError::IncompatibleEncoding);
            }
        }

        Ok(Self::unchecked_create_block(
            accounts,
            transactions,
            entries,
            block_meta,
            created_at,
        ))
    }

    pub const fn unchecked_create_block(
        accounts: Vec<Arc<MessageAccount>>,
        transactions: Vec<Arc<MessageTransaction>>,
        entries: Vec<Arc<MessageEntry>>,
        block_meta: Arc<MessageBlockMeta>,
        created_at: MessageBlockCreatedAt,
    ) -> Self {
        Self::Block(MessageBlock {
            accounts,
            transactions,
            entries,
            block_meta,
            created_at,
        })
    }
}

#[derive(Debug)]
pub struct MessageParserProst;

impl MessageParserProst {
    pub fn parse(update: SubscribeUpdate) -> Result<Message, MessageParseError> {
        let created_at = update
            .created_at
            .ok_or(MessageParseError::FieldNotDefined("created_at"))?;

        Ok(
            match update
                .update_oneof
                .ok_or(MessageParseError::FieldNotDefined("update_oneof"))?
            {
                UpdateOneof::Slot(message) => Message::Slot(MessageSlot::Prost {
                    slot: message.slot,
                    parent: message.parent,
                    commitment: CommitmentLevelProto::try_from(message.status)
                        .map_err(|_| MessageParseError::InvalidEnumValue(message.status))?,
                    dead_error: message.dead_error,
                    created_at,
                }),
                UpdateOneof::Account(message) => {
                    let account = message
                        .account
                        .ok_or(MessageParseError::FieldNotDefined("account"))?;
                    Message::Account(MessageAccount::Prost {
                        pubkey: account
                            .pubkey
                            .as_slice()
                            .try_into()
                            .map_err(|_| MessageParseError::InvalidPubkey)?,
                        owner: account
                            .owner
                            .as_slice()
                            .try_into()
                            .map_err(|_| MessageParseError::InvalidPubkey)?,
                        nonempty_txn_signature: account.txn_signature.is_some(),
                        account,
                        slot: message.slot,
                        is_startup: message.is_startup,
                        created_at,
                    })
                }
                UpdateOneof::Transaction(message) => {
                    let transaction = message
                        .transaction
                        .ok_or(MessageParseError::FieldNotDefined("transaction"))?;
                    let meta = transaction
                        .meta
                        .as_ref()
                        .ok_or(MessageParseError::FieldNotDefined("meta"))?;

                    Message::Transaction(MessageTransaction::Prost {
                        signature: transaction
                            .signature
                            .as_slice()
                            .try_into()
                            .map_err(|_| MessageParseError::InvalidSignature)?,
                        error: meta.err.clone(),
                        account_keys: MessageTransaction::gen_account_keys_prost(
                            &transaction,
                            meta,
                        )?,
                        transaction,
                        slot: message.slot,
                        created_at,
                    })
                }
                UpdateOneof::TransactionStatus(_) => {
                    return Err(MessageParseError::InvalidUpdateMessage("TransactionStatus"))
                }
                UpdateOneof::Entry(entry) => {
                    Message::Entry(MessageEntry::Prost { entry, created_at })
                }
                UpdateOneof::BlockMeta(block_meta) => Message::BlockMeta(MessageBlockMeta::Prost {
                    block_meta,
                    created_at,
                }),
                UpdateOneof::Block(message) => {
                    let accounts = message
                        .accounts
                        .into_iter()
                        .map(|account| {
                            Ok(Arc::new(MessageAccount::Prost {
                                pubkey: account
                                    .pubkey
                                    .as_slice()
                                    .try_into()
                                    .map_err(|_| MessageParseError::InvalidPubkey)?,
                                owner: account
                                    .owner
                                    .as_slice()
                                    .try_into()
                                    .map_err(|_| MessageParseError::InvalidPubkey)?,
                                nonempty_txn_signature: account.txn_signature.is_some(),
                                account,
                                slot: message.slot,
                                is_startup: false,
                                created_at,
                            }))
                        })
                        .collect::<Result<_, MessageParseError>>()?;

                    let transactions = message
                        .transactions
                        .into_iter()
                        .map(|transaction| {
                            let meta = transaction
                                .meta
                                .as_ref()
                                .ok_or(MessageParseError::FieldNotDefined("meta"))?;

                            Ok(Arc::new(MessageTransaction::Prost {
                                signature: transaction
                                    .signature
                                    .as_slice()
                                    .try_into()
                                    .map_err(|_| MessageParseError::InvalidSignature)?,
                                error: meta.err.clone(),
                                account_keys: MessageTransaction::gen_account_keys_prost(
                                    &transaction,
                                    meta,
                                )?,
                                transaction,
                                slot: message.slot,
                                created_at,
                            }))
                        })
                        .collect::<Result<_, MessageParseError>>()?;

                    let entries = message
                        .entries
                        .into_iter()
                        .map(|entry| Arc::new(MessageEntry::Prost { entry, created_at }))
                        .collect();

                    Message::Block(MessageBlock {
                        accounts,
                        transactions,
                        entries,
                        block_meta: Arc::new(MessageBlockMeta::Prost {
                            block_meta: SubscribeUpdateBlockMeta {
                                slot: message.slot,
                                blockhash: message.blockhash,
                                rewards: message.rewards,
                                block_time: message.block_time,
                                block_height: message.block_height,
                                parent_slot: message.parent_slot,
                                parent_blockhash: message.parent_blockhash,
                                executed_transaction_count: message.executed_transaction_count,
                                entries_count: message.entries_count,
                            },
                            created_at,
                        }),
                        created_at: created_at.into(),
                    })
                }
                UpdateOneof::Ping(_) => {
                    return Err(MessageParseError::InvalidUpdateMessage("Ping"))
                }
                UpdateOneof::Pong(_) => {
                    return Err(MessageParseError::InvalidUpdateMessage("Pong"))
                }
            },
        )
    }
}

#[derive(Debug, Clone)]
pub enum MessageSlot {
    Limited,
    Prost {
        slot: Slot,
        parent: Option<Slot>,
        commitment: CommitmentLevelProto,
        dead_error: Option<String>,
        created_at: Timestamp,
    },
}

impl MessageSlot {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }

    pub fn commitment(&self) -> CommitmentLevelProto {
        match self {
            Self::Limited => todo!(),
            Self::Prost { commitment, .. } => *commitment,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum MessageAccount {
    Limited,
    Prost {
        pubkey: Pubkey,
        owner: Pubkey,
        nonempty_txn_signature: bool,
        account: SubscribeUpdateAccountInfo,
        slot: Slot,
        is_startup: bool,
        created_at: Timestamp,
    },
}

impl MessageAccount {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }

    pub fn pubkey(&self) -> &Pubkey {
        match self {
            Self::Limited => todo!(),
            Self::Prost { pubkey, .. } => pubkey,
        }
    }

    pub fn owner(&self) -> &Pubkey {
        match self {
            Self::Limited => todo!(),
            Self::Prost { owner, .. } => owner,
        }
    }

    pub fn lamports(&self) -> u64 {
        match self {
            Self::Limited => todo!(),
            Self::Prost { account, .. } => account.lamports,
        }
    }

    pub fn data(&self) -> &[u8] {
        match self {
            Self::Limited => todo!(),
            Self::Prost { account, .. } => &account.data,
        }
    }

    pub fn nonempty_txn_signature(&self) -> bool {
        match self {
            Self::Limited => todo!(),
            Self::Prost {
                nonempty_txn_signature,
                ..
            } => *nonempty_txn_signature,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum MessageTransaction {
    Limited,
    Prost {
        signature: Signature,
        error: Option<TransactionError>,
        account_keys: HashSet<Pubkey>,
        transaction: SubscribeUpdateTransactionInfo,
        slot: Slot,
        created_at: Timestamp,
    },
}

impl MessageTransaction {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }

    fn gen_account_keys_prost(
        transaction: &SubscribeUpdateTransactionInfo,
        meta: &TransactionStatusMeta,
    ) -> Result<HashSet<Pubkey>, MessageParseError> {
        let mut account_keys = HashSet::new();

        // static account keys
        if let Some(pubkeys) = transaction
            .transaction
            .as_ref()
            .ok_or(MessageParseError::FieldNotDefined("transaction"))?
            .message
            .as_ref()
            .map(|msg| msg.account_keys.as_slice())
        {
            for pubkey in pubkeys {
                account_keys.insert(
                    Pubkey::try_from(pubkey.as_slice())
                        .map_err(|_| MessageParseError::InvalidPubkey)?,
                );
            }
        }
        // dynamic account keys
        for pubkey in meta.loaded_writable_addresses.iter() {
            account_keys.insert(
                Pubkey::try_from(pubkey.as_slice())
                    .map_err(|_| MessageParseError::InvalidPubkey)?,
            );
        }
        for pubkey in meta.loaded_readonly_addresses.iter() {
            account_keys.insert(
                Pubkey::try_from(pubkey.as_slice())
                    .map_err(|_| MessageParseError::InvalidPubkey)?,
            );
        }

        Ok(account_keys)
    }

    pub fn vote(&self) -> bool {
        match self {
            Self::Limited => todo!(),
            Self::Prost { transaction, .. } => transaction.is_vote,
        }
    }

    pub fn failed(&self) -> bool {
        match self {
            Self::Limited => todo!(),
            Self::Prost { error, .. } => error.is_some(),
        }
    }

    pub fn signature(&self) -> &Signature {
        match self {
            Self::Limited => todo!(),
            Self::Prost { signature, .. } => signature,
        }
    }

    pub fn account_keys(&self) -> &HashSet<Pubkey> {
        match self {
            Self::Limited => todo!(),
            Self::Prost { account_keys, .. } => account_keys,
        }
    }
}

#[derive(Debug, Clone)]
pub enum MessageEntry {
    Limited,
    Prost {
        entry: SubscribeUpdateEntry,
        created_at: Timestamp,
    },
}

impl MessageEntry {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }
}

#[derive(Debug, Clone)]
pub enum MessageBlockMeta {
    Limited,
    Prost {
        block_meta: SubscribeUpdateBlockMeta,
        created_at: Timestamp,
    },
}

impl MessageBlockMeta {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageBlock {
    pub accounts: Vec<Arc<MessageAccount>>,
    pub transactions: Vec<Arc<MessageTransaction>>,
    pub entries: Vec<Arc<MessageEntry>>,
    pub block_meta: Arc<MessageBlockMeta>,
    pub created_at: MessageBlockCreatedAt,
}

impl MessageBlock {
    pub const fn encoding(&self) -> MessageParserEncoding {
        self.created_at.encoding()
    }
}

#[derive(Debug, Clone)]
pub enum MessageBlockCreatedAt {
    Limited,
    Prost(Timestamp),
}

impl From<Timestamp> for MessageBlockCreatedAt {
    fn from(value: Timestamp) -> Self {
        Self::Prost(value)
    }
}

impl MessageBlockCreatedAt {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited => MessageParserEncoding::Limited,
            Self::Prost(_) => MessageParserEncoding::Prost,
        }
    }
}

use {
    crate::protobuf::decode::{
        LimitedDecode, SubscribeUpdateLimitedDecode, UpdateOneofLimitedDecode,
        UpdateOneofLimitedDecodeAccount, UpdateOneofLimitedDecodeEntry,
        UpdateOneofLimitedDecodeSlot, UpdateOneofLimitedDecodeTransaction,
    },
    prost::Message as _,
    prost_types::Timestamp,
    richat_proto::{
        convert_from,
        geyser::{
            subscribe_update::UpdateOneof, SlotStatus, SubscribeUpdate, SubscribeUpdateAccountInfo,
            SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdateTransactionInfo,
        },
        solana::storage::confirmed_block::{Transaction, TransactionError, TransactionStatusMeta},
    },
    serde::{Deserialize, Serialize},
    solana_account::ReadableAccount,
    solana_sdk::{
        clock::{Epoch, Slot},
        pubkey::{Pubkey, PUBKEY_BYTES},
        signature::{Signature, SIGNATURE_BYTES},
    },
    solana_transaction_status::{
        ConfirmedBlock, TransactionWithStatusMeta, VersionedTransactionWithStatusMeta,
    },
    std::{collections::HashSet, ops::Range, sync::Arc},
    thiserror::Error,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageParserEncoding {
    /// Use optimized parser to extract only required fields
    Limited,
    /// Parse full message with `prost`
    Prost,
}

#[derive(Debug, Clone, Copy)]
pub enum MessageRef<'a> {
    Slot(&'a MessageSlot),
    Account(&'a MessageAccount),
    Transaction(&'a MessageTransaction),
    Entry(&'a MessageEntry),
    BlockMeta(&'a MessageBlockMeta),
    Block(&'a MessageBlock),
}

impl<'a> From<&'a Message> for MessageRef<'a> {
    fn from(message: &'a Message) -> Self {
        match message {
            Message::Slot(msg) => Self::Slot(msg),
            Message::Account(msg) => Self::Account(msg),
            Message::Transaction(msg) => Self::Transaction(msg),
            Message::Entry(msg) => Self::Entry(msg),
            Message::BlockMeta(msg) => Self::BlockMeta(msg),
            Message::Block(msg) => Self::Block(msg),
        }
    }
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
            MessageParserEncoding::Limited => MessageParserLimited::parse(data),
            MessageParserEncoding::Prost => MessageParserProst::parse(data),
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

        Ok(Self::Block(Self::unchecked_create_block(
            accounts,
            transactions,
            entries,
            block_meta,
            created_at,
        )))
    }

    pub const fn unchecked_create_block(
        accounts: Vec<Arc<MessageAccount>>,
        transactions: Vec<Arc<MessageTransaction>>,
        entries: Vec<Arc<MessageEntry>>,
        block_meta: Arc<MessageBlockMeta>,
        created_at: MessageBlockCreatedAt,
    ) -> MessageBlock {
        MessageBlock {
            accounts,
            transactions,
            entries,
            block_meta,
            created_at,
        }
    }

    pub fn slot(&self) -> Slot {
        match self {
            Self::Slot(msg) => msg.slot(),
            Self::Account(msg) => msg.slot(),
            Self::Transaction(msg) => msg.slot(),
            Self::Entry(msg) => msg.slot(),
            Self::BlockMeta(msg) => msg.slot(),
            Self::Block(msg) => msg.slot(),
        }
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        match self {
            Self::Slot(msg) => msg.created_at(),
            Self::Account(msg) => msg.created_at(),
            Self::Transaction(msg) => msg.created_at(),
            Self::Entry(msg) => msg.created_at(),
            Self::BlockMeta(msg) => msg.created_at(),
            Self::Block(msg) => msg.created_at(),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Slot(msg) => msg.size(),
            Self::Account(msg) => msg.size(),
            Self::Transaction(msg) => msg.size(),
            Self::Entry(msg) => msg.size(),
            Self::BlockMeta(msg) => msg.size(),
            Self::Block(msg) => msg.size(),
        }
    }
}

#[derive(Debug)]
pub struct MessageParserLimited;

impl MessageParserLimited {
    pub fn parse(data: Vec<u8>) -> Result<Message, MessageParseError> {
        let update = SubscribeUpdateLimitedDecode::decode(data.as_slice())?;
        let created_at = update
            .created_at
            .ok_or(MessageParseError::FieldNotDefined("created_at"))?;

        Ok(
            match update
                .update_oneof
                .ok_or(MessageParseError::FieldNotDefined("update_oneof"))?
            {
                UpdateOneofLimitedDecode::Slot(range) => {
                    let message = UpdateOneofLimitedDecodeSlot::decode(
                        &data.as_slice()[range.start..range.end],
                    )?;
                    Message::Slot(MessageSlot::Limited {
                        slot: message.slot,
                        parent: message.parent,
                        status: SlotStatus::try_from(message.status)
                            .map_err(|_| MessageParseError::InvalidEnumValue(message.status))?,
                        dead_error: message.dead_error,
                        created_at,
                        buffer: data,
                        range,
                    })
                }
                UpdateOneofLimitedDecode::Account(range) => {
                    let message = UpdateOneofLimitedDecodeAccount::decode(
                        &data.as_slice()[range.start..range.end],
                    )?;

                    if !message.account {
                        return Err(MessageParseError::FieldNotDefined("account"));
                    }

                    let mut data_range = message.data;
                    data_range.start += range.start;
                    data_range.end += range.start;

                    Message::Account(MessageAccount::Limited {
                        pubkey: message.pubkey,
                        owner: message.owner,
                        lamports: message.lamports,
                        executable: message.executable,
                        rent_epoch: message.rent_epoch,
                        data: data_range,
                        txn_signature_offset: message.txn_signature_offset,
                        write_version: message.write_version,
                        slot: message.slot,
                        is_startup: message.is_startup,
                        created_at,
                        buffer: data,
                        range,
                    })
                }
                UpdateOneofLimitedDecode::Transaction(range) => {
                    let message = UpdateOneofLimitedDecodeTransaction::decode(
                        &data.as_slice()[range.start..range.end],
                    )?;
                    let mut transaction_range = message
                        .transaction
                        .ok_or(MessageParseError::FieldNotDefined("transaction"))?;
                    transaction_range.start += range.start;
                    transaction_range.end += range.start;

                    let transaction = SubscribeUpdateTransactionInfo::decode(
                        &data.as_slice()[transaction_range.start..transaction_range.end],
                    )?;

                    let meta = transaction
                        .meta
                        .as_ref()
                        .ok_or(MessageParseError::FieldNotDefined("meta"))?;

                    let account_keys =
                        MessageTransaction::gen_account_keys_prost(&transaction, meta)?;

                    Message::Transaction(MessageTransaction::Limited {
                        signature: transaction
                            .signature
                            .as_slice()
                            .try_into()
                            .map_err(|_| MessageParseError::InvalidSignature)?,
                        error: meta.err.clone(),
                        account_keys,
                        transaction_range,
                        transaction,
                        slot: message.slot,
                        created_at,
                        buffer: data,
                        range,
                    })
                }
                UpdateOneofLimitedDecode::TransactionStatus(_) => {
                    return Err(MessageParseError::InvalidUpdateMessage("TransactionStatus"))
                }
                UpdateOneofLimitedDecode::Entry(range) => {
                    let entry = UpdateOneofLimitedDecodeEntry::decode(
                        &data.as_slice()[range.start..range.end],
                    )?;
                    Message::Entry(MessageEntry::Limited {
                        slot: entry.slot,
                        index: entry.index,
                        executed_transaction_count: entry.executed_transaction_count,
                        created_at,
                        buffer: data,
                        range,
                    })
                }
                UpdateOneofLimitedDecode::BlockMeta(range) => {
                    let block_meta =
                        SubscribeUpdateBlockMeta::decode(&data.as_slice()[range.start..range.end])?;

                    let block_height = block_meta
                        .block_height
                        .map(|v| v.block_height)
                        .ok_or(MessageParseError::FieldNotDefined("block_height"))?;

                    Message::BlockMeta(MessageBlockMeta::Limited {
                        block_meta,
                        block_height,
                        created_at,
                        buffer: data,
                        range,
                    })
                }
                UpdateOneofLimitedDecode::Block(_) => {
                    return Err(MessageParseError::InvalidUpdateMessage("Block"))
                }
                UpdateOneofLimitedDecode::Ping(_) => {
                    return Err(MessageParseError::InvalidUpdateMessage("Ping"))
                }
                UpdateOneofLimitedDecode::Pong(_) => {
                    return Err(MessageParseError::InvalidUpdateMessage("Pong"))
                }
            },
        )
    }
}

#[derive(Debug)]
pub struct MessageParserProst;

impl MessageParserProst {
    pub fn parse(data: Vec<u8>) -> Result<Message, MessageParseError> {
        let update = SubscribeUpdate::decode(data.as_slice())?;
        let encoded_len = data.len();

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
                    status: SlotStatus::try_from(message.status)
                        .map_err(|_| MessageParseError::InvalidEnumValue(message.status))?,
                    dead_error: message.dead_error,
                    created_at,
                    size: encoded_len,
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
                        account,
                        slot: message.slot,
                        is_startup: message.is_startup,
                        created_at,
                        size: PUBKEY_BYTES + PUBKEY_BYTES + encoded_len + 20,
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

                    let account_keys =
                        MessageTransaction::gen_account_keys_prost(&transaction, meta)?;
                    let account_keys_capacity = account_keys.capacity();

                    Message::Transaction(MessageTransaction::Prost {
                        signature: transaction
                            .signature
                            .as_slice()
                            .try_into()
                            .map_err(|_| MessageParseError::InvalidSignature)?,
                        error: meta.err.clone(),
                        account_keys,
                        transaction,
                        slot: message.slot,
                        created_at,
                        size: encoded_len + SIGNATURE_BYTES + account_keys_capacity * PUBKEY_BYTES,
                    })
                }
                UpdateOneof::TransactionStatus(_) => {
                    return Err(MessageParseError::InvalidUpdateMessage("TransactionStatus"))
                }
                UpdateOneof::Entry(entry) => Message::Entry(MessageEntry::Prost {
                    entry,
                    created_at,
                    size: encoded_len,
                }),
                UpdateOneof::BlockMeta(block_meta) => {
                    let block_height = block_meta
                        .block_height
                        .map(|v| v.block_height)
                        .ok_or(MessageParseError::FieldNotDefined("block_height"))?;
                    Message::BlockMeta(MessageBlockMeta::Prost {
                        block_meta,
                        block_height,
                        created_at,
                        size: encoded_len,
                    })
                }
                UpdateOneof::Block(message) => {
                    let accounts = message
                        .accounts
                        .into_iter()
                        .map(|account| {
                            let encoded_len = account.encoded_len();
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
                                account,
                                slot: message.slot,
                                is_startup: false,
                                created_at,
                                size: PUBKEY_BYTES + PUBKEY_BYTES + encoded_len + 32,
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

                            let account_keys =
                                MessageTransaction::gen_account_keys_prost(&transaction, meta)?;
                            let account_keys_capacity = account_keys.capacity();

                            Ok(Arc::new(MessageTransaction::Prost {
                                signature: transaction
                                    .signature
                                    .as_slice()
                                    .try_into()
                                    .map_err(|_| MessageParseError::InvalidSignature)?,
                                error: meta.err.clone(),
                                account_keys,
                                transaction,
                                slot: message.slot,
                                created_at,
                                size: encoded_len
                                    + SIGNATURE_BYTES
                                    + account_keys_capacity * PUBKEY_BYTES,
                            }))
                        })
                        .collect::<Result<_, MessageParseError>>()?;

                    let entries = message
                        .entries
                        .into_iter()
                        .map(|entry| {
                            let encoded_len = entry.encoded_len();
                            Arc::new(MessageEntry::Prost {
                                entry,
                                created_at,
                                size: encoded_len,
                            })
                        })
                        .collect();

                    let block_meta = SubscribeUpdateBlockMeta {
                        slot: message.slot,
                        blockhash: message.blockhash,
                        rewards: message.rewards,
                        block_time: message.block_time,
                        block_height: message.block_height,
                        parent_slot: message.parent_slot,
                        parent_blockhash: message.parent_blockhash,
                        executed_transaction_count: message.executed_transaction_count,
                        entries_count: message.entries_count,
                    };
                    let encoded_len = block_meta.encoded_len();
                    let block_height = block_meta
                        .block_height
                        .map(|v| v.block_height)
                        .ok_or(MessageParseError::FieldNotDefined("block_height"))?;

                    Message::Block(MessageBlock {
                        accounts,
                        transactions,
                        entries,
                        block_meta: Arc::new(MessageBlockMeta::Prost {
                            block_meta,
                            block_height,
                            created_at,
                            size: encoded_len,
                        }),
                        created_at: MessageBlockCreatedAt::Prost(created_at),
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
    Limited {
        slot: Slot,
        parent: Option<Slot>,
        status: SlotStatus,
        dead_error: Option<Range<usize>>,
        created_at: Timestamp,
        buffer: Vec<u8>,
        range: Range<usize>,
    },
    Prost {
        slot: Slot,
        parent: Option<Slot>,
        status: SlotStatus,
        dead_error: Option<String>,
        created_at: Timestamp,
        size: usize,
    },
}

impl MessageSlot {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited { .. } => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        match self {
            Self::Limited { created_at, .. } => MessageBlockCreatedAt::Limited(*created_at),
            Self::Prost { created_at, .. } => MessageBlockCreatedAt::Prost(*created_at),
        }
    }

    pub const fn slot(&self) -> Slot {
        match self {
            Self::Limited { slot, .. } => *slot,
            Self::Prost { slot, .. } => *slot,
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Limited { buffer, .. } => buffer.len() + 64,
            Self::Prost { size, .. } => *size,
        }
    }

    pub const fn status(&self) -> SlotStatus {
        match self {
            Self::Limited { status, .. } => *status,
            Self::Prost { status, .. } => *status,
        }
    }

    pub const fn parent(&self) -> Option<Slot> {
        match self {
            Self::Limited { parent, .. } => *parent,
            Self::Prost { parent, .. } => *parent,
        }
    }

    pub fn dead_error(&self) -> Option<&str> {
        match self {
            Self::Limited {
                dead_error, buffer, ..
            } => dead_error.as_ref().map(|range| unsafe {
                std::str::from_utf8_unchecked(&buffer.as_slice()[range.start..range.end])
            }),
            Self::Prost { dead_error, .. } => dead_error.as_deref(),
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum MessageAccount {
    Limited {
        pubkey: Pubkey,
        owner: Pubkey,
        lamports: u64,
        executable: bool,
        rent_epoch: Epoch,
        data: Range<usize>,
        txn_signature_offset: Option<usize>,
        write_version: u64,
        slot: Slot,
        is_startup: bool,
        created_at: Timestamp,
        buffer: Vec<u8>,
        range: Range<usize>,
    },
    Prost {
        pubkey: Pubkey,
        owner: Pubkey,
        account: SubscribeUpdateAccountInfo,
        slot: Slot,
        is_startup: bool,
        created_at: Timestamp,
        size: usize,
    },
}

impl MessageAccount {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited { .. } => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }

    pub const fn slot(&self) -> Slot {
        match self {
            Self::Limited { slot, .. } => *slot,
            Self::Prost { slot, .. } => *slot,
        }
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        match self {
            Self::Limited { created_at, .. } => MessageBlockCreatedAt::Limited(*created_at),
            Self::Prost { created_at, .. } => MessageBlockCreatedAt::Prost(*created_at),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Limited { buffer, .. } => buffer.len() + PUBKEY_BYTES * 2 + 86,
            Self::Prost { size, .. } => *size,
        }
    }

    pub const fn pubkey(&self) -> &Pubkey {
        match self {
            Self::Limited { pubkey, .. } => pubkey,
            Self::Prost { pubkey, .. } => pubkey,
        }
    }

    pub const fn write_version(&self) -> u64 {
        match self {
            Self::Limited { write_version, .. } => *write_version,
            Self::Prost { account, .. } => account.write_version,
        }
    }

    pub const fn nonempty_txn_signature(&self) -> bool {
        match self {
            Self::Limited {
                txn_signature_offset,
                ..
            } => txn_signature_offset.is_some(),
            Self::Prost { account, .. } => account.txn_signature.is_some(),
        }
    }
}

impl ReadableAccount for MessageAccount {
    fn lamports(&self) -> u64 {
        match self {
            Self::Limited { lamports, .. } => *lamports,
            Self::Prost { account, .. } => account.lamports,
        }
    }

    fn data(&self) -> &[u8] {
        match self {
            Self::Limited { data, buffer, .. } => &buffer.as_slice()[data.start..data.end],
            Self::Prost { account, .. } => &account.data,
        }
    }

    fn owner(&self) -> &Pubkey {
        match self {
            Self::Limited { owner, .. } => owner,
            Self::Prost { owner, .. } => owner,
        }
    }

    fn executable(&self) -> bool {
        match self {
            Self::Limited { executable, .. } => *executable,
            Self::Prost { account, .. } => account.executable,
        }
    }

    fn rent_epoch(&self) -> Epoch {
        match self {
            Self::Limited { rent_epoch, .. } => *rent_epoch,
            Self::Prost { account, .. } => account.rent_epoch,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum MessageTransaction {
    Limited {
        signature: Signature,
        error: Option<TransactionError>,
        account_keys: HashSet<Pubkey>,
        transaction_range: Range<usize>,
        transaction: SubscribeUpdateTransactionInfo,
        slot: Slot,
        created_at: Timestamp,
        buffer: Vec<u8>,
        range: Range<usize>,
    },
    Prost {
        signature: Signature,
        error: Option<TransactionError>,
        account_keys: HashSet<Pubkey>,
        transaction: SubscribeUpdateTransactionInfo,
        slot: Slot,
        created_at: Timestamp,
        size: usize,
    },
}

impl MessageTransaction {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited { .. } => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }

    pub const fn slot(&self) -> Slot {
        match self {
            Self::Limited { slot, .. } => *slot,
            Self::Prost { slot, .. } => *slot,
        }
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        match self {
            Self::Limited { created_at, .. } => MessageBlockCreatedAt::Limited(*created_at),
            Self::Prost { created_at, .. } => MessageBlockCreatedAt::Prost(*created_at),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Limited {
                account_keys,
                buffer,
                ..
            } => buffer.len() * 2 + account_keys.capacity() * PUBKEY_BYTES,
            Self::Prost { size, .. } => *size,
        }
    }

    pub fn gen_account_keys_prost(
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

    pub const fn signature(&self) -> &Signature {
        match self {
            Self::Limited { signature, .. } => signature,
            Self::Prost { signature, .. } => signature,
        }
    }

    pub const fn vote(&self) -> bool {
        match self {
            Self::Limited { transaction, .. } => transaction.is_vote,
            Self::Prost { transaction, .. } => transaction.is_vote,
        }
    }

    pub const fn index(&self) -> u64 {
        match self {
            Self::Limited { transaction, .. } => transaction.index,
            Self::Prost { transaction, .. } => transaction.index,
        }
    }

    pub const fn failed(&self) -> bool {
        match self {
            Self::Limited { error, .. } => error.is_some(),
            Self::Prost { error, .. } => error.is_some(),
        }
    }

    pub const fn error(&self) -> &Option<TransactionError> {
        match self {
            Self::Limited { error, .. } => error,
            Self::Prost { error, .. } => error,
        }
    }

    pub fn transaction(&self) -> Result<&Transaction, &'static str> {
        match self {
            Self::Limited { transaction, .. } => {
                transaction.transaction.as_ref().ok_or("FieldNotDefined")
            }
            Self::Prost { transaction, .. } => {
                transaction.transaction.as_ref().ok_or("FieldNotDefined")
            }
        }
    }

    pub fn transaction_meta(&self) -> Result<&TransactionStatusMeta, &'static str> {
        match self {
            Self::Limited { transaction, .. } => transaction.meta.as_ref().ok_or("FieldNotDefined"),
            Self::Prost { transaction, .. } => transaction.meta.as_ref().ok_or("FieldNotDefined"),
        }
    }

    pub fn as_versioned_transaction_with_status_meta(
        &self,
    ) -> Result<VersionedTransactionWithStatusMeta, &'static str> {
        Ok(VersionedTransactionWithStatusMeta {
            transaction: convert_from::create_tx_versioned(self.transaction()?.clone())?,
            meta: convert_from::create_tx_meta(self.transaction_meta()?.clone())?,
        })
    }

    pub const fn account_keys(&self) -> &HashSet<Pubkey> {
        match self {
            Self::Limited { account_keys, .. } => account_keys,
            Self::Prost { account_keys, .. } => account_keys,
        }
    }
}

#[derive(Debug, Clone)]
pub enum MessageEntry {
    Limited {
        slot: Slot,
        index: u64,
        executed_transaction_count: u64,
        created_at: Timestamp,
        buffer: Vec<u8>,
        range: Range<usize>,
    },
    Prost {
        entry: SubscribeUpdateEntry,
        created_at: Timestamp,
        size: usize,
    },
}

impl MessageEntry {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited { .. } => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }

    pub const fn slot(&self) -> Slot {
        match self {
            Self::Limited { slot, .. } => *slot,
            Self::Prost { entry, .. } => entry.slot,
        }
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        match self {
            Self::Limited { created_at, .. } => MessageBlockCreatedAt::Limited(*created_at),
            Self::Prost { created_at, .. } => MessageBlockCreatedAt::Prost(*created_at),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Limited { buffer, .. } => buffer.len() + 52,
            Self::Prost { size, .. } => *size,
        }
    }

    pub const fn index(&self) -> u64 {
        match self {
            Self::Limited { index, .. } => *index,
            Self::Prost { entry, .. } => entry.index,
        }
    }

    pub const fn executed_transaction_count(&self) -> u64 {
        match self {
            Self::Limited {
                executed_transaction_count,
                ..
            } => *executed_transaction_count,
            Self::Prost { entry, .. } => entry.executed_transaction_count,
        }
    }
}

#[derive(Debug, Clone)]
pub enum MessageBlockMeta {
    Limited {
        block_meta: SubscribeUpdateBlockMeta,
        block_height: Slot,
        created_at: Timestamp,
        buffer: Vec<u8>,
        range: Range<usize>,
    },
    Prost {
        block_meta: SubscribeUpdateBlockMeta,
        block_height: Slot,
        created_at: Timestamp,
        size: usize,
    },
}

impl MessageBlockMeta {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited { .. } => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }

    pub const fn slot(&self) -> Slot {
        match self {
            Self::Limited { block_meta, .. } => block_meta.slot,
            Self::Prost { block_meta, .. } => block_meta.slot,
        }
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        match self {
            Self::Limited { created_at, .. } => MessageBlockCreatedAt::Limited(*created_at),
            Self::Prost { created_at, .. } => MessageBlockCreatedAt::Prost(*created_at),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Limited { buffer, .. } => buffer.len() * 2,
            Self::Prost { size, .. } => *size,
        }
    }

    pub fn blockhash(&self) -> &str {
        match self {
            Self::Limited { block_meta, .. } => &block_meta.blockhash,
            Self::Prost { block_meta, .. } => &block_meta.blockhash,
        }
    }

    pub const fn block_height(&self) -> Slot {
        match self {
            Self::Limited { block_height, .. } => *block_height,
            Self::Prost { block_height, .. } => *block_height,
        }
    }

    pub const fn executed_transaction_count(&self) -> u64 {
        match self {
            Self::Limited { block_meta, .. } => block_meta.executed_transaction_count,
            Self::Prost { block_meta, .. } => block_meta.executed_transaction_count,
        }
    }

    pub const fn entries_count(&self) -> u64 {
        match self {
            Self::Limited { block_meta, .. } => block_meta.entries_count,
            Self::Prost { block_meta, .. } => block_meta.entries_count,
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

    pub fn slot(&self) -> Slot {
        self.block_meta.as_ref().slot()
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        self.created_at
    }

    pub fn size(&self) -> usize {
        self.accounts
            .iter()
            .map(|m| m.size())
            .chain(self.transactions.iter().map(|m| m.size()))
            .chain(self.entries.iter().map(|m| m.size()))
            .sum::<usize>()
            + self.block_meta.size()
    }

    pub fn as_confirmed_block(&self) -> Result<ConfirmedBlock, &'static str> {
        Ok(match self.block_meta.as_ref() {
            MessageBlockMeta::Limited { block_meta, .. } => ConfirmedBlock {
                previous_blockhash: block_meta.parent_blockhash.clone(),
                blockhash: block_meta.blockhash.clone(),
                parent_slot: block_meta.parent_slot,
                transactions: self
                    .transactions
                    .iter()
                    .map(|tx| {
                        tx.as_versioned_transaction_with_status_meta()
                            .map(TransactionWithStatusMeta::Complete)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                rewards: block_meta
                    .rewards
                    .as_ref()
                    .map(|r| {
                        r.rewards
                            .iter()
                            .cloned()
                            .map(convert_from::create_reward)
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .transpose()?
                    .unwrap_or_default(),
                num_partitions: block_meta
                    .rewards
                    .as_ref()
                    .and_then(|r| r.num_partitions)
                    .map(|np| np.num_partitions),
                block_time: block_meta.block_time.map(|bt| bt.timestamp),
                block_height: block_meta.block_height.map(|bh| bh.block_height),
            },
            MessageBlockMeta::Prost { block_meta, .. } => ConfirmedBlock {
                previous_blockhash: block_meta.parent_blockhash.clone(),
                blockhash: block_meta.blockhash.clone(),
                parent_slot: block_meta.parent_slot,
                transactions: self
                    .transactions
                    .iter()
                    .map(|tx| {
                        tx.as_versioned_transaction_with_status_meta()
                            .map(TransactionWithStatusMeta::Complete)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                rewards: block_meta
                    .rewards
                    .as_ref()
                    .map(|r| {
                        r.rewards
                            .iter()
                            .cloned()
                            .map(convert_from::create_reward)
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .transpose()?
                    .unwrap_or_default(),
                num_partitions: block_meta
                    .rewards
                    .as_ref()
                    .and_then(|r| r.num_partitions)
                    .map(|np| np.num_partitions),
                block_time: block_meta.block_time.map(|bt| bt.timestamp),
                block_height: block_meta.block_height.map(|bh| bh.block_height),
            },
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageBlockCreatedAt {
    Limited(Timestamp),
    Prost(Timestamp),
}

impl From<MessageBlockCreatedAt> for Timestamp {
    fn from(value: MessageBlockCreatedAt) -> Self {
        match value {
            MessageBlockCreatedAt::Limited(timestamp) => timestamp,
            MessageBlockCreatedAt::Prost(timestamp) => timestamp,
        }
    }
}

impl MessageBlockCreatedAt {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited(_) => MessageParserEncoding::Limited,
            Self::Prost(_) => MessageParserEncoding::Prost,
        }
    }

    pub const fn as_millis(&self) -> u64 {
        match self {
            Self::Limited(ts) => ts.seconds as u64 * 1_000 + (ts.nanos / 1_000_000) as u64,
            Self::Prost(ts) => ts.seconds as u64 * 1_000 + (ts.nanos / 1_000_000) as u64,
        }
    }
}

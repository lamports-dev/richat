use {
    crate::{
        filter::{FilteredUpdate, FilteredUpdateType},
        protobuf::SubscribeUpdateMessage,
    },
    prost::Message as _,
    prost_types::Timestamp,
    solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature},
    std::collections::HashSet,
    thiserror::Error,
    yellowstone_grpc_proto::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel as CommitmentLevelProto,
            SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
            SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdateSlot,
            SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
            SubscribeUpdateTransactionStatus,
        },
        solana::storage::confirmed_block::TransactionError,
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
}

#[derive(Debug, Error)]
pub enum MessageEncodeError {
    #[error("Filtered update doesn't match existed message")]
    MessageMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseType {
    /// Use optimized parser to extract only required fields
    Limited,
    /// Parse full message with `prost`
    Prost,
}

#[derive(Debug, Clone)]
pub enum Message {
    Limited(MessageParsedLimited),
    Prost(MessageParsedProst),
}

impl Message {
    pub fn parse(data: Vec<u8>, parse_type: ParseType) -> Result<Self, MessageParseError> {
        match parse_type {
            ParseType::Limited => MessageParsedLimited::new(data).map(Self::Limited),
            ParseType::Prost => {
                let update = SubscribeUpdate::decode(data.as_slice())?;
                MessageParsedProst::new(
                    update
                        .update_oneof
                        .ok_or(MessageParseError::FieldNotDefined("update_oneof"))?,
                    update
                        .created_at
                        .ok_or(MessageParseError::FieldNotDefined("created_at"))?,
                )
                .map(Self::Prost)
            }
        }
    }

    pub fn get_values(&self) -> MessageValues {
        match self {
            Self::Limited(_parsed) => todo!(),
            Self::Prost(parsed) => match &parsed.message {
                MessageParsedProstEnum::Slot { commitment, .. } => {
                    MessageValues::Slot(MessageValuesSlot {
                        commitment: *commitment,
                    })
                }
                MessageParsedProstEnum::Account {
                    pubkey,
                    owner,
                    nonempty_txn_signature,
                    account,
                    ..
                } => MessageValues::Account(MessageValuesAccount {
                    pubkey: *pubkey,
                    owner: *owner,
                    lamports: account.lamports,
                    nonempty_txn_signature: *nonempty_txn_signature,
                    data: account.data.as_slice(),
                }),
                MessageParsedProstEnum::Transaction {
                    signature,
                    error,
                    account_keys,
                    transaction,
                    ..
                } => MessageValues::Transaction(MessageValuesTransaction {
                    vote: transaction.is_vote,
                    failed: error.is_some(),
                    signature: *signature,
                    account_keys,
                }),
                MessageParsedProstEnum::Entry(_) => MessageValues::Entry,
                MessageParsedProstEnum::BlockMeta(_) => MessageValues::BlockMeta,
            },
        }
    }

    pub fn encode(&self, update: &FilteredUpdate) -> Result<Vec<u8>, MessageEncodeError> {
        Ok(match self {
            Self::Limited(_parsed) => todo!(),
            Self::Prost(parsed) => {
                SubscribeUpdateMessage {
                    filters: &update.filters,
                    update_oneof: Some(match (&update.filtered_update, &parsed.message) {
                        (
                            FilteredUpdateType::Slot,
                            MessageParsedProstEnum::Slot {
                                slot,
                                parent,
                                commitment,
                                dead_error,
                            },
                        ) => UpdateOneof::Slot(SubscribeUpdateSlot {
                            slot: *slot,
                            parent: *parent,
                            status: *commitment as i32,
                            dead_error: dead_error.clone(),
                        }),
                        (
                            FilteredUpdateType::Account(data_slices),
                            MessageParsedProstEnum::Account {
                                account,
                                slot,
                                is_startup,
                                ..
                            },
                        ) => UpdateOneof::Account(SubscribeUpdateAccount {
                            account: Some(account.clone()), // data_slice todo
                            slot: *slot,
                            is_startup: *is_startup,
                        }),
                        (
                            FilteredUpdateType::Transaction,
                            MessageParsedProstEnum::Transaction {
                                transaction, slot, ..
                            },
                        ) => UpdateOneof::Transaction(SubscribeUpdateTransaction {
                            transaction: Some(transaction.clone()),
                            slot: *slot,
                        }),
                        (
                            FilteredUpdateType::TransactionStatus,
                            MessageParsedProstEnum::Transaction {
                                signature,
                                error,
                                transaction,
                                slot,
                                ..
                            },
                        ) => UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
                            slot: *slot,
                            signature: signature.as_ref().to_vec(),
                            is_vote: transaction.is_vote,
                            index: transaction.index,
                            err: error.clone(),
                        }),
                        (FilteredUpdateType::Entry, MessageParsedProstEnum::Entry(msg)) => {
                            UpdateOneof::Entry(msg.clone())
                        }
                        (FilteredUpdateType::BlockMeta, MessageParsedProstEnum::BlockMeta(msg)) => {
                            UpdateOneof::BlockMeta(msg.clone())
                        }
                        // (FilteredUpdateType::Block, MessageParsedProstEnum::Block(???)) => {}
                        _ => {
                            return Err(MessageEncodeError::MessageMismatch);
                        }
                    }),
                    created_at: Some(parsed.created_at),
                }
                .encode_to_vec()
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct MessageParsedLimited;

impl MessageParsedLimited {
    pub fn new(_data: Vec<u8>) -> Result<Self, MessageParseError> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct MessageParsedProst {
    message: MessageParsedProstEnum,
    created_at: Timestamp,
}

impl MessageParsedProst {
    pub fn new(value: UpdateOneof, created_at: Timestamp) -> Result<Self, MessageParseError> {
        value.try_into().map(|message| Self {
            message,
            created_at,
        })
    }
}

#[derive(Debug, Clone)]
enum MessageParsedProstEnum {
    Slot {
        slot: Slot,
        parent: Option<Slot>,
        commitment: CommitmentLevelProto,
        dead_error: Option<String>,
    },
    Account {
        pubkey: Pubkey,
        owner: Pubkey,
        nonempty_txn_signature: bool,
        account: SubscribeUpdateAccountInfo,
        slot: Slot,
        is_startup: bool,
    },
    Transaction {
        signature: Signature,
        error: Option<TransactionError>,
        account_keys: HashSet<Pubkey>,
        transaction: SubscribeUpdateTransactionInfo,
        slot: Slot,
    },
    Entry(SubscribeUpdateEntry),
    BlockMeta(SubscribeUpdateBlockMeta),
    // Block(Arc<MessageBlock>),
}

impl TryFrom<UpdateOneof> for MessageParsedProstEnum {
    type Error = MessageParseError;

    fn try_from(value: UpdateOneof) -> Result<Self, Self::Error> {
        Ok(match value {
            UpdateOneof::Slot(msg) => Self::Slot {
                slot: msg.slot,
                parent: msg.parent,
                commitment: CommitmentLevelProto::try_from(msg.status)
                    .map_err(|_| MessageParseError::InvalidEnumValue(msg.status))?,
                dead_error: msg.dead_error,
            },
            UpdateOneof::Account(msg) => {
                let account = msg
                    .account
                    .ok_or(MessageParseError::FieldNotDefined("account"))?;
                Self::Account {
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
                    slot: msg.slot,
                    is_startup: msg.is_startup,
                }
            }
            UpdateOneof::Transaction(msg) => {
                let transaction = msg
                    .transaction
                    .ok_or(MessageParseError::FieldNotDefined("transaction"))?;
                let meta = transaction
                    .meta
                    .as_ref()
                    .ok_or(MessageParseError::FieldNotDefined("meta"))?;
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

                Self::Transaction {
                    signature: transaction
                        .signature
                        .as_slice()
                        .try_into()
                        .map_err(|_| MessageParseError::InvalidSignature)?,
                    error: meta.err.clone(),
                    account_keys,
                    transaction,
                    slot: msg.slot,
                }
            }
            UpdateOneof::TransactionStatus(_) => {
                return Err(MessageParseError::InvalidUpdateMessage("TransactionStatus"))
            }
            UpdateOneof::Entry(msg) => Self::Entry(msg),
            UpdateOneof::BlockMeta(msg) => Self::BlockMeta(msg),
            UpdateOneof::Block(msg) => todo!(),
            UpdateOneof::Ping(_) => return Err(MessageParseError::InvalidUpdateMessage("Ping")),
            UpdateOneof::Pong(_) => return Err(MessageParseError::InvalidUpdateMessage("Pong")),
        })
    }
}

#[derive(Debug, Clone)]
pub enum MessageValues<'a> {
    Slot(MessageValuesSlot),
    Account(MessageValuesAccount<'a>),
    Transaction(MessageValuesTransaction<'a>),
    Entry,
    BlockMeta,
    // Block
}

#[derive(Debug, Clone)]
pub struct MessageValuesSlot {
    pub commitment: CommitmentLevelProto,
}

#[derive(Debug, Clone)]
pub struct MessageValuesAccount<'a> {
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub lamports: u64,
    pub nonempty_txn_signature: bool,
    pub data: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct MessageValuesTransaction<'a> {
    pub vote: bool,
    pub failed: bool,
    pub signature: Signature,
    pub account_keys: &'a HashSet<Pubkey>,
}

use {
    prost::Message as _,
    prost_types::Timestamp,
    solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature},
    std::collections::HashSet,
    thiserror::Error,
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel as CommitmentLevelProto, SubscribeUpdate,
        SubscribeUpdateAccountInfo, SubscribeUpdateBlockMeta, SubscribeUpdateEntry,
        SubscribeUpdateTransactionInfo,
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
                    failed,
                    account_keys,
                    transaction,
                    ..
                } => MessageValues::Transaction(MessageValuesTransaction {
                    vote: transaction.is_vote,
                    failed: *failed,
                    signature: *signature,
                    account_keys,
                }),
                MessageParsedProstEnum::Entry(_) => MessageValues::Entry,
                MessageParsedProstEnum::BlockMeta(_) => MessageValues::BlockMeta,
            },
        }
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
        failed: bool,
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
                    failed: meta.err.is_some(),
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

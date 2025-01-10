use {
    prost::Message as _,
    prost_types::Timestamp,
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    std::collections::HashSet,
    thiserror::Error,
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel as CommitmentLevelProto, SubscribeUpdate,
        SubscribeUpdateAccount, SubscribeUpdateBlockMeta, SubscribeUpdateEntry,
        SubscribeUpdateSlot, SubscribeUpdateTransaction,
    },
};

#[derive(Debug, Error)]
pub enum MessageParseError {
    #[error(transparent)]
    Prost(#[from] prost::DecodeError),
    #[error("Field `{0}` should be defined")]
    FieldNotDefined(&'static str),
    #[error("Invalid update: {0}")]
    InvalidUpdate(&'static str),
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
        todo!()
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
    Account(SubscribeUpdateAccount),
    Slot(SubscribeUpdateSlot),
    Transaction(SubscribeUpdateTransaction),
    Entry(SubscribeUpdateEntry),
    BlockMeta(SubscribeUpdateBlockMeta),
    // Block(Arc<MessageBlock>),
}

impl TryFrom<UpdateOneof> for MessageParsedProstEnum {
    type Error = MessageParseError;

    fn try_from(value: UpdateOneof) -> Result<Self, Self::Error> {
        Ok(match value {
            UpdateOneof::Slot(msg) => Self::Slot(msg),
            UpdateOneof::Account(msg) => Self::Account(msg),
            UpdateOneof::Transaction(msg) => Self::Transaction(msg),
            UpdateOneof::TransactionStatus(_) => {
                return Err(MessageParseError::InvalidUpdate("TransactionStatus"))
            }
            UpdateOneof::Entry(msg) => Self::Entry(msg),
            UpdateOneof::BlockMeta(msg) => Self::BlockMeta(msg),
            UpdateOneof::Block(msg) => todo!(),
            UpdateOneof::Ping(_) => return Err(MessageParseError::InvalidUpdate("Ping")),
            UpdateOneof::Pong(_) => return Err(MessageParseError::InvalidUpdate("Pong")),
        })
    }
}

#[derive(Debug, Clone)]
pub enum MessageValues<'a> {
    Slot(MessageValuesSlot),
    Account(MessageValuesAccount<'a>),
    Transaction(MessageValuesTransaction),
    Entry,
    BlockMeta,
    // Block
}

#[derive(Debug, Clone)]
pub struct MessageValuesAccount<'a> {
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub lamports: u64,
    pub txn_signature: Option<Signature>,
    pub data: &'a [u8],
}

#[derive(Debug, Clone)]
pub struct MessageValuesSlot {
    pub commitment: CommitmentLevelProto,
}

#[derive(Debug, Clone)]
pub struct MessageValuesTransaction {
    pub vote: bool,
    pub failed: bool,
    pub signature: Signature,
    pub account_keys: HashSet<Pubkey>,
}

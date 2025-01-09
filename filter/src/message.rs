use {
    prost::Message as _,
    prost_types::Timestamp,
    thiserror::Error,
    yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, SubscribeUpdate, SubscribeUpdateAccount,
        SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdateSlot,
        SubscribeUpdateTransaction,
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
    Limited(MessageLimitedParsed),
    Prost(MessageProstParsed),
}

impl Message {
    pub fn parse(data: Vec<u8>, parse_type: ParseType) -> Result<Self, MessageParseError> {
        match parse_type {
            ParseType::Limited => MessageLimitedParsed::new(data).map(Self::Limited),
            ParseType::Prost => {
                let update = SubscribeUpdate::decode(data.as_slice())?;
                MessageProstParsed::new(
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
}

#[derive(Debug, Clone)]
pub struct MessageLimitedParsed;

impl MessageLimitedParsed {
    pub fn new(_data: Vec<u8>) -> Result<Self, MessageParseError> {
        unimplemented!()
    }
}

#[derive(Debug, Clone)]
pub struct MessageProstParsed {
    message: MessageProstParsedEnum,
    created_at: Timestamp,
}

impl MessageProstParsed {
    pub fn new(value: UpdateOneof, created_at: Timestamp) -> Result<Self, MessageParseError> {
        value.try_into().map(|message| Self {
            message,
            created_at,
        })
    }
}

#[derive(Debug, Clone)]
enum MessageProstParsedEnum {
    Account(SubscribeUpdateAccount),
    Slot(SubscribeUpdateSlot),
    Transaction(SubscribeUpdateTransaction),
    // Block(Arc<MessageBlock>),
    Entry(SubscribeUpdateEntry),
    BlockMeta(SubscribeUpdateBlockMeta),
}

impl TryFrom<UpdateOneof> for MessageProstParsedEnum {
    type Error = MessageParseError;

    fn try_from(value: UpdateOneof) -> Result<Self, Self::Error> {
        Ok(match value {
            UpdateOneof::Account(msg) => Self::Account(msg),
            UpdateOneof::Slot(msg) => Self::Slot(msg),
            UpdateOneof::Transaction(msg) => Self::Transaction(msg),
            UpdateOneof::TransactionStatus(_) => {
                return Err(MessageParseError::InvalidUpdate("TransactionStatus"))
            }
            UpdateOneof::Block(msg) => todo!(),
            UpdateOneof::Ping(_) => return Err(MessageParseError::InvalidUpdate("Ping")),
            UpdateOneof::Pong(_) => return Err(MessageParseError::InvalidUpdate("Pong")),
            UpdateOneof::BlockMeta(msg) => Self::BlockMeta(msg),
            UpdateOneof::Entry(msg) => Self::Entry(msg),
        })
    }
}

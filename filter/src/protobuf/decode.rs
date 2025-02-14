use {
    prost::{
        bytes::Buf,
        encoding::{self, check_wire_type, decode_key, decode_varint, DecodeContext, WireType},
        DecodeError,
    },
    prost_types::Timestamp,
    solana_sdk::clock::Slot,
    std::{borrow::Cow, ops::Range},
};

fn decode_error(
    description: impl Into<Cow<'static, str>>,
    stack: &[(&'static str, &'static str)],
) -> DecodeError {
    let mut error = DecodeError::new(description);
    for (message, field) in stack {
        error.push(message, field);
    }
    error
}

pub trait LimitedDecode: Default {
    fn decode(mut buf: impl Buf) -> Result<Self, DecodeError> {
        let buf_len = buf.remaining();
        let mut message = Self::default();
        while buf.has_remaining() {
            let (tag, wire_type) = decode_key(&mut buf)?;
            message.merge_field(tag, wire_type, &mut buf, buf_len)?;
        }
        Ok(message)
    }

    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        buf_len: usize,
    ) -> Result<(), DecodeError>;
}

#[derive(Debug, Default)]
pub struct SubscribeUpdateLimitedDecode {
    pub update_oneof: Option<UpdateOneofLimitedDecode>,
    pub created_at: Option<Timestamp>,
}

impl LimitedDecode for SubscribeUpdateLimitedDecode {
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        buf_len: usize,
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &'static str = "SubscribeUpdateLimitedDecode";
        check_wire_type(WireType::LengthDelimited, wire_type)?;
        match tag {
            1u32 => {
                let len = decode_varint(buf)? as usize;
                if len > buf.remaining() {
                    return Err(decode_error(
                        "buffer underflow",
                        &[(STRUCT_NAME, "filters")],
                    ));
                }
                buf.advance(len);
                Ok(())
            }
            2u32 | 3u32 | 4u32 | 10u32 | 5u32 | 6u32 | 9u32 | 7u32 | 8u32 => {
                let value = &mut self.update_oneof;
                UpdateOneofLimitedDecode::merge(value, tag, buf, buf_len).map_err(|mut error| {
                    error.push(STRUCT_NAME, "update_oneof");
                    error
                })
            }
            11u32 => {
                let value = &mut self.created_at;
                encoding::message::merge(
                    WireType::LengthDelimited,
                    value.get_or_insert_with(Default::default),
                    buf,
                    DecodeContext::default(),
                )
                .map_err(|mut error| {
                    error.push(STRUCT_NAME, "created_at");
                    error
                })
            }
            _ => encoding::skip_field(
                WireType::LengthDelimited,
                tag,
                buf,
                DecodeContext::default(),
            ),
        }
    }
}

#[derive(Debug)]
pub enum UpdateOneofLimitedDecode {
    Account(Range<usize>),
    Slot(Range<usize>),
    Transaction(Range<usize>),
    TransactionStatus(Range<usize>),
    Block(Range<usize>),
    Ping(Range<usize>),
    Pong(Range<usize>),
    BlockMeta(Range<usize>),
    Entry(Range<usize>),
}

impl UpdateOneofLimitedDecode {
    pub fn merge(
        field: &mut Option<Self>,
        tag: u32,
        buf: &mut impl Buf,
        buf_len: usize,
    ) -> Result<(), DecodeError> {
        let len = decode_varint(buf)? as usize;
        if len > buf.remaining() {
            return Err(DecodeError::new("buffer underflow"));
        }

        let start = buf_len - buf.remaining();
        buf.advance(len);
        let end = buf_len - buf.remaining();
        let range = Range { start, end };

        match tag {
            2u32 => match field {
                Some(Self::Account(_)) => Err(DecodeError::new("merge is not supported")),
                _ => {
                    *field = Some(Self::Account(range));
                    Ok(())
                }
            },
            3u32 => match field {
                Some(Self::Slot(_)) => Err(DecodeError::new("merge is not supported")),
                _ => {
                    *field = Some(Self::Slot(range));
                    Ok(())
                }
            },
            4u32 => match field {
                Some(Self::Transaction(_)) => Err(DecodeError::new("merge is not supported")),
                _ => {
                    *field = Some(Self::Transaction(range));
                    Ok(())
                }
            },
            10u32 => match field {
                Some(Self::TransactionStatus(_)) => Err(DecodeError::new("merge is not supported")),
                _ => {
                    *field = Some(Self::TransactionStatus(range));
                    Ok(())
                }
            },
            5u32 => match field {
                Some(Self::Block(_)) => Err(DecodeError::new("merge is not supported")),
                _ => {
                    *field = Some(Self::Block(range));
                    Ok(())
                }
            },
            6u32 => match field {
                Some(Self::Ping(_)) => Err(DecodeError::new("merge is not supported")),
                _ => {
                    *field = Some(Self::Ping(range));
                    Ok(())
                }
            },
            9u32 => match field {
                Some(Self::Pong(_)) => Err(DecodeError::new("merge is not supported")),
                _ => {
                    *field = Some(Self::Pong(range));
                    Ok(())
                }
            },
            7u32 => match field {
                Some(Self::BlockMeta(_)) => Err(DecodeError::new("merge is not supported")),
                _ => {
                    *field = Some(Self::BlockMeta(range));
                    Ok(())
                }
            },
            8u32 => match field {
                Some(Self::Entry(_)) => Err(DecodeError::new("merge is not supported")),
                _ => {
                    *field = Some(Self::Entry(range));
                    Ok(())
                }
            },
            _ => unreachable!(),
        }
    }
}

// #[derive(Debug, Default)]
// pub struct UpdateOneofLimitedDecodeAccount;

#[derive(Debug, Default)]
pub struct UpdateOneofLimitedDecodeSlot {
    pub slot: Slot,
    pub parent: Option<Slot>,
    pub status: i32,
    pub dead_error: Option<Range<usize>>,
}

impl LimitedDecode for UpdateOneofLimitedDecodeSlot {
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        buf_len: usize,
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &'static str = "UpdateOneofLimitedDecodeSlot";
        let ctx = DecodeContext::default();
        match tag {
            1u32 => {
                let value = &mut self.slot;
                encoding::uint64::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "slot");
                    error
                })
            }
            2u32 => {
                let value = &mut self.parent;
                encoding::uint64::merge(
                    wire_type,
                    value.get_or_insert_with(Default::default),
                    buf,
                    ctx,
                )
                .map_err(|mut error| {
                    error.push(STRUCT_NAME, "parent");
                    error
                })
            }
            3u32 => {
                let value = &mut self.status;
                encoding::int32::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "status");
                    error
                })
            }
            4u32 => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                let len = decode_varint(buf)? as usize;
                if len > buf.remaining() {
                    return Err(decode_error(
                        "buffer underflow",
                        &[(STRUCT_NAME, "dead_error")],
                    ));
                }

                let start = buf_len - buf.remaining();
                buf.advance(len);
                let end = buf_len - buf.remaining();
                self.dead_error = Some(Range { start, end });
                Ok(())
            }
            _ => encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }
}

#[derive(Debug, Default)]
pub struct UpdateOneofLimitedDecodeTransaction;

impl LimitedDecode for UpdateOneofLimitedDecodeTransaction {
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        buf_len: usize,
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &'static str = "UpdateOneofLimitedDecodeTransaction";
        let ctx = DecodeContext::default();
        todo!()
    }
}

// #[derive(Debug, Default)]
// pub struct UpdateOneofLimitedDecodeBlockMeta;

// #[derive(Debug, Default)]
// pub struct UpdateOneofLimitedDecodeEntry;

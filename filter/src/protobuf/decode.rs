use {
    prost::{
        DecodeError,
        bytes::Buf,
        encoding::{self, DecodeContext, WireType, check_wire_type, decode_key, decode_varint},
    },
    prost_types::Timestamp,
    solana_clock::{Epoch, Slot},
    solana_pubkey::{PUBKEY_BYTES, Pubkey},
    solana_signature::SIGNATURE_BYTES,
    std::{borrow::Cow, collections::HashSet, mem::MaybeUninit, ops::Range},
};

#[allow(deprecated)]
fn create_decode_error(description: impl Into<Cow<'static, str>>) -> DecodeError {
    DecodeError::new(description)
}

fn decode_error(
    description: impl Into<Cow<'static, str>>,
    stack: &[(&'static str, &'static str)],
) -> DecodeError {
    let mut error = create_decode_error(description);
    for (message, field) in stack {
        error.push(message, field);
    }
    error
}

fn decode_pubkey(
    buf: &mut impl Buf,
    wire_type: WireType,
    struct_name: &'static str,
    field: &'static str,
) -> Result<Pubkey, DecodeError> {
    check_wire_type(WireType::LengthDelimited, wire_type)?;
    let len = decode_varint(buf)? as usize;
    if len > buf.remaining() {
        return Err(decode_error("buffer underflow", &[(struct_name, field)]));
    }
    if len != PUBKEY_BYTES {
        return Err(decode_error(
            "invalid pubkey length",
            &[(struct_name, field)],
        ));
    }
    let mut pubkey = MaybeUninit::<[u8; PUBKEY_BYTES]>::uninit();
    buf.copy_to_slice(unsafe { &mut *pubkey.as_mut_ptr() });
    Ok(unsafe { pubkey.assume_init() }.into())
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
        const STRUCT_NAME: &str = "SubscribeUpdateLimitedDecode";
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
            #[allow(clippy::manual_range_patterns)]
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
            return Err(create_decode_error("buffer underflow"));
        }

        let start = buf_len - buf.remaining();
        buf.advance(len);
        let end = buf_len - buf.remaining();
        let range = Range { start, end };

        match tag {
            2u32 => match field {
                Some(Self::Account(_)) => Err(create_decode_error("merge is not supported")),
                _ => {
                    *field = Some(Self::Account(range));
                    Ok(())
                }
            },
            3u32 => match field {
                Some(Self::Slot(_)) => Err(create_decode_error("merge is not supported")),
                _ => {
                    *field = Some(Self::Slot(range));
                    Ok(())
                }
            },
            4u32 => match field {
                Some(Self::Transaction(_)) => Err(create_decode_error("merge is not supported")),
                _ => {
                    *field = Some(Self::Transaction(range));
                    Ok(())
                }
            },
            10u32 => match field {
                Some(Self::TransactionStatus(_)) => {
                    Err(create_decode_error("merge is not supported"))
                }
                _ => {
                    *field = Some(Self::TransactionStatus(range));
                    Ok(())
                }
            },
            5u32 => match field {
                Some(Self::Block(_)) => Err(create_decode_error("merge is not supported")),
                _ => {
                    *field = Some(Self::Block(range));
                    Ok(())
                }
            },
            6u32 => match field {
                Some(Self::Ping(_)) => Err(create_decode_error("merge is not supported")),
                _ => {
                    *field = Some(Self::Ping(range));
                    Ok(())
                }
            },
            9u32 => match field {
                Some(Self::Pong(_)) => Err(create_decode_error("merge is not supported")),
                _ => {
                    *field = Some(Self::Pong(range));
                    Ok(())
                }
            },
            7u32 => match field {
                Some(Self::BlockMeta(_)) => Err(create_decode_error("merge is not supported")),
                _ => {
                    *field = Some(Self::BlockMeta(range));
                    Ok(())
                }
            },
            8u32 => match field {
                Some(Self::Entry(_)) => Err(create_decode_error("merge is not supported")),
                _ => {
                    *field = Some(Self::Entry(range));
                    Ok(())
                }
            },
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
pub struct UpdateOneofLimitedDecodeAccount {
    pub account: usize,
    pub pubkey: Pubkey,
    pub owner: Pubkey,
    pub lamports: u64,
    pub executable: bool,
    pub rent_epoch: Epoch,
    pub data: Range<usize>,
    pub txn_signature_offset: Option<usize>,
    pub write_version: usize,
    pub slot: Slot,
    pub is_startup: bool,
}

impl Default for UpdateOneofLimitedDecodeAccount {
    fn default() -> Self {
        Self {
            account: usize::MAX,
            pubkey: Pubkey::default(),
            owner: Pubkey::default(),
            lamports: u64::default(),
            executable: bool::default(),
            rent_epoch: Epoch::default(),
            data: Range::default(),
            txn_signature_offset: Option::default(),
            write_version: usize::default(),
            slot: Slot::default(),
            is_startup: bool::default(),
        }
    }
}

impl LimitedDecode for UpdateOneofLimitedDecodeAccount {
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        buf_len: usize,
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &str = "UpdateOneofLimitedDecodeAccount";
        let ctx = DecodeContext::default();
        match tag {
            1u32 => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                self.account = buf_len - buf.remaining();
                self.account_merge(buf, buf_len).map_err(|mut error| {
                    error.push(STRUCT_NAME, "account");
                    error
                })
            }
            2u32 => {
                let value = &mut self.slot;
                encoding::uint64::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "slot");
                    error
                })
            }
            3u32 => {
                let value = &mut self.is_startup;
                encoding::bool::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "is_startup");
                    error
                })
            }
            _ => encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }
}

impl UpdateOneofLimitedDecodeAccount {
    fn account_merge(&mut self, buf: &mut impl Buf, buf_len: usize) -> Result<(), DecodeError> {
        let len = decode_varint(buf)?;
        let remaining = buf.remaining();
        if len > remaining as u64 {
            return Err(create_decode_error("buffer underflow"));
        }

        let limit = remaining - len as usize;
        while buf.remaining() > limit {
            let (tag, wire_type) = decode_key(buf)?;
            self.account_merge_field(tag, wire_type, buf, buf_len)?;
        }

        if buf.remaining() != limit {
            return Err(create_decode_error("delimited length exceeded"));
        }
        Ok(())
    }

    fn account_merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        buf_len: usize,
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &str = "UpdateOneofLimitedDecodeAccount";
        let ctx = DecodeContext::default();
        match tag {
            1u32 => {
                self.pubkey = decode_pubkey(buf, wire_type, STRUCT_NAME, "pubkey")?;
                Ok(())
            }
            2u32 => {
                let value = &mut self.lamports;
                encoding::uint64::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "lamports");
                    error
                })
            }
            3u32 => {
                self.owner = decode_pubkey(buf, wire_type, STRUCT_NAME, "owner")?;
                Ok(())
            }
            4u32 => {
                let value = &mut self.executable;
                encoding::bool::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "executable");
                    error
                })
            }
            5u32 => {
                let value = &mut self.rent_epoch;
                encoding::uint64::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "rent_epoch");
                    error
                })
            }
            6u32 => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                let len = decode_varint(buf)? as usize;
                if len > buf.remaining() {
                    return Err(decode_error("buffer underflow", &[(STRUCT_NAME, "data")]));
                }

                let start = buf_len - buf.remaining();
                buf.advance(len);
                let end = buf_len - buf.remaining();
                self.data = Range { start, end };
                Ok(())
            }
            7u32 => {
                self.write_version = buf_len - buf.remaining();
                let mut value = 0;
                encoding::uint64::merge(wire_type, &mut value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "write_version");
                    error
                })
            }
            8u32 => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                let len = decode_varint(buf)? as usize;
                if len > buf.remaining() {
                    return Err(decode_error(
                        "buffer underflow",
                        &[(STRUCT_NAME, "txn_signature")],
                    ));
                }
                if len != SIGNATURE_BYTES {
                    return Err(decode_error(
                        "invalid signature length",
                        &[(STRUCT_NAME, "txn_signature")],
                    ));
                }
                self.txn_signature_offset = Some(buf_len - buf.remaining());
                buf.advance(len);
                Ok(())
            }
            _ => encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }
}

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
        const STRUCT_NAME: &str = "UpdateOneofLimitedDecodeSlot";
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
pub struct UpdateOneofLimitedDecodeTransaction {
    pub transaction: Option<Range<usize>>,
    pub slot: Slot,
}

impl LimitedDecode for UpdateOneofLimitedDecodeTransaction {
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        buf_len: usize,
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &str = "UpdateOneofLimitedDecodeTransaction";
        let ctx = DecodeContext::default();
        match tag {
            1u32 => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                let len = decode_varint(buf)? as usize;
                if len > buf.remaining() {
                    return Err(decode_error(
                        "buffer underflow",
                        &[(STRUCT_NAME, "transaction")],
                    ));
                }

                let start = buf_len - buf.remaining();
                buf.advance(len);
                let end = buf_len - buf.remaining();
                self.transaction = Some(Range { start, end });
                Ok(())
            }
            2u32 => {
                let value = &mut self.slot;
                encoding::uint64::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "slot");
                    error
                })
            }
            _ => encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }
}

#[derive(Debug, Default)]
pub struct UpdateOneofLimitedDecodeTransactionInfo {
    pub signature_offset: Option<usize>,
    pub is_vote: bool,
    pub index: u64,
    pub account_keys: HashSet<Pubkey>,
    pub err: Option<Range<usize>>,
}

impl LimitedDecode for UpdateOneofLimitedDecodeTransactionInfo {
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        buf_len: usize,
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &str = "UpdateOneofLimitedDecodeTransactionInfo";
        let ctx = DecodeContext::default();
        match tag {
            1u32 => {
                // signature
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                let len = decode_varint(buf)? as usize;
                if len > buf.remaining() {
                    return Err(decode_error(
                        "buffer underflow",
                        &[(STRUCT_NAME, "signature")],
                    ));
                }
                if len != SIGNATURE_BYTES {
                    return Err(decode_error(
                        "invalid signature length",
                        &[(STRUCT_NAME, "signature")],
                    ));
                }
                self.signature_offset = Some(buf_len - buf.remaining());
                buf.advance(len);
                Ok(())
            }
            2u32 => {
                // is_vote
                let value = &mut self.is_vote;
                encoding::bool::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "is_vote");
                    error
                })
            }
            3u32 => {
                // transaction (nested message containing account_keys)
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                self.merge_transaction(buf, buf_len).map_err(|mut error| {
                    error.push(STRUCT_NAME, "transaction");
                    error
                })
            }
            4u32 => {
                // meta (nested message containing loaded addresses and err)
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                self.merge_meta(buf, buf_len).map_err(|mut error| {
                    error.push(STRUCT_NAME, "meta");
                    error
                })
            }
            5u32 => {
                // index
                let value = &mut self.index;
                encoding::uint64::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "index");
                    error
                })
            }
            _ => encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }
}

impl UpdateOneofLimitedDecodeTransactionInfo {
    fn merge_transaction(&mut self, buf: &mut impl Buf, buf_len: usize) -> Result<(), DecodeError> {
        let len = decode_varint(buf)?;
        let remaining = buf.remaining();
        if len > remaining as u64 {
            return Err(create_decode_error("buffer underflow"));
        }

        let limit = remaining - len as usize;
        while buf.remaining() > limit {
            let (tag, wire_type) = decode_key(buf)?;
            // Transaction: message = 2
            if tag == 2 {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                self.merge_transaction_message(buf, buf_len)?;
            } else {
                encoding::skip_field(wire_type, tag, buf, DecodeContext::default())?;
            }
        }
        Ok(())
    }

    fn merge_transaction_message(
        &mut self,
        buf: &mut impl Buf,
        _buf_len: usize,
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &str = "UpdateOneofLimitedDecodeTransactionInfo";
        let len = decode_varint(buf)?;
        let remaining = buf.remaining();
        if len > remaining as u64 {
            return Err(create_decode_error("buffer underflow"));
        }

        let limit = remaining - len as usize;
        while buf.remaining() > limit {
            let (tag, wire_type) = decode_key(buf)?;
            // Message: account_keys = 2
            if tag == 2 {
                self.account_keys.insert(decode_pubkey(
                    buf,
                    wire_type,
                    STRUCT_NAME,
                    "account_keys",
                )?);
            } else {
                encoding::skip_field(wire_type, tag, buf, DecodeContext::default())?;
            }
        }
        Ok(())
    }

    fn merge_meta(&mut self, buf: &mut impl Buf, buf_len: usize) -> Result<(), DecodeError> {
        const STRUCT_NAME: &str = "UpdateOneofLimitedDecodeTransactionInfo";
        let len = decode_varint(buf)?;
        let remaining = buf.remaining();
        if len > remaining as u64 {
            return Err(create_decode_error("buffer underflow"));
        }

        let limit = remaining - len as usize;
        while buf.remaining() > limit {
            let (tag, wire_type) = decode_key(buf)?;
            match tag {
                1u32 => {
                    // err
                    check_wire_type(WireType::LengthDelimited, wire_type)?;
                    let len = decode_varint(buf)? as usize;
                    if len > buf.remaining() {
                        return Err(create_decode_error("buffer underflow"));
                    }
                    let start = buf_len - buf.remaining();
                    buf.advance(len);
                    let end = buf_len - buf.remaining();
                    self.err = Some(Range { start, end });
                }
                12u32 => {
                    // loaded_writable_addresses
                    self.account_keys.insert(decode_pubkey(
                        buf,
                        wire_type,
                        STRUCT_NAME,
                        "loaded_writable_addresses",
                    )?);
                }
                13u32 => {
                    // loaded_readonly_addresses
                    self.account_keys.insert(decode_pubkey(
                        buf,
                        wire_type,
                        STRUCT_NAME,
                        "loaded_readonly_addresses",
                    )?);
                }
                _ => {
                    encoding::skip_field(wire_type, tag, buf, DecodeContext::default())?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct UpdateOneofLimitedDecodeEntry {
    pub slot: Slot,
    pub index: u64,
    pub executed_transaction_count: u64,
}

impl LimitedDecode for UpdateOneofLimitedDecodeEntry {
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: WireType,
        buf: &mut impl Buf,
        _buf_len: usize,
    ) -> Result<(), DecodeError> {
        const STRUCT_NAME: &str = "UpdateOneofLimitedDecodeEntry";
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
                let value = &mut self.index;
                encoding::uint64::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "index");
                    error
                })
            }
            3u32 => encoding::uint64::merge(wire_type, &mut 0, buf, ctx).map_err(|mut error| {
                error.push(STRUCT_NAME, "num_hashes");
                error
            }),
            4u32 => {
                check_wire_type(WireType::LengthDelimited, wire_type)?;
                let len = decode_varint(buf)? as usize;
                if len > buf.remaining() {
                    Err(create_decode_error("buffer underflow"))
                } else {
                    buf.advance(len);
                    Ok(())
                }
            }
            .map_err(|mut error| {
                error.push(STRUCT_NAME, "hash");
                error
            }),
            5u32 => {
                let value = &mut self.executed_transaction_count;
                encoding::uint64::merge(wire_type, value, buf, ctx).map_err(|mut error| {
                    error.push(STRUCT_NAME, "executed_transaction_count");
                    error
                })
            }
            6u32 => encoding::uint64::merge(wire_type, &mut 0, buf, ctx).map_err(|mut error| {
                error.push(STRUCT_NAME, "starting_transaction_index");
                error
            }),
            _ => encoding::skip_field(wire_type, tag, buf, ctx),
        }
    }
}

use {
    crate::protobuf::{bytes_encode, bytes_encoded_len},
    prost::{
        bytes::{Buf, BufMut},
        encoding::{
            self, encode_key, encode_varint, encoded_len_varint, key_len, message, DecodeContext,
            WireType,
        },
        DecodeError, Message,
    },
    prost_types::Timestamp,
    richat_proto::geyser::subscribe_update::UpdateOneof,
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    std::borrow::Cow,
};

#[derive(Debug)]
pub struct SubscribeUpdateMessageLimited<'a> {
    pub filters: &'a [&'a str],
    pub update: UpdateOneofLimitedEncode<'a>,
    pub created_at: Timestamp,
}

impl<'a> Message for SubscribeUpdateMessageLimited<'a> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        for filter in self.filters {
            bytes_encode(1, filter.as_bytes(), buf);
        }
        self.update.encode(buf);
        message::encode(11, &self.created_at, buf);
    }

    fn encoded_len(&self) -> usize {
        self.filters
            .iter()
            .map(|filter| bytes_encoded_len(1, filter.as_bytes()))
            .sum::<usize>()
            + self.update.encoded_len()
            + message::encoded_len(11, &self.created_at)
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

#[derive(Debug)]
pub enum UpdateOneofLimitedEncode<'a> {
    Account(UpdateOneofLimitedEncodeAccount<'a>),
    Slot(&'a [u8]),
    Transaction(&'a [u8]),
}

impl<'a> UpdateOneofLimitedEncode<'a> {
    const fn tag(&self) -> u32 {
        match self {
            Self::Account(_) => 2u32,
            Self::Slot(_) => 3u32,
            Self::Transaction(_) => 4u32,
            // Self::TransactionStatus(_) => 10u32,
            // Self::Block(_) => 5u32,
            // Self::Ping(_) => 6u32,
            // Self::Pong(_) => 9u32,
            // Self::BlockMeta(_) => 7u32,
            // Self::Entry(_) => 8u32,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Account(account) => account.encoded_len(),
            Self::Slot(slice) => slice.len(),
            Self::Transaction(slice) => slice.len(),
            // Self::TransactionStatus(_) => 10u32,
            // Self::Block(_) => 5u32,
            // Self::Ping(_) => 6u32,
            // Self::Pong(_) => 9u32,
            // Self::BlockMeta(_) => 7u32,
            // Self::Entry(_) => 8u32,
        }
    }

    pub fn encode(&self, buf: &mut impl BufMut) {
        encode_key(self.tag(), WireType::LengthDelimited, buf);
        encode_varint(self.len() as u64, buf);
        match self {
            Self::Account(account) => account.encode_raw(buf),
            Self::Slot(slice) => buf.put_slice(slice),
            Self::Transaction(slice) => buf.put_slice(slice),
            // Self::TransactionStatus(_) => 10u32,
            // Self::Block(_) => 5u32,
            // Self::Ping(_) => 6u32,
            // Self::Pong(_) => 9u32,
            // Self::BlockMeta(_) => 7u32,
            // Self::Entry(_) => 8u32,
        }
    }

    pub fn encoded_len(&self) -> usize {
        let len = self.len();
        key_len(self.tag()) + encoded_len_varint(len as u64) + len
    }
}

#[derive(Debug)]
pub enum UpdateOneofLimitedEncodeAccount<'a> {
    Slice(&'a [u8]),
    Fields {
        pubkey: &'a Pubkey,
        lamports: u64,
        owner: &'a Pubkey,
        executable: bool,
        rent_epoch: u64,
        data: Cow<'a, [u8]>,
        write_version: u64,
        txn_signature: Option<&'a [u8]>,
        slot: Slot,
        is_startup: bool,
    },
}

impl<'a> UpdateOneofLimitedEncodeAccount<'a> {
    fn account_encode_raw(&self, buf: &mut impl BufMut) {
        match self {
            Self::Slice(_) => unreachable!(),
            Self::Fields {
                pubkey,
                lamports,
                owner,
                executable,
                rent_epoch,
                data,
                write_version,
                txn_signature,
                ..
            } => {
                bytes_encode(1u32, pubkey.as_ref(), buf);
                if *lamports != 0u64 {
                    encoding::uint64::encode(2u32, lamports, buf);
                }
                bytes_encode(3u32, owner.as_ref(), buf);
                if !executable {
                    encoding::bool::encode(4u32, executable, buf);
                }
                if *rent_epoch != 0u64 {
                    encoding::uint64::encode(5u32, rent_epoch, buf);
                }
                if !data.is_empty() {
                    bytes_encode(6u32, data, buf);
                }
                if *write_version != 0u64 {
                    encoding::uint64::encode(7u32, write_version, buf);
                }
                if let Some(value) = txn_signature {
                    bytes_encode(8u32, value, buf);
                }
            }
        }
    }

    fn account_encoded_len(&self) -> usize {
        match self {
            Self::Slice(_) => unreachable!(),
            Self::Fields {
                pubkey,
                lamports,
                owner,
                executable,
                rent_epoch,
                data,
                write_version,
                txn_signature,
                ..
            } => {
                bytes_encoded_len(1u32, pubkey.as_ref())
                    + if *lamports != 0u64 {
                        encoding::uint64::encoded_len(2u32, lamports)
                    } else {
                        0
                    }
                    + bytes_encoded_len(3u32, owner.as_ref())
                    + if !executable {
                        encoding::bool::encoded_len(4u32, executable)
                    } else {
                        0
                    }
                    + if *rent_epoch != 0u64 {
                        encoding::uint64::encoded_len(5u32, rent_epoch)
                    } else {
                        0
                    }
                    + if !data.is_empty() {
                        bytes_encoded_len(6u32, data)
                    } else {
                        0
                    }
                    + if *write_version != 0u64 {
                        encoding::uint64::encoded_len(7u32, write_version)
                    } else {
                        0
                    }
                    + txn_signature
                        .as_ref()
                        .map_or(0, |value| bytes_encoded_len(8u32, value))
            }
        }
    }
}

impl<'a> Message for UpdateOneofLimitedEncodeAccount<'a> {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        match self {
            Self::Slice(slice) => buf.put_slice(slice),
            Self::Fields {
                slot, is_startup, ..
            } => {
                encode_key(1u32, WireType::LengthDelimited, buf);
                encode_varint(self.account_encoded_len() as u64, buf);
                self.account_encode_raw(buf);
                if *slot != 0u64 {
                    encoding::uint64::encode(2u32, slot, buf);
                }
                if !is_startup {
                    encoding::bool::encode(3u32, is_startup, buf);
                }
            }
        }
    }

    fn encoded_len(&self) -> usize {
        match self {
            Self::Slice(slice) => slice.len(),
            Self::Fields {
                slot, is_startup, ..
            } => {
                let account_len = self.account_encoded_len();
                key_len(1u32)
                    + encoded_len_varint(account_len as u64)
                    + account_len
                    + if *slot != 0u64 {
                        encoding::uint64::encoded_len(2u32, slot)
                    } else {
                        0
                    }
                    + if !is_startup {
                        encoding::bool::encoded_len(3u32, is_startup)
                    } else {
                        0
                    }
            }
        }
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct SubscribeUpdateMessageProst<'a> {
    pub filters: &'a [&'a str],
    pub update: UpdateOneof,
    pub created_at: Timestamp,
}

impl<'a> Message for SubscribeUpdateMessageProst<'a> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        for filter in self.filters {
            bytes_encode(1, filter.as_bytes(), buf);
        }
        self.update.encode(buf);
        message::encode(11, &self.created_at, buf);
    }

    fn encoded_len(&self) -> usize {
        self.filters
            .iter()
            .map(|filter| bytes_encoded_len(1, filter.as_bytes()))
            .sum::<usize>()
            + self.update.encoded_len()
            + message::encoded_len(11, &self.created_at)
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl Buf,
        _ctx: DecodeContext,
    ) -> Result<(), DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

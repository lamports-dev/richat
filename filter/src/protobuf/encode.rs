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
    richat_proto::{geyser::subscribe_update::UpdateOneof, solana::storage::confirmed_block},
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    std::borrow::Cow,
};

#[derive(Debug)]
pub struct SubscribeUpdateMessageLimited<'a> {
    pub filters: &'a [&'a str],
    pub update: UpdateOneofLimitedEncode<'a>,
    pub created_at: Timestamp,
}

impl Message for SubscribeUpdateMessageLimited<'_> {
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
    TransactionStatus(UpdateOneofLimitedEncodeTransactionStatus<'a>),
    Block(UpdateOneofLimitedEncodeBlock<'a>),
    BlockMeta(&'a [u8]),
    Entry(&'a [u8]),
}

impl UpdateOneofLimitedEncode<'_> {
    const fn tag(&self) -> u32 {
        match self {
            Self::Account(_) => 2u32,
            Self::Slot(_) => 3u32,
            Self::Transaction(_) => 4u32,
            Self::TransactionStatus(_) => 10u32,
            Self::Block(_) => 5u32,
            Self::BlockMeta(_) => 7u32,
            Self::Entry(_) => 8u32,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Account(account) => account.encoded_len(),
            Self::Slot(slice) => slice.len(),
            Self::Transaction(slice) => slice.len(),
            Self::TransactionStatus(tx_status) => tx_status.encoded_len(),
            Self::Block(block) => block.encoded_len(),
            Self::BlockMeta(slice) => slice.len(),
            Self::Entry(slice) => slice.len(),
        }
    }

    pub fn encode(&self, buf: &mut impl BufMut) {
        encode_key(self.tag(), WireType::LengthDelimited, buf);
        encode_varint(self.len() as u64, buf);
        match self {
            Self::Account(account) => account.encode_raw(buf),
            Self::Slot(slice) => buf.put_slice(slice),
            Self::Transaction(slice) => buf.put_slice(slice),
            Self::TransactionStatus(tx_status) => tx_status.encode_raw(buf),
            Self::Block(block) => block.encode_raw(buf),
            Self::BlockMeta(slice) => buf.put_slice(slice),
            Self::Entry(slice) => buf.put_slice(slice),
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
        account: UpdateOneofLimitedEncodeAccountInner<'a>,
        slot: Slot,
        is_startup: bool,
    },
}

impl Message for UpdateOneofLimitedEncodeAccount<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        match self {
            Self::Slice(slice) => buf.put_slice(slice),
            Self::Fields {
                account,
                slot,
                is_startup,
            } => {
                encode_key(1u32, WireType::LengthDelimited, buf);
                encode_varint(account.encoded_len() as u64, buf);
                account.encode_raw(buf);
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
                account,
                slot,
                is_startup,
            } => {
                let account_len = account.encoded_len();
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
    ) -> Result<(), DecodeError>
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
pub struct UpdateOneofLimitedEncodeAccountInner<'a> {
    pub pubkey: &'a Pubkey,
    pub lamports: u64,
    pub owner: &'a Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Cow<'a, [u8]>,
    pub write_version: u64,
    pub txn_signature: Option<&'a [u8]>,
}

impl Message for UpdateOneofLimitedEncodeAccountInner<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        bytes_encode(1u32, self.pubkey.as_ref(), buf);
        if self.lamports != 0u64 {
            encoding::uint64::encode(2u32, &self.lamports, buf);
        }
        bytes_encode(3u32, self.owner.as_ref(), buf);
        if self.executable {
            encoding::bool::encode(4u32, &self.executable, buf);
        }
        if self.rent_epoch != 0u64 {
            encoding::uint64::encode(5u32, &self.rent_epoch, buf);
        }
        if !self.data.is_empty() {
            bytes_encode(6u32, self.data.as_ref(), buf);
        }
        if self.write_version != 0u64 {
            encoding::uint64::encode(7u32, &self.write_version, buf);
        }
        if let Some(value) = self.txn_signature {
            bytes_encode(8u32, value, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        bytes_encoded_len(1u32, self.pubkey.as_ref())
            + if self.lamports != 0u64 {
                encoding::uint64::encoded_len(2u32, &self.lamports)
            } else {
                0
            }
            + bytes_encoded_len(3u32, self.owner.as_ref())
            + if self.executable {
                encoding::bool::encoded_len(4u32, &self.executable)
            } else {
                0
            }
            + if self.rent_epoch != 0u64 {
                encoding::uint64::encoded_len(5u32, &self.rent_epoch)
            } else {
                0
            }
            + if !self.data.is_empty() {
                bytes_encoded_len(6u32, self.data.as_ref())
            } else {
                0
            }
            + if self.write_version != 0u64 {
                encoding::uint64::encoded_len(7u32, &self.write_version)
            } else {
                0
            }
            + self
                .txn_signature
                .as_ref()
                .map_or(0, |value| bytes_encoded_len(8u32, value))
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

    fn clear(&mut self) {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct UpdateOneofLimitedEncodeTransactionStatus<'a> {
    pub slot: u64,
    pub signature: &'a [u8],
    pub is_vote: bool,
    pub index: u64,
    pub err: Option<confirmed_block::TransactionError>,
}

impl Message for UpdateOneofLimitedEncodeTransactionStatus<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        if self.slot != 0u64 {
            encoding::uint64::encode(1u32, &self.slot, buf);
        }
        if !self.signature.is_empty() {
            bytes_encode(2u32, self.signature, buf);
        }
        if self.is_vote {
            encoding::bool::encode(3u32, &self.is_vote, buf);
        }
        if self.index != 0u64 {
            encoding::uint64::encode(4u32, &self.index, buf);
        }
        if let Some(msg) = &self.err {
            encoding::message::encode(5u32, msg, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        (if self.slot != 0u64 {
            encoding::uint64::encoded_len(1u32, &self.slot)
        } else {
            0
        }) + if !self.signature.is_empty() {
            bytes_encoded_len(2u32, self.signature)
        } else {
            0
        } + if self.is_vote {
            encoding::bool::encoded_len(3u32, &self.is_vote)
        } else {
            0
        } + if self.index != 0u64 {
            encoding::uint64::encoded_len(4u32, &self.index)
        } else {
            0
        } + self
            .err
            .as_ref()
            .map_or(0, |msg| encoding::message::encoded_len(5u32, msg))
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

    fn clear(&mut self) {
        unimplemented!()
    }
}

#[derive(Debug)]
pub struct UpdateOneofLimitedEncodeBlock<'a> {
    pub slot: u64,
    pub blockhash: &'a str,
    pub rewards: Option<confirmed_block::Rewards>,
    pub block_time: Option<confirmed_block::UnixTimestamp>,
    pub block_height: Option<confirmed_block::BlockHeight>,
    pub parent_slot: u64,
    pub parent_blockhash: &'a str,
    pub executed_transaction_count: u64,
    pub transactions: Vec<&'a [u8]>,
    pub updated_account_count: u64,
    pub accounts: Vec<UpdateOneofLimitedEncodeAccountInner<'a>>,
    pub entries_count: u64,
    pub entries: Vec<&'a [u8]>,
}

impl Message for UpdateOneofLimitedEncodeBlock<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        if self.slot != 0u64 {
            encoding::uint64::encode(1u32, &self.slot, buf);
        }
        if !self.blockhash.is_empty() {
            bytes_encode(2u32, self.blockhash.as_ref(), buf);
        }
        if let Some(msg) = &self.rewards {
            encoding::message::encode(3u32, msg, buf);
        }
        if let Some(msg) = &self.block_time {
            encoding::message::encode(4u32, msg, buf);
        }
        if let Some(msg) = &self.block_height {
            encoding::message::encode(5u32, msg, buf);
        }
        for slice in &self.transactions {
            encode_key(6u32, WireType::LengthDelimited, buf);
            encode_varint(slice.len() as u64, buf);
            buf.put_slice(slice);
        }
        if self.parent_slot != 0u64 {
            encoding::uint64::encode(7u32, &self.parent_slot, buf);
        }
        if !self.parent_blockhash.is_empty() {
            bytes_encode(8u32, self.parent_blockhash.as_ref(), buf);
        }
        if self.executed_transaction_count != 0u64 {
            encoding::uint64::encode(9u32, &self.executed_transaction_count, buf);
        }
        if self.updated_account_count != 0u64 {
            encoding::uint64::encode(10u32, &self.updated_account_count, buf);
        }
        for msg in &self.accounts {
            encoding::message::encode(11u32, msg, buf);
        }
        if self.entries_count != 0u64 {
            encoding::uint64::encode(12u32, &self.entries_count, buf);
        }
        for slice in &self.entries {
            encode_key(13u32, WireType::LengthDelimited, buf);
            encode_varint(slice.len() as u64, buf);
            buf.put_slice(slice);
        }
    }

    fn encoded_len(&self) -> usize {
        (if self.slot != 0u64 {
            encoding::uint64::encoded_len(1u32, &self.slot)
        } else {
            0
        }) + if !self.blockhash.is_empty() {
            bytes_encoded_len(2u32, self.blockhash.as_ref())
        } else {
            0
        } + self
            .rewards
            .as_ref()
            .map_or(0, |msg| encoding::message::encoded_len(3u32, msg))
            + self
                .block_time
                .as_ref()
                .map_or(0, |msg| encoding::message::encoded_len(4u32, msg))
            + self
                .block_height
                .as_ref()
                .map_or(0, |msg| encoding::message::encoded_len(5u32, msg))
            + (key_len(6u32) * self.transactions.len()
                + self
                    .transactions
                    .iter()
                    .map(|slice| slice.len())
                    .map(|len| len + encoded_len_varint(len as u64))
                    .sum::<usize>())
            + if self.parent_slot != 0u64 {
                encoding::uint64::encoded_len(7u32, &self.parent_slot)
            } else {
                0
            }
            + if !self.parent_blockhash.is_empty() {
                bytes_encoded_len(8u32, self.parent_blockhash.as_ref())
            } else {
                0
            }
            + if self.executed_transaction_count != 0u64 {
                encoding::uint64::encoded_len(9u32, &self.executed_transaction_count)
            } else {
                0
            }
            + if self.updated_account_count != 0u64 {
                encoding::uint64::encoded_len(10u32, &self.updated_account_count)
            } else {
                0
            }
            + encoding::message::encoded_len_repeated(11u32, &self.accounts)
            + if self.entries_count != 0u64 {
                encoding::uint64::encoded_len(12u32, &self.entries_count)
            } else {
                0
            }
            + (key_len(13u32) * self.entries.len()
                + self
                    .entries
                    .iter()
                    .map(|slice| slice.len())
                    .map(|len| len + encoded_len_varint(len as u64))
                    .sum::<usize>())
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

impl Message for SubscribeUpdateMessageProst<'_> {
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

use {
    super::{bytes_encode, bytes_encoded_len, RewardWrapper},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
    bytes::BufMut,
    prost::encoding,
    solana_account_decoder::parse_token::UiTokenAmount,
    solana_sdk::{
        clock::Slot,
        instruction::CompiledInstruction,
        message::{
            v0::{LoadedMessage, MessageAddressTableLookup},
            LegacyMessage, MessageHeader, SanitizedMessage,
        },
        pubkey::{Pubkey, PUBKEY_BYTES},
        signature::{Signature, SIGNATURE_BYTES},
        transaction::{SanitizedTransaction, TransactionError},
        transaction_context::TransactionReturnData,
    },
    solana_transaction_status::{
        InnerInstruction, InnerInstructions, TransactionStatusMeta, TransactionTokenBalance,
    },
    std::{cell::RefCell, marker::PhantomData, ops::Deref},
};

#[derive(Debug)]
pub struct Transaction<'a> {
    slot: Slot,
    transaction: &'a ReplicaTransactionInfoV2<'a>,
}

impl<'a> Transaction<'a> {
    pub const fn new(slot: Slot, transaction: &'a ReplicaTransactionInfoV2<'a>) -> Self {
        Self { slot, transaction }
    }
}

impl prost::Message for Transaction<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        let tx = ReplicaWrapper(self.transaction);
        encoding::message::encode(1, &tx, buf);
        if self.slot != 0 {
            encoding::uint64::encode(2, &self.slot, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let tx = ReplicaWrapper(self.transaction);
        encoding::message::encoded_len(1, &tx)
            + if self.slot != 0 {
                encoding::uint64::encoded_len(2, &self.slot)
            } else {
                0
            }
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
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
struct ReplicaWrapper<'a>(&'a ReplicaTransactionInfoV2<'a>);

impl<'a> Deref for ReplicaWrapper<'a> {
    type Target = ReplicaTransactionInfoV2<'a>;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl prost::Message for ReplicaWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        let index = self.index as u64;

        bytes_encode(1, self.signature.as_ref(), buf);
        if self.is_vote {
            encoding::bool::encode(2, &self.is_vote, buf)
        }
        encoding::message::encode(3, &SanitizedTransactionWrapper(self.transaction), buf);
        encoding::message::encode(
            4,
            &TransactionStatusMetaWrapper(self.transaction_status_meta),
            buf,
        );
        if index != 0 {
            encoding::uint64::encode(5, &index, buf)
        }
    }

    fn encoded_len(&self) -> usize {
        let index = self.index as u64;

        bytes_encoded_len(1, self.signature.as_ref())
            + if self.is_vote {
                encoding::bool::encoded_len(2, &self.is_vote)
            } else {
                0
            }
            + encoding::message::encoded_len(3, &SanitizedTransactionWrapper(self.transaction))
            + encoding::message::encoded_len(
                4,
                &TransactionStatusMetaWrapper(self.transaction_status_meta),
            )
            + if index != 0 {
                encoding::uint64::encoded_len(5, &index)
            } else {
                0
            }
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

#[derive(Debug)]
struct SanitizedTransactionWrapper<'a>(&'a SanitizedTransaction);

impl Deref for SanitizedTransactionWrapper<'_> {
    type Target = SanitizedTransaction;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl prost::Message for SanitizedTransactionWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        signatures_encode(1, self.signatures(), buf);
        encoding::message::encode(2, &SanitizedMessageWrapper(self.message()), buf);
    }

    fn encoded_len(&self) -> usize {
        signatures_encoded_len(1, self.signatures())
            + encoding::message::encoded_len(2, &SanitizedMessageWrapper(self.message()))
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

fn signatures_encode(tag: u32, signatures: &[Signature], buf: &mut impl BufMut) {
    for signature in signatures {
        bytes_encode(tag, signature.as_ref(), buf)
    }
}

const fn signatures_encoded_len(tag: u32, signatures: &[Signature]) -> usize {
    (encoding::key_len(tag)
        + encoding::encoded_len_varint(SIGNATURE_BYTES as u64)
        + SIGNATURE_BYTES)
        * signatures.len()
}

#[derive(Debug)]
struct SanitizedMessageWrapper<'a>(&'a SanitizedMessage);

impl Deref for SanitizedMessageWrapper<'_> {
    type Target = SanitizedMessage;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl prost::Message for SanitizedMessageWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        match self.deref() {
            SanitizedMessage::Legacy(LegacyMessage { message, .. }) => {
                encoding::message::encode(1, &MessageHeaderWrapper(message.header), buf);
                pubkeys_encode(2, &message.account_keys, buf);
                bytes_encode(3, message.recent_blockhash.as_ref(), buf);
                encoding::message::encode_repeated(
                    4,
                    CompiledInstructionWrapper::new(&message.instructions),
                    buf,
                );
                versioned_encode(5, false, buf);
                encoding::message::encode_repeated(
                    6,
                    MessageAddressTableLookupWrapper::new(&[]),
                    buf,
                );
            }
            SanitizedMessage::V0(LoadedMessage { message, .. }) => {
                encoding::message::encode(1, &MessageHeaderWrapper(message.header), buf);
                pubkeys_encode(2, &message.account_keys, buf);
                bytes_encode(3, message.recent_blockhash.as_ref(), buf);
                encoding::message::encode_repeated(
                    4,
                    CompiledInstructionWrapper::new(&message.instructions),
                    buf,
                );
                versioned_encode(5, true, buf);
                encoding::message::encode_repeated(
                    6,
                    MessageAddressTableLookupWrapper::new(&message.address_table_lookups),
                    buf,
                );
            }
        }
    }

    fn encoded_len(&self) -> usize {
        match self.deref() {
            SanitizedMessage::Legacy(LegacyMessage { message, .. }) => {
                encoding::message::encoded_len(1, &MessageHeaderWrapper(message.header))
                    + pubkeys_encoded_len(2, &message.account_keys)
                    + bytes_encoded_len(3, message.recent_blockhash.as_ref())
                    + encoding::message::encoded_len_repeated(
                        4,
                        CompiledInstructionWrapper::new(&message.instructions),
                    )
                    + versioned_encoded_len(5, false)
                    + encoding::message::encoded_len_repeated(
                        6,
                        MessageAddressTableLookupWrapper::new(&[]),
                    )
            }
            SanitizedMessage::V0(LoadedMessage { message, .. }) => {
                encoding::message::encoded_len(1, &MessageHeaderWrapper(message.header))
                    + pubkeys_encoded_len(2, &message.account_keys)
                    + bytes_encoded_len(3, message.recent_blockhash.as_ref())
                    + encoding::message::encoded_len_repeated(
                        4,
                        CompiledInstructionWrapper::new(&message.instructions),
                    )
                    + versioned_encoded_len(5, true)
                    + encoding::message::encoded_len_repeated(
                        6,
                        MessageAddressTableLookupWrapper::new(&message.address_table_lookups),
                    )
            }
        }
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

#[derive(Debug)]
struct MessageHeaderWrapper(MessageHeader);

impl Deref for MessageHeaderWrapper {
    type Target = MessageHeader;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl prost::Message for MessageHeaderWrapper {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        let num_required_signatures = self.num_required_signatures as u32;
        let num_readonly_signed_accounts = self.num_readonly_signed_accounts as u32;
        let num_readonly_unsigned_accounts = self.num_readonly_unsigned_accounts as u32;
        if num_required_signatures != 0 {
            encoding::uint32::encode(1, &num_required_signatures, buf)
        }
        if num_readonly_signed_accounts != 0 {
            encoding::uint32::encode(2, &num_readonly_signed_accounts, buf)
        }
        if num_readonly_unsigned_accounts != 0 {
            encoding::uint32::encode(3, &num_readonly_unsigned_accounts, buf)
        }
    }

    fn encoded_len(&self) -> usize {
        let num_required_signatures = self.num_required_signatures as u32;
        let num_readonly_signed_accounts = self.num_readonly_signed_accounts as u32;
        let num_readonly_unsigned_accounts = self.num_readonly_unsigned_accounts as u32;
        (if num_required_signatures != 0 {
            encoding::uint32::encoded_len(1, &num_required_signatures)
        } else {
            0
        }) + if num_readonly_signed_accounts != 0 {
            encoding::uint32::encoded_len(2, &num_readonly_signed_accounts)
        } else {
            0
        } + if num_readonly_unsigned_accounts != 0 {
            encoding::uint32::encoded_len(3, &num_readonly_unsigned_accounts)
        } else {
            0
        }
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

fn pubkeys_encode(tag: u32, pubkeys: &[Pubkey], buf: &mut impl BufMut) {
    for pubkey in pubkeys {
        bytes_encode(tag, pubkey.as_ref(), buf);
    }
}

const fn pubkeys_encoded_len(tag: u32, pubkeys: &[Pubkey]) -> usize {
    (encoding::key_len(tag) + encoding::encoded_len_varint(PUBKEY_BYTES as u64) + PUBKEY_BYTES)
        * pubkeys.len()
}

fn versioned_encode(tag: u32, versioned: bool, buf: &mut impl BufMut) {
    if versioned {
        encoding::bool::encode(tag, &versioned, buf)
    }
}

fn versioned_encoded_len(tag: u32, versioned: bool) -> usize {
    if versioned {
        encoding::bool::encoded_len(tag, &versioned)
    } else {
        0
    }
}

#[repr(transparent)]
#[derive(Debug)]
struct MessageAddressTableLookupWrapper<'a>(MessageAddressTableLookup, PhantomData<&'a ()>);

impl MessageAddressTableLookupWrapper<'_> {
    const fn new(address_table_lookups: &[MessageAddressTableLookup]) -> &[Self] {
        // SAFETY: the compiler guarantees that
        // `align_of::<MessageAddressTableLookupWrapper>() == align_of::<MessageAddressTableLookup>()`,
        // `size_of::<MessageAddressTableLookupWrapper>() == size_of::<MessageAddressTableLookup>()`,
        // the alignment of `MessageAddressTableLookupWrapper` and `MessageAddressTableLookup` are identical.
        unsafe { std::mem::transmute(address_table_lookups) }
    }
}

impl Deref for MessageAddressTableLookupWrapper<'_> {
    type Target = MessageAddressTableLookup;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl prost::Message for MessageAddressTableLookupWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        bytes_encode(1, self.account_key.as_ref(), buf);
        if !self.writable_indexes.is_empty() {
            bytes_encode(2, &self.writable_indexes, buf)
        };
        if !self.readonly_indexes.is_empty() {
            bytes_encode(3, &self.readonly_indexes, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        bytes_encoded_len(1, self.account_key.as_ref())
            + if !self.writable_indexes.is_empty() {
                bytes_encoded_len(2, &self.writable_indexes)
            } else {
                0
            }
            + if !self.readonly_indexes.is_empty() {
                bytes_encoded_len(3, &self.readonly_indexes)
            } else {
                0
            }
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

#[derive(Debug)]
struct TransactionStatusMetaWrapper<'a>(&'a TransactionStatusMeta);

impl Deref for TransactionStatusMetaWrapper<'_> {
    type Target = TransactionStatusMeta;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl prost::Message for TransactionStatusMetaWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        if let Err(err) = &self.status {
            encoding::message::encode(1, &TransactionErrorWrapper(err), buf);
        }
        if self.fee != 0 {
            encoding::uint64::encode(2, &self.fee, buf);
        }
        encoding::uint64::encode_packed(3, &self.pre_balances, buf);
        encoding::uint64::encode_packed(4, &self.post_balances, buf);
        if let Some(inner_instructions) = &self.inner_instructions {
            encoding::message::encode_repeated(
                5,
                InnerInstructionsWrapper::new(inner_instructions),
                buf,
            );
        }
        if let Some(log_messages) = &self.log_messages {
            encoding::string::encode_repeated(6, log_messages, buf);
        }
        if let Some(pre_token_balances) = &self.pre_token_balances {
            encoding::message::encode_repeated(
                7,
                TransactionTokenBalanceWrapper::new(pre_token_balances),
                buf,
            );
        }
        if let Some(post_token_balances) = &self.post_token_balances {
            encoding::message::encode_repeated(
                8,
                TransactionTokenBalanceWrapper::new(post_token_balances),
                buf,
            );
        }
        if let Some(rewards) = &self.rewards {
            encoding::message::encode_repeated(9, RewardWrapper::new(rewards), buf);
        }
        if self.inner_instructions.is_none() {
            encoding::bool::encode(10, &self.inner_instructions.is_none(), buf);
        }
        if self.log_messages.is_none() {
            encoding::bool::encode(11, &self.log_messages.is_none(), buf);
        }
        pubkeys_encode(12, &self.loaded_addresses.writable, buf);
        pubkeys_encode(13, &self.loaded_addresses.readonly, buf);
        if let Some(return_data) = &self.return_data {
            encoding::message::encode(14, &TransactionReturnDataWrapper(return_data), buf);
        }
        if self.return_data.is_none() {
            encoding::bool::encode(15, &self.return_data.is_none(), buf);
        }
        if let Some(compute_units_consumed) = self.compute_units_consumed {
            encoding::uint64::encode(16, &compute_units_consumed, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        self.status.as_ref().err().map_or(0, |error| {
            encoding::message::encoded_len(1, &TransactionErrorWrapper(error))
        }) + if self.fee != 0 {
            encoding::uint64::encoded_len(2, &self.fee)
        } else {
            0
        } + encoding::uint64::encoded_len_packed(3, &self.pre_balances)
            + encoding::uint64::encoded_len_packed(4, &self.post_balances)
            + self
                .inner_instructions
                .as_ref()
                .map_or(0, |inner_instructions| {
                    encoding::message::encoded_len_repeated(
                        5,
                        InnerInstructionsWrapper::new(inner_instructions),
                    )
                })
            + self.log_messages.as_ref().map_or(0, |log_messages| {
                encoding::string::encoded_len_repeated(6, log_messages)
            })
            + self
                .pre_token_balances
                .as_ref()
                .map_or(0, |pre_token_balances| {
                    encoding::message::encoded_len_repeated(
                        7,
                        TransactionTokenBalanceWrapper::new(pre_token_balances),
                    )
                })
            + self
                .post_token_balances
                .as_ref()
                .map_or(0, |post_token_balances| {
                    encoding::message::encoded_len_repeated(
                        8,
                        TransactionTokenBalanceWrapper::new(post_token_balances),
                    )
                })
            + self.rewards.as_ref().map_or(0, |rewards| {
                encoding::message::encoded_len_repeated(9, RewardWrapper::new(rewards))
            })
            + if self.inner_instructions.is_none() {
                encoding::bool::encoded_len(10, &self.inner_instructions.is_none())
            } else {
                0
            }
            + if self.log_messages.is_none() {
                encoding::bool::encoded_len(11, &self.log_messages.is_none())
            } else {
                0
            }
            + pubkeys_encoded_len(12, &self.loaded_addresses.writable)
            + pubkeys_encoded_len(13, &self.loaded_addresses.readonly)
            + self.return_data.as_ref().map_or(0, |return_data| {
                encoding::message::encoded_len(14, &TransactionReturnDataWrapper(return_data))
            })
            + if self.return_data.is_none() {
                encoding::bool::encoded_len(15, &self.return_data.is_none())
            } else {
                0
            }
            + self
                .compute_units_consumed
                .map_or(0, |compute_units_consumed| {
                    encoding::uint64::encoded_len(16, &compute_units_consumed)
                })
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

const BUFFER_CAPACITY: usize = 1024;

thread_local! {
    static BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(BUFFER_CAPACITY));
}

#[derive(Debug)]
struct TransactionErrorWrapper<'a>(&'a TransactionError);

impl prost::Message for TransactionErrorWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        BUFFER.with(|cell| {
            let borrow = cell.borrow();
            let data = &*borrow;
            if !data.is_empty() {
                encoding::bytes::encode(1, data, buf)
            }
        })
    }

    fn encoded_len(&self) -> usize {
        BUFFER.with(|cell| {
            let mut borrow_mut = cell.borrow_mut();
            borrow_mut.clear();
            bincode::serialize_into(&mut *borrow_mut, &self.0)
                .expect("failed to serialize transaction error into buffer");
            let data = &*borrow_mut;
            if !data.is_empty() {
                encoding::bytes::encoded_len(1, data)
            } else {
                0
            }
        })
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

#[repr(transparent)]
#[derive(Debug)]
struct InnerInstructionsWrapper<'a>(InnerInstructions, PhantomData<&'a ()>);

impl InnerInstructionsWrapper<'_> {
    const fn new(inner_instructions: &[InnerInstructions]) -> &[Self] {
        // SAFETY: the compiler guarantees that
        // `align_of::<InnerInstructionsWrapper>() == align_of::<InnerInstructions>()`,
        // `size_of::<InnerInstructionsWrapper>() == size_of::<InnerInstructions>()`,
        // the alignment of `InnerInstructionsWrapper` and `InnerInstructions` are identical.
        unsafe { std::mem::transmute(inner_instructions) }
    }
}

impl Deref for InnerInstructionsWrapper<'_> {
    type Target = InnerInstructions;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl prost::Message for InnerInstructionsWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        let index = self.index as u32;

        if index != 0 {
            encoding::uint32::encode(1, &index, buf)
        }
        encoding::message::encode_repeated(
            2,
            InnerInstructionWrapper::new(&self.instructions),
            buf,
        );
    }

    fn encoded_len(&self) -> usize {
        let index = self.index as u32;

        (if index != 0 {
            encoding::uint32::encoded_len(1, &index)
        } else {
            0
        }) + encoding::message::encoded_len_repeated(
            2,
            InnerInstructionWrapper::new(&self.instructions),
        )
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

#[repr(transparent)]
#[derive(Debug)]
struct InnerInstructionWrapper<'a>(InnerInstruction, PhantomData<&'a ()>);

impl InnerInstructionWrapper<'_> {
    const fn new(inner_instructions: &[InnerInstruction]) -> &[Self] {
        // SAFETY: the compiler guarantees that
        // `align_of::<InnerInstructionWrapper>() == align_of::<InnerInstruction>()`,
        // `size_of::<InnerInstructionWrapper>() == size_of::<InnerInstruction>()`,
        // the alignment of `InnerInstructionWrapper` and `InnerInstruction` are identical.
        unsafe { std::mem::transmute(inner_instructions) }
    }
}

impl Deref for InnerInstructionWrapper<'_> {
    type Target = InnerInstruction;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl prost::Message for InnerInstructionWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        let program_id_index = self.instruction.program_id_index as u32;

        if program_id_index != 0 {
            encoding::uint32::encode(1, &program_id_index, buf);
        }
        if !self.instruction.accounts.is_empty() {
            bytes_encode(2, &self.instruction.accounts, buf);
        }
        if !self.instruction.data.is_empty() {
            bytes_encode(3, &self.instruction.data, buf);
        }

        if let Some(stack_height) = self.stack_height {
            encoding::uint32::encode(4, &stack_height, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let program_id_index = self.instruction.program_id_index as u32;

        (if program_id_index != 0 {
            encoding::uint32::encoded_len(1, &program_id_index)
        } else {
            0
        }) + if !self.instruction.accounts.is_empty() {
            bytes_encoded_len(2, &self.instruction.accounts)
        } else {
            0
        } + if !self.instruction.data.is_empty() {
            bytes_encoded_len(3, &self.instruction.data)
        } else {
            0
        } + self.stack_height.map_or(0, |stack_height| {
            encoding::uint32::encoded_len(4, &stack_height)
        })
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

#[repr(transparent)]
#[derive(Debug)]
struct CompiledInstructionWrapper<'a>(CompiledInstruction, PhantomData<&'a ()>);

impl CompiledInstructionWrapper<'_> {
    const fn new(compiled_instructions: &[CompiledInstruction]) -> &[Self] {
        // SAFETY: the compiler guarantees that
        // `align_of::<CompiledInstructionWrapper>() == align_of::<CompiledInstruction>()`,
        // `size_of::<CompiledInstructionWrapper>() == size_of::<CompiledInstruction>()`,
        // the alignment of `CompiledInstructionWrapper` and `CompiledInstruction` are identical.
        unsafe { std::mem::transmute(compiled_instructions) }
    }
}

impl Deref for CompiledInstructionWrapper<'_> {
    type Target = CompiledInstruction;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl prost::Message for CompiledInstructionWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        let program_id_index = self.program_id_index as u32;
        if program_id_index != 0 {
            encoding::uint32::encode(1, &program_id_index, buf)
        }
        if !self.accounts.is_empty() {
            bytes_encode(2, &self.accounts, buf);
        }
        if !self.data.is_empty() {
            bytes_encode(3, &self.data, buf)
        }
    }

    fn encoded_len(&self) -> usize {
        let program_id_index = self.program_id_index as u32;
        (if program_id_index != 0 {
            encoding::uint32::encoded_len(1, &program_id_index)
        } else {
            0
        }) + if !self.accounts.is_empty() {
            bytes_encoded_len(2, &self.accounts)
        } else {
            0
        } + if !self.data.is_empty() {
            bytes_encoded_len(3, &self.data)
        } else {
            0
        }
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

#[repr(transparent)]
#[derive(Debug)]
struct TransactionTokenBalanceWrapper<'a>(TransactionTokenBalance, PhantomData<&'a ()>);

impl TransactionTokenBalanceWrapper<'_> {
    const fn new(transaction_token_balances: &[TransactionTokenBalance]) -> &[Self] {
        // SAFETY: the compiler guarantees that
        // `align_of::<TransactionTokenBalanceWrapper>() == align_of::<TransactionTokenBalance>()`,
        // `size_of::<TransactionTokenBalanceWrapper>() == size_of::<TransactionTokenBalance>()`,
        // the alignment of `TransactionTokenBalanceWrapper` and `TransactionTokenBalance` are identical.
        unsafe { std::mem::transmute(transaction_token_balances) }
    }
}

impl Deref for TransactionTokenBalanceWrapper<'_> {
    type Target = TransactionTokenBalance;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl prost::Message for TransactionTokenBalanceWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        let account_index = self.account_index as u32;

        if account_index != 0 {
            encoding::uint32::encode(1, &account_index, buf)
        }
        if !self.mint.is_empty() {
            encoding::string::encode(2, &self.mint, buf)
        }
        encoding::message::encode(3, &UiTokenAmountWrapper(&self.ui_token_amount), buf);
        if !self.owner.is_empty() {
            encoding::string::encode(4, &self.owner, buf)
        }
        if !self.program_id.is_empty() {
            encoding::string::encode(5, &self.program_id, buf)
        }
    }

    fn encoded_len(&self) -> usize {
        let account_index = self.account_index as u32;

        (if account_index != 0 {
            encoding::uint32::encoded_len(1, &account_index)
        } else {
            0
        }) + if !self.mint.is_empty() {
            encoding::string::encoded_len(2, &self.mint)
        } else {
            0
        } + encoding::message::encoded_len(3, &UiTokenAmountWrapper(&self.ui_token_amount))
            + if !self.owner.is_empty() {
                encoding::string::encoded_len(4, &self.owner)
            } else {
                0
            }
            + if !self.program_id.is_empty() {
                encoding::string::encoded_len(5, &self.program_id)
            } else {
                0
            }
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

#[derive(Debug)]
struct UiTokenAmountWrapper<'a>(&'a UiTokenAmount);

impl Deref for UiTokenAmountWrapper<'_> {
    type Target = UiTokenAmount;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl prost::Message for UiTokenAmountWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        let ui_amount = self.ui_amount.unwrap_or_default();
        let decimals = self.decimals as u32;

        if ui_amount != 0f64 {
            encoding::double::encode(1, &ui_amount, buf)
        }
        if decimals != 0 {
            encoding::uint32::encode(2, &decimals, buf)
        }
        if !self.amount.is_empty() {
            encoding::string::encode(3, &self.amount, buf)
        }
        if !self.ui_amount_string.is_empty() {
            encoding::string::encode(4, &self.ui_amount_string, buf)
        }
    }

    fn encoded_len(&self) -> usize {
        let ui_amount = self.ui_amount.unwrap_or_default();
        let decimals = self.decimals as u32;

        (if ui_amount != 0f64 {
            encoding::double::encoded_len(1, &ui_amount)
        } else {
            0
        }) + if decimals != 0 {
            encoding::uint32::encoded_len(2, &decimals)
        } else {
            0
        } + if !self.amount.is_empty() {
            encoding::string::encoded_len(3, &self.amount)
        } else {
            0
        } + if !self.ui_amount_string.is_empty() {
            encoding::string::encoded_len(4, &self.ui_amount_string)
        } else {
            0
        }
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

#[derive(Debug)]
struct TransactionReturnDataWrapper<'a>(&'a TransactionReturnData);

impl Deref for TransactionReturnDataWrapper<'_> {
    type Target = TransactionReturnData;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl prost::Message for TransactionReturnDataWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        bytes_encode(1, self.program_id.as_ref(), buf);
        if !self.data.is_empty() {
            bytes_encode(2, &self.data, buf)
        }
    }

    fn encoded_len(&self) -> usize {
        bytes_encoded_len(1, self.program_id.as_ref())
            + if !self.data.is_empty() {
                bytes_encoded_len(2, &self.data)
            } else {
                0
            }
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

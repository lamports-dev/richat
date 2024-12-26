use {
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
    prost::encoding, solana_sdk::clock::Slot,
};

#[derive(Debug)]
pub struct Transaction<'a> {
    slot: Slot,
    transaction: &'a ReplicaTransactionInfoV2<'a>,
}

impl<'a> prost::Message for Transaction<'a> {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        replica::encode_replica_transaction_info(1, self.transaction, buf);
        encoding::uint64::encode(2, &self.slot, buf)
    }

    fn encoded_len(&self) -> usize {
        replica::replica_transaction_info_encoded_len(1, self.transaction)
            + encoding::uint64::encoded_len(2, &self.slot)
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

impl<'a> Transaction<'a> {
    pub const fn new(slot: Slot, transaction: &'a ReplicaTransactionInfoV2<'a>) -> Self {
        Self { slot, transaction }
    }
}

pub mod replica {
    use {
        super::super::{bytes_encode, bytes_encoded_len},
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
        bytes::BufMut,
        prost::encoding,
    };

    #[derive(Debug)]
    struct Wrapper<'a>(&'a ReplicaTransactionInfoV2<'a>);

    impl<'a> prost::Message for Wrapper<'a> {
        fn encode_raw(&self, buf: &mut impl bytes::BufMut)
        where
            Self: Sized,
        {
            let index = self.0.index as u64;

            bytes_encode(1, self.0.signature.as_ref(), buf);
            if self.0.is_vote {
                encoding::bool::encode(2, &self.0.is_vote, buf)
            }
            super::sanitized_transaction::encode_sanitized_transaction(3, self.0.transaction, buf);
            super::transaction_status_meta::encode_transaction_status_meta(
                4,
                self.0.transaction_status_meta,
                buf,
            );
            if index != 0 {
                encoding::uint64::encode(5, &index, buf)
            }
        }
        fn encoded_len(&self) -> usize {
            let index = self.0.index as u64;

            bytes_encoded_len(1, self.0.signature.as_ref())
                + if self.0.is_vote {
                    encoding::bool::encoded_len(2, &self.0.is_vote)
                } else {
                    0
                }
                + super::sanitized_transaction::sanitized_transaction_encoded_len(
                    3,
                    self.0.transaction,
                )
                + super::transaction_status_meta::transaction_status_meta_encoded_len(
                    4,
                    self.0.transaction_status_meta,
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

    pub fn encode_replica_transaction_info(
        tag: u32,
        transaction: &ReplicaTransactionInfoV2<'_>,
        buf: &mut impl BufMut,
    ) {
        let wrapper = Wrapper(transaction);
        encoding::message::encode(tag, &wrapper, buf)
    }

    pub fn replica_transaction_info_encoded_len(
        tag: u32,
        transaction: &ReplicaTransactionInfoV2<'_>,
    ) -> usize {
        let wrapper = Wrapper(transaction);
        encoding::message::encoded_len(tag, &wrapper)
    }
}

pub mod sanitized_transaction {
    use {bytes::BufMut, prost::encoding, solana_sdk::transaction::SanitizedTransaction};

    #[derive(Debug)]
    struct Wrapper<'a>(&'a SanitizedTransaction);

    impl<'a> prost::Message for Wrapper<'a> {
        fn encode_raw(&self, buf: &mut impl bytes::BufMut)
        where
            Self: Sized,
        {
            super::signatures::encode_signatures(1, self.0.signatures(), buf);
            super::sanitazed_message::encode_sanitized_message(2, self.0.message(), buf)
        }
        fn encoded_len(&self) -> usize {
            super::signatures::signatures_encoded_len(1, self.0.signatures())
                + super::sanitazed_message::sanitized_message_encoded_len(2, self.0.message())
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

    pub fn encode_sanitized_transaction(
        tag: u32,
        sanitized: &SanitizedTransaction,
        buf: &mut impl BufMut,
    ) {
        let wrapper = Wrapper(sanitized);
        encoding::message::encode(tag, &wrapper, buf)
    }

    pub fn sanitized_transaction_encoded_len(tag: u32, sanitized: &SanitizedTransaction) -> usize {
        let wrapper = Wrapper(sanitized);
        encoding::message::encoded_len(tag, &wrapper)
    }
}

pub mod signatures {
    use {
        super::super::{bytes_encode, iter_encoded_len},
        bytes::BufMut,
        solana_sdk::signature::Signature,
    };

    pub fn encode_signatures(tag: u32, signatures: &[Signature], buf: &mut impl BufMut) {
        let signatures = signatures.iter().map(|signature| signature.as_ref());
        for value in signatures {
            bytes_encode(tag, value, buf)
        }
    }

    pub fn signatures_encoded_len(tag: u32, signatures: &[Signature]) -> usize {
        iter_encoded_len(
            tag,
            signatures.iter().map(|signature| signature.as_ref().len()),
            signatures.len(),
        )
    }
}

pub mod sanitazed_message {
    use {
        bytes::BufMut,
        prost::encoding,
        solana_sdk::message::{v0::LoadedMessage, LegacyMessage, SanitizedMessage},
    };
    #[derive(Debug)]
    struct Wrapper<'a>(&'a SanitizedMessage);

    impl<'a> prost::Message for Wrapper<'a> {
        fn encode_raw(&self, buf: &mut impl bytes::BufMut)
        where
            Self: Sized,
        {
            match self.0 {
                SanitizedMessage::Legacy(LegacyMessage { message, .. }) => {
                    super::message_header::encode_message_header(1, message.header, buf);
                    super::pubkeys::encode_pubkeys(2, &message.account_keys, buf);
                    super::recent_blockhash::encode_recent_blockhash(
                        3,
                        message.recent_blockhash.as_ref(),
                        buf,
                    );
                    super::compiled_instructions::encode_compiled_instructions(
                        4,
                        &message.instructions,
                        buf,
                    );
                    super::versioned::encode_versioned(5, false, buf);
                    super::address_table_lookups::encode_address_table_lookups(6, &[], buf)
                }
                SanitizedMessage::V0(LoadedMessage { message, .. }) => {
                    super::message_header::encode_message_header(1, message.header, buf);
                    super::pubkeys::encode_pubkeys(2, &message.account_keys, buf);
                    super::recent_blockhash::encode_recent_blockhash(
                        3,
                        message.recent_blockhash.as_ref(),
                        buf,
                    );
                    super::compiled_instructions::encode_compiled_instructions(
                        4,
                        &message.instructions,
                        buf,
                    );
                    super::versioned::encode_versioned(5, true, buf);
                    super::address_table_lookups::encode_address_table_lookups(
                        6,
                        &message.address_table_lookups,
                        buf,
                    )
                }
            }
        }
        fn encoded_len(&self) -> usize {
            match self.0 {
                SanitizedMessage::Legacy(LegacyMessage { message, .. }) => {
                    super::message_header::message_header_encoded_len(1, message.header)
                        + super::pubkeys::pubkeys_encoded_len(2, &message.account_keys)
                        + super::recent_blockhash::recent_blockhash_encoded_len(
                            3,
                            message.recent_blockhash.as_ref(),
                        )
                        + super::compiled_instructions::compiled_instructions_encoded_len(
                            4,
                            &message.instructions,
                        )
                        + super::versioned::versioned_encoded_len(5, false)
                        + super::address_table_lookups::address_table_lookups_encoded_len(6, &[])
                }
                SanitizedMessage::V0(LoadedMessage { message, .. }) => {
                    super::message_header::message_header_encoded_len(1, message.header)
                        + super::pubkeys::pubkeys_encoded_len(2, &message.account_keys)
                        + super::recent_blockhash::recent_blockhash_encoded_len(
                            3,
                            message.recent_blockhash.as_ref(),
                        )
                        + super::compiled_instructions::compiled_instructions_encoded_len(
                            4,
                            &message.instructions,
                        )
                        + super::versioned::versioned_encoded_len(5, true)
                        + super::address_table_lookups::address_table_lookups_encoded_len(
                            6,
                            &message.address_table_lookups,
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

    pub fn encode_sanitized_message(tag: u32, sanitized: &SanitizedMessage, buf: &mut impl BufMut) {
        let wrapper = Wrapper(sanitized);
        encoding::message::encode(tag, &wrapper, buf)
    }

    pub fn sanitized_message_encoded_len(tag: u32, sanitized: &SanitizedMessage) -> usize {
        let wrapper = Wrapper(sanitized);
        encoding::message::encoded_len(tag, &wrapper)
    }
}

pub mod message_header {
    use {bytes::BufMut, prost::encoding, solana_sdk::message::MessageHeader};

    #[derive(Debug)]
    struct Wrapper(MessageHeader);

    impl prost::Message for Wrapper {
        fn encode_raw(&self, buf: &mut impl bytes::BufMut)
        where
            Self: Sized,
        {
            let num_required_signatures = self.0.num_required_signatures as u32;
            let num_readonly_signed_accounts = self.0.num_readonly_signed_accounts as u32;
            let num_readonly_unsigned_accounts = self.0.num_readonly_unsigned_accounts as u32;
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
            let num_required_signatures = self.0.num_required_signatures as u32;
            let num_readonly_signed_accounts = self.0.num_readonly_signed_accounts as u32;
            let num_readonly_unsigned_accounts = self.0.num_readonly_unsigned_accounts as u32;
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

    pub fn encode_message_header(tag: u32, header: MessageHeader, buf: &mut impl BufMut) {
        let wrapper = Wrapper(header);
        encoding::message::encode(tag, &wrapper, buf)
    }

    pub fn message_header_encoded_len(tag: u32, header: MessageHeader) -> usize {
        let wrapper = Wrapper(header);
        encoding::message::encoded_len(tag, &wrapper)
    }
}

pub mod pubkeys {
    use {
        super::super::{bytes_encode, iter_encoded_len},
        bytes::BufMut,
        solana_sdk::pubkey::Pubkey,
    };

    pub fn encode_pubkeys(tag: u32, pubkeys: &[Pubkey], buf: &mut impl BufMut) {
        let iter = pubkeys.iter().map(|key| key.as_ref());
        for value in iter {
            bytes_encode(tag, value, buf);
        }
    }

    pub fn pubkeys_encoded_len(tag: u32, pubkeys: &[Pubkey]) -> usize {
        iter_encoded_len(
            tag,
            pubkeys.iter().map(|pubkey| pubkey.as_ref().len()),
            pubkeys.len(),
        )
    }
}

pub mod recent_blockhash {
    use {
        super::super::{bytes_encode, bytes_encoded_len},
        bytes::BufMut,
    };

    pub fn encode_recent_blockhash(tag: u32, pubkey: &[u8], buf: &mut impl BufMut) {
        bytes_encode(tag, pubkey, buf)
    }

    pub fn recent_blockhash_encoded_len(tag: u32, pubkey: &[u8]) -> usize {
        bytes_encoded_len(tag, pubkey)
    }
}

pub mod versioned {
    use {bytes::BufMut, prost::encoding};

    pub fn encode_versioned(tag: u32, versioned: bool, buf: &mut impl BufMut) {
        if versioned {
            encoding::bool::encode(tag, &versioned, buf)
        }
    }

    pub fn versioned_encoded_len(tag: u32, versioned: bool) -> usize {
        if versioned {
            encoding::bool::encoded_len(tag, &versioned)
        } else {
            0
        }
    }
}

pub mod address_table_lookups {
    use {
        super::super::{bytes_encode, bytes_encoded_len},
        bytes::BufMut,
        prost::encoding,
        solana_sdk::message::v0::MessageAddressTableLookup,
        std::marker::PhantomData,
    };

    #[repr(transparent)]
    #[derive(Debug)]
    struct Wrapper<'a>(MessageAddressTableLookup, PhantomData<&'a ()>);

    impl<'a> prost::Message for Wrapper<'a> {
        fn encode_raw(&self, buf: &mut impl bytes::BufMut)
        where
            Self: Sized,
        {
            bytes_encode(1, self.0.account_key.as_ref(), buf);
            bytes_encode(2, &self.0.writable_indexes, buf);
            bytes_encode(3, &self.0.readonly_indexes, buf)
        }
        fn encoded_len(&self) -> usize {
            bytes_encoded_len(1, self.0.account_key.as_ref())
                + bytes_encoded_len(2, &self.0.writable_indexes)
                + bytes_encoded_len(3, &self.0.readonly_indexes)
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

    pub fn encode_address_table_lookups(
        tag: u32,
        address_table_lookups: &[MessageAddressTableLookup],
        buf: &mut impl BufMut,
    ) {
        encoding::message::encode_repeated(tag, to_wrapper(address_table_lookups), buf)
    }

    pub fn address_table_lookups_encoded_len(
        tag: u32,
        address_table_lookups: &[MessageAddressTableLookup],
    ) -> usize {
        encoding::message::encoded_len_repeated(tag, to_wrapper(address_table_lookups))
    }

    const fn to_wrapper<'a>(
        address_table_lookups: &'a [MessageAddressTableLookup],
    ) -> &'a [Wrapper<'a>] {
        // SAFETY: the compiler guarantees that `align_of::<Wrapper>() == align_of::<MessageAddressTableLookup>()`, `size_of::<Wrapper>() == size_of::<MessageAddressTableLookup>()`, the alignment of `Wrapper` and `MessageAddressTableLookup` are identical.
        unsafe {
            std::slice::from_raw_parts(
                address_table_lookups.as_ptr() as *const Wrapper<'a>,
                address_table_lookups.len(),
            )
        }
    }
}

pub mod transaction_status_meta {
    use {
        super::super::{encode_rewards, rewards_encoded_len},
        bytes::BufMut,
        prost::encoding,
        solana_transaction_status::TransactionStatusMeta,
    };

    #[derive(Debug)]
    struct Wrapper<'a>(&'a TransactionStatusMeta);

    impl<'a> prost::Message for Wrapper<'a> {
        fn encode_raw(&self, buf: &mut impl bytes::BufMut)
        where
            Self: Sized,
        {
            if let Err(ref err) = self.0.status {
                super::transaction_error::encode_transaction_error(1, err, buf)
            }
            if self.0.fee != 0 {
                encoding::uint64::encode(2, &self.0.fee, buf)
            }
            encoding::uint64::encode_repeated(3, &self.0.pre_balances, buf);
            encoding::uint64::encode_repeated(4, &self.0.post_balances, buf);
            if let Some(ref inner_instructions) = self.0.inner_instructions {
                super::inner_instructions::encode_inner_instructions_vec(5, inner_instructions, buf)
            }
            if let Some(ref log_messages) = self.0.log_messages {
                encoding::string::encode_repeated(6, log_messages, buf)
            }
            if let Some(ref pre_token_balances) = self.0.pre_token_balances {
                super::transaction_token_balance::encode_transaction_token_balances(
                    7,
                    pre_token_balances,
                    buf,
                )
            }
            if let Some(ref post_token_balances) = self.0.post_token_balances {
                super::transaction_token_balance::encode_transaction_token_balances(
                    8,
                    post_token_balances,
                    buf,
                )
            }
            if let Some(ref rewards) = self.0.rewards {
                encode_rewards(9, rewards, buf)
            }
            if self.0.inner_instructions.is_none() {
                encoding::bool::encode(10, &self.0.inner_instructions.is_none(), buf)
            }
            if self.0.log_messages.is_none() {
                encoding::bool::encode(11, &self.0.log_messages.is_none(), buf)
            }
            super::loaded_addresses::encode_loaded_writable_addresses(
                12,
                &self.0.loaded_addresses,
                buf,
            );
            super::loaded_addresses::encode_loaded_readonly_addresses(
                13,
                &self.0.loaded_addresses,
                buf,
            );
            if let Some(ref return_data) = self.0.return_data {
                super::return_data::encode_transaction_return_data(14, return_data, buf)
            }
            if self.0.return_data.is_none() {
                encoding::bool::encode(15, &self.0.return_data.is_none(), buf)
            }
            if let Some(compute_units_consumed) = self.0.compute_units_consumed {
                if compute_units_consumed != 0 {
                    encoding::uint64::encode(16, &compute_units_consumed, buf)
                }
            }
        }
        fn encoded_len(&self) -> usize {
            self.0.status.as_ref().err().map_or(0, |error| {
                super::transaction_error::transaction_error_encoded_len(1, error)
            }) + if self.0.fee != 0 {
                encoding::uint64::encoded_len(2, &self.0.fee)
            } else {
                0
            } + encoding::uint64::encoded_len_repeated(3, &self.0.pre_balances)
                + encoding::uint64::encoded_len_repeated(4, &self.0.post_balances)
                + self
                    .0
                    .inner_instructions
                    .as_ref()
                    .map_or(0, |inner_instructions| {
                        super::inner_instructions::inner_instructions_vec_encoded_len(
                            5,
                            inner_instructions,
                        )
                    })
                + self.0.log_messages.as_ref().map_or(0, |log_messages| {
                    encoding::string::encoded_len_repeated(6, log_messages)
                })
                + self
                    .0
                    .pre_token_balances
                    .as_ref()
                    .map_or(0, |pre_token_balances| {
                        super::transaction_token_balance::transaction_token_balances_encoded_len(
                            7,
                            pre_token_balances,
                        )
                    })
                + self
                    .0
                    .post_token_balances
                    .as_ref()
                    .map_or(0, |post_token_balances| {
                        super::transaction_token_balance::transaction_token_balances_encoded_len(
                            8,
                            post_token_balances,
                        )
                    })
                + self
                    .0
                    .rewards
                    .as_ref()
                    .map_or(0, |rewards| rewards_encoded_len(9, rewards))
                + if self.0.inner_instructions.is_none() {
                    encoding::bool::encoded_len(10, &self.0.inner_instructions.is_none())
                } else {
                    0
                }
                + if self.0.log_messages.is_none() {
                    encoding::bool::encoded_len(11, &self.0.log_messages.is_none())
                } else {
                    0
                }
                + super::loaded_addresses::loaded_writable_addresses_encoded_len(
                    12,
                    &self.0.loaded_addresses,
                )
                + super::loaded_addresses::loaded_readonly_addresses_encoded_len(
                    13,
                    &self.0.loaded_addresses,
                )
                + self.0.return_data.as_ref().map_or(0, |return_data| {
                    super::return_data::transaction_return_data_encoded_len(14, return_data)
                })
                + if self.0.return_data.is_none() {
                    encoding::bool::encoded_len(15, &self.0.return_data.is_none())
                } else {
                    0
                }
                + self
                    .0
                    .compute_units_consumed
                    .map_or(0, |compute_units_consumed| {
                        if compute_units_consumed != 0 {
                            encoding::uint64::encoded_len(16, &compute_units_consumed)
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

    pub fn encode_transaction_status_meta(
        tag: u32,
        transaction_status_meta: &TransactionStatusMeta,
        buf: &mut impl BufMut,
    ) {
        let wrapper = Wrapper(transaction_status_meta);
        encoding::message::encode(tag, &wrapper, buf)
    }

    pub fn transaction_status_meta_encoded_len(
        tag: u32,
        transaction_status_meta: &TransactionStatusMeta,
    ) -> usize {
        let wrapper = Wrapper(transaction_status_meta);
        encoding::message::encoded_len(tag, &wrapper)
    }
}

pub mod transaction_error {
    use {
        bytes::BufMut, prost::encoding, solana_sdk::transaction::TransactionError,
        std::cell::RefCell,
    };

    #[derive(Debug)]
    struct Wrapper<'a>(&'a TransactionError);

    impl<'a> prost::Message for Wrapper<'a> {
        fn encode_raw(&self, buf: &mut impl bytes::BufMut)
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

    const BUFFER_CAPACITY: usize = 1024;

    thread_local! {
        static BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(BUFFER_CAPACITY));
    }

    pub fn encode_transaction_error(tag: u32, error: &TransactionError, buf: &mut impl BufMut) {
        let wrapper = Wrapper(error);
        encoding::message::encode(tag, &wrapper, buf)
    }

    pub fn transaction_error_encoded_len(tag: u32, error: &TransactionError) -> usize {
        let wrapper = Wrapper(error);
        encoding::message::encoded_len(tag, &wrapper)
    }
}

pub mod inner_instructions {
    use {
        bytes::BufMut, prost::encoding, solana_transaction_status::InnerInstructions,
        std::marker::PhantomData,
    };

    #[repr(transparent)]
    #[derive(Debug)]
    struct Wrapper<'a>(InnerInstructions, PhantomData<&'a ()>);

    impl<'a> prost::Message for Wrapper<'a> {
        fn encode_raw(&self, buf: &mut impl BufMut)
        where
            Self: Sized,
        {
            let index = self.0.index as u32;

            if index != 0 {
                encoding::uint32::encode(1, &index, buf)
            }
            super::inner_instruction::encode_inner_instruction_vec(2, &self.0.instructions, buf)
        }
        fn encoded_len(&self) -> usize {
            let index = self.0.index as u32;

            (if index != 0 {
                encoding::uint32::encoded_len(1, &index)
            } else {
                0
            }) + super::inner_instruction::inner_instruction_vec_encoded_len(
                2,
                &self.0.instructions,
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

    pub fn encode_inner_instructions_vec(
        tag: u32,
        inner_instructions: &[InnerInstructions],
        buf: &mut impl BufMut,
    ) {
        encoding::message::encode_repeated(tag, to_wrapper(inner_instructions), buf)
    }

    pub fn inner_instructions_vec_encoded_len(
        tag: u32,
        inner_instructions: &[InnerInstructions],
    ) -> usize {
        encoding::message::encoded_len_repeated(tag, to_wrapper(inner_instructions))
    }

    const fn to_wrapper<'a>(inner_instructions: &'a [InnerInstructions]) -> &'a [Wrapper<'a>] {
        // SAFETY: the compiler guarantees that `align_of::<Wrapper>() == align_of::<InnerInstructions>()`, `size_of::<Wrapper>() == size_of::<InnerInstructions>()`, the alignment of `Wrapper` and `InnerInstructions` are identical.
        unsafe {
            std::slice::from_raw_parts(
                inner_instructions.as_ptr() as *const Wrapper<'a>,
                inner_instructions.len(),
            )
        }
    }
}

pub mod inner_instruction {
    use {
        bytes::BufMut, prost::encoding, solana_transaction_status::InnerInstruction,
        std::marker::PhantomData,
    };

    #[repr(transparent)]
    #[derive(Debug)]
    struct Wrapper<'a>(InnerInstruction, PhantomData<&'a ()>);

    impl<'a> prost::Message for Wrapper<'a> {
        fn encode_raw(&self, buf: &mut impl BufMut)
        where
            Self: Sized,
        {
            super::compiled_instruction::encode_compiled_instruction(1, &self.0.instruction, buf);
            if let Some(stack_height) = self.0.stack_height {
                if stack_height != 0 {
                    encoding::uint32::encode(2, &stack_height, buf)
                }
            }
        }
        fn encoded_len(&self) -> usize {
            super::compiled_instruction::compiled_instruction_encoded_len(1, &self.0.instruction)
                + self.0.stack_height.map_or(0, |stack_height| {
                    if stack_height != 0 {
                        encoding::uint32::encoded_len(2, &stack_height)
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

    pub fn encode_inner_instruction_vec(
        tag: u32,
        inner_instructions: &[InnerInstruction],
        buf: &mut impl BufMut,
    ) {
        encoding::message::encode_repeated(tag, to_wrapper(inner_instructions), buf)
    }

    pub fn inner_instruction_vec_encoded_len(
        tag: u32,
        inner_instructions: &[InnerInstruction],
    ) -> usize {
        encoding::message::encoded_len_repeated(tag, to_wrapper(inner_instructions))
    }

    const fn to_wrapper<'a>(inner_instructions: &'a [InnerInstruction]) -> &'a [Wrapper<'a>] {
        // SAFETY: the compiler guarantees that `align_of::<Wrapper>() == align_of::<InnerInstruction>()`, `size_of::<Wrapper>() == size_of::<InnerInstruction>()`, the alignment of `Wrapper` and `InnerInstruction` are identical.
        unsafe {
            std::slice::from_raw_parts(
                inner_instructions.as_ptr() as *const Wrapper<'a>,
                inner_instructions.len(),
            )
        }
    }
}

pub mod compiled_instructions {
    use {
        super::super::{bytes_encode, bytes_encoded_len},
        bytes::BufMut,
        prost::encoding,
        solana_sdk::instruction::CompiledInstruction,
        std::marker::PhantomData,
    };

    #[repr(transparent)]
    #[derive(Debug)]
    struct Wrapper<'a>(CompiledInstruction, PhantomData<&'a ()>);

    impl<'a> prost::Message for Wrapper<'a> {
        fn encode_raw(&self, buf: &mut impl BufMut)
        where
            Self: Sized,
        {
            let program_id_index = self.0.program_id_index as u32;
            if program_id_index != 0 {
                encoding::uint32::encode(1, &program_id_index, buf)
            }
            bytes_encode(2, &self.0.accounts, buf);
            bytes_encode(3, &self.0.data, buf)
        }
        fn encoded_len(&self) -> usize {
            let program_id_index = self.0.program_id_index as u32;
            (if program_id_index != 0 {
                encoding::uint32::encoded_len(1, &program_id_index)
            } else {
                0
            }) + bytes_encoded_len(2, &self.0.accounts)
                + bytes_encoded_len(3, &self.0.data)
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

    pub fn encode_compiled_instructions(
        tag: u32,
        compiled_instructions: &[CompiledInstruction],
        buf: &mut impl BufMut,
    ) {
        encoding::message::encode_repeated(tag, to_wrapper(compiled_instructions), buf)
    }

    pub fn compiled_instructions_encoded_len(
        tag: u32,
        compiled_instructions: &[CompiledInstruction],
    ) -> usize {
        encoding::message::encoded_len_repeated(tag, to_wrapper(compiled_instructions))
    }

    const fn to_wrapper<'a>(compiled_instructions: &'a [CompiledInstruction]) -> &'a [Wrapper<'a>] {
        // SAFETY: the compiler guarantees that `align_of::<Wrapper>() == align_of::<CompiledInstruction>()`, `size_of::<Wrapper>() == size_of::<CompiledInstruction>()`, the alignment of `Wrapper` and `CompiledInstruction` are identical.
        unsafe {
            std::slice::from_raw_parts(
                compiled_instructions.as_ptr() as *const Wrapper<'a>,
                compiled_instructions.len(),
            )
        }
    }
}

pub mod compiled_instruction {
    use {
        super::super::{bytes_encode, bytes_encoded_len},
        bytes::BufMut,
        prost::encoding,
        solana_sdk::instruction::CompiledInstruction,
    };

    #[derive(Debug)]
    struct Wrapper<'a>(&'a CompiledInstruction);

    impl<'a> prost::Message for Wrapper<'a> {
        fn encode_raw(&self, buf: &mut impl BufMut)
        where
            Self: Sized,
        {
            let program_id_index = self.0.program_id_index as u32;
            if program_id_index != 0 {
                encoding::uint32::encode(1, &program_id_index, buf)
            }
            bytes_encode(2, &self.0.accounts, buf);
            bytes_encode(3, &self.0.data, buf)
        }
        fn encoded_len(&self) -> usize {
            let program_id_index = self.0.program_id_index as u32;
            (if program_id_index != 0 {
                encoding::uint32::encoded_len(1, &program_id_index)
            } else {
                0
            }) + bytes_encoded_len(2, &self.0.accounts)
                + bytes_encoded_len(3, &self.0.data)
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

    pub fn encode_compiled_instruction(
        tag: u32,
        compiled_instruction: &CompiledInstruction,
        buf: &mut impl BufMut,
    ) {
        let wrapper = Wrapper(compiled_instruction);
        encoding::message::encode(tag, &wrapper, buf)
    }

    pub fn compiled_instruction_encoded_len(
        tag: u32,
        compiled_instruction: &CompiledInstruction,
    ) -> usize {
        let wrapper = Wrapper(compiled_instruction);
        encoding::message::encoded_len(tag, &wrapper)
    }
}

pub mod transaction_token_balance {
    use {
        bytes::BufMut, prost::encoding, solana_transaction_status::TransactionTokenBalance,
        std::marker::PhantomData,
    };

    #[repr(transparent)]
    #[derive(Debug)]
    struct Wrapper<'a>(TransactionTokenBalance, PhantomData<&'a ()>);

    impl<'a> prost::Message for Wrapper<'a> {
        fn encode_raw(&self, buf: &mut impl bytes::BufMut)
        where
            Self: Sized,
        {
            let account_index = self.0.account_index as u32;

            if account_index != 0 {
                encoding::uint32::encode(1, &account_index, buf)
            }
            if !self.0.mint.is_empty() {
                encoding::string::encode(2, &self.0.mint, buf)
            }
            super::ui_token_amount::encode_ui_token_amount(3, &self.0.ui_token_amount, buf);
            if !self.0.owner.is_empty() {
                encoding::string::encode(4, &self.0.owner, buf)
            }
            if !self.0.program_id.is_empty() {
                encoding::string::encode(5, &self.0.program_id, buf)
            }
        }
        fn encoded_len(&self) -> usize {
            let account_index = self.0.account_index as u32;

            (if account_index != 0 {
                encoding::uint32::encoded_len(1, &account_index)
            } else {
                0
            }) + if !self.0.mint.is_empty() {
                encoding::string::encoded_len(2, &self.0.mint)
            } else {
                0
            } + super::ui_token_amount::ui_token_amount_encoded_len(3, &self.0.ui_token_amount)
                + if !self.0.owner.is_empty() {
                    encoding::string::encoded_len(4, &self.0.owner)
                } else {
                    0
                }
                + if !self.0.program_id.is_empty() {
                    encoding::string::encoded_len(5, &self.0.program_id)
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

    pub fn encode_transaction_token_balances(
        tag: u32,
        transaction_token_balances: &[TransactionTokenBalance],
        buf: &mut impl BufMut,
    ) {
        encoding::message::encode_repeated(tag, to_wrapper(transaction_token_balances), buf)
    }

    pub fn transaction_token_balances_encoded_len(
        tag: u32,
        transaction_token_balances: &[TransactionTokenBalance],
    ) -> usize {
        encoding::message::encoded_len_repeated(tag, to_wrapper(transaction_token_balances))
    }

    const fn to_wrapper<'a>(
        transaction_token_balances: &'a [TransactionTokenBalance],
    ) -> &'a [Wrapper<'a>] {
        // SAFETY: the compiler guarantees that `align_of::<Wrapper>() == align_of::<TransactionTokenBalance>()`, `size_of::<Wrapper>() == size_of::<TransactionTokenBalance>()`, the alignment of `Wrapper` and `TransactionTokenBalance` are identical.
        unsafe {
            std::slice::from_raw_parts(
                transaction_token_balances.as_ptr() as *const Wrapper<'a>,
                transaction_token_balances.len(),
            )
        }
    }
}

pub mod ui_token_amount {
    use {bytes::BufMut, prost::encoding, solana_account_decoder::parse_token::UiTokenAmount};

    #[derive(Debug)]
    struct Wrapper<'a>(&'a UiTokenAmount);

    impl<'a> prost::Message for Wrapper<'a> {
        fn encode_raw(&self, buf: &mut impl bytes::BufMut)
        where
            Self: Sized,
        {
            let decimals = self.0.decimals as u32;
            if let Some(ui_amount) = self.0.ui_amount {
                if ui_amount != 0. {
                    encoding::double::encode(1, &ui_amount, buf)
                }
            }
            if decimals != 0 {
                encoding::uint32::encode(2, &decimals, buf)
            }
            if !self.0.amount.is_empty() {
                encoding::string::encode(3, &self.0.amount, buf)
            }
            if !self.0.ui_amount_string.is_empty() {
                encoding::string::encode(4, &self.0.ui_amount_string, buf)
            }
        }
        fn encoded_len(&self) -> usize {
            let decimals = self.0.decimals as u32;
            self.0.ui_amount.map_or(0, |ui_amount| {
                if ui_amount != 0. {
                    encoding::double::encoded_len(1, &ui_amount)
                } else {
                    0
                }
            }) + if decimals != 0 {
                encoding::uint32::encoded_len(2, &decimals)
            } else {
                0
            } + if !self.0.amount.is_empty() {
                encoding::string::encoded_len(3, &self.0.amount)
            } else {
                0
            } + if !self.0.ui_amount_string.is_empty() {
                encoding::string::encoded_len(4, &self.0.ui_amount_string)
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

    pub fn encode_ui_token_amount(
        tag: u32,
        ui_token_amount: &UiTokenAmount,
        buf: &mut impl BufMut,
    ) {
        let wrapper = Wrapper(ui_token_amount);
        encoding::message::encode(tag, &wrapper, buf)
    }

    pub fn ui_token_amount_encoded_len(tag: u32, ui_token_amount: &UiTokenAmount) -> usize {
        let wrapper = Wrapper(ui_token_amount);
        encoding::message::encoded_len(tag, &wrapper)
    }
}

pub mod loaded_addresses {
    use {
        super::super::bytes_encode, crate::protobuf::encoding::iter_encoded_len, bytes::BufMut,
        solana_sdk::message::v0::LoadedAddresses,
    };

    pub fn encode_loaded_writable_addresses(
        tag: u32,
        loaded_addresses: &LoadedAddresses,
        buf: &mut impl BufMut,
    ) {
        let writable_addresses = loaded_addresses.writable.iter().map(|key| key.as_ref());
        for value in writable_addresses {
            bytes_encode(tag, value, buf)
        }
    }

    pub fn loaded_writable_addresses_encoded_len(
        tag: u32,
        loaded_addresses: &LoadedAddresses,
    ) -> usize {
        iter_encoded_len(
            tag,
            loaded_addresses
                .writable
                .iter()
                .map(|pubkey| pubkey.as_ref().len()),
            loaded_addresses.len(),
        )
    }

    pub fn encode_loaded_readonly_addresses(
        tag: u32,
        loaded_addresses: &LoadedAddresses,
        buf: &mut impl BufMut,
    ) {
        let readonly_addresses = loaded_addresses.readonly.iter().map(|key| key.as_ref());
        for value in readonly_addresses {
            bytes_encode(tag, value, buf)
        }
    }

    pub fn loaded_readonly_addresses_encoded_len(
        tag: u32,
        loaded_addresses: &LoadedAddresses,
    ) -> usize {
        iter_encoded_len(
            tag,
            loaded_addresses
                .readonly
                .iter()
                .map(|pubkey| pubkey.as_ref().len()),
            loaded_addresses.len(),
        )
    }
}

mod return_data {
    use {
        super::super::{bytes_encode, bytes_encoded_len},
        bytes::BufMut,
        prost::encoding::{self, WireType},
        solana_sdk::transaction_context::TransactionReturnData,
    };

    #[derive(Debug)]
    struct Wrapper<'a>(&'a TransactionReturnData);

    impl<'a> prost::Message for Wrapper<'a> {
        fn encode_raw(&self, buf: &mut impl bytes::BufMut)
        where
            Self: Sized,
        {
            bytes_encode(1, self.0.program_id.as_ref(), buf);
            bytes_encode(2, &self.0.data, buf)
        }
        fn encoded_len(&self) -> usize {
            bytes_encoded_len(1, self.0.program_id.as_ref()) + bytes_encoded_len(2, &self.0.data)
        }
        fn clear(&mut self) {
            unimplemented!()
        }
        fn merge_field(
            &mut self,
            _tag: u32,
            _wire_type: WireType,
            _buf: &mut impl bytes::Buf,
            _ctx: encoding::DecodeContext,
        ) -> Result<(), prost::DecodeError>
        where
            Self: Sized,
        {
            unimplemented!()
        }
    }

    pub fn encode_transaction_return_data(
        tag: u32,
        return_data: &TransactionReturnData,
        buf: &mut impl BufMut,
    ) {
        let wrapper = Wrapper(return_data);
        encoding::message::encode(tag, &wrapper, buf)
    }

    pub fn transaction_return_data_encoded_len(
        tag: u32,
        return_data: &TransactionReturnData,
    ) -> usize {
        let wrapper = Wrapper(return_data);
        encoding::message::encoded_len(tag, &wrapper)
    }
}

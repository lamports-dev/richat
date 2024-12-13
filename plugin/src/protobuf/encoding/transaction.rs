use {
    super::{bytes_encode, bytes_encoded_len, field_encoded_len},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
    prost::encoding::{self, encode_key, encode_varint, message, WireType},
    solana_sdk::{clock::Slot, signature::Signature},
};

#[derive(Debug)]
pub struct Transaction<'a> {
    slot: Slot,
    signature: Signature,
    is_vote: bool,
    transaction: proto::Transaction,
    transaction_status_meta: proto::TransactionStatusMeta<'a>,
    index: usize,
}

impl<'a> prost::Message for Transaction<'a> {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        let index = self.index as u64;

        encode_key(1, WireType::LengthDelimited, buf);
        encode_varint(self.transaction_encoded_len() as u64, buf);

        bytes_encode(1, self.signature.as_ref(), buf);
        encoding::bool::encode(2, &self.is_vote, buf);
        message::encode(3, &self.transaction, buf);
        message::encode(4, &self.transaction_status_meta, buf);
        encoding::uint64::encode(5, &index, buf);

        encoding::uint64::encode(2, &self.slot, buf)
    }
    fn encoded_len(&self) -> usize {
        field_encoded_len(1, self.transaction_encoded_len())
            + encoding::uint64::encoded_len(2, &self.slot)
    }
    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl hyper::body::Buf,
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

impl Transaction {
    pub fn new(slot: Slot, transaction: &ReplicaTransactionInfoV2<'_>) -> Self {
        Self {
            slot,
            signature: *transaction.signature,
            is_vote: transaction.is_vote,
            transaction: proto::convert_to::create_transaction(transaction.transaction),
            transaction_status_meta: proto::convert_to::create_transaction_meta(
                transaction.transaction_status_meta,
            ),
            index: transaction.index,
        }
    }
    fn transaction_encoded_len(&self) -> usize {
        let index = self.index as u64;

        bytes_encoded_len(1u32, self.signature.as_ref())
            + encoding::bool::encoded_len(2u32, &self.is_vote)
            + message::encoded_len(3u32, &self.transaction)
            + message::encoded_len(4u32, &self.transaction_status_meta)
            + encoding::uint64::encoded_len(5u32, &index)
    }
}

mod proto {
    pub fn encode_transaction(
        slot: Slot,
        transaction: &ReplicaTransactionInfoV2<'_>,
        buf: &mut impl BufMut,
    ) {
        let index = transaction.index as u64;

        encoding::encode_key(1, WireType::LengthDelimited, buf);
        encoding::encode_varint(transaction_encoded_len(transaction) as u64, buf);

        bytes_encode(1, transaction.signature.as_ref(), buf);
        encoding::bool::encode(2, &transaction.is_vote, buf);
        encode_sanitazed_transaction(&transaction.transaction, buf);
        encode_transaction_status_meta(&transaction.transaction_status_meta, buf);
        encoding::uint64::encode(5, &index, buf);

        encoding::uint64::encode(2, &slot, buf)
    }

    pub fn transaction_encoded_len(transaction: &ReplicaTransactionInfoV2<'_>) -> usize {
        0
    }

    pub fn encode_sanitazed_transaction(sanitazed: &SanitizedTransaction, buf: &mut impl BufMut) {
        let signatures = sanitazed
            .signatures()
            .iter()
            .map(|signature| <solana_sdk::signature::Signature as AsRef<[u8]>>::as_ref(signature));
        for value in signatures {
            bytes_encode(1, value, buf)
        }
        encode_sanitazed_message(sanitazed.message(), buf)
    }

    pub fn sanitazed_transaction_encoded_len(sanitazed: &SanitizedTransaction) -> usize {
        0
    }

    pub fn encode_transaction_status_meta(
        transaction_status_meta: &TransactionStatusMeta,
        buf: &mut impl BufMut,
    ) {
    }

    pub fn transaction_status_meta_encoded_len(
        transaction_status_meta: &TransactionStatusMeta,
    ) -> usize {
        0
    }

    pub fn encode_sanitazed_message(sanitazed: &SanitizedMessage, buf: &mut impl BufMut) {
        match sanitazed {
            SanitizedMessage::Legacy(LegacyMessage { message, .. }) => {
                encode_message_header(&message.header, buf);
                encode_pubkeys(&message.account_keys, buf);
                encode_recent_blockhash(&message.recent_blockhash.to_bytes(), buf);
                encode_compiled_instructions(&message.instructions, buf);
            }
            SanitizedMessage::V0(LoadedMessage { message, .. }) => {
                encode_message_header(&message.header, buf);
                encode_pubkeys(&message.account_keys, buf);
                encode_recent_blockhash(&message.recent_blockhash.to_bytes(), buf);
                encode_compiled_instructions(&message.instructions, buf);
            }
        }
    }

    pub fn encode_message_header(
        header: &solana_sdk::message::MessageHeader,
        buf: &mut impl BufMut,
    ) {
        let num_required_signatures = header.num_required_signatures as u32;
        let num_readonly_signed_accounts = header.num_readonly_signed_accounts as u32;
        let num_readonly_unsigned_accounts = header.num_readonly_unsigned_accounts as u32;
        encoding::encode_key(1, WireType::LengthDelimited, buf);
        encoding::encode_varint(
            message_header_encoded_len((
                num_required_signatures,
                num_readonly_signed_accounts,
                num_readonly_unsigned_accounts,
            )) as u64,
            buf,
        );
        encoding::uint32::encode(1, &num_required_signatures, buf);
        encoding::uint32::encode(2, &num_readonly_signed_accounts, buf);
        encoding::uint32::encode(3, &num_readonly_unsigned_accounts, buf)
    }

    pub fn message_header_encoded_len(header: (u32, u32, u32)) -> usize {
        encoding::uint32::encoded_len(1, &header.0)
            + encoding::uint32::encoded_len(2, &header.1)
            + encoding::uint32::encoded_len(3, &header.2)
    }

    pub fn encode_pubkeys(pubkeys: &[Pubkey], buf: &mut impl BufMut) {
        let iter = pubkeys
            .iter()
            .map(|key| <Pubkey as AsRef<[u8]>>::as_ref(key));
        for value in iter {
            bytes_encode(2, value, buf);
        }
    }

    pub fn encode_recent_blockhash(pubkey: &[u8], buf: &mut impl BufMut) {
        bytes_encode(3, pubkey, buf)
    }

    pub fn encode_compiled_instructions(
        compiled_instructions: &[solana_sdk::instruction::CompiledInstruction],
        buf: &mut impl BufMut,
    ) {
        encoding::encode_key(4, WireType::LengthDelimited, buf);
        encoding::encode_varint(
            compiled_instructions_encoded_len(compiled_instructions) as u64,
            buf,
        );
        for compiled_instruction in compiled_instructions {
            encode_compiled_instruction(compiled_instruction, buf)
        }
    }

    pub fn compiled_instructions_encoded_len(
        compiled_instructions: &[solana_sdk::instruction::CompiledInstruction],
    ) -> usize {
        encoding::key_len(4) * compiled_instructions.len()
            + compiled_instructions
                .iter()
                .map(compiled_instruction_encoded_len)
                .map(|len| len + encoding::encoded_len_varint(len as u64))
                .sum::<usize>()
    }

    pub fn encode_compiled_instruction(
        compiled_instruction: &solana_sdk::instruction::CompiledInstruction,
        buf: &mut impl BufMut,
    ) {
        let program_id_index = compiled_instruction.program_id_index as u32;
        encoding::uint32::encode(1, &program_id_index, buf);
        bytes_encode(2, &compiled_instruction.accounts, buf);
        bytes_encode(3, &compiled_instruction.data, buf)
    }

    pub fn compiled_instruction_encoded_len(
        compiled_instruction: &solana_sdk::instruction::CompiledInstruction,
    ) -> usize {
        let program_id_index = compiled_instruction.program_id_index as u32;
        encoding::uint32::encoded_len(1, &program_id_index)
            + bytes_encoded_len(2, &compiled_instruction.accounts)
            + bytes_encoded_len(3, &compiled_instruction.data)
    }

    pub mod convert_to {
        use {
            crate::protobuf::encoding::proto::convert_to,
            solana_sdk::{
                instruction::CompiledInstruction,
                message::{
                    v0::{LoadedMessage, MessageAddressTableLookup},
                    LegacyMessage, MessageHeader, SanitizedMessage,
                },
                pubkey::Pubkey,
                signature::Signature,
                transaction::{SanitizedTransaction, TransactionError},
                transaction_context::TransactionReturnData,
            },
            solana_transaction_status::{
                InnerInstruction, InnerInstructions, TransactionStatusMeta, TransactionTokenBalance,
            },
        };

        pub fn create_message(message: &SanitizedMessage) -> super::Message {
            match message {
                SanitizedMessage::Legacy(LegacyMessage { message, .. }) => super::Message {
                    header: Some(create_header(&message.header)),
                    account_keys: create_pubkeys(&message.account_keys),
                    recent_blockhash: message.recent_blockhash.to_bytes().into(),
                    instructions: create_instructions(&message.instructions),
                    versioned: false,
                    address_table_lookups: Vec::new(),
                },
                SanitizedMessage::V0(LoadedMessage { message, .. }) => super::Message {
                    header: Some(create_header(&message.header)),
                    account_keys: create_pubkeys(&message.account_keys),
                    recent_blockhash: message.recent_blockhash.to_bytes().into(),
                    instructions: create_instructions(&message.instructions),
                    versioned: true,
                    address_table_lookups: create_lookups(&message.address_table_lookups),
                },
            }
        }

        pub fn create_pubkeys(pubkeys: &[Pubkey]) -> Vec<Vec<u8>> {
            pubkeys
                .iter()
                .map(|key| <Pubkey as AsRef<[u8]>>::as_ref(key).into())
                .collect() // TODO: try to remove allocation
        }

        pub fn create_instructions(ixs: &[CompiledInstruction]) -> Vec<super::CompiledInstruction> {
            ixs.iter().map(create_instruction).collect() // TODO: try to remove allocation
        }

        pub fn create_instruction(ix: &CompiledInstruction) -> super::CompiledInstruction {
            super::CompiledInstruction {
                program_id_index: ix.program_id_index as u32,
                accounts: ix.accounts.clone(), // TODO: try to remove allocation
                data: ix.data.clone(),         // TODO: try to remove allocation
            }
        }

        pub fn create_lookups(
            lookups: &[MessageAddressTableLookup],
        ) -> Vec<super::MessageAddressTableLookup> {
            lookups.iter().map(create_lookup).collect() // TODO: try to remove allocation
        }

        pub fn create_lookup(
            lookup: &MessageAddressTableLookup,
        ) -> super::MessageAddressTableLookup {
            super::MessageAddressTableLookup {
                account_key: <Pubkey as AsRef<[u8]>>::as_ref(&lookup.account_key).into(),
                writable_indexes: lookup.writable_indexes.clone(), // TODO: try to remove allocation
                readonly_indexes: lookup.readonly_indexes.clone(), // TODO: try to remove allocation
            }
        }

        pub fn create_transaction_meta(
            meta: &TransactionStatusMeta,
        ) -> super::TransactionStatusMeta {
            let TransactionStatusMeta {
                status,
                fee,
                pre_balances,
                post_balances,
                inner_instructions,
                log_messages,
                pre_token_balances,
                post_token_balances,
                rewards,
                loaded_addresses,
                return_data,
                compute_units_consumed,
            } = meta;
            let err = create_transaction_error(status);
            let inner_instructions_none = inner_instructions.is_none();
            let inner_instructions = inner_instructions
                .as_deref()
                .map(create_inner_instructions_vec)
                .unwrap_or_default();
            let log_messages_none = log_messages.is_none();
            let log_messages = log_messages.clone().unwrap_or_default(); // TODO: try to remove allocation
            let pre_token_balances = pre_token_balances
                .as_deref()
                .map(create_token_balances)
                .unwrap_or_default();
            let post_token_balances = post_token_balances
                .as_deref()
                .map(create_token_balances)
                .unwrap_or_default();
            let rewards = rewards
                .as_deref()
                .map(convert_to::create_rewards)
                .unwrap_or_default();
            let loaded_writable_addresses = create_pubkeys(&loaded_addresses.writable);
            let loaded_readonly_addresses = create_pubkeys(&loaded_addresses.readonly);

            super::TransactionStatusMeta {
                err,
                fee: *fee,
                pre_balances: pre_balances.clone(),
                post_balances: post_balances.clone(),
                inner_instructions,
                inner_instructions_none,
                log_messages,
                log_messages_none,
                pre_token_balances,
                post_token_balances,
                rewards,
                loaded_writable_addresses,
                loaded_readonly_addresses,
                return_data: return_data.as_ref().map(create_return_data),
                return_data_none: return_data.is_none(),
                compute_units_consumed: *compute_units_consumed,
            }
        }

        pub fn create_transaction_error(
            status: &Result<(), TransactionError>,
        ) -> Option<super::TransactionError> {
            match status {
                Ok(()) => None,
                Err(err) => Some(super::TransactionError {
                    err: bincode::serialize(&err).expect("transaction error to serialize to bytes"), // TODO: try to remove allocation
                }),
            }
        }

        pub fn create_inner_instructions_vec(
            ixs: &[InnerInstructions],
        ) -> Vec<super::InnerInstructions> {
            ixs.iter().map(create_inner_instructions).collect() // TODO: try to remove allocation
        }

        pub fn create_inner_instructions(
            instructions: &InnerInstructions,
        ) -> super::InnerInstructions {
            super::InnerInstructions {
                index: instructions.index as u32,
                instructions: create_inner_instruction_vec(&instructions.instructions),
            }
        }

        pub fn create_inner_instruction_vec(
            ixs: &[InnerInstruction],
        ) -> Vec<super::InnerInstruction> {
            ixs.iter().map(create_inner_instruction).collect() // TODO: try to remove allocation
        }

        pub fn create_inner_instruction(instruction: &InnerInstruction) -> super::InnerInstruction {
            super::InnerInstruction {
                program_id_index: instruction.instruction.program_id_index as u32,
                accounts: instruction.instruction.accounts.clone(), // TODO: try to remove allocation
                data: instruction.instruction.data.clone(), // TODO: try to remove allocation
                stack_height: instruction.stack_height,
            }
        }

        pub fn create_token_balances(
            balances: &[TransactionTokenBalance],
        ) -> Vec<super::TokenBalance> {
            balances.iter().map(create_token_balance).collect() // TODO: try to remove allocation
        }

        pub fn create_token_balance(balance: &TransactionTokenBalance) -> super::TokenBalance {
            super::TokenBalance {
                account_index: balance.account_index as u32,
                mint: balance.mint.clone(), // TODO: try to remove allocation
                ui_token_amount: Some(super::UiTokenAmount {
                    ui_amount: balance.ui_token_amount.ui_amount.unwrap_or_default(),
                    decimals: balance.ui_token_amount.decimals as u32,
                    amount: balance.ui_token_amount.amount.clone(), // TODO: try to remove allocation
                    ui_amount_string: balance.ui_token_amount.ui_amount_string.clone(), // TODO: try to remove allocation
                }),
                owner: balance.owner.clone(), // TODO: try to remove allocation
                program_id: balance.program_id.clone(), // TODO: try to remove allocation
            }
        }

        pub fn create_return_data(return_data: &TransactionReturnData) -> super::ReturnData<'_> {
            let program_id = return_data.program_id.to_bytes();
            let r = program_id.as_ref();
            let data = return_data.data.as_ref();
            super::ReturnData {
                program_id: r,
                data,
            }
        }
    }

    use {
        crate::protobuf::encoding::{
            bytes_encode, bytes_encode_repeated, bytes_encoded_len, bytes_encoded_len_repeated,
            proto, str_encode_repeated, str_encoded_len_repeated,
        },
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
        prost::{
            bytes::BufMut,
            encoding::{self, message, DecodeContext, WireType},
        },
        solana_sdk::{
            clock::Slot,
            message::{v0::LoadedMessage, LegacyMessage, SanitizedMessage},
            pubkey::Pubkey,
            transaction::SanitizedTransaction,
        },
        solana_transaction_status::TransactionStatusMeta,
    };

    #[derive(PartialEq, Debug)]
    pub struct Transaction<'a> {
        pub signatures: &'a [&'a [u8]],
        pub message: Option<Message<'a>>,
    }

    impl<'a> prost::Message for Transaction<'a> {
        fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
        where
            Self: Sized,
        {
            bytes_encode_repeated(1, self.signatures, buf);
            if let Some(ref message) = self.message {
                message::encode(2, message, buf)
            }
        }
        fn encoded_len(&self) -> usize {
            bytes_encoded_len_repeated(1, self.signatures)
                + self
                    .message
                    .as_ref()
                    .map_or(0, |message| message::encoded_len(2, message))
        }
        fn merge_field(
            &mut self,
            tag: u32,
            wire_type: WireType,
            buf: &mut impl hyper::body::Buf,
            ctx: DecodeContext,
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

    #[derive(PartialEq, Debug)]
    pub struct Message<'a> {
        pub header: Option<MessageHeader>,
        pub account_keys: &'a [&'a [u8]],
        pub recent_blockhash: &'a [u8],
        pub instructions: &'a [CompiledInstruction<'a>],
        pub versioned: bool,
        pub address_table_lookups: &'a [MessageAddressTableLookup<'a>],
    }

    impl<'a> prost::Message for Message<'a> {
        fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
        where
            Self: Sized,
        {
            if let Some(ref header) = self.header {
                message::encode(1, header, buf)
            }
            bytes_encode_repeated(2, self.account_keys, buf);
            bytes_encode(3, self.recent_blockhash, buf);
            message::encode_repeated(4, self.instructions, buf);
            encoding::bool::encode(5, &self.versioned, buf);
            message::encode_repeated(6, self.address_table_lookups, buf)
        }
        fn encoded_len(&self) -> usize {
            self.header
                .as_ref()
                .map_or(0, |header| message::encoded_len(1, header))
                + bytes_encoded_len_repeated(2, self.account_keys)
                + bytes_encoded_len(3, self.recent_blockhash)
                + message::encoded_len_repeated(4, self.instructions)
                + encoding::bool::encoded_len(5, &self.versioned)
                + message::encoded_len_repeated(6, self.address_table_lookups)
        }
        fn merge_field(
            &mut self,
            tag: u32,
            wire_type: WireType,
            buf: &mut impl hyper::body::Buf,
            ctx: DecodeContext,
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

    #[derive(PartialEq, Debug)]
    pub struct MessageHeader {
        pub num_required_signatures: u32,
        pub num_readonly_signed_accounts: u32,
        pub num_readonly_unsigned_accounts: u32,
    }

    impl prost::Message for MessageHeader {
        fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
        where
            Self: Sized,
        {
            encoding::uint32::encode(1, &self.num_required_signatures, buf);
            encoding::uint32::encode(2, &self.num_readonly_signed_accounts, buf);
            encoding::uint32::encode(3, &self.num_readonly_unsigned_accounts, buf)
        }
        fn encoded_len(&self) -> usize {
            encoding::uint32::encoded_len(1, &self.num_required_signatures)
                + encoding::uint32::encoded_len(2, &self.num_readonly_signed_accounts)
                + encoding::uint32::encoded_len(3, &self.num_readonly_unsigned_accounts)
        }
        fn merge_field(
            &mut self,
            tag: u32,
            wire_type: WireType,
            buf: &mut impl hyper::body::Buf,
            ctx: DecodeContext,
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

    #[derive(PartialEq, Debug)]
    pub struct MessageAddressTableLookup<'a> {
        pub account_key: &'a [u8],
        pub writable_indexes: &'a [u8],
        pub readonly_indexes: &'a [u8],
    }

    impl<'a> prost::Message for MessageAddressTableLookup<'a> {
        fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
        where
            Self: Sized,
        {
            bytes_encode(1, self.account_key, buf);
            bytes_encode(2, self.writable_indexes, buf);
            bytes_encode(3, self.readonly_indexes, buf)
        }
        fn encoded_len(&self) -> usize {
            bytes_encoded_len(1, self.account_key)
                + bytes_encoded_len(2, self.writable_indexes)
                + bytes_encoded_len(3, self.readonly_indexes)
        }
        fn merge_field(
            &mut self,
            tag: u32,
            wire_type: WireType,
            buf: &mut impl hyper::body::Buf,
            ctx: DecodeContext,
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

    #[derive(PartialEq, Debug)]
    pub struct TransactionStatusMeta<'a> {
        pub err: Option<TransactionError<'a>>,
        pub fee: u64,
        pub pre_balances: &'a [u64],
        pub post_balances: &'a [u64],
        pub inner_instructions: &'a [InnerInstructions<'a>],
        pub inner_instructions_none: bool,
        pub log_messages: &'a [&'a str],
        pub log_messages_none: bool,
        pub pre_token_balances: &'a [TokenBalance<'a>],
        pub post_token_balances: &'a [TokenBalance<'a>],
        pub rewards: &'a [proto::Reward],
        pub loaded_writable_addresses: &'a [&'a [u8]],
        pub loaded_readonly_addresses: &'a [&'a [u8]],
        pub return_data: Option<ReturnData<'a>>,
        pub return_data_none: bool,
        pub compute_units_consumed: Option<u64>,
    }

    impl<'a> prost::Message for TransactionStatusMeta<'a> {
        fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
        where
            Self: Sized,
        {
            if let Some(ref err) = self.err {
                message::encode(1, err, buf)
            }
            encoding::uint64::encode(2, &self.fee, buf);
            encoding::uint64::encode_repeated(3, self.pre_balances, buf);
            encoding::uint64::encode_repeated(4, self.post_balances, buf);
            message::encode_repeated(5, self.inner_instructions, buf);
            str_encode_repeated(6, self.log_messages, buf);
            message::encode_repeated(7, self.pre_token_balances, buf);
            message::encode_repeated(8, self.post_token_balances, buf);
            message::encode_repeated(9, self.rewards, buf);
            encoding::bool::encode(10, &self.inner_instructions_none, buf);
            encoding::bool::encode(11, &self.log_messages_none, buf);
            bytes_encode_repeated(12, self.loaded_writable_addresses, buf);
            bytes_encode_repeated(13, self.loaded_readonly_addresses, buf);
            if let Some(ref return_data) = self.return_data {
                message::encode(14, return_data, buf)
            }
            encoding::bool::encode(15, &self.return_data_none, buf);
            if let Some(ref compute_units_consumed) = self.compute_units_consumed {
                encoding::uint64::encode(16, compute_units_consumed, buf)
            }
        }
        fn encoded_len(&self) -> usize {
            self.err
                .as_ref()
                .map_or(0, |err| message::encoded_len(1, err))
                + encoding::uint64::encoded_len(2, &self.fee)
                + encoding::uint64::encoded_len_repeated(3, self.pre_balances)
                + encoding::uint64::encoded_len_repeated(4, self.post_balances)
                + message::encoded_len_repeated(5, self.inner_instructions)
                + str_encoded_len_repeated(6, self.log_messages)
                + message::encoded_len_repeated(7, self.pre_token_balances)
                + message::encoded_len_repeated(8, self.post_token_balances)
                + message::encoded_len_repeated(9, self.rewards)
                + encoding::bool::encoded_len(10, &self.inner_instructions_none)
                + encoding::bool::encoded_len(11, &self.log_messages_none)
                + bytes_encoded_len_repeated(12, self.loaded_writable_addresses)
                + bytes_encoded_len_repeated(13, self.loaded_readonly_addresses)
                + self
                    .return_data
                    .as_ref()
                    .map_or(0, |return_data| message::encoded_len(14, return_data))
                + encoding::bool::encoded_len(15, &self.return_data_none)
                + self
                    .compute_units_consumed
                    .as_ref()
                    .map_or(0, |compute_units_consumed| {
                        encoding::uint64::encoded_len(16, compute_units_consumed)
                    })
        }
        fn merge_field(
            &mut self,
            tag: u32,
            wire_type: WireType,
            buf: &mut impl hyper::body::Buf,
            ctx: DecodeContext,
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

    #[derive(PartialEq, Debug)]
    pub struct TransactionError<'a> {
        pub err: &'a [u8],
    }

    impl<'a> prost::Message for TransactionError<'a> {
        fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
        where
            Self: Sized,
        {
            bytes_encode(1, self.err, buf)
        }
        fn encoded_len(&self) -> usize {
            bytes_encoded_len(1, self.err)
        }
        fn merge_field(
            &mut self,
            tag: u32,
            wire_type: WireType,
            buf: &mut impl hyper::body::Buf,
            ctx: DecodeContext,
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

    #[derive(PartialEq, Debug)]
    pub struct InnerInstructions<'a> {
        pub index: u32,
        pub instructions: &'a [InnerInstruction<'a>],
    }

    impl<'a> prost::Message for InnerInstructions<'a> {
        fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
        where
            Self: Sized,
        {
            encoding::uint32::encode(1, &self.index, buf);
            for instruction in self.instructions {
                message::encode(2, instruction, buf)
            }
        }
        fn encoded_len(&self) -> usize {
            encoding::uint32::encoded_len(1, &self.index)
                + message::encoded_len_repeated(2, self.instructions)
        }
        fn merge_field(
            &mut self,
            tag: u32,
            wire_type: WireType,
            buf: &mut impl hyper::body::Buf,
            ctx: DecodeContext,
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

    #[derive(PartialEq, Debug)]
    pub struct InnerInstruction<'a> {
        pub program_id_index: u32,
        pub accounts: &'a [u8],
        pub data: &'a [u8],
        pub stack_height: Option<u32>,
    }

    impl<'a> prost::Message for InnerInstruction<'a> {
        fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
        where
            Self: Sized,
        {
            encoding::uint32::encode(1, &self.program_id_index, buf);
            bytes_encode(2, self.accounts, buf);
            bytes_encode(3, self.data, buf);
            if let Some(ref stack_height) = self.stack_height {
                encoding::uint32::encode(4, stack_height, buf)
            }
        }
        fn encoded_len(&self) -> usize {
            encoding::uint32::encoded_len(1, &self.program_id_index)
                + bytes_encoded_len(2, self.accounts)
                + bytes_encoded_len(3, self.data)
                + self.stack_height.as_ref().map_or(0, |stack_height| {
                    encoding::uint32::encoded_len(4, stack_height)
                })
        }
        fn merge_field(
            &mut self,
            tag: u32,
            wire_type: WireType,
            buf: &mut impl hyper::body::Buf,
            ctx: DecodeContext,
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

    #[derive(PartialEq, Debug)]
    pub struct CompiledInstruction<'a> {
        pub program_id_index: u32,
        pub accounts: &'a [u8],
        pub data: &'a [u8],
    }

    impl<'a> prost::Message for CompiledInstruction<'a> {
        fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
        where
            Self: Sized,
        {
            encoding::uint32::encode(1, &self.program_id_index, buf);
            bytes_encode(2, self.accounts, buf);
            bytes_encode(3, self.data, buf)
        }
        fn encoded_len(&self) -> usize {
            encoding::uint32::encoded_len(1, &self.program_id_index)
                + bytes_encoded_len(2, self.accounts)
                + bytes_encoded_len(3, self.data)
        }
        fn merge_field(
            &mut self,
            tag: u32,
            wire_type: WireType,
            buf: &mut impl hyper::body::Buf,
            ctx: DecodeContext,
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

    #[derive(PartialEq, Debug)]
    pub struct TokenBalance<'a> {
        pub account_index: u32,
        pub mint: &'a str,
        pub ui_token_amount: Option<UiTokenAmount<'a>>,
        pub owner: &'a str,
        pub program_id: &'a str,
    }

    impl<'a> prost::Message for TokenBalance<'a> {
        fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
        where
            Self: Sized,
        {
            encoding::uint32::encode(1, &self.account_index, buf);
            bytes_encode(2, self.mint.as_ref(), buf);
            if let Some(ref ui_token_amount) = self.ui_token_amount {
                message::encode(3, ui_token_amount, buf)
            }
            bytes_encode(4, self.owner.as_ref(), buf);
            bytes_encode(5, self.program_id.as_ref(), buf)
        }
        fn encoded_len(&self) -> usize {
            encoding::uint32::encoded_len(1, &self.account_index)
                + bytes_encoded_len(2, self.mint.as_ref())
                + self.ui_token_amount.as_ref().map_or(0, |ui_token_amount| {
                    message::encoded_len(3, ui_token_amount)
                })
                + bytes_encoded_len(4, self.owner.as_ref())
                + bytes_encoded_len(5, self.program_id.as_ref())
        }
        fn clear(&mut self) {
            unimplemented!()
        }
        fn merge_field(
            &mut self,
            _tag: u32,
            _wire_type: WireType,
            _buf: &mut impl hyper::body::Buf,
            _ctx: DecodeContext,
        ) -> Result<(), prost::DecodeError>
        where
            Self: Sized,
        {
            unimplemented!()
        }
    }

    #[derive(PartialEq, Debug)]
    pub struct UiTokenAmount<'a> {
        pub ui_amount: f64,
        pub decimals: u32,
        pub amount: &'a str,
        pub ui_amount_string: &'a str,
    }

    impl<'a> prost::Message for UiTokenAmount<'a> {
        fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
        where
            Self: Sized,
        {
            encoding::double::encode(1, &self.ui_amount, buf);
            encoding::uint32::encode(2, &self.decimals, buf);
            bytes_encode(3, self.amount.as_ref(), buf);
            bytes_encode(4, self.ui_amount_string.as_ref(), buf)
        }
        fn encoded_len(&self) -> usize {
            encoding::double::encoded_len(1, &self.ui_amount)
                + encoding::uint32::encoded_len(2, &self.decimals)
                + bytes_encoded_len(3, self.amount.as_ref())
                + bytes_encoded_len(4, self.ui_amount_string.as_ref())
        }
        fn merge_field(
            &mut self,
            _tag: u32,
            _wire_type: WireType,
            _buf: &mut impl hyper::body::Buf,
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

    #[derive(PartialEq, Debug)]
    pub struct ReturnData<'a> {
        pub program_id: &'a [u8],
        pub data: &'a [u8],
    }

    impl<'a> prost::Message for ReturnData<'a> {
        fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut)
        where
            Self: Sized,
        {
            bytes_encode(1, self.program_id, buf);
            bytes_encode(2, self.data, buf)
        }
        fn encoded_len(&self) -> usize {
            bytes_encoded_len(1, self.program_id) + bytes_encoded_len(2, self.data)
        }
        fn merge_field(
            &mut self,
            _tag: u32,
            _wire_type: WireType,
            _buf: &mut impl hyper::body::Buf,
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
}

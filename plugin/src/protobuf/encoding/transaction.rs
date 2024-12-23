use {
    super::{
        bytes_encode, bytes_encoded_len, encode_rewards, field_encoded_len, iter_encoded_len,
        rewards_encoded_len,
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
    prost::{
        bytes::BufMut,
        encoding::{self, encode_key, encode_varint, WireType},
    },
    solana_account_decoder::parse_token::UiTokenAmount,
    solana_sdk::{
        clock::Slot,
        instruction::CompiledInstruction,
        message::{
            v0::{LoadedAddresses, LoadedMessage, MessageAddressTableLookup},
            LegacyMessage, SanitizedMessage,
        },
        pubkey::Pubkey,
        signature::Signature,
        transaction::{SanitizedTransaction, TransactionError},
        transaction_context::TransactionReturnData,
    },
    solana_transaction_status::{
        InnerInstruction, InnerInstructions, TransactionStatusMeta, TransactionTokenBalance,
    },
    std::cell::RefCell,
};

#[derive(Debug)]
pub struct Transaction<'a> {
    slot: Slot,
    transaction: &'a ReplicaTransactionInfoV2<'a>,
}

impl<'a> prost::Message for Transaction<'a> {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        encode_key(1, WireType::LengthDelimited, buf);
        encode_varint(
            replica_transaction_info_encoded_len(self.transaction) as u64,
            buf,
        );

        encode_replica_transaction_info(self.transaction, buf);

        if self.slot != 0 {
            encoding::uint64::encode(2, &self.slot, buf)
        }
    }

    fn encoded_len(&self) -> usize {
        field_encoded_len(1, replica_transaction_info_encoded_len(self.transaction))
            + if self.slot != 0 {
                encoding::uint64::encoded_len(2, &self.slot)
            } else {
                0
            }
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

impl<'a> Transaction<'a> {
    pub const fn new(slot: Slot, transaction: &'a ReplicaTransactionInfoV2<'a>) -> Self {
        Self { slot, transaction }
    }
}

fn encode_replica_transaction_info(
    transaction: &ReplicaTransactionInfoV2<'_>,
    buf: &mut impl BufMut,
) {
    let index = transaction.index as u64;

    bytes_encode(1, transaction.signature.as_ref(), buf);
    if transaction.is_vote {
        encoding::bool::encode(2, &transaction.is_vote, buf)
    }
    encode_sanitazed_transaction(transaction.transaction, buf);
    encode_transaction_status_meta(transaction.transaction_status_meta, buf);
    if index != 0 {
        encoding::uint64::encode(5, &index, buf)
    }
}

fn replica_transaction_info_encoded_len(transaction: &ReplicaTransactionInfoV2<'_>) -> usize {
    let index = transaction.index as u64;

    bytes_encoded_len(1, transaction.signature.as_ref())
        + if transaction.is_vote {
            encoding::bool::encoded_len(2, &transaction.is_vote)
        } else {
            0
        }
        + sanitazed_transaction_encoded_len(transaction.transaction)
        + transaction_status_meta_encoded_len(transaction.transaction_status_meta)
        + if index != 0 {
            encoding::uint64::encoded_len(5, &index)
        } else {
            0
        }
}

fn encode_sanitazed_transaction(sanitazed: &SanitizedTransaction, buf: &mut impl BufMut) {
    encode_key(3, WireType::LengthDelimited, buf);
    encode_varint(sanitazed_transaction_encoded_len(sanitazed) as u64, buf);
    encode_signatures(sanitazed.signatures(), buf);
    encode_sanitazed_message(sanitazed.message(), buf)
}

fn sanitazed_transaction_encoded_len(sanitazed: &SanitizedTransaction) -> usize {
    let len = signatures_encoded_len(sanitazed.signatures())
        + sanitazed_message_encoded_len(sanitazed.message());
    field_encoded_len(3, len)
}

fn encode_signatures(signatures: &[Signature], buf: &mut impl BufMut) {
    let signatures = signatures.iter().map(|signature| signature.as_ref());
    for value in signatures {
        bytes_encode(1, value, buf)
    }
}

fn signatures_encoded_len(signatures: &[Signature]) -> usize {
    iter_encoded_len(
        1,
        signatures.iter().map(|signature| signature.as_ref().len()),
        signatures.len(),
    )
}

fn encode_sanitazed_message(sanitazed: &SanitizedMessage, buf: &mut impl BufMut) {
    encode_key(2, WireType::LengthDelimited, buf);
    encode_varint(sanitazed_message_encoded_len(sanitazed) as u64, buf);
    match sanitazed {
        SanitizedMessage::Legacy(LegacyMessage { message, .. }) => {
            encode_message_header(message.header, buf);
            encode_pubkeys(&message.account_keys, buf);
            encode_recent_blockhash(message.recent_blockhash.as_ref(), buf);
            encode_compiled_instructions(&message.instructions, buf);
            encode_versioned(false, buf);
            encode_address_table_lookups(&[], buf);
        }
        SanitizedMessage::V0(LoadedMessage { message, .. }) => {
            encode_message_header(message.header, buf);
            encode_pubkeys(&message.account_keys, buf);
            encode_recent_blockhash(message.recent_blockhash.as_ref(), buf);
            encode_compiled_instructions(&message.instructions, buf);
            encode_versioned(true, buf);
            encode_address_table_lookups(&message.address_table_lookups, buf)
        }
    }
}

fn sanitazed_message_encoded_len(sanitazed: &SanitizedMessage) -> usize {
    let len = match sanitazed {
        SanitizedMessage::Legacy(LegacyMessage { message, .. }) => {
            let num_required_signatures = message.header.num_required_signatures as u32;
            let num_readonly_signed_accounts = message.header.num_readonly_signed_accounts as u32;
            let num_readonly_unsigned_accounts =
                message.header.num_readonly_unsigned_accounts as u32;
            message_header_encoded_len((
                num_required_signatures,
                num_readonly_signed_accounts,
                num_readonly_unsigned_accounts,
            )) + pubkeys_encoded_len(&message.account_keys)
                + recent_blockhash_encoded_len(message.recent_blockhash.as_ref())
                + compiled_instructions_encoded_len(&message.instructions)
                + versioned_encoded_len(false)
                + address_table_lookups_encoded_len(&[])
        }
        SanitizedMessage::V0(LoadedMessage { message, .. }) => {
            let num_required_signatures = message.header.num_required_signatures as u32;
            let num_readonly_signed_accounts = message.header.num_readonly_signed_accounts as u32;
            let num_readonly_unsigned_accounts =
                message.header.num_readonly_unsigned_accounts as u32;
            message_header_encoded_len((
                num_required_signatures,
                num_readonly_signed_accounts,
                num_readonly_unsigned_accounts,
            )) + pubkeys_encoded_len(&message.account_keys)
                + recent_blockhash_encoded_len(message.recent_blockhash.as_ref())
                + compiled_instructions_encoded_len(&message.instructions)
                + versioned_encoded_len(true)
                + address_table_lookups_encoded_len(&message.address_table_lookups)
        }
    };
    field_encoded_len(2, len)
}

fn encode_message_header(header: solana_sdk::message::MessageHeader, buf: &mut impl BufMut) {
    let num_required_signatures = header.num_required_signatures as u32;
    let num_readonly_signed_accounts = header.num_readonly_signed_accounts as u32;
    let num_readonly_unsigned_accounts = header.num_readonly_unsigned_accounts as u32;
    encode_key(1, WireType::LengthDelimited, buf);
    encode_varint(
        message_header_encoded_len((
            num_required_signatures,
            num_readonly_signed_accounts,
            num_readonly_unsigned_accounts,
        )) as u64,
        buf,
    );
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

fn message_header_encoded_len(header: (u32, u32, u32)) -> usize {
    let len = if header.0 != 0 {
        encoding::uint32::encoded_len(1, &header.0)
    } else {
        0
    } + if header.1 != 0 {
        encoding::uint32::encoded_len(2, &header.1)
    } else {
        0
    } + if header.2 != 0 {
        encoding::uint32::encoded_len(3, &header.2)
    } else {
        0
    };
    field_encoded_len(1, len)
}

fn encode_pubkeys(pubkeys: &[Pubkey], buf: &mut impl BufMut) {
    let iter = pubkeys.iter().map(|key| key.as_ref());
    for value in iter {
        bytes_encode(2, value, buf);
    }
}

fn pubkeys_encoded_len(pubkeys: &[Pubkey]) -> usize {
    iter_encoded_len(
        2,
        pubkeys.iter().map(|pubkey| pubkey.as_ref().len()),
        pubkeys.len(),
    )
}

fn encode_recent_blockhash(pubkey: &[u8], buf: &mut impl BufMut) {
    bytes_encode(3, pubkey, buf)
}

fn recent_blockhash_encoded_len(pubkey: &[u8]) -> usize {
    bytes_encoded_len(3, pubkey)
}

fn encode_compiled_instructions(
    compiled_instructions: &[CompiledInstruction],
    buf: &mut impl BufMut,
) {
    encode_key(4, WireType::LengthDelimited, buf);
    encode_varint(
        compiled_instructions_encoded_len(compiled_instructions) as u64,
        buf,
    );
    for compiled_instruction in compiled_instructions {
        encode_compiled_instruction(compiled_instruction, buf)
    }
}

fn compiled_instructions_encoded_len(compiled_instructions: &[CompiledInstruction]) -> usize {
    iter_encoded_len(
        4,
        compiled_instructions
            .iter()
            .map(compiled_instruction_encoded_len),
        compiled_instructions.len(),
    )
}

fn encode_compiled_instruction(compiled_instruction: &CompiledInstruction, buf: &mut impl BufMut) {
    let program_id_index = compiled_instruction.program_id_index as u32;
    if program_id_index != 0 {
        encoding::uint32::encode(1, &program_id_index, buf)
    }
    bytes_encode(2, &compiled_instruction.accounts, buf);
    bytes_encode(3, &compiled_instruction.data, buf)
}

fn compiled_instruction_encoded_len(compiled_instruction: &CompiledInstruction) -> usize {
    let program_id_index = compiled_instruction.program_id_index as u32;
    (if program_id_index != 0 {
        encoding::uint32::encoded_len(1, &program_id_index)
    } else {
        0
    }) + bytes_encoded_len(2, &compiled_instruction.accounts)
        + bytes_encoded_len(3, &compiled_instruction.data)
}

fn encode_versioned(versioned: bool, buf: &mut impl BufMut) {
    if versioned {
        encoding::bool::encode(5, &versioned, buf)
    }
}

fn versioned_encoded_len(versioned: bool) -> usize {
    if versioned {
        encoding::bool::encoded_len(5, &versioned)
    } else {
        0
    }
}

fn encode_address_table_lookups(
    address_table_lookups: &[MessageAddressTableLookup],
    buf: &mut impl BufMut,
) {
    encode_key(6, WireType::LengthDelimited, buf);
    encode_varint(
        address_table_lookups_encoded_len(address_table_lookups) as u64,
        buf,
    );
    for address_table_lookup in address_table_lookups {
        encode_address_table_lookup(address_table_lookup, buf)
    }
}

fn address_table_lookups_encoded_len(address_table_lookups: &[MessageAddressTableLookup]) -> usize {
    iter_encoded_len(
        6,
        address_table_lookups
            .iter()
            .map(address_table_lookup_encoded_len),
        address_table_lookups.len(),
    )
}

fn encode_address_table_lookup(
    address_table_lookup: &MessageAddressTableLookup,
    buf: &mut impl BufMut,
) {
    bytes_encode(1, address_table_lookup.account_key.as_ref(), buf);
    bytes_encode(2, &address_table_lookup.writable_indexes, buf);
    bytes_encode(3, &address_table_lookup.readonly_indexes, buf)
}

fn address_table_lookup_encoded_len(address_table_lookup: &MessageAddressTableLookup) -> usize {
    bytes_encoded_len(1, address_table_lookup.account_key.as_ref())
        + bytes_encoded_len(2, &address_table_lookup.writable_indexes)
        + bytes_encoded_len(3, &address_table_lookup.readonly_indexes)
}

fn encode_transaction_status_meta(
    transaction_status_meta: &TransactionStatusMeta,
    buf: &mut impl BufMut,
) {
    encode_key(4, WireType::LengthDelimited, buf);
    encode_varint(
        transaction_status_meta_encoded_len(transaction_status_meta) as u64,
        buf,
    );
    if let Err(ref err) = transaction_status_meta.status {
        encode_transaction_error(err, buf)
    }
    if transaction_status_meta.fee != 0 {
        encoding::uint64::encode(2, &transaction_status_meta.fee, buf)
    }
    encoding::uint64::encode_repeated(3, &transaction_status_meta.pre_balances, buf);
    encoding::uint64::encode_repeated(4, &transaction_status_meta.post_balances, buf);
    if let Some(ref inner_instructions) = transaction_status_meta.inner_instructions {
        encode_inner_instructions_vec(inner_instructions, buf)
    }
    if let Some(ref log_messages) = transaction_status_meta.log_messages {
        encoding::string::encode_repeated(6, log_messages, buf)
    }
    if let Some(ref pre_token_balances) = transaction_status_meta.pre_token_balances {
        encode_transaction_token_balances(7, pre_token_balances, buf)
    }
    if let Some(ref post_token_balances) = transaction_status_meta.post_token_balances {
        encode_transaction_token_balances(8, post_token_balances, buf)
    }
    if let Some(ref rewards) = transaction_status_meta.rewards {
        encode_rewards(9, rewards, buf)
    }
    if transaction_status_meta.inner_instructions.is_none() {
        encoding::bool::encode(
            10,
            &transaction_status_meta.inner_instructions.is_none(),
            buf,
        )
    }
    if transaction_status_meta.log_messages.is_none() {
        encoding::bool::encode(11, &transaction_status_meta.log_messages.is_none(), buf)
    }
    encode_loaded_writable_addresses(&transaction_status_meta.loaded_addresses, buf);
    encode_loaded_readonly_addresses(&transaction_status_meta.loaded_addresses, buf);
    if let Some(ref return_data) = transaction_status_meta.return_data {
        encode_transaction_return_data(return_data, buf)
    }
    if transaction_status_meta.return_data.is_none() {
        encoding::bool::encode(15, &transaction_status_meta.return_data.is_none(), buf)
    }
    if let Some(ref compute_units_consumed) = transaction_status_meta.compute_units_consumed {
        if *compute_units_consumed != 0 {
            encoding::uint64::encode(16, compute_units_consumed, buf)
        }
    }
}

fn transaction_status_meta_encoded_len(transaction_status_meta: &TransactionStatusMeta) -> usize {
    let len = transaction_status_meta
        .status
        .as_ref()
        .err()
        .map_or(0, transaction_error_encoded_len)
        + if transaction_status_meta.fee != 0 {
            encoding::uint64::encoded_len(2, &transaction_status_meta.fee)
        } else {
            0
        }
        + encoding::uint64::encoded_len_repeated(3, &transaction_status_meta.pre_balances)
        + encoding::uint64::encoded_len_repeated(4, &transaction_status_meta.post_balances)
        + transaction_status_meta
            .inner_instructions
            .as_ref()
            .map_or(0, |inner_instructions| {
                inner_instructions_vec_encoded_len(inner_instructions)
            })
        + transaction_status_meta
            .log_messages
            .as_ref()
            .map_or(0, |log_messages| {
                encoding::string::encoded_len_repeated(6, log_messages)
            })
        + transaction_status_meta
            .pre_token_balances
            .as_ref()
            .map_or(0, |pre_token_balances| {
                transaction_token_balances_encoded_len(7, pre_token_balances)
            })
        + transaction_status_meta
            .post_token_balances
            .as_ref()
            .map_or(0, |post_token_balances| {
                transaction_token_balances_encoded_len(8, post_token_balances)
            })
        + transaction_status_meta
            .rewards
            .as_ref()
            .map_or(0, |rewards| rewards_encoded_len(9, rewards))
        + if transaction_status_meta.inner_instructions.is_none() {
            encoding::bool::encoded_len(10, &transaction_status_meta.inner_instructions.is_none())
        } else {
            0
        }
        + if transaction_status_meta.log_messages.is_none() {
            encoding::bool::encoded_len(11, &transaction_status_meta.log_messages.is_none())
        } else {
            0
        }
        + loaded_writable_addresses_encoded_len(&transaction_status_meta.loaded_addresses)
        + loaded_readonly_addresses_encoded_len(&transaction_status_meta.loaded_addresses)
        + transaction_status_meta
            .return_data
            .as_ref()
            .map_or(0, |return_data| {
                transaction_return_data_encoded_len(return_data)
            })
        + if transaction_status_meta.return_data.is_none() {
            encoding::bool::encoded_len(15, &transaction_status_meta.return_data.is_none())
        } else {
            0
        }
        + transaction_status_meta
            .compute_units_consumed
            .as_ref()
            .map_or(0, |compute_units_consumed| {
                if *compute_units_consumed != 0 {
                    encoding::uint64::encoded_len(16, compute_units_consumed)
                } else {
                    0
                }
            });
    field_encoded_len(4, len)
}

const BUFFER_CAPACITY: usize = 1024;

thread_local! {
    static BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(BUFFER_CAPACITY));
}

fn encode_transaction_error(error: &TransactionError, buf: &mut impl BufMut) {
    encode_key(1, WireType::LengthDelimited, buf);
    encode_varint(transaction_error_encoded_len(error) as u64, buf);
    BUFFER.with(|cell| {
        let borrow = cell.borrow();
        encoding::bytes::encode(1, &*borrow, buf)
    })
}

fn transaction_error_encoded_len(error: &TransactionError) -> usize {
    let len = BUFFER.with(|cell| {
        let mut borrow_mut = cell.borrow_mut();
        borrow_mut.clear();
        bincode::serialize_into(&mut *borrow_mut, &error)
            .expect("failed to serialize transaction error into buffer");
        encoding::bytes::encoded_len(1, &*borrow_mut)
    });
    field_encoded_len(1, len)
}

fn encode_inner_instructions_vec(inner_instructions: &[InnerInstructions], buf: &mut impl BufMut) {
    encode_key(5, WireType::LengthDelimited, buf);
    encode_varint(
        inner_instructions_vec_encoded_len(inner_instructions) as u64,
        buf,
    );
    for value in inner_instructions {
        encode_inner_instructions(value, buf)
    }
}

fn inner_instructions_vec_encoded_len(inner_instructions: &[InnerInstructions]) -> usize {
    iter_encoded_len(
        5,
        inner_instructions
            .iter()
            .map(inner_instructions_encoded_len),
        inner_instructions.len(),
    )
}

fn encode_inner_instructions(inner_instructions: &InnerInstructions, buf: &mut impl BufMut) {
    let index = inner_instructions.index as u32;

    if index != 0 {
        encoding::uint32::encode(1, &index, buf)
    }
    encode_inner_instruction_vec(&inner_instructions.instructions, buf)
}

fn inner_instructions_encoded_len(inner_instructions: &InnerInstructions) -> usize {
    let index = inner_instructions.index as u32;

    (if index != 0 {
        encoding::uint32::encoded_len(1, &index)
    } else {
        0
    }) + inner_instruction_vec_encoded_len(&inner_instructions.instructions)
}

fn encode_inner_instruction_vec(inner_instructions: &[InnerInstruction], buf: &mut impl BufMut) {
    encode_key(2, WireType::LengthDelimited, buf);
    encode_varint(
        inner_instruction_vec_encoded_len(inner_instructions) as u64,
        buf,
    );
    for inner_instruction in inner_instructions {
        encode_inner_instruction(inner_instruction, buf)
    }
}

fn inner_instruction_vec_encoded_len(inner_instructions: &[InnerInstruction]) -> usize {
    iter_encoded_len(
        2,
        inner_instructions.iter().map(inner_instruction_encoded_len),
        inner_instructions.len(),
    )
}

fn encode_inner_instruction(inner_instruction: &InnerInstruction, buf: &mut impl BufMut) {
    encode_key(1, WireType::LengthDelimited, buf);
    encode_varint(
        compiled_instruction_encoded_len(&inner_instruction.instruction) as u64,
        buf,
    );
    encode_compiled_instruction(&inner_instruction.instruction, buf);

    if let Some(ref stack_height) = inner_instruction.stack_height {
        if *stack_height != 0 {
            encoding::uint32::encode(2, stack_height, buf)
        }
    }
}

fn inner_instruction_encoded_len(inner_instruction: &InnerInstruction) -> usize {
    compiled_instruction_encoded_len(&inner_instruction.instruction)
        + inner_instruction.stack_height.map_or(0, |stack_height| {
            if stack_height != 0 {
                encoding::uint32::encoded_len(2, &stack_height)
            } else {
                0
            }
        })
}

fn encode_transaction_token_balances(
    tag: u32,
    transaction_token_balances: &[TransactionTokenBalance],
    buf: &mut impl BufMut,
) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(
        transaction_token_balances_encoded_len(tag, transaction_token_balances) as u64,
        buf,
    );
    for transaction_token_balance in transaction_token_balances {
        encode_transaction_token_balance(transaction_token_balance, buf)
    }
}

fn transaction_token_balances_encoded_len(
    tag: u32,
    transaction_token_balances: &[TransactionTokenBalance],
) -> usize {
    iter_encoded_len(
        tag,
        transaction_token_balances
            .iter()
            .map(transaction_token_balance_encoded_len),
        transaction_token_balances.len(),
    )
}

fn encode_transaction_token_balance(
    transaction_token_balance: &TransactionTokenBalance,
    buf: &mut impl BufMut,
) {
    let account_index = transaction_token_balance.account_index as u32;

    if account_index != 0 {
        encoding::uint32::encode(1, &account_index, buf)
    }
    if !transaction_token_balance.mint.is_empty() {
        encoding::string::encode(2, &transaction_token_balance.mint, buf);
    }
    encode_ui_token_amount(&transaction_token_balance.ui_token_amount, buf);
    if !transaction_token_balance.owner.is_empty() {
        encoding::string::encode(4, &transaction_token_balance.owner, buf);
    }
    if !transaction_token_balance.program_id.is_empty() {
        encoding::string::encode(5, &transaction_token_balance.program_id, buf)
    }
}

fn transaction_token_balance_encoded_len(
    transaction_token_balance: &TransactionTokenBalance,
) -> usize {
    let account_index = transaction_token_balance.account_index as u32;

    (if account_index != 0 {
        encoding::uint32::encoded_len(1, &account_index)
    } else {
        0
    }) + !if transaction_token_balance.mint.is_empty() {
        encoding::string::encoded_len(2, &transaction_token_balance.mint)
    } else {
        0
    } + ui_token_amount_encoded_len(&transaction_token_balance.ui_token_amount)
        + if !transaction_token_balance.owner.is_empty() {
            encoding::string::encoded_len(4, &transaction_token_balance.owner)
        } else {
            0
        }
        + if !transaction_token_balance.program_id.is_empty() {
            encoding::string::encoded_len(5, &transaction_token_balance.program_id)
        } else {
            0
        }
}

fn encode_ui_token_amount(ui_token_amount: &UiTokenAmount, buf: &mut impl BufMut) {
    let decimals = ui_token_amount.decimals as u32;

    encode_key(3, WireType::LengthDelimited, buf);
    encode_varint(ui_token_amount_encoded_len(ui_token_amount) as u64, buf);
    if let Some(ref ui_amount) = ui_token_amount.ui_amount {
        if *ui_amount != 0. {
            encoding::double::encode(1, ui_amount, buf)
        }
    }
    if decimals != 0 {
        encoding::uint32::encode(2, &decimals, buf)
    }
    if !ui_token_amount.amount.is_empty() {
        encoding::string::encode(3, &ui_token_amount.amount, buf)
    }
    if !ui_token_amount.ui_amount_string.is_empty() {
        encoding::string::encode(4, &ui_token_amount.ui_amount_string, buf)
    }
}

fn ui_token_amount_encoded_len(ui_token_amount: &UiTokenAmount) -> usize {
    let decimals = ui_token_amount.decimals as u32;
    ui_token_amount.ui_amount.map_or(0, |ui_amount| {
        if ui_amount != 0. {
            encoding::double::encoded_len(1, &ui_amount)
        } else {
            0
        }
    }) + if decimals != 0 {
        encoding::uint32::encoded_len(2, &decimals)
    } else {
        0
    } + if !ui_token_amount.amount.is_empty() {
        encoding::string::encoded_len(3, &ui_token_amount.amount)
    } else {
        0
    } + if !ui_token_amount.ui_amount_string.is_empty() {
        encoding::string::encoded_len(4, &ui_token_amount.ui_amount_string)
    } else {
        0
    }
}

fn encode_loaded_writable_addresses(loaded_addresses: &LoadedAddresses, buf: &mut impl BufMut) {
    let writable_addresses = loaded_addresses.writable.iter().map(|key| key.as_ref());
    for value in writable_addresses {
        bytes_encode(12, value, buf)
    }
}

fn loaded_writable_addresses_encoded_len(loaded_addresses: &LoadedAddresses) -> usize {
    iter_encoded_len(
        12,
        loaded_addresses
            .writable
            .iter()
            .map(|pubkey| pubkey.as_ref().len()),
        loaded_addresses.len(),
    )
}

fn encode_loaded_readonly_addresses(loaded_addresses: &LoadedAddresses, buf: &mut impl BufMut) {
    let readonly_addresses = loaded_addresses.readonly.iter().map(|key| key.as_ref());
    for value in readonly_addresses {
        bytes_encode(13, value, buf)
    }
}

fn loaded_readonly_addresses_encoded_len(loaded_addresses: &LoadedAddresses) -> usize {
    iter_encoded_len(
        12,
        loaded_addresses
            .readonly
            .iter()
            .map(|pubkey| pubkey.as_ref().len()),
        loaded_addresses.len(),
    )
}

fn encode_transaction_return_data(return_data: &TransactionReturnData, buf: &mut impl BufMut) {
    encode_key(14, WireType::LengthDelimited, buf);
    encode_varint(transaction_return_data_encoded_len(return_data) as u64, buf);
    bytes_encode(1, return_data.program_id.as_ref(), buf);
    bytes_encode(2, &return_data.data, buf)
}

fn transaction_return_data_encoded_len(return_data: &TransactionReturnData) -> usize {
    let len = bytes_encoded_len(1, return_data.program_id.as_ref())
        + bytes_encoded_len(2, &return_data.data);
    field_encoded_len(14, len)
}

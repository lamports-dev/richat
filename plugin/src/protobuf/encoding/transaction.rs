use {
    super::{bytes_encode, bytes_encoded_len, field_encoded_len},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
    prost::{
        bytes::BufMut,
        encoding::{self, encode_key, encode_varint, message, WireType},
    },
    solana_sdk::{
        clock::Slot,
        message::{v0::LoadedMessage, LegacyMessage, SanitizedMessage},
        pubkey::Pubkey,
        signature::Signature,
        transaction::SanitizedTransaction,
    },
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
    let index = transaction.index as u64;

    bytes_encoded_len(1, transaction.signature.as_ref())
            + encoding::bool::encoded_len(2, &transaction.is_vote)
            + sanitazed_transaction_encoded_len(transaction.transaction)
            + 0 // TransactionStatusMeta
            + encoding::uint64::encoded_len(5, &index)
}

pub fn encode_sanitazed_transaction(sanitazed: &SanitizedTransaction, buf: &mut impl BufMut) {
    encoding::encode_key(3, WireType::LengthDelimited, buf);
    encoding::encode_varint(sanitazed_transaction_encoded_len(sanitazed) as u64, buf);
    let signatures = sanitazed
        .signatures()
        .iter()
        .map(|signature| signature.as_ref());
    for value in signatures {
        bytes_encode(1, value, buf)
    }
    encode_sanitazed_message(sanitazed.message(), buf)
}

pub fn sanitazed_transaction_encoded_len(sanitazed: &SanitizedTransaction) -> usize {
    let signatures = sanitazed.signatures();
    encoding::key_len(3) * signatures.len()
        + signatures
            .iter()
            .map(|signature| signature.as_ref().len())
            .sum::<usize>()
        + sanitazed_message_encoded_len(sanitazed.message())
}

pub fn encode_sanitazed_message(sanitazed: &SanitizedMessage, buf: &mut impl BufMut) {
    encoding::encode_key(2, WireType::LengthDelimited, buf);
    encoding::encode_varint(sanitazed_message_encoded_len(sanitazed) as u64, buf);
    match sanitazed {
        SanitizedMessage::Legacy(LegacyMessage { message, .. }) => {
            encode_message_header(&message.header, buf);
            encode_pubkeys(&message.account_keys, buf);
            encode_recent_blockhash(&message.recent_blockhash.to_bytes(), buf);
            encode_compiled_instructions(&message.instructions, buf);
            encode_versioned(false, buf);
            encode_address_table_lookups(&[], buf);
        }
        SanitizedMessage::V0(LoadedMessage { message, .. }) => {
            encode_message_header(&message.header, buf);
            encode_pubkeys(&message.account_keys, buf);
            encode_recent_blockhash(&message.recent_blockhash.to_bytes(), buf);
            encode_compiled_instructions(&message.instructions, buf);
            encode_versioned(true, buf);
            encode_address_table_lookups(&message.address_table_lookups, buf)
        }
    }
}

pub fn sanitazed_message_encoded_len(sanitazed: &SanitizedMessage) -> usize {
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
                + recent_blockhash_encoded_len(&message.recent_blockhash.to_bytes())
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
                + recent_blockhash_encoded_len(&message.recent_blockhash.to_bytes())
                + compiled_instructions_encoded_len(&message.instructions)
                + versioned_encoded_len(true)
                + address_table_lookups_encoded_len(&message.address_table_lookups)
        }
    };
    encoding::key_len(2) + encoding::encoded_len_varint(len as u64) + len
}

pub fn encode_message_header(header: &solana_sdk::message::MessageHeader, buf: &mut impl BufMut) {
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
    let len = encoding::uint32::encoded_len(1, &header.0)
        + encoding::uint32::encoded_len(2, &header.1)
        + encoding::uint32::encoded_len(3, &header.2);
    encoding::key_len(1) + encoding::encoded_len_varint(len as u64) + len
}

pub fn encode_pubkeys(pubkeys: &[Pubkey], buf: &mut impl BufMut) {
    let iter = pubkeys.iter().map(|key| key.as_ref());
    for value in iter {
        bytes_encode(2, value, buf);
    }
}

pub fn pubkeys_encoded_len(pubkeys: &[Pubkey]) -> usize {
    encoding::key_len(2) * pubkeys.len()
        + pubkeys
            .iter()
            .map(|pubkey| {
                let pubkey = pubkey.to_bytes();
                pubkey.len() + encoding::encoded_len_varint(pubkey.len() as u64)
            })
            .sum::<usize>()
}

pub fn encode_recent_blockhash(pubkey: &[u8], buf: &mut impl BufMut) {
    bytes_encode(3, pubkey, buf)
}

pub fn recent_blockhash_encoded_len(pubkey: &[u8]) -> usize {
    bytes_encoded_len(3, pubkey)
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
    let len = encoding::uint32::encoded_len(1, &program_id_index)
        + bytes_encoded_len(2, &compiled_instruction.accounts)
        + bytes_encoded_len(3, &compiled_instruction.data);
    encoding::key_len(4) + encoding::encoded_len_varint(len as u64) + len
}

pub fn encode_versioned(versioned: bool, buf: &mut impl BufMut) {
    encoding::bool::encode(5, &versioned, buf)
}

pub fn versioned_encoded_len(versioned: bool) -> usize {
    encoding::bool::encoded_len(5, &versioned)
}

pub fn encode_address_table_lookups(
    address_table_lookups: &[solana_sdk::message::v0::MessageAddressTableLookup],
    buf: &mut impl BufMut,
) {
    encoding::encode_key(6, WireType::LengthDelimited, buf);
    encoding::encode_varint(
        address_table_lookups_encoded_len(address_table_lookups) as u64,
        buf,
    );
    for address_table_lookup in address_table_lookups {
        encode_address_table_lookup(address_table_lookup, buf)
    }
}

pub fn address_table_lookups_encoded_len(
    address_table_lookup: &[solana_sdk::message::v0::MessageAddressTableLookup],
) -> usize {
    encoding::key_len(6) * address_table_lookup.len()
        + address_table_lookup
            .iter()
            .map(address_table_lookup_encoded_len)
            .map(|len| len + encoding::encoded_len_varint(len as u64))
            .sum::<usize>()
}

pub fn encode_address_table_lookup(
    address_table_lookup: &solana_sdk::message::v0::MessageAddressTableLookup,
    buf: &mut impl BufMut,
) {
    bytes_encode(1, &address_table_lookup.account_key.to_bytes(), buf);
    bytes_encode(2, &address_table_lookup.writable_indexes, buf);
    bytes_encode(3, &address_table_lookup.readonly_indexes, buf)
}

pub fn address_table_lookup_encoded_len(
    address_table_lookup: &solana_sdk::message::v0::MessageAddressTableLookup,
) -> usize {
    bytes_encoded_len(1, &address_table_lookup.account_key.to_bytes())
        + bytes_encoded_len(2, &address_table_lookup.writable_indexes)
        + bytes_encoded_len(3, &address_table_lookup.readonly_indexes)
}

pub fn encode_transaction_status_meta(
    transaction_status_meta: &solana_transaction_status::TransactionStatusMeta,
    buf: &mut impl BufMut,
) {
    let loaded_writable_addresses = transaction_status_meta
        .loaded_addresses
        .writable
        .iter()
        .map(|key| key.as_ref());
    let loaded_readonly_addresses = transaction_status_meta
        .loaded_addresses
        .readonly
        .iter()
        .map(|key| key.as_ref());

    encoding::encode_key(4, WireType::LengthDelimited, buf);
    encoding::encode_varint(
        transaction_status_meta_encoded_len(transaction_status_meta) as u64,
        buf,
    );
    if let Err(ref err) = transaction_status_meta.status {
        encode_transaction_error(err, buf)
    }
    encoding::uint64::encode(2, &transaction_status_meta.fee, buf);
    encoding::uint64::encode_repeated(3, &transaction_status_meta.pre_balances, buf);
    encoding::uint64::encode_repeated(4, &transaction_status_meta.post_balances, buf);
    if let Some(ref inner_instructions) = transaction_status_meta.inner_instructions {
        encode_inner_instructions_vec(&inner_instructions, buf)
    }
    if let Some(ref log_messages) = transaction_status_meta.log_messages {
        encoding::string::encode_repeated(6, log_messages, buf);
    }
    if let Some(ref pre_token_balances) = transaction_status_meta.pre_token_balances {
        encode_transaction_token_balances(7, pre_token_balances, buf)
    }
    if let Some(ref post_token_balances) = transaction_status_meta.post_token_balances {
        encode_transaction_token_balances(8, post_token_balances, buf)
    }
    if let Some(ref rewards) = transaction_status_meta.rewards {
        encode_rewards(rewards, buf)
    }
    encoding::bool::encode(
        10,
        &transaction_status_meta.inner_instructions.is_none(),
        buf,
    );
    encoding::bool::encode(11, &transaction_status_meta.log_messages.is_none(), buf);
    for value in loaded_writable_addresses {
        bytes_encode(12, value, buf)
    }
    for value in loaded_readonly_addresses {
        bytes_encode(13, value, buf)
    }
    if let Some(ref return_data) = transaction_status_meta.return_data {
        encode_transaction_return_data(return_data, buf)
    }
    encoding::bool::encode(15, &transaction_status_meta.return_data.is_none(), buf);
    if let Some(ref compute_units_consumed) = transaction_status_meta.compute_units_consumed {
        encoding::uint64::encode(16, compute_units_consumed, buf)
    }
}

pub fn transaction_status_meta_encoded_len(
    transaction_status_meta: &solana_transaction_status::TransactionStatusMeta,
) -> usize {
    let len = 0;
    encoding::key_len(4) + encoding::encoded_len_varint(len as u64) + len
}

pub fn encode_transaction_error(
    error: &solana_sdk::transaction::TransactionError,
    buf: &mut impl BufMut,
) {
    encoding::encode_key(1, WireType::LengthDelimited, buf);
    encoding::encode_varint(transaction_error_encoded_len(error) as u64, buf);
}

pub fn transaction_error_encoded_len(error: &solana_sdk::transaction::TransactionError) -> usize {
    0
}

pub fn encode_inner_instructions_vec(
    inner_instructions: &[solana_transaction_status::InnerInstructions],
    buf: &mut impl BufMut,
) {
    encoding::encode_key(5, WireType::LengthDelimited, buf);
    encoding::encode_varint(
        inner_instructions_vec_encoded_len(inner_instructions) as u64,
        buf,
    );
}

pub fn inner_instructions_vec_encoded_len(
    inner_instructions: &[solana_transaction_status::InnerInstructions],
) -> usize {
    0
}

pub fn encode_inner_instructions(
    inner_instructions: &solana_transaction_status::InnerInstructions,
    buf: &mut impl BufMut,
) {
}

pub fn inner_instructions_encoded_len(
    inner_instructions: &solana_transaction_status::InnerInstructions,
) -> usize {
    0
}

pub fn encode_transaction_token_balances(
    tag: u32,
    transaction_token_balances: &[solana_transaction_status::TransactionTokenBalance],
    buf: &mut impl BufMut,
) {
    encoding::encode_key(tag, WireType::LengthDelimited, buf);
    encoding::encode_varint(
        transaction_token_balances_encoded_len(tag, transaction_token_balances) as u64,
        buf,
    );
    for transaction_token_balance in transaction_token_balances {
        encode_transaction_token_balance(transaction_token_balance, buf)
    }
}

pub fn transaction_token_balances_encoded_len(
    tag: u32,
    transaction_token_balances: &[solana_transaction_status::TransactionTokenBalance],
) -> usize {
    encoding::key_len(tag) * transaction_token_balances.len()
        + transaction_token_balances
            .iter()
            .map(transaction_token_balance_encoded_len)
            .map(|len| len + encoding::encoded_len_varint(len as u64))
            .sum::<usize>()
}

pub fn encode_transaction_token_balance(
    transaction_token_balance: &solana_transaction_status::TransactionTokenBalance,
    buf: &mut impl BufMut,
) {
    let account_index = transaction_token_balance.account_index as u32;

    encoding::uint32::encode(1, &account_index, buf);
    encoding::string::encode(2, &transaction_token_balance.mint, buf);
    encode_ui_token_amount(&transaction_token_balance.ui_token_amount, buf);
    encoding::string::encode(4, &transaction_token_balance.owner, buf);
    encoding::string::encode(5, &transaction_token_balance.program_id, buf)
}

pub fn transaction_token_balance_encoded_len(
    transaction_token_balance: &solana_transaction_status::TransactionTokenBalance,
) -> usize {
    let account_index = transaction_token_balance.account_index as u32;

    encoding::uint32::encoded_len(1, &account_index)
        + encoding::string::encoded_len(2, &transaction_token_balance.mint)
        + ui_token_amount_encoded_len(&transaction_token_balance.ui_token_amount)
        + encoding::string::encoded_len(4, &transaction_token_balance.owner)
        + encoding::string::encoded_len(5, &transaction_token_balance.program_id)
}

pub fn encode_ui_token_amount(
    ui_token_amount: &solana_account_decoder_client_types::token::UiTokenAmount,
    buf: &mut impl BufMut,
) {
    let decimals = ui_token_amount.decimals as u32;

    encoding::encode_key(3, WireType::LengthDelimited, buf);
    encoding::encode_varint(ui_token_amount_encoded_len(ui_token_amount) as u64, buf);
    if let Some(ref ui_amount) = ui_token_amount.ui_amount {
        encoding::double::encode(1, ui_amount, buf)
    }
    encoding::uint32::encode(2, &decimals, buf);
    encoding::string::encode(3, &ui_token_amount.amount, buf);
    encoding::string::encode(4, &ui_token_amount.ui_amount_string, buf)
}

pub fn ui_token_amount_encoded_len(
    ui_token_amount: &solana_account_decoder_client_types::token::UiTokenAmount,
) -> usize {
    let decimals = ui_token_amount.decimals as u32;
    ui_token_amount
        .ui_amount
        .map_or(0, |ui_amount| encoding::double::encoded_len(1, &ui_amount))
        + encoding::uint32::encoded_len(2, &decimals)
        + encoding::string::encoded_len(3, &ui_token_amount.amount)
        + encoding::string::encoded_len(4, &ui_token_amount.ui_amount_string)
}

pub fn encode_rewards(rewards: &[solana_transaction_status::Reward], buf: &mut impl BufMut) {
    encoding::encode_key(9, WireType::LengthDelimited, buf);
    encoding::encode_varint(rewards_encoded_len(rewards) as u64, buf);
    for reward in rewards {
        encode_reward(reward, buf)
    }
}

pub fn rewards_encoded_len(rewards: &[solana_transaction_status::Reward]) -> usize {
    encoding::key_len(9) * rewards.len()
        + rewards
            .iter()
            .map(reward_encoded_len)
            .map(|len| len + encoding::encoded_len_varint(len as u64))
            .sum::<usize>()
}

pub const fn reward_type_as_i32(reward_type: &solana_transaction_status::RewardType) -> i32 {
    use solana_transaction_status::RewardType::*;
    match reward_type {
        Fee => 0,
        Rent => 1,
        Staking => 2,
        Voting => 3,
    }
}

pub fn encode_reward(reward: &solana_transaction_status::Reward, buf: &mut impl BufMut) {
    const NUM_STRINGS: [&str; 256] = [
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16",
        "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31",
        "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46",
        "47", "48", "49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61",
        "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72", "73", "74", "75", "76",
        "77", "78", "79", "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91",
        "92", "93", "94", "95", "96", "97", "98", "99", "100", "101", "102", "103", "104", "105",
        "106", "107", "108", "109", "110", "111", "112", "113", "114", "115", "116", "117", "118",
        "119", "120", "121", "122", "123", "124", "125", "126", "127", "128", "129", "130", "131",
        "132", "133", "134", "135", "136", "137", "138", "139", "140", "141", "142", "143", "144",
        "145", "146", "147", "148", "149", "150", "151", "152", "153", "154", "155", "156", "157",
        "158", "159", "160", "161", "162", "163", "164", "165", "166", "167", "168", "169", "170",
        "171", "172", "173", "174", "175", "176", "177", "178", "179", "180", "181", "182", "183",
        "184", "185", "186", "187", "188", "189", "190", "191", "192", "193", "194", "195", "196",
        "197", "198", "199", "200", "201", "202", "203", "204", "205", "206", "207", "208", "209",
        "210", "211", "212", "213", "214", "215", "216", "217", "218", "219", "220", "221", "222",
        "223", "224", "225", "226", "227", "228", "229", "230", "231", "232", "233", "234", "235",
        "236", "237", "238", "239", "240", "241", "242", "243", "244", "245", "246", "247", "248",
        "249", "250", "251", "252", "253", "254", "255",
    ];

    const fn u8_to_static_str(num: u8) -> &'static str {
        NUM_STRINGS[num as usize]
    }

    encoding::string::encode(1, &reward.pubkey, buf);
    encoding::int64::encode(2, &reward.lamports, buf);
    encoding::uint64::encode(3, &reward.post_balance, buf);
    if let Some(ref reward_type) = reward.reward_type {
        let reward_type = reward_type_as_i32(reward_type);
        encoding::int32::encode(4, &reward_type, buf)
    }
    if let Some(commission) = reward.commission {
        bytes_encode(5, u8_to_static_str(commission).as_ref(), buf);
    }
}

pub fn reward_encoded_len(reward: &solana_transaction_status::Reward) -> usize {
    encoding::string::encoded_len(1, &reward.pubkey)
        + encoding::int64::encoded_len(2, &reward.lamports)
        + encoding::uint64::encoded_len(3, &reward.post_balance)
        + reward.reward_type.map_or(0, |reward_type| {
            let reward_type = reward_type_as_i32(&reward_type);
            encoding::int32::encoded_len(4, &reward_type)
        })
        + reward
            .commission
            .map_or(0, |commission| bytes_encoded_len(5, &[commission]))
}

pub fn encode_transaction_return_data(
    return_data: &solana_sdk::transaction_context::TransactionReturnData,
    buf: &mut impl BufMut,
) {
    encoding::encode_key(14, WireType::LengthDelimited, buf);
    encoding::encode_varint(transaction_return_data_encoded_len(return_data) as u64, buf);
    bytes_encode(1, &return_data.program_id.to_bytes(), buf);
    bytes_encode(2, &return_data.data, buf)
}

pub fn transaction_return_data_encoded_len(
    return_data: &solana_sdk::transaction_context::TransactionReturnData,
) -> usize {
    let len = bytes_encoded_len(1, &return_data.program_id.to_bytes())
        + bytes_encoded_len(2, &return_data.data);
    encoding::key_len(14) + encoding::encoded_len_varint(len as u64) + len
}

#![allow(dead_code)] // FIXME: remove it!!
#![no_main]

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;

#[derive(Arbitrary, Debug, Clone)]
pub struct FuzzCompiledInstruction {
    program_id_index: u8,
    accounts: Vec<u8>,
    data: Vec<u8>,
}

#[derive(Arbitrary, Debug, Clone)]
pub struct FuzzLoadedAddresses {
    writable: Vec<[u8; 32]>,
    readonly: Vec<[u8; 32]>,
}

pub mod sanitized {
    use arbitrary::Arbitrary;
    use std::borrow::Cow;

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzLegacyMessageInner {
        header: FuzzMessageHeader,
        account_keys: Vec<[u8; 32]>,
        recent_blockhash: [u8; 32],
        instructions: Vec<super::FuzzCompiledInstruction>,
    }

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzMessageAddressTableLookup {
        account_key: [u8; 32],
        writable_indexes: Vec<u8>,
        readonly_indexes: Vec<u8>,
    }

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzMessageHeader {
        num_required_signatures: u8,
        num_readonly_signed_accounts: u8,
        num_readonly_unsigned_accounts: u8,
    }

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzLoadedMessageInner {
        header: FuzzMessageHeader,
        account_keys: Vec<[u8; 32]>,
        recent_blockhash: [u8; 32],
        instructions: Vec<super::FuzzCompiledInstruction>,
        address_table_lookups: Vec<FuzzMessageAddressTableLookup>,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzLegacyMessage<'a> {
        message: Cow<'a, FuzzLegacyMessageInner>,
        is_writable_account_cache: Vec<bool>,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzLoadedMessage<'a> {
        message: Cow<'a, FuzzLoadedMessageInner>,
        loaded_addresses: Cow<'a, super::FuzzLoadedAddresses>,
        is_writable_account_cache: Vec<bool>,
    }

    #[derive(Arbitrary, Debug)]
    pub enum FuzzSanitizedMessage<'a> {
        Legacy(FuzzLegacyMessage<'a>),
        V0(FuzzLoadedMessage<'a>),
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzSanitizedTransaction<'a> {
        message: FuzzSanitizedMessage<'a>,
        message_hash: [u8; 32],
        is_simple_vote_tx: bool,
        signatures: Vec<&'a [u8]>,
    }
}

pub mod status_meta {
    use arbitrary::Arbitrary;

    #[derive(Arbitrary, Debug)]
    pub enum FuzzTransactionError {
        Err,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzInnerInstruction {
        instruction: super::FuzzCompiledInstruction,
        stack_height: Option<u32>,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzInnerInstructions {
        index: u8,
        instructions: Vec<FuzzInnerInstruction>,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzUiTokenAmount {
        ui_amount: Option<f64>,
        decimals: u8,
        amount: String,
        ui_amount_string: String,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzTransactionTokenBalance {
        account_index: u8,
        mint: String,
        ui_token_amount: FuzzUiTokenAmount,
        owner: String,
        program_id: String,
    }

    #[derive(Arbitrary, Debug)]
    pub enum FuzzRewardType {
        Fee,
        Rent,
        Staking,
        Voting,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzReward {
        pubkey: String,
        lamports: i64,
        post_balance: u64,
        reward_type: Option<FuzzRewardType>,
        commission: Option<u8>,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzTransactionReturnData {
        program_id: [u8; 32],
        data: Vec<u8>,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzTransactionStatusMeta {
        status: Result<(), FuzzTransactionError>,
        fee: u64,
        pre_balances: Vec<u64>,
        post_balances: Vec<u64>,
        inner_instructions: Option<Vec<FuzzInnerInstructions>>,
        log_messages: Option<Vec<String>>,
        pre_token_balances: Option<Vec<FuzzTransactionTokenBalance>>,
        post_token_balances: Option<Vec<FuzzTransactionTokenBalance>>,
        rewards: Option<Vec<FuzzReward>>,
        loaded_addresses: super::FuzzLoadedAddresses,
        return_data: Option<FuzzTransactionReturnData>,
        compute_units_consumed: u64,
    }
}

#[derive(Arbitrary, Debug)]
pub struct FuzzTransaction<'a> {
    pub signature: &'a [u8],
    pub is_vote: bool,
    pub transaction: sanitized::FuzzSanitizedTransaction<'a>,
    pub transaction_status_meta: status_meta::FuzzTransactionStatusMeta,
    pub index: usize,
}

#[derive(Arbitrary, Debug)]
pub struct FuzzTransactionMessage<'a> {
    slot: u64,
    transaction: FuzzTransaction<'a>,
}

fuzz_target!(|_fuzz_message: FuzzTransactionMessage| {});

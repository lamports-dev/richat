#![allow(unused)] // FIXME: remove it!!!
#![no_main]

use std::collections::HashSet;

use sanitized::FuzzSanitizedMessage;
use solana_sdk::{
    hash::Hash,
    message::{SimpleAddressLoader, VersionedMessage},
    pubkey::Pubkey,
    signature::Signature,
    signer::Signer,
    signers::Signers,
    transaction::{SanitizedTransaction, SanitizedVersionedTransaction, VersionedTransaction},
};

use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2;
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use solana_transaction_status::TransactionStatusMeta;

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
        pub header: FuzzMessageHeader,
        pub account_keys: Vec<[u8; 32]>,
        pub recent_blockhash: [u8; 32],
        pub instructions: Vec<super::FuzzCompiledInstruction>,
    }

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzMessageAddressTableLookup {
        pub account_key: [u8; 32],
        pub writable_indexes: Vec<u8>,
        pub readonly_indexes: Vec<u8>,
    }

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzMessageHeader {
        pub num_required_signatures: u8,
        pub num_readonly_signed_accounts: u8,
        pub num_readonly_unsigned_accounts: u8,
    }

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzLoadedMessageInner {
        pub header: FuzzMessageHeader,
        pub account_keys: Vec<[u8; 32]>,
        pub recent_blockhash: [u8; 32],
        pub instructions: Vec<super::FuzzCompiledInstruction>,
        pub address_table_lookups: Vec<FuzzMessageAddressTableLookup>,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzLegacyMessage<'a> {
        pub message: Cow<'a, FuzzLegacyMessageInner>,
        pub is_writable_account_cache: Vec<bool>,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzLoadedMessage<'a> {
        pub message: Cow<'a, FuzzLoadedMessageInner>,
        pub loaded_addresses: Cow<'a, super::FuzzLoadedAddresses>,
        pub is_writable_account_cache: Vec<bool>,
    }

    #[derive(Arbitrary, Debug)]
    pub enum FuzzSanitizedMessage<'a> {
        Legacy(FuzzLegacyMessage<'a>),
        V0(FuzzLoadedMessage<'a>),
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzSanitizedTransaction<'a> {
        pub message: FuzzSanitizedMessage<'a>,
        pub message_hash: [u8; 32],
        pub is_simple_vote_tx: bool,
        pub signatures: Vec<&'a [u8]>,
    }
}

pub mod status_meta {
    use arbitrary::Arbitrary;
    use solana_transaction_status::RewardType;

    #[derive(Arbitrary, Debug)]
    pub enum FuzzTransactionError {
        Err,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzInnerInstruction {
        pub instruction: super::FuzzCompiledInstruction,
        pub stack_height: Option<u32>,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzInnerInstructions {
        pub index: u8,
        pub instructions: Vec<FuzzInnerInstruction>,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzUiTokenAmount {
        pub ui_amount: Option<f64>,
        pub decimals: u8,
        pub amount: String,
        pub ui_amount_string: String,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzTransactionTokenBalance {
        pub account_index: u8,
        pub mint: String,
        pub ui_token_amount: FuzzUiTokenAmount,
        pub owner: String,
        pub program_id: String,
    }

    #[derive(Arbitrary, Debug)]
    pub enum FuzzRewardType {
        Fee,
        Rent,
        Staking,
        Voting,
    }

    impl FuzzRewardType {
        pub const fn into_solana(self) -> RewardType {
            match self {
                FuzzRewardType::Fee => RewardType::Fee,
                FuzzRewardType::Rent => RewardType::Rent,
                FuzzRewardType::Staking => RewardType::Staking,
                FuzzRewardType::Voting => RewardType::Voting,
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzReward {
        pub pubkey: String,
        pub lamports: i64,
        pub post_balance: u64,
        pub reward_type: Option<FuzzRewardType>,
        pub commission: Option<u8>,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzTransactionReturnData {
        pub program_id: [u8; 32],
        pub data: Vec<u8>,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzTransactionStatusMeta {
        pub status: Result<(), FuzzTransactionError>,
        pub fee: u64,
        pub pre_balances: Vec<u64>,
        pub post_balances: Vec<u64>,
        pub inner_instructions: Option<Vec<FuzzInnerInstructions>>,
        pub log_messages: Option<Vec<String>>,
        pub pre_token_balances: Option<Vec<FuzzTransactionTokenBalance>>,
        pub post_token_balances: Option<Vec<FuzzTransactionTokenBalance>>,
        pub rewards: Option<Vec<FuzzReward>>,
        pub loaded_addresses: super::FuzzLoadedAddresses,
        pub return_data: Option<FuzzTransactionReturnData>,
        pub compute_units_consumed: Option<u64>,
    }
}

#[derive(Arbitrary, Debug)]
pub struct FuzzTransaction<'a> {
    pub signature: [u8; 64],
    pub is_vote: bool,
    pub transaction: sanitized::FuzzSanitizedTransaction<'a>,
    pub transaction_status_meta: status_meta::FuzzTransactionStatusMeta,
    pub index: usize,
}

#[derive(Arbitrary, Debug)]
pub struct FuzzTransactionMessage<'a> {
    pub slot: u64,
    pub transaction: FuzzTransaction<'a>,
}

pub struct FuzzSigner;

impl Signers for FuzzSigner {
    fn pubkeys(&self) -> Vec<Pubkey> {
        vec![Pubkey::new_unique()]
    }
    fn try_pubkeys(&self) -> Result<Vec<Pubkey>, solana_sdk::signer::SignerError> {
        Ok(vec![Pubkey::new_unique()])
    }
    fn sign_message(&self, message: &[u8]) -> Vec<Signature> {
        vec![Signature::new_unique()]
    }
    fn try_sign_message(
        &self,
        message: &[u8],
    ) -> Result<Vec<Signature>, solana_sdk::signer::SignerError> {
        Ok(vec![Signature::new_unique()])
    }
    fn is_interactive(&self) -> bool {
        false
    }
}

fuzz_target!(|fuzz_message: FuzzTransactionMessage| {
    let versioned_message = match fuzz_message.transaction.transaction.message {
        FuzzSanitizedMessage::Legacy(legacy) => todo!(),
        FuzzSanitizedMessage::V0(v0) => todo!(),
    };
    let versioned_transaction = VersionedTransaction::try_new(versioned_message, &FuzzSigner)
        .expect("failed to define `VersionedTransaction`");
    let sanitized_versioned_transaction =
        SanitizedVersionedTransaction::try_new(versioned_transaction)
            .expect("failed to define `SanitizedVersionedTransaction`");
    let sanitized_transaction = SanitizedTransaction::try_new(
        sanitized_versioned_transaction,
        Hash::new_from_array(fuzz_message.transaction.transaction.message_hash),
        fuzz_message.transaction.transaction.is_simple_vote_tx,
        SimpleAddressLoader::Disabled,
        &HashSet::new(),
    )
    .expect("failed to define `SanitizedTransaction`");
    let transaction_status_meta = TransactionStatusMeta {
        status: Ok(()), // TODO
        fee: fuzz_message.transaction.transaction_status_meta.fee,
        pre_balances: fuzz_message
            .transaction
            .transaction_status_meta
            .pre_balances,
        post_balances: fuzz_message
            .transaction
            .transaction_status_meta
            .post_balances,
        inner_instructions: todo!(), // TODO
        log_messages: fuzz_message
            .transaction
            .transaction_status_meta
            .log_messages,
        pre_token_balances: todo!(),  // TODO
        post_token_balances: todo!(), // TODO
        rewards: todo!(),             // TODO
        loaded_addresses: todo!(),    // TODO
        return_data: todo!(),         // TODO
        compute_units_consumed: fuzz_message
            .transaction
            .transaction_status_meta
            .compute_units_consumed,
    };
    let replica = ReplicaTransactionInfoV2 {
        signature: &Signature::from(fuzz_message.transaction.signature),
        is_vote: fuzz_message.transaction.is_vote,
        transaction: &sanitized_transaction,
        transaction_status_meta: &transaction_status_meta,
        index: fuzz_message.transaction.index,
    };
});

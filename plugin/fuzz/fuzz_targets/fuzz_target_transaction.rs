#![allow(unused)] // FIXME: remove it!!!
#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
    arbitrary::Arbitrary,
    libfuzzer_sys::fuzz_target,
    richat_plugin::protobuf::ProtobufMessage,
    sanitized::FuzzSanitizedMessage,
    solana_account_decoder::parse_token::UiTokenAmount,
    solana_sdk::{
        hash::Hash,
        instruction::CompiledInstruction,
        message::{v0::LoadedAddresses, SimpleAddressLoader, VersionedMessage},
        pubkey::Pubkey,
        signature::Signature,
        signer::Signer,
        signers::Signers,
        transaction::{SanitizedTransaction, SanitizedVersionedTransaction, VersionedTransaction},
        transaction_context::TransactionReturnData,
    },
    solana_transaction_status::{
        InnerInstruction, InnerInstructions, Reward, TransactionStatusMeta, TransactionTokenBalance,
    },
    status_meta::FuzzRewardType,
    std::collections::HashSet,
};

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
    use {arbitrary::Arbitrary, std::borrow::Cow};

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
    use {arbitrary::Arbitrary, solana_transaction_status::RewardType};

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
    let mut buf = Vec::new();
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
    let status = ();
    let inner_instructions = fuzz_message
        .transaction
        .transaction_status_meta
        .inner_instructions
        .map(|inner_instructions| {
            inner_instructions
                .into_iter()
                .map(|inner_instructions| {
                    let instructions = inner_instructions
                        .instructions
                        .into_iter()
                        .map(|inner_instruction| {
                            let instruction = CompiledInstruction {
                                program_id_index: inner_instruction.instruction.program_id_index,
                                accounts: inner_instruction.instruction.accounts,
                                data: inner_instruction.instruction.data,
                            };
                            InnerInstruction {
                                instruction,
                                stack_height: inner_instruction.stack_height,
                            }
                        })
                        .collect();
                    InnerInstructions {
                        index: inner_instructions.index,
                        instructions,
                    }
                })
                .collect::<Vec<_>>()
        });
    let pre_token_balances = fuzz_message
        .transaction
        .transaction_status_meta
        .pre_token_balances
        .map(|pre_token_balances| {
            pre_token_balances
                .into_iter()
                .map(|token_balance| {
                    let ui_token_amount = UiTokenAmount {
                        ui_amount: token_balance.ui_token_amount.ui_amount,
                        amount: token_balance.ui_token_amount.amount,
                        decimals: token_balance.ui_token_amount.decimals,
                        ui_amount_string: token_balance.ui_token_amount.ui_amount_string,
                    };
                    TransactionTokenBalance {
                        account_index: token_balance.account_index,
                        mint: token_balance.mint,
                        ui_token_amount,
                        owner: token_balance.owner,
                        program_id: token_balance.program_id,
                    }
                })
                .collect::<Vec<_>>()
        });
    let post_token_balances = fuzz_message
        .transaction
        .transaction_status_meta
        .post_token_balances
        .map(|post_token_balances| {
            post_token_balances
                .into_iter()
                .map(|token_balance| {
                    let ui_token_amount = UiTokenAmount {
                        ui_amount: token_balance.ui_token_amount.ui_amount,
                        amount: token_balance.ui_token_amount.amount,
                        decimals: token_balance.ui_token_amount.decimals,
                        ui_amount_string: token_balance.ui_token_amount.ui_amount_string,
                    };
                    TransactionTokenBalance {
                        account_index: token_balance.account_index,
                        mint: token_balance.mint,
                        ui_token_amount,
                        owner: token_balance.owner,
                        program_id: token_balance.program_id,
                    }
                })
                .collect::<Vec<_>>()
        });
    let rewards = fuzz_message
        .transaction
        .transaction_status_meta
        .rewards
        .map(|rewards| {
            rewards
                .into_iter()
                .map(|reward| Reward {
                    pubkey: reward.pubkey,
                    lamports: reward.lamports,
                    post_balance: reward.post_balance,
                    reward_type: reward.reward_type.map(FuzzRewardType::into_solana),
                    commission: reward.commission,
                })
                .collect::<Vec<_>>()
        });
    let loaded_addresses = LoadedAddresses {
        writable: fuzz_message
            .transaction
            .transaction_status_meta
            .loaded_addresses
            .writable
            .into_iter()
            .map(Pubkey::new_from_array)
            .collect(),
        readonly: fuzz_message
            .transaction
            .transaction_status_meta
            .loaded_addresses
            .readonly
            .into_iter()
            .map(Pubkey::new_from_array)
            .collect(),
    };
    let return_data = fuzz_message
        .transaction
        .transaction_status_meta
        .return_data
        .map(|return_data| TransactionReturnData {
            program_id: Pubkey::new_from_array(return_data.program_id),
            data: return_data.data,
        });
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
        inner_instructions,
        log_messages: fuzz_message
            .transaction
            .transaction_status_meta
            .log_messages,
        pre_token_balances,
        post_token_balances,
        rewards,
        loaded_addresses,
        return_data,
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
    let message = ProtobufMessage::Transaction {
        slot: fuzz_message.slot,
        transaction: &replica,
    };
    message.encode(&mut buf);
    assert!(!buf.is_empty())
});

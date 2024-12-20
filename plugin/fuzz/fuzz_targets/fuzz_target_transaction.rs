#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
    arbitrary::Arbitrary,
    libfuzzer_sys::fuzz_target,
    richat_plugin::protobuf::ProtobufMessage,
    solana_sdk::{
        hash::Hash,
        instruction::CompiledInstruction,
        message::{v0::LoadedAddresses, SimpleAddressLoader},
        pubkey::Pubkey,
        signature::Signature,
        signers::Signers,
        transaction::{SanitizedTransaction, SanitizedVersionedTransaction, VersionedTransaction},
    },
    solana_transaction_status::TransactionStatusMeta,
    status_meta::{
        FuzzInnerInstructions, FuzzReward, FuzzTransactionReturnData, FuzzTransactionTokenBalance,
    },
    std::collections::HashSet,
};

pub struct FuzzSigner;

impl Signers for FuzzSigner {
    fn pubkeys(&self) -> Vec<Pubkey> {
        vec![Pubkey::new_unique()]
    }
    fn try_pubkeys(&self) -> Result<Vec<Pubkey>, solana_sdk::signer::SignerError> {
        Ok(vec![Pubkey::new_unique()])
    }
    fn sign_message(&self, _message: &[u8]) -> Vec<Signature> {
        vec![Signature::new_unique()]
    }
    fn try_sign_message(
        &self,
        _message: &[u8],
    ) -> Result<Vec<Signature>, solana_sdk::signer::SignerError> {
        Ok(vec![Signature::new_unique()])
    }
    fn is_interactive(&self) -> bool {
        false
    }
}

#[derive(Arbitrary, Debug, Clone)]
pub struct FuzzCompiledInstruction {
    program_id_index: u8,
    accounts: Vec<u8>,
    data: Vec<u8>,
}

impl FuzzCompiledInstruction {
    pub fn into_solana(self) -> CompiledInstruction {
        CompiledInstruction {
            program_id_index: self.program_id_index,
            accounts: self.accounts,
            data: self.data,
        }
    }
}

#[derive(Arbitrary, Debug, Clone)]
pub struct FuzzLoadedAddresses {
    writable: Vec<[u8; 32]>,
    readonly: Vec<[u8; 32]>,
}

impl FuzzLoadedAddresses {
    pub fn into_solana(self) -> LoadedAddresses {
        LoadedAddresses {
            writable: self
                .writable
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
            readonly: self
                .readonly
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
        }
    }
}

pub mod sanitized {
    use {
        crate::FuzzCompiledInstruction,
        arbitrary::Arbitrary,
        solana_sdk::{
            hash::Hash,
            message::{
                legacy,
                v0::{self, MessageAddressTableLookup},
                LegacyMessage, MessageHeader, VersionedMessage,
            },
            pubkey::Pubkey,
        },
        std::borrow::Cow,
    };

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzLegacyMessageInner {
        pub header: FuzzMessageHeader,
        pub account_keys: Vec<[u8; 32]>,
        pub recent_blockhash: [u8; 32],
        pub instructions: Vec<super::FuzzCompiledInstruction>,
    }

    impl FuzzLegacyMessageInner {
        pub fn into_solana(self) -> legacy::Message {
            let header = self.header.into_solana();
            let account_keys = self
                .account_keys
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect();
            let recent_blockhash = Hash::new_from_array(self.recent_blockhash);
            let instructions = self
                .instructions
                .into_iter()
                .map(FuzzCompiledInstruction::into_solana)
                .collect();
            legacy::Message {
                header,
                account_keys,
                recent_blockhash,
                instructions,
            }
        }
    }

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzMessageAddressTableLookup {
        pub account_key: [u8; 32],
        pub writable_indexes: Vec<u8>,
        pub readonly_indexes: Vec<u8>,
    }

    impl FuzzMessageAddressTableLookup {
        pub fn into_solana(self) -> MessageAddressTableLookup {
            MessageAddressTableLookup {
                account_key: Pubkey::new_from_array(self.account_key),
                writable_indexes: self.writable_indexes,
                readonly_indexes: self.readonly_indexes,
            }
        }
    }

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzMessageHeader {
        pub num_required_signatures: u8,
        pub num_readonly_signed_accounts: u8,
        pub num_readonly_unsigned_accounts: u8,
    }

    impl FuzzMessageHeader {
        pub fn into_solana(self) -> MessageHeader {
            MessageHeader {
                num_required_signatures: self.num_required_signatures,
                num_readonly_signed_accounts: self.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: self.num_readonly_unsigned_accounts,
            }
        }
    }

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzLoadedMessageInner {
        pub header: FuzzMessageHeader,
        pub account_keys: Vec<[u8; 32]>,
        pub recent_blockhash: [u8; 32],
        pub instructions: Vec<super::FuzzCompiledInstruction>,
        pub address_table_lookups: Vec<FuzzMessageAddressTableLookup>,
    }

    impl FuzzLoadedMessageInner {
        pub fn into_solana(self) -> v0::Message {
            let header = self.header.into_solana();
            let account_keys = self
                .account_keys
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect();
            let recent_blockhash = Hash::new_from_array(self.recent_blockhash);
            let instructions = self
                .instructions
                .into_iter()
                .map(FuzzCompiledInstruction::into_solana)
                .collect();
            let address_table_lookups = self
                .address_table_lookups
                .into_iter()
                .map(FuzzMessageAddressTableLookup::into_solana)
                .collect();
            v0::Message {
                header,
                account_keys,
                recent_blockhash,
                instructions,
                address_table_lookups,
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzLegacyMessage<'a> {
        pub message: Cow<'a, FuzzLegacyMessageInner>,
        pub is_writable_account_cache: Vec<bool>,
    }

    impl<'a> FuzzLegacyMessage<'a> {
        pub fn into_solana(self) -> LegacyMessage<'static> {
            LegacyMessage {
                message: Cow::Owned(self.message.into_owned().into_solana()),
                is_writable_account_cache: self.is_writable_account_cache,
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzLoadedMessage<'a> {
        pub message: Cow<'a, FuzzLoadedMessageInner>,
        pub loaded_addresses: Cow<'a, super::FuzzLoadedAddresses>,
        pub is_writable_account_cache: Vec<bool>,
    }

    impl<'a> FuzzLoadedMessage<'a> {
        pub fn into_solana(self) -> v0::LoadedMessage<'static> {
            v0::LoadedMessage {
                message: Cow::Owned(self.message.into_owned().into_solana()),
                loaded_addresses: Cow::Owned(self.loaded_addresses.into_owned().into_solana()),
                is_writable_account_cache: self.is_writable_account_cache,
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub enum FuzzSanitizedMessage<'a> {
        Legacy(FuzzLegacyMessage<'a>),
        V0(FuzzLoadedMessage<'a>),
    }

    impl<'a> FuzzSanitizedMessage<'a> {
        pub fn into_solana(self) -> VersionedMessage {
            match self {
                Self::Legacy(legacy) => {
                    VersionedMessage::Legacy(legacy.message.into_owned().into_solana())
                }
                Self::V0(v0) => VersionedMessage::V0(v0.message.into_owned().into_solana()),
            }
        }
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
    use {
        arbitrary::Arbitrary,
        solana_account_decoder::parse_token::UiTokenAmount,
        solana_sdk::{pubkey::Pubkey, transaction_context::TransactionReturnData},
        solana_transaction_status::{
            InnerInstruction, InnerInstructions, Reward, RewardType, TransactionTokenBalance,
        },
    };

    #[derive(Arbitrary, Debug)]
    pub enum FuzzTransactionError {
        Err,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzInnerInstruction {
        pub instruction: super::FuzzCompiledInstruction,
        pub stack_height: Option<u32>,
    }

    impl FuzzInnerInstruction {
        pub fn into_solana(self) -> InnerInstruction {
            let instruction = self.instruction.into_solana();
            InnerInstruction {
                instruction,
                stack_height: self.stack_height,
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzInnerInstructions {
        pub index: u8,
        pub instructions: Vec<FuzzInnerInstruction>,
    }

    impl FuzzInnerInstructions {
        pub fn into_solana(self) -> InnerInstructions {
            let instructions = self
                .instructions
                .into_iter()
                .map(FuzzInnerInstruction::into_solana)
                .collect();
            InnerInstructions {
                index: self.index,
                instructions,
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzUiTokenAmount {
        pub ui_amount: Option<f64>,
        pub decimals: u8,
        pub amount: String,
        pub ui_amount_string: String,
    }

    impl FuzzUiTokenAmount {
        pub fn into_solana(self) -> UiTokenAmount {
            UiTokenAmount {
                ui_amount: self.ui_amount,
                amount: self.amount,
                decimals: self.decimals,
                ui_amount_string: self.ui_amount_string,
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzTransactionTokenBalance {
        pub account_index: u8,
        pub mint: String,
        pub ui_token_amount: FuzzUiTokenAmount,
        pub owner: String,
        pub program_id: String,
    }

    impl FuzzTransactionTokenBalance {
        pub fn into_solana(self) -> TransactionTokenBalance {
            TransactionTokenBalance {
                account_index: self.account_index,
                mint: self.mint,
                ui_token_amount: self.ui_token_amount.into_solana(),
                owner: self.owner,
                program_id: self.program_id,
            }
        }
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

    impl FuzzReward {
        pub fn into_solana(self) -> Reward {
            Reward {
                pubkey: self.pubkey,
                lamports: self.lamports,
                post_balance: self.post_balance,
                reward_type: self.reward_type.map(FuzzRewardType::into_solana),
                commission: self.commission,
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzTransactionReturnData {
        pub program_id: [u8; 32],
        pub data: Vec<u8>,
    }

    impl FuzzTransactionReturnData {
        pub fn into_solana(self) -> TransactionReturnData {
            TransactionReturnData {
                program_id: Pubkey::new_from_array(self.program_id),
                data: self.data,
            }
        }
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

fuzz_target!(|fuzz_message: FuzzTransactionMessage| {
    let mut buf = Vec::new();
    let versioned_message = fuzz_message.transaction.transaction.message.into_solana();
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
    let _status = ();
    let inner_instructions = fuzz_message
        .transaction
        .transaction_status_meta
        .inner_instructions
        .map(|inner_instructions| {
            inner_instructions
                .into_iter()
                .map(FuzzInnerInstructions::into_solana)
                .collect()
        });
    let pre_token_balances = fuzz_message
        .transaction
        .transaction_status_meta
        .pre_token_balances
        .map(|pre_token_balances| {
            pre_token_balances
                .into_iter()
                .map(FuzzTransactionTokenBalance::into_solana)
                .collect()
        });
    let post_token_balances = fuzz_message
        .transaction
        .transaction_status_meta
        .post_token_balances
        .map(|post_token_balances| {
            post_token_balances
                .into_iter()
                .map(FuzzTransactionTokenBalance::into_solana)
                .collect()
        });
    let rewards = fuzz_message
        .transaction
        .transaction_status_meta
        .rewards
        .map(|rewards| {
            rewards
                .into_iter()
                .map(FuzzReward::into_solana)
                .collect::<Vec<_>>()
        });
    let loaded_addresses = fuzz_message
        .transaction
        .transaction_status_meta
        .loaded_addresses
        .into_solana();
    let return_data = fuzz_message
        .transaction
        .transaction_status_meta
        .return_data
        .map(FuzzTransactionReturnData::into_solana);
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

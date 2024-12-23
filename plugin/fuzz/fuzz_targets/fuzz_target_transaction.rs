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
        pubkey::{Pubkey, PUBKEY_BYTES},
        signature::{Signature, SIGNATURE_BYTES},
        transaction::{
            SanitizedTransaction, SanitizedVersionedTransaction, TransactionError,
            VersionedTransaction,
        },
    },
    std::collections::HashSet,
};

pub mod signer {
    use solana_sdk::{pubkey::Pubkey, signature::Signature, signers::Signers};

    pub struct SimpleSigner;

    impl Signers for SimpleSigner {
        fn pubkeys(&self) -> Vec<Pubkey> {
            vec![Pubkey::new_unique()]
        }
        fn try_pubkeys(&self) -> std::result::Result<Vec<Pubkey>, solana_sdk::signer::SignerError> {
            Ok(vec![Pubkey::new_unique()])
        }
        fn sign_message(&self, _message: &[u8]) -> Vec<Signature> {
            vec![Signature::new_unique()]
        }
        fn try_sign_message(
            &self,
            _message: &[u8],
        ) -> std::result::Result<Vec<Signature>, solana_sdk::signer::SignerError> {
            Ok(vec![Signature::new_unique()])
        }
        fn is_interactive(&self) -> bool {
            false
        }
    }
}

#[derive(Arbitrary, Debug, Clone)]
pub struct FuzzCompiledInstruction {
    program_id_index: u8,
    accounts: Vec<u8>,
    data: Vec<u8>,
}

impl From<FuzzCompiledInstruction> for CompiledInstruction {
    fn from(value: FuzzCompiledInstruction) -> Self {
        CompiledInstruction {
            program_id_index: value.program_id_index,
            accounts: value.accounts,
            data: value.data,
        }
    }
}

#[derive(Arbitrary, Debug, Clone)]
pub struct FuzzLoadedAddresses {
    writable: Vec<[u8; PUBKEY_BYTES]>,
    readonly: Vec<[u8; PUBKEY_BYTES]>,
}

impl From<FuzzLoadedAddresses> for LoadedAddresses {
    fn from(value: FuzzLoadedAddresses) -> Self {
        LoadedAddresses {
            writable: value
                .writable
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
            readonly: value
                .readonly
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
        }
    }
}

pub mod sanitized {
    use {
        arbitrary::Arbitrary,
        solana_sdk::{
            hash::Hash,
            keccak::HASH_BYTES,
            message::{
                legacy,
                v0::{self, MessageAddressTableLookup},
                LegacyMessage, MessageHeader, VersionedMessage,
            },
            pubkey::{Pubkey, PUBKEY_BYTES},
        },
        std::borrow::Cow,
    };

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzLegacyMessageInner {
        pub header: FuzzMessageHeader,
        pub account_keys: Vec<[u8; PUBKEY_BYTES]>,
        pub recent_blockhash: [u8; HASH_BYTES],
        pub instructions: Vec<super::FuzzCompiledInstruction>,
    }

    impl From<FuzzLegacyMessageInner> for legacy::Message {
        fn from(value: FuzzLegacyMessageInner) -> Self {
            legacy::Message {
                header: value.header.into(),
                account_keys: value
                    .account_keys
                    .into_iter()
                    .map(Pubkey::new_from_array)
                    .collect(),
                recent_blockhash: Hash::new_from_array(value.recent_blockhash),
                instructions: value.instructions.into_iter().map(Into::into).collect(),
            }
        }
    }

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzMessageAddressTableLookup {
        pub account_key: [u8; PUBKEY_BYTES],
        pub writable_indexes: Vec<u8>,
        pub readonly_indexes: Vec<u8>,
    }

    impl From<FuzzMessageAddressTableLookup> for MessageAddressTableLookup {
        fn from(value: FuzzMessageAddressTableLookup) -> Self {
            MessageAddressTableLookup {
                account_key: Pubkey::new_from_array(value.account_key),
                writable_indexes: value.writable_indexes,
                readonly_indexes: value.readonly_indexes,
            }
        }
    }

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzMessageHeader {
        pub num_required_signatures: u8,
        pub num_readonly_signed_accounts: u8,
        pub num_readonly_unsigned_accounts: u8,
    }

    impl From<FuzzMessageHeader> for MessageHeader {
        fn from(value: FuzzMessageHeader) -> Self {
            MessageHeader {
                num_required_signatures: value.num_required_signatures,
                num_readonly_signed_accounts: value.num_readonly_signed_accounts,
                num_readonly_unsigned_accounts: value.num_readonly_unsigned_accounts,
            }
        }
    }

    #[derive(Arbitrary, Debug, Clone)]
    pub struct FuzzLoadedMessageInner {
        pub header: FuzzMessageHeader,
        pub account_keys: Vec<[u8; PUBKEY_BYTES]>,
        pub recent_blockhash: [u8; HASH_BYTES],
        pub instructions: Vec<super::FuzzCompiledInstruction>,
        pub address_table_lookups: Vec<FuzzMessageAddressTableLookup>,
    }

    impl From<FuzzLoadedMessageInner> for v0::Message {
        fn from(value: FuzzLoadedMessageInner) -> Self {
            v0::Message {
                header: value.header.into(),
                account_keys: value
                    .account_keys
                    .into_iter()
                    .map(Pubkey::new_from_array)
                    .collect(),
                recent_blockhash: Hash::new_from_array(value.recent_blockhash),
                instructions: value.instructions.into_iter().map(Into::into).collect(),
                address_table_lookups: value
                    .address_table_lookups
                    .into_iter()
                    .map(Into::into)
                    .collect(),
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzLegacyMessage<'a> {
        pub message: Cow<'a, FuzzLegacyMessageInner>,
        pub is_writable_account_cache: Vec<bool>,
    }

    impl<'a> From<FuzzLegacyMessage<'a>> for LegacyMessage<'static> {
        fn from(value: FuzzLegacyMessage<'a>) -> Self {
            LegacyMessage {
                message: Cow::Owned(value.message.into_owned().into()),
                is_writable_account_cache: value.is_writable_account_cache,
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzLoadedMessage<'a> {
        pub message: Cow<'a, FuzzLoadedMessageInner>,
        pub loaded_addresses: Cow<'a, super::FuzzLoadedAddresses>,
        pub is_writable_account_cache: Vec<bool>,
    }

    impl<'a> From<FuzzLoadedMessage<'a>> for v0::LoadedMessage<'static> {
        fn from(value: FuzzLoadedMessage<'a>) -> Self {
            v0::LoadedMessage {
                message: Cow::Owned(value.message.into_owned().into()),
                loaded_addresses: Cow::Owned(value.loaded_addresses.into_owned().into()),
                is_writable_account_cache: value.is_writable_account_cache,
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub enum FuzzSanitizedMessage<'a> {
        Legacy(FuzzLegacyMessage<'a>),
        V0(FuzzLoadedMessage<'a>),
    }

    impl<'a> From<FuzzSanitizedMessage<'a>> for VersionedMessage {
        fn from(value: FuzzSanitizedMessage<'a>) -> Self {
            match value {
                FuzzSanitizedMessage::Legacy(legacy) => {
                    VersionedMessage::Legacy(legacy.message.into_owned().into())
                }
                FuzzSanitizedMessage::V0(v0) => {
                    VersionedMessage::V0(v0.message.into_owned().into())
                }
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzSanitizedTransaction<'a> {
        pub message: FuzzSanitizedMessage<'a>,
        pub message_hash: [u8; HASH_BYTES],
        pub is_simple_vote_tx: bool,
        pub signatures: Vec<&'a [u8]>,
    }
}

pub mod status_meta {
    use {
        arbitrary::Arbitrary,
        solana_account_decoder::parse_token::UiTokenAmount,
        solana_sdk::{
            pubkey::{Pubkey, PUBKEY_BYTES},
            transaction_context::TransactionReturnData,
        },
        solana_transaction_status::{
            InnerInstruction, InnerInstructions, Reward, RewardType, TransactionTokenBalance,
        },
    };

    #[derive(Arbitrary, Debug)]
    pub enum FuzzTransactionError {
        Zero,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzInnerInstruction {
        pub instruction: super::FuzzCompiledInstruction,
        pub stack_height: Option<u32>,
    }

    impl From<FuzzInnerInstruction> for InnerInstruction {
        fn from(value: FuzzInnerInstruction) -> Self {
            let instruction = value.instruction.into();
            InnerInstruction {
                instruction,
                stack_height: value.stack_height,
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzInnerInstructions {
        pub index: u8,
        pub instructions: Vec<FuzzInnerInstruction>,
    }

    impl From<FuzzInnerInstructions> for InnerInstructions {
        fn from(value: FuzzInnerInstructions) -> Self {
            let instructions = value.instructions.into_iter().map(Into::into).collect();
            InnerInstructions {
                index: value.index,
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

    impl From<FuzzUiTokenAmount> for UiTokenAmount {
        fn from(value: FuzzUiTokenAmount) -> Self {
            UiTokenAmount {
                ui_amount: value.ui_amount,
                amount: value.amount,
                decimals: value.decimals,
                ui_amount_string: value.ui_amount_string,
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

    impl From<FuzzTransactionTokenBalance> for TransactionTokenBalance {
        fn from(value: FuzzTransactionTokenBalance) -> Self {
            TransactionTokenBalance {
                account_index: value.account_index,
                mint: value.mint,
                ui_token_amount: value.ui_token_amount.into(),
                owner: value.owner,
                program_id: value.program_id,
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

    impl From<FuzzRewardType> for RewardType {
        fn from(value: FuzzRewardType) -> Self {
            match value {
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

    impl From<FuzzReward> for Reward {
        fn from(value: FuzzReward) -> Self {
            Reward {
                pubkey: value.pubkey,
                lamports: value.lamports,
                post_balance: value.post_balance,
                reward_type: value.reward_type.map(Into::into),
                commission: value.commission,
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzTransactionReturnData {
        pub program_id: [u8; PUBKEY_BYTES],
        pub data: Vec<u8>,
    }

    impl From<FuzzTransactionReturnData> for TransactionReturnData {
        fn from(value: FuzzTransactionReturnData) -> Self {
            TransactionReturnData {
                program_id: Pubkey::new_from_array(value.program_id),
                data: value.data,
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
    pub signature: [u8; SIGNATURE_BYTES],
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
    let versioned_message = fuzz_message.transaction.transaction.message.into();
    let simple_signer = signer::SimpleSigner;
    let versioned_transaction = VersionedTransaction::try_new(versioned_message, &simple_signer)
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
    let inner_instructions = fuzz_message
        .transaction
        .transaction_status_meta
        .inner_instructions
        .map(|inner_instructions| inner_instructions.into_iter().map(Into::into).collect());
    let pre_token_balances = fuzz_message
        .transaction
        .transaction_status_meta
        .pre_token_balances
        .map(|pre_token_balances| pre_token_balances.into_iter().map(Into::into).collect());
    let post_token_balances = fuzz_message
        .transaction
        .transaction_status_meta
        .post_token_balances
        .map(|post_token_balances| post_token_balances.into_iter().map(Into::into).collect());
    let rewards = fuzz_message
        .transaction
        .transaction_status_meta
        .rewards
        .map(|rewards| rewards.into_iter().map(Into::into).collect::<Vec<_>>());
    let loaded_addresses = fuzz_message
        .transaction
        .transaction_status_meta
        .loaded_addresses
        .into();
    let return_data = fuzz_message
        .transaction
        .transaction_status_meta
        .return_data
        .map(Into::into);
    let transaction_status_meta = solana_transaction_status::TransactionStatusMeta {
        status: match fuzz_message.transaction.transaction_status_meta.status {
            Ok(_) => Ok(()),
            Err(_) => Err(TransactionError::UnsupportedVersion),
        },
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

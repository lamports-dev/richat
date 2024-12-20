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
        signers::Signers,
        transaction::{SanitizedTransaction, SanitizedVersionedTransaction, VersionedTransaction},
    },
    solana_transaction_status::TransactionStatusMeta,
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

impl Into<CompiledInstruction> for FuzzCompiledInstruction {
    fn into(self) -> CompiledInstruction {
        CompiledInstruction {
            program_id_index: self.program_id_index,
            accounts: self.accounts,
            data: self.data,
        }
    }
}

#[derive(Arbitrary, Debug, Clone)]
pub struct FuzzLoadedAddresses {
    writable: Vec<[u8; PUBKEY_BYTES]>,
    readonly: Vec<[u8; PUBKEY_BYTES]>,
}

impl Into<LoadedAddresses> for FuzzLoadedAddresses {
    fn into(self) -> LoadedAddresses {
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

    impl Into<legacy::Message> for FuzzLegacyMessageInner {
        fn into(self) -> legacy::Message {
            let header = self.header.into_solana();
            let account_keys = self
                .account_keys
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect();
            let recent_blockhash = Hash::new_from_array(self.recent_blockhash);
            let instructions = self.instructions.into_iter().map(Into::into).collect();
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
        pub account_key: [u8; PUBKEY_BYTES],
        pub writable_indexes: Vec<u8>,
        pub readonly_indexes: Vec<u8>,
    }

    impl Into<MessageAddressTableLookup> for FuzzMessageAddressTableLookup {
        fn into(self) -> MessageAddressTableLookup {
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
        pub account_keys: Vec<[u8; PUBKEY_BYTES]>,
        pub recent_blockhash: [u8; HASH_BYTES],
        pub instructions: Vec<super::FuzzCompiledInstruction>,
        pub address_table_lookups: Vec<FuzzMessageAddressTableLookup>,
    }

    impl Into<v0::Message> for FuzzLoadedMessageInner {
        fn into(self) -> v0::Message {
            let header = self.header.into_solana();
            let account_keys = self
                .account_keys
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect();
            let recent_blockhash = Hash::new_from_array(self.recent_blockhash);
            let instructions = self.instructions.into_iter().map(Into::into).collect();
            let address_table_lookups = self
                .address_table_lookups
                .into_iter()
                .map(Into::into)
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

    impl<'a> Into<LegacyMessage<'static>> for FuzzLegacyMessage<'a> {
        fn into(self) -> LegacyMessage<'static> {
            LegacyMessage {
                message: Cow::Owned(self.message.into_owned().into()),
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

    impl<'a> Into<v0::LoadedMessage<'static>> for FuzzLoadedMessage<'a> {
        fn into(self) -> v0::LoadedMessage<'static> {
            v0::LoadedMessage {
                message: Cow::Owned(self.message.into_owned().into()),
                loaded_addresses: Cow::Owned(self.loaded_addresses.into_owned().into()),
                is_writable_account_cache: self.is_writable_account_cache,
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub enum FuzzSanitizedMessage<'a> {
        Legacy(FuzzLegacyMessage<'a>),
        V0(FuzzLoadedMessage<'a>),
    }

    impl<'a> Into<VersionedMessage> for FuzzSanitizedMessage<'a> {
        fn into(self) -> VersionedMessage {
            match self {
                Self::Legacy(legacy) => {
                    VersionedMessage::Legacy(legacy.message.into_owned().into())
                }
                Self::V0(v0) => VersionedMessage::V0(v0.message.into_owned().into()),
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
        Err,
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzInnerInstruction {
        pub instruction: super::FuzzCompiledInstruction,
        pub stack_height: Option<u32>,
    }

    impl Into<InnerInstruction> for FuzzInnerInstruction {
        fn into(self) -> InnerInstruction {
            let instruction = self.instruction.into();
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

    impl Into<InnerInstructions> for FuzzInnerInstructions {
        fn into(self) -> InnerInstructions {
            let instructions = self.instructions.into_iter().map(Into::into).collect();
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

    impl Into<UiTokenAmount> for FuzzUiTokenAmount {
        fn into(self) -> UiTokenAmount {
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

    impl Into<TransactionTokenBalance> for FuzzTransactionTokenBalance {
        fn into(self) -> TransactionTokenBalance {
            TransactionTokenBalance {
                account_index: self.account_index,
                mint: self.mint,
                ui_token_amount: self.ui_token_amount.into(),
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

    impl Into<RewardType> for FuzzRewardType {
        fn into(self) -> RewardType {
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

    impl Into<Reward> for FuzzReward {
        fn into(self) -> Reward {
            Reward {
                pubkey: self.pubkey,
                lamports: self.lamports,
                post_balance: self.post_balance,
                reward_type: self.reward_type.map(Into::into),
                commission: self.commission,
            }
        }
    }

    #[derive(Arbitrary, Debug)]
    pub struct FuzzTransactionReturnData {
        pub program_id: [u8; PUBKEY_BYTES],
        pub data: Vec<u8>,
    }

    impl Into<TransactionReturnData> for FuzzTransactionReturnData {
        fn into(self) -> TransactionReturnData {
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

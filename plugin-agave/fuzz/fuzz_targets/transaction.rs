#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
    arbitrary::Arbitrary,
    richat_plugin_agave::protobuf::ProtobufMessage,
    solana_account_decoder::parse_token::UiTokenAmount,
    solana_sdk::{
        hash::{Hash, HASH_BYTES},
        instruction::{CompiledInstruction, InstructionError},
        message::{
            legacy, v0, LegacyMessage, MessageHeader, SimpleAddressLoader, VersionedMessage,
        },
        pubkey::{Pubkey, PUBKEY_BYTES},
        signature::{Signature, SIGNATURE_BYTES},
        signer::SignerError,
        signers::Signers,
        transaction::{
            SanitizedTransaction, SanitizedVersionedTransaction, TransactionError,
            VersionedTransaction,
        },
        transaction_context::TransactionReturnData,
    },
    solana_transaction_status::{
        InnerInstruction, InnerInstructions, Reward, RewardType, TransactionStatusMeta,
        TransactionTokenBalance,
    },
    std::{borrow::Cow, collections::HashSet, time::SystemTime},
};

#[derive(Debug)]
struct SimpleSigner;

impl Signers for SimpleSigner {
    fn pubkeys(&self) -> Vec<Pubkey> {
        vec![Pubkey::new_unique()]
    }

    fn try_pubkeys(&self) -> Result<Vec<Pubkey>, SignerError> {
        Ok(vec![Pubkey::new_unique()])
    }

    fn sign_message(&self, _message: &[u8]) -> Vec<Signature> {
        vec![Signature::new_unique()]
    }

    fn try_sign_message(&self, _message: &[u8]) -> Result<Vec<Signature>, SignerError> {
        Ok(vec![Signature::new_unique()])
    }

    fn is_interactive(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, Arbitrary)]
struct FuzzMessageHeader {
    num_required_signatures: u8,
    num_readonly_signed_accounts: u8,
    num_readonly_unsigned_accounts: u8,
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

#[derive(Debug, Clone, Arbitrary)]
struct FuzzCompiledInstruction {
    program_id_index: u8,
    accounts: Vec<u8>,
    data: Vec<u8>,
}

impl From<FuzzCompiledInstruction> for CompiledInstruction {
    fn from(fuzz: FuzzCompiledInstruction) -> Self {
        Self {
            program_id_index: fuzz.program_id_index,
            accounts: fuzz.accounts,
            data: fuzz.data,
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
struct FuzzLegacyMessageInner {
    header: FuzzMessageHeader,
    account_keys: Vec<[u8; PUBKEY_BYTES]>,
    recent_blockhash: [u8; HASH_BYTES],
    instructions: Vec<FuzzCompiledInstruction>,
}

impl From<FuzzLegacyMessageInner> for legacy::Message {
    fn from(fuzz: FuzzLegacyMessageInner) -> Self {
        Self {
            header: fuzz.header.into(),
            account_keys: fuzz
                .account_keys
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
            recent_blockhash: Hash::new_from_array(fuzz.recent_blockhash),
            instructions: fuzz.instructions.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Arbitrary)]
struct FuzzLegacyMessage {
    message: FuzzLegacyMessageInner,
    is_writable_account_cache: Vec<bool>,
}

impl From<FuzzLegacyMessage> for LegacyMessage<'static> {
    fn from(fuzz: FuzzLegacyMessage) -> Self {
        Self {
            message: Cow::Owned(fuzz.message.into()),
            is_writable_account_cache: fuzz.is_writable_account_cache,
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
struct FuzzLoadedMessageInner {
    header: FuzzMessageHeader,
    account_keys: Vec<[u8; PUBKEY_BYTES]>,
    recent_blockhash: [u8; HASH_BYTES],
    instructions: Vec<FuzzCompiledInstruction>,
    address_table_lookups: Vec<FuzzMessageAddressTableLookup>,
}

#[derive(Debug, Clone, Arbitrary)]
struct FuzzMessageAddressTableLookup {
    account_key: [u8; PUBKEY_BYTES],
    writable_indexes: Vec<u8>,
    readonly_indexes: Vec<u8>,
}

impl From<FuzzMessageAddressTableLookup> for v0::MessageAddressTableLookup {
    fn from(fuzz: FuzzMessageAddressTableLookup) -> Self {
        Self {
            account_key: Pubkey::new_from_array(fuzz.account_key),
            writable_indexes: fuzz.writable_indexes,
            readonly_indexes: fuzz.readonly_indexes,
        }
    }
}

impl From<FuzzLoadedMessageInner> for v0::Message {
    fn from(fuzz: FuzzLoadedMessageInner) -> Self {
        Self {
            header: fuzz.header.into(),
            account_keys: fuzz
                .account_keys
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
            recent_blockhash: Hash::new_from_array(fuzz.recent_blockhash),
            instructions: fuzz.instructions.into_iter().map(Into::into).collect(),
            address_table_lookups: fuzz
                .address_table_lookups
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
struct FuzzLoadedAddresses {
    writable: Vec<[u8; PUBKEY_BYTES]>,
    readonly: Vec<[u8; PUBKEY_BYTES]>,
}

impl From<FuzzLoadedAddresses> for v0::LoadedAddresses {
    fn from(fuzz: FuzzLoadedAddresses) -> Self {
        Self {
            writable: fuzz
                .writable
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
            readonly: fuzz
                .readonly
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
        }
    }
}

#[derive(Debug, Arbitrary)]
struct FuzzLoadedMessage {
    message: FuzzLoadedMessageInner,
    loaded_addresses: FuzzLoadedAddresses,
    is_writable_account_cache: Vec<bool>,
}

impl From<FuzzLoadedMessage> for v0::LoadedMessage<'static> {
    fn from(fuzz: FuzzLoadedMessage) -> v0::LoadedMessage<'static> {
        Self {
            message: Cow::Owned(fuzz.message.into()),
            loaded_addresses: Cow::Owned(fuzz.loaded_addresses.into()),
            is_writable_account_cache: fuzz.is_writable_account_cache,
        }
    }
}

#[derive(Debug, Arbitrary)]
enum FuzzSanitizedMessage {
    Legacy(FuzzLegacyMessage),
    V0(FuzzLoadedMessage),
}

impl From<FuzzSanitizedMessage> for VersionedMessage {
    fn from(fuzz: FuzzSanitizedMessage) -> Self {
        match fuzz {
            FuzzSanitizedMessage::Legacy(legacy) => Self::Legacy(legacy.message.into()),
            FuzzSanitizedMessage::V0(v0) => Self::V0(v0.message.into()),
        }
    }
}

#[derive(Debug, Arbitrary)]
struct FuzzSanitizedTransaction {
    message: FuzzSanitizedMessage,
    message_hash: [u8; HASH_BYTES],
    is_simple_vote_tx: bool,
    // signatures: Vec<[u8; SIGNATURE_BYTES]>,
}

impl TryFrom<FuzzSanitizedTransaction> for SanitizedTransaction {
    type Error = ();

    fn try_from(fuzz: FuzzSanitizedTransaction) -> Result<Self, Self::Error> {
        let address_loader = match &fuzz.message {
            FuzzSanitizedMessage::Legacy(_) => SimpleAddressLoader::Disabled,
            FuzzSanitizedMessage::V0(msg) => {
                SimpleAddressLoader::Enabled(msg.loaded_addresses.clone().into())
            }
        };

        let versioned_transaction =
            VersionedTransaction::try_new(fuzz.message.into(), &SimpleSigner).map_err(|_| ())?;
        let sanitized_versioned_transaction =
            SanitizedVersionedTransaction::try_new(versioned_transaction).map_err(|_| ())?;

        SanitizedTransaction::try_new(
            sanitized_versioned_transaction,
            Hash::new_from_array(fuzz.message_hash),
            fuzz.is_simple_vote_tx,
            address_loader,
            &HashSet::new(),
        )
        .map_err(|_| ())
    }
}

#[derive(Debug, Arbitrary)]
enum FuzzInstructionError {
    GenericError,
    InvalidArgument,
    InvalidInstructionData,
    InvalidAccountData,
    AccountDataTooSmall,
    InsufficientFunds,
    IncorrectProgramId,
    MissingRequiredSignature,
    AccountAlreadyInitialized,
    UninitializedAccount,
    UnbalancedInstruction,
    ModifiedProgramId,
    ExternalAccountLamportSpend,
    ExternalAccountDataModified,
    ReadonlyLamportChange,
    ReadonlyDataModified,
    DuplicateAccountIndex,
    ExecutableModified,
    RentEpochModified,
    NotEnoughAccountKeys,
    AccountDataSizeChanged,
    AccountNotExecutable,
    AccountBorrowFailed,
    AccountBorrowOutstanding,
    DuplicateAccountOutOfSync,
    Custom(u32),
    InvalidError,
    ExecutableDataModified,
    ExecutableLamportChange,
    ExecutableAccountNotRentExempt,
    UnsupportedProgramId,
    CallDepth,
    MissingAccount,
    ReentrancyNotAllowed,
    MaxSeedLengthExceeded,
    InvalidSeeds,
    InvalidRealloc,
    ComputationalBudgetExceeded,
    PrivilegeEscalation,
    ProgramEnvironmentSetupFailure,
    ProgramFailedToComplete,
    ProgramFailedToCompile,
    Immutable,
    IncorrectAuthority,
    BorshIoError(String),
    AccountNotRentExempt,
    InvalidAccountOwner,
    ArithmeticOverflow,
    UnsupportedSysvar,
    IllegalOwner,
    MaxAccountsDataAllocationsExceeded,
    MaxAccountsExceeded,
    MaxInstructionTraceLengthExceeded,
    BuiltinProgramsMustConsumeComputeUnits,
}

impl From<FuzzInstructionError> for InstructionError {
    fn from(fuzz: FuzzInstructionError) -> Self {
        use FuzzInstructionError::*;
        match fuzz {
            GenericError => Self::GenericError,
            InvalidArgument => Self::InvalidArgument,
            InvalidInstructionData => Self::InvalidInstructionData,
            InvalidAccountData => Self::InvalidAccountData,
            AccountDataTooSmall => Self::AccountDataTooSmall,
            InsufficientFunds => Self::InsufficientFunds,
            IncorrectProgramId => Self::IncorrectProgramId,
            MissingRequiredSignature => Self::MissingRequiredSignature,
            AccountAlreadyInitialized => Self::AccountAlreadyInitialized,
            UninitializedAccount => Self::UninitializedAccount,
            UnbalancedInstruction => Self::UnbalancedInstruction,
            ModifiedProgramId => Self::ModifiedProgramId,
            ExternalAccountLamportSpend => Self::ExternalAccountLamportSpend,
            ExternalAccountDataModified => Self::ExternalAccountDataModified,
            ReadonlyLamportChange => Self::ReadonlyLamportChange,
            ReadonlyDataModified => Self::ReadonlyDataModified,
            DuplicateAccountIndex => Self::DuplicateAccountIndex,
            ExecutableModified => Self::ExecutableModified,
            RentEpochModified => Self::RentEpochModified,
            NotEnoughAccountKeys => Self::NotEnoughAccountKeys,
            AccountDataSizeChanged => Self::AccountDataSizeChanged,
            AccountNotExecutable => Self::AccountNotExecutable,
            AccountBorrowFailed => Self::AccountBorrowFailed,
            AccountBorrowOutstanding => Self::AccountBorrowOutstanding,
            DuplicateAccountOutOfSync => Self::DuplicateAccountOutOfSync,
            Custom(value) => Self::Custom(value),
            InvalidError => Self::InvalidError,
            ExecutableDataModified => Self::ExecutableDataModified,
            ExecutableLamportChange => Self::ExecutableLamportChange,
            ExecutableAccountNotRentExempt => Self::ExecutableAccountNotRentExempt,
            UnsupportedProgramId => Self::UnsupportedProgramId,
            CallDepth => Self::CallDepth,
            MissingAccount => Self::MissingAccount,
            ReentrancyNotAllowed => Self::ReentrancyNotAllowed,
            MaxSeedLengthExceeded => Self::MaxSeedLengthExceeded,
            InvalidSeeds => Self::InvalidSeeds,
            InvalidRealloc => Self::InvalidRealloc,
            ComputationalBudgetExceeded => Self::ComputationalBudgetExceeded,
            PrivilegeEscalation => Self::PrivilegeEscalation,
            ProgramEnvironmentSetupFailure => Self::ProgramEnvironmentSetupFailure,
            ProgramFailedToComplete => Self::ProgramFailedToComplete,
            ProgramFailedToCompile => Self::ProgramFailedToCompile,
            Immutable => Self::Immutable,
            IncorrectAuthority => Self::IncorrectAuthority,
            BorshIoError(value) => Self::BorshIoError(value),
            AccountNotRentExempt => Self::AccountNotRentExempt,
            InvalidAccountOwner => Self::InvalidAccountOwner,
            ArithmeticOverflow => Self::ArithmeticOverflow,
            UnsupportedSysvar => Self::UnsupportedSysvar,
            IllegalOwner => Self::IllegalOwner,
            MaxAccountsDataAllocationsExceeded => Self::MaxAccountsDataAllocationsExceeded,
            MaxAccountsExceeded => Self::MaxAccountsExceeded,
            MaxInstructionTraceLengthExceeded => Self::MaxInstructionTraceLengthExceeded,
            BuiltinProgramsMustConsumeComputeUnits => Self::BuiltinProgramsMustConsumeComputeUnits,
        }
    }
}

#[derive(Debug, Arbitrary)]
enum FuzzTransactionError {
    AccountInUse,
    AccountLoadedTwice,
    AccountNotFound,
    ProgramAccountNotFound,
    InsufficientFundsForFee,
    InvalidAccountForFee,
    AlreadyProcessed,
    BlockhashNotFound,
    InstructionError(u8, FuzzInstructionError),
    CallChainTooDeep,
    MissingSignatureForFee,
    InvalidAccountIndex,
    SignatureFailure,
    InvalidProgramForExecution,
    SanitizeFailure,
    ClusterMaintenance,
    AccountBorrowOutstanding,
    WouldExceedMaxBlockCostLimit,
    UnsupportedVersion,
    InvalidWritableAccount,
    WouldExceedMaxAccountCostLimit,
    WouldExceedAccountDataBlockLimit,
    TooManyAccountLocks,
    AddressLookupTableNotFound,
    InvalidAddressLookupTableOwner,
    InvalidAddressLookupTableData,
    InvalidAddressLookupTableIndex,
    InvalidRentPayingAccount,
    WouldExceedMaxVoteCostLimit,
    WouldExceedAccountDataTotalLimit,
    DuplicateInstruction(u8),
    InsufficientFundsForRent { account_index: u8 },
    MaxLoadedAccountsDataSizeExceeded,
    InvalidLoadedAccountsDataSizeLimit,
    ResanitizationNeeded,
    ProgramExecutionTemporarilyRestricted { account_index: u8 },
    UnbalancedTransaction,
    ProgramCacheHitMaxLimit,
}

impl From<FuzzTransactionError> for TransactionError {
    fn from(fuzz: FuzzTransactionError) -> Self {
        use FuzzTransactionError::*;
        match fuzz {
            AccountInUse => Self::AccountInUse,
            AccountLoadedTwice => Self::AccountLoadedTwice,
            AccountNotFound => Self::AccountNotFound,
            ProgramAccountNotFound => Self::ProgramAccountNotFound,
            InsufficientFundsForFee => Self::InsufficientFundsForFee,
            InvalidAccountForFee => Self::InvalidAccountForFee,
            AlreadyProcessed => Self::AlreadyProcessed,
            BlockhashNotFound => Self::BlockhashNotFound,
            InstructionError(value, instruction_error) => {
                Self::InstructionError(value, instruction_error.into())
            }
            CallChainTooDeep => Self::CallChainTooDeep,
            MissingSignatureForFee => Self::MissingSignatureForFee,
            InvalidAccountIndex => Self::InvalidAccountIndex,
            SignatureFailure => Self::SignatureFailure,
            InvalidProgramForExecution => Self::InvalidProgramForExecution,
            SanitizeFailure => Self::SanitizeFailure,
            ClusterMaintenance => Self::ClusterMaintenance,
            AccountBorrowOutstanding => Self::AccountBorrowOutstanding,
            WouldExceedMaxBlockCostLimit => Self::WouldExceedMaxBlockCostLimit,
            UnsupportedVersion => Self::UnsupportedVersion,
            InvalidWritableAccount => Self::InvalidWritableAccount,
            WouldExceedMaxAccountCostLimit => Self::WouldExceedMaxAccountCostLimit,
            WouldExceedAccountDataBlockLimit => Self::WouldExceedAccountDataBlockLimit,
            TooManyAccountLocks => Self::TooManyAccountLocks,
            AddressLookupTableNotFound => Self::AddressLookupTableNotFound,
            InvalidAddressLookupTableOwner => Self::InvalidAddressLookupTableOwner,
            InvalidAddressLookupTableData => Self::InvalidAddressLookupTableData,
            InvalidAddressLookupTableIndex => Self::InvalidAddressLookupTableIndex,
            InvalidRentPayingAccount => Self::InvalidRentPayingAccount,
            WouldExceedMaxVoteCostLimit => Self::WouldExceedMaxVoteCostLimit,
            WouldExceedAccountDataTotalLimit => Self::WouldExceedAccountDataTotalLimit,
            DuplicateInstruction(value) => Self::DuplicateInstruction(value),
            InsufficientFundsForRent { account_index } => {
                Self::InsufficientFundsForRent { account_index }
            }
            MaxLoadedAccountsDataSizeExceeded => Self::MaxLoadedAccountsDataSizeExceeded,
            InvalidLoadedAccountsDataSizeLimit => Self::InvalidLoadedAccountsDataSizeLimit,
            ResanitizationNeeded => Self::ResanitizationNeeded,
            ProgramExecutionTemporarilyRestricted { account_index } => {
                Self::ProgramExecutionTemporarilyRestricted { account_index }
            }
            UnbalancedTransaction => Self::UnbalancedTransaction,
            ProgramCacheHitMaxLimit => Self::ProgramCacheHitMaxLimit,
        }
    }
}

#[derive(Debug, Arbitrary)]
struct FuzzInnerInstruction {
    instruction: FuzzCompiledInstruction,
    stack_height: Option<u32>,
}

impl From<FuzzInnerInstruction> for InnerInstruction {
    fn from(fuzz: FuzzInnerInstruction) -> Self {
        Self {
            instruction: fuzz.instruction.into(),
            stack_height: fuzz.stack_height,
        }
    }
}

#[derive(Debug, Arbitrary)]
struct FuzzInnerInstructions {
    index: u8,
    instructions: Vec<FuzzInnerInstruction>,
}

impl From<FuzzInnerInstructions> for InnerInstructions {
    fn from(fuzz: FuzzInnerInstructions) -> Self {
        Self {
            index: fuzz.index,
            instructions: fuzz.instructions.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Arbitrary)]
struct FuzzUiTokenAmount {
    ui_amount: Option<f64>,
    decimals: u8,
    amount: String,
    ui_amount_string: String,
}

impl From<FuzzUiTokenAmount> for UiTokenAmount {
    fn from(fuzz: FuzzUiTokenAmount) -> Self {
        Self {
            ui_amount: fuzz.ui_amount,
            amount: fuzz.amount,
            decimals: fuzz.decimals,
            ui_amount_string: fuzz.ui_amount_string,
        }
    }
}

#[derive(Debug, Arbitrary)]
struct FuzzTransactionTokenBalance {
    account_index: u8,
    mint: String,
    ui_token_amount: FuzzUiTokenAmount,
    owner: String,
    program_id: String,
}

impl From<FuzzTransactionTokenBalance> for TransactionTokenBalance {
    fn from(fuzz: FuzzTransactionTokenBalance) -> Self {
        Self {
            account_index: fuzz.account_index,
            mint: fuzz.mint,
            ui_token_amount: fuzz.ui_token_amount.into(),
            owner: fuzz.owner,
            program_id: fuzz.program_id,
        }
    }
}

#[derive(Debug, Arbitrary)]
enum FuzzRewardType {
    Fee,
    Rent,
    Staking,
    Voting,
}

impl From<FuzzRewardType> for RewardType {
    fn from(fuzz: FuzzRewardType) -> Self {
        match fuzz {
            FuzzRewardType::Fee => RewardType::Fee,
            FuzzRewardType::Rent => RewardType::Rent,
            FuzzRewardType::Staking => RewardType::Staking,
            FuzzRewardType::Voting => RewardType::Voting,
        }
    }
}

#[derive(Debug, Arbitrary)]
struct FuzzReward {
    pubkey: String,
    lamports: i64,
    post_balance: u64,
    reward_type: Option<FuzzRewardType>,
    commission: Option<u8>,
}

impl From<FuzzReward> for Reward {
    fn from(fuzz: FuzzReward) -> Self {
        Self {
            pubkey: fuzz.pubkey,
            lamports: fuzz.lamports,
            post_balance: fuzz.post_balance,
            reward_type: fuzz.reward_type.map(Into::into),
            commission: fuzz.commission,
        }
    }
}

#[derive(Debug, Arbitrary)]
struct FuzzTransactionReturnData {
    program_id: [u8; PUBKEY_BYTES],
    data: Vec<u8>,
}

impl From<FuzzTransactionReturnData> for TransactionReturnData {
    fn from(fuzz: FuzzTransactionReturnData) -> Self {
        Self {
            program_id: Pubkey::new_from_array(fuzz.program_id),
            data: fuzz.data,
        }
    }
}

#[derive(Debug, Arbitrary)]
struct FuzzTransactionStatusMeta {
    status: Result<(), FuzzTransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
    inner_instructions: Option<Vec<FuzzInnerInstructions>>,
    log_messages: Option<Vec<String>>,
    pre_token_balances: Option<Vec<FuzzTransactionTokenBalance>>,
    post_token_balances: Option<Vec<FuzzTransactionTokenBalance>>,
    rewards: Option<Vec<FuzzReward>>,
    loaded_addresses: FuzzLoadedAddresses,
    return_data: Option<FuzzTransactionReturnData>,
    compute_units_consumed: Option<u64>,
    cost_units: Option<u64>,
}

impl From<FuzzTransactionStatusMeta> for TransactionStatusMeta {
    fn from(fuzz: FuzzTransactionStatusMeta) -> Self {
        Self {
            status: fuzz.status.map_err(Into::into),
            fee: fuzz.fee,
            pre_balances: fuzz.pre_balances,
            post_balances: fuzz.post_balances,
            inner_instructions: fuzz
                .inner_instructions
                .map(|inner_instructions| inner_instructions.into_iter().map(Into::into).collect()),
            log_messages: fuzz.log_messages,
            pre_token_balances: fuzz
                .pre_token_balances
                .map(|pre_token_balances| pre_token_balances.into_iter().map(Into::into).collect()),
            post_token_balances: fuzz.post_token_balances.map(|post_token_balances| {
                post_token_balances.into_iter().map(Into::into).collect()
            }),
            rewards: fuzz
                .rewards
                .map(|rewards| rewards.into_iter().map(Into::into).collect()),
            loaded_addresses: fuzz.loaded_addresses.into(),
            return_data: fuzz.return_data.map(Into::into),
            compute_units_consumed: fuzz.compute_units_consumed,
            cost_units: fuzz.cost_units,
        }
    }
}

#[derive(Debug, Arbitrary)]
struct FuzzTransaction {
    signature: [u8; SIGNATURE_BYTES],
    is_vote: bool,
    transaction: FuzzSanitizedTransaction,
    transaction_status_meta: FuzzTransactionStatusMeta,
    index: usize,
}

#[derive(Debug, Arbitrary)]
struct FuzzTransactionMessage {
    slot: u64,
    transaction: FuzzTransaction,
}

libfuzzer_sys::fuzz_target!(|fuzz_message: FuzzTransactionMessage| {
    let Ok(transaction) = fuzz_message.transaction.transaction.try_into() else {
        return;
    };

    let replica = ReplicaTransactionInfoV2 {
        signature: &Signature::from(fuzz_message.transaction.signature),
        is_vote: fuzz_message.transaction.is_vote,
        transaction: &transaction,
        transaction_status_meta: &fuzz_message.transaction.transaction_status_meta.into(),
        index: fuzz_message.transaction.index,
    };

    let message = ProtobufMessage::Transaction {
        slot: fuzz_message.slot,
        transaction: &replica,
    };
    let created_at = SystemTime::now();

    let vec_prost = message.encode_prost(created_at);
    let vec_raw = message.encode_raw(created_at);

    assert_eq!(
        vec_prost,
        vec_raw,
        "prost hex: {}",
        const_hex::encode(&vec_prost)
    );
});

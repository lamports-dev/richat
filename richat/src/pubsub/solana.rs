use {
    crate::{
        channel::ParsedMessage,
        pubsub::{filter::TransactionFilter, SubscriptionId},
    },
    arrayvec::ArrayVec,
    jsonrpsee_types::{
        ErrorCode, ErrorObject, ErrorObjectOwned, Id, Params, Request, Response, ResponsePayload,
        TwoPointZero,
    },
    richat_filter::{
        config::MAX_FILTERS,
        message::{MessageAccount, MessageBlock, MessageTransaction},
    },
    richat_proto::convert_from,
    serde::{de, Deserialize},
    serde_json::value::RawValue,
    solana_account::ReadableAccount,
    solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig},
    solana_rpc_client_api::{
        config::{
            RpcAccountInfoConfig, RpcBlockSubscribeConfig, RpcBlockSubscribeFilter,
            RpcProgramAccountsConfig, RpcSignatureSubscribeConfig, RpcTransactionLogsConfig,
            RpcTransactionLogsFilter,
        },
        filter::RpcFilterType,
    },
    solana_sdk::{
        commitment_config::{CommitmentConfig, CommitmentLevel},
        pubkey::Pubkey,
        signature::Signature,
        transaction::TransactionError,
    },
    solana_transaction_status::{BlockEncodingOptions, TransactionDetails, UiTransactionEncoding},
    spl_token_2022::{
        generic_token_account::GenericTokenAccount,
        instruction::TokenInstruction as SplToken2022Instruction,
        state::Account as SplToken2022Account,
    },
    std::{
        borrow::Cow,
        collections::{hash_map::DefaultHasher, HashSet},
        hash::{Hash, Hasher},
        str::FromStr,
    },
};

#[derive(Debug)]
pub struct SubscribeMessage {
    pub id: Id<'static>,
    pub config: SubscribeConfig,
}

impl SubscribeMessage {
    pub fn parse(
        message: &[u8],
        enable_block_subscription: bool,
        enable_transaction_subscription: bool,
        enable_token_owner_subscription: bool,
    ) -> Result<Option<Self>, Response<'static, ()>> {
        let call: Request = serde_json::from_slice(message).map_err(|_error| Response {
            jsonrpc: Some(TwoPointZero),
            payload: ResponsePayload::error(ErrorObjectOwned::from(ErrorCode::ParseError)),
            id: Id::Null,
        })?;

        let config = SubscribeConfig::parse(
            &call.method,
            call.params,
            enable_block_subscription,
            enable_transaction_subscription,
            enable_token_owner_subscription,
        )
        .map_err(|error| Response {
            jsonrpc: Some(TwoPointZero),
            payload: ResponsePayload::error(error),
            id: call.id.clone().into_owned(),
        })?;

        Ok(Some(Self {
            id: call.id.into_owned(),
            config,
        }))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscribeMethod {
    Account,
    Program,
    Logs,
    Signature,
    Slot,
    SlotsUpdates,
    Block,
    Root,
    Transaction,
    TokenOwner,
}

impl SubscribeMethod {
    pub const fn get_message_methods(message: &ParsedMessage) -> &[Self] {
        match message {
            ParsedMessage::Slot(_) => &[Self::Slot, Self::SlotsUpdates, Self::Root],
            ParsedMessage::Account(_) => &[Self::Account, Self::Program],
            ParsedMessage::Transaction(_) => &[
                Self::Logs,
                Self::Signature,
                Self::Transaction,
                Self::TokenOwner,
            ],
            ParsedMessage::Entry(_) => &[],
            ParsedMessage::BlockMeta(_) => &[],
            ParsedMessage::Block(_) => &[Self::Block],
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Account => "account",
            Self::Program => "program",
            Self::Logs => "logs",
            Self::Signature => "signature",
            Self::Slot => "slot",
            Self::SlotsUpdates => "slotsupdates",
            Self::Block => "block",
            Self::Root => "root",
            Self::Transaction => "transaction",
            Self::TokenOwner => "tokenowner",
        }
    }
}

pub type SubscribeConfigHashId = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BlockSubscriptionKind {
    All,
    MentionsAccountOrProgram(Pubkey),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LogsSubscriptionKind {
    All,
    AllWithVotes,
    Single(Pubkey),
}

#[derive(Debug, Hash)]
pub enum SubscribeConfig {
    Account {
        pubkey: Pubkey,
        encoding: UiAccountEncoding,
        data_slice: Option<UiDataSliceConfig>,
        commitment: CommitmentConfig,
    },
    Program {
        pubkey: Pubkey,
        filters: ArrayVec<RpcFilterType, MAX_FILTERS>,
        encoding: UiAccountEncoding,
        data_slice: Option<UiDataSliceConfig>,
        commitment: CommitmentConfig,
    },
    Logs {
        kind: LogsSubscriptionKind,
        commitment: CommitmentConfig,
    },
    Signature {
        signature: Signature,
        commitment: CommitmentConfig,
    },
    Slot,
    SlotsUpdates,
    Block {
        commitment: CommitmentConfig,
        encoding: UiTransactionEncoding,
        kind: BlockSubscriptionKind,
        transaction_details: TransactionDetails,
        show_rewards: bool,
        max_supported_transaction_version: Option<u8>,
    },
    Root,
    Transaction {
        filter: TransactionFilter,
        encoding: UiTransactionEncoding,
        transaction_details: TransactionDetails,
        show_rewards: bool,
        max_supported_transaction_version: Option<u8>,
        commitment: CommitmentConfig,
    },
    TokenOwner {
        pubkey: Pubkey,
        account_encoding: UiAccountEncoding,
        transaction_encoding: UiTransactionEncoding,
        transaction_details: TransactionDetails,
        transaction_show_rewards: bool,
        transaction_max_supported_transaction_version: Option<u8>,
        commitment: CommitmentConfig,
    },
    Unsubscribe {
        id: SubscriptionId,
    },
    GetVersion,
    GetVersionRichat,
}

impl SubscribeConfig {
    pub fn parse(
        method: &str,
        params: Option<Cow<'_, RawValue>>,
        enable_block_subscription: bool,
        enable_transaction_subscription: bool,
        enable_token_owner_subscription: bool,
    ) -> Result<Self, ErrorObjectOwned> {
        match method {
            "accountSubscribe" => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    pubkey: String,
                    #[serde(default)]
                    config: Option<RpcAccountInfoConfig>,
                }

                let ReqParams { pubkey, config } = parse_params(params)?;
                let RpcAccountInfoConfig {
                    encoding,
                    data_slice,
                    commitment,
                    min_context_slot: _, // ignored
                } = config.unwrap_or_default();
                Ok(SubscribeConfig::Account {
                    pubkey: param::<Pubkey>(&pubkey, "pubkey")?,
                    commitment: commitment.unwrap_or_default(),
                    data_slice,
                    encoding: encoding.unwrap_or(UiAccountEncoding::Binary),
                })
            }
            "programSubscribe" => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    pubkey: String,
                    #[serde(default)]
                    config: Option<RpcProgramAccountsConfig>,
                }

                let ReqParams { pubkey, config } = parse_params(params)?;
                let config = config.unwrap_or_default();
                Ok(SubscribeConfig::Program {
                    pubkey: param::<Pubkey>(&pubkey, "pubkey")?,
                    filters: param_filters(config.filters.unwrap_or_default())?,
                    encoding: config
                        .account_config
                        .encoding
                        .unwrap_or(UiAccountEncoding::Binary),
                    data_slice: config.account_config.data_slice,
                    commitment: config.account_config.commitment.unwrap_or_default(),
                })
            }
            "logsSubscribe" => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    filter: RpcTransactionLogsFilter,
                    #[serde(default)]
                    config: Option<RpcTransactionLogsConfig>,
                }

                let ReqParams { filter, config } = parse_params(params)?;
                Ok(SubscribeConfig::Logs {
                    kind: match filter {
                        RpcTransactionLogsFilter::All => LogsSubscriptionKind::All,
                        RpcTransactionLogsFilter::AllWithVotes => {
                            LogsSubscriptionKind::AllWithVotes
                        }
                        RpcTransactionLogsFilter::Mentions(keys) => {
                            if keys.len() != 1 {
                                return Err(invalid_params(
                                    "Invalid Request: Only 1 address supported",
                                ));
                            }
                            LogsSubscriptionKind::Single(param::<Pubkey>(&keys[0], "mentions")?)
                        }
                    },
                    commitment: config.and_then(|c| c.commitment).unwrap_or_default(),
                })
            }
            "signatureSubscribe" => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    signature: String,
                    #[serde(default)]
                    config: Option<RpcSignatureSubscribeConfig>,
                }

                let ReqParams { signature, config } = parse_params(params)?;
                let config = config.unwrap_or_default();
                Ok(SubscribeConfig::Signature {
                    signature: param::<Signature>(&signature, "signature")?,
                    commitment: config.commitment.unwrap_or_default(),
                })
            }
            "slotSubscribe" => {
                expect_no_params(params)?;
                Ok(SubscribeConfig::Slot)
            }
            "slotsUpdatesSubscribe" => {
                expect_no_params(params)?;
                Ok(SubscribeConfig::SlotsUpdates)
            }
            "blockSubscribe" => {
                if !enable_block_subscription {
                    return Err(ErrorCode::MethodNotFound.into());
                }

                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    filter: RpcBlockSubscribeFilter,
                    #[serde(default)]
                    config: Option<RpcBlockSubscribeConfig>,
                }

                let ReqParams { filter, config } = parse_params(params)?;
                let config = config.unwrap_or_default();
                let commitment = config.commitment.unwrap_or_default();
                check_is_at_least_confirmed(commitment)?;
                Ok(SubscribeConfig::Block {
                    commitment: config.commitment.unwrap_or_default(),
                    encoding: config.encoding.unwrap_or(UiTransactionEncoding::Base64),
                    kind: match filter {
                        RpcBlockSubscribeFilter::All => BlockSubscriptionKind::All,
                        RpcBlockSubscribeFilter::MentionsAccountOrProgram(key) => {
                            BlockSubscriptionKind::MentionsAccountOrProgram(param::<Pubkey>(
                                &key,
                                "mentions_account_or_program",
                            )?)
                        }
                    },
                    transaction_details: config.transaction_details.unwrap_or_default(),
                    show_rewards: config.show_rewards.unwrap_or_default(),
                    max_supported_transaction_version: config.max_supported_transaction_version,
                })
            }
            // "voteSubscribe" => {
            //     if !enable_vote_subscription {
            //         return Err(ErrorCode::MethodNotFound.into());
            //     }

            //     expect_no_params(params)?;
            //     Ok(SubscribeConfig::Vote)
            // }
            "rootSubscribe" => {
                expect_no_params(params)?;
                Ok(SubscribeConfig::Root)
            }
            "transactionSubscribe" => {
                if !enable_transaction_subscription {
                    return Err(ErrorCode::MethodNotFound.into());
                }

                #[derive(Debug, Default, Deserialize)]
                #[serde(default)]
                struct ReqTransactionSubscribeFilterAccounts {
                    include: Vec<String>,
                    exclude: Vec<String>,
                    required: Vec<String>,
                }

                #[derive(Debug, Default, Deserialize)]
                #[serde(deny_unknown_fields, default)]
                struct ReqTransactionSubscribeFilter {
                    vote: Option<bool>,
                    failed: Option<bool>,
                    signature: Option<String>,
                    accounts: ReqTransactionSubscribeFilterAccounts,
                }

                #[derive(Debug, Default, Deserialize)]
                #[serde(rename_all = "camelCase")]
                struct ReqTransactionSubscribeConfig {
                    #[serde(flatten)]
                    pub commitment: Option<CommitmentConfig>,
                    pub encoding: Option<UiTransactionEncoding>,
                    pub transaction_details: Option<TransactionDetails>,
                    pub show_rewards: Option<bool>,
                    pub max_supported_transaction_version: Option<u8>,
                }

                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    filter: ReqTransactionSubscribeFilter,
                    #[serde(default)]
                    config: Option<ReqTransactionSubscribeConfig>,
                }

                let ReqParams { filter, config } = parse_params(params)?;
                let config = config.unwrap_or_default();
                Ok(SubscribeConfig::Transaction {
                    filter: TransactionFilter {
                        vote: filter.vote,
                        failed: filter.failed,
                        signature: filter
                            .signature
                            .map(|signature| param::<Signature>(&signature, "signature"))
                            .transpose()?,
                        account_include: param_set_pubkey(&filter.accounts.include)?,
                        account_exclude: param_set_pubkey(&filter.accounts.exclude)?,
                        account_required: param_set_pubkey(&filter.accounts.required)?,
                    },
                    encoding: config.encoding.unwrap_or(UiTransactionEncoding::Base64),
                    transaction_details: config.transaction_details.unwrap_or_default(),
                    show_rewards: config.show_rewards.unwrap_or_default(),
                    max_supported_transaction_version: config.max_supported_transaction_version,
                    commitment: config.commitment.unwrap_or_default(),
                })
            }
            "tokenOwnerSubscribe" => {
                if !enable_token_owner_subscription {
                    return Err(ErrorCode::MethodNotFound.into());
                }

                #[derive(Debug, Default, Deserialize)]
                #[serde(rename_all = "camelCase")]
                struct ReqTokenOwnerSubscribeConfig {
                    pub account_encoding: Option<UiAccountEncoding>,
                    pub transaction_encoding: Option<UiTransactionEncoding>,
                    pub transaction_details: Option<TransactionDetails>,
                    pub transaction_show_rewards: Option<bool>,
                    pub transaction_max_supported_transaction_version: Option<u8>,
                    #[serde(flatten)]
                    pub commitment: Option<CommitmentConfig>,
                }

                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    pubkey: String,
                    #[serde(default)]
                    config: Option<ReqTokenOwnerSubscribeConfig>,
                }

                let ReqParams { pubkey, config } = parse_params(params)?;
                let config = config.unwrap_or_default();

                Ok(Self::TokenOwner {
                    pubkey: param::<Pubkey>(&pubkey, "pubkey")?,
                    account_encoding: config.account_encoding.unwrap_or(UiAccountEncoding::Binary),
                    transaction_encoding: config
                        .transaction_encoding
                        .unwrap_or(UiTransactionEncoding::Base64),
                    transaction_details: config.transaction_details.unwrap_or_default(),
                    transaction_show_rewards: config.transaction_show_rewards.unwrap_or_default(),
                    transaction_max_supported_transaction_version: config
                        .transaction_max_supported_transaction_version,
                    commitment: config.commitment.unwrap_or_default(),
                })
            }
            "accountUnsubscribe"
            | "programUnsubscribe"
            | "logsUnsubscribe"
            | "signatureUnsubscribe"
            | "slotUnsubscribe"
            | "slotsUpdatesUnsubscribe"
            | "blockUnsubscribe"
            | "voteUnsubscribe"
            | "rootUnsubscribe"
            | "transactionUnsubscribe"
            | "tokenOwnerUnsubscribe" => {
                if (method == "blockUnsubscribe" && !enable_block_subscription)
                    || (method == "transactionUnsubscribe" && !enable_transaction_subscription)
                    || (method == "tokenOwnerUnsubscribe" && !enable_token_owner_subscription)
                {
                    return Err(ErrorCode::MethodNotFound.into());
                }

                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    id: SubscriptionId,
                }

                let ReqParams { id } = parse_params(params)?;
                Ok(SubscribeConfig::Unsubscribe { id })
            }
            "getVersion" => Ok(SubscribeConfig::GetVersion),
            "getVersionRichat" => Ok(SubscribeConfig::GetVersionRichat),
            _ => Err(ErrorCode::MethodNotFound.into()),
        }
    }

    pub fn get_hash_id(&self) -> SubscribeConfigHashId {
        let mut state = DefaultHasher::new();
        <Self as Hash>::hash(self, &mut state);
        state.finish()
    }

    pub fn commitment(&self) -> CommitmentLevel {
        match self {
            Self::Account { commitment, .. } => commitment.commitment,
            Self::Program { commitment, .. } => commitment.commitment,
            Self::Logs { commitment, .. } => commitment.commitment,
            Self::Signature { commitment, .. } => commitment.commitment,
            Self::Slot => CommitmentLevel::Processed,
            Self::SlotsUpdates => CommitmentLevel::Processed,
            Self::Block { commitment, .. } => commitment.commitment,
            Self::Root => CommitmentLevel::Processed,
            Self::Transaction { commitment, .. } => commitment.commitment,
            Self::TokenOwner { commitment, .. } => commitment.commitment,
            Self::Unsubscribe { .. } => unreachable!(),
            Self::GetVersion => unreachable!(),
            Self::GetVersionRichat => unreachable!(),
        }
    }

    pub fn method(&self) -> SubscribeMethod {
        match self {
            Self::Account { .. } => SubscribeMethod::Account,
            Self::Program { .. } => SubscribeMethod::Program,
            Self::Logs { .. } => SubscribeMethod::Logs,
            Self::Signature { .. } => SubscribeMethod::Signature,
            Self::Slot => SubscribeMethod::Slot,
            Self::SlotsUpdates => SubscribeMethod::SlotsUpdates,
            Self::Block { .. } => SubscribeMethod::Block,
            Self::Root => SubscribeMethod::Root,
            Self::Transaction { .. } => SubscribeMethod::Transaction,
            Self::TokenOwner { .. } => SubscribeMethod::TokenOwner,
            Self::Unsubscribe { .. } => unreachable!(),
            Self::GetVersion => unreachable!(),
            Self::GetVersionRichat => unreachable!(),
        }
    }

    pub fn filter_account(
        &self,
        message: &MessageAccount,
    ) -> Option<(UiAccountEncoding, Option<UiDataSliceConfig>)> {
        match self {
            Self::Account {
                pubkey,
                encoding,
                data_slice,
                ..
            } if pubkey == message.pubkey() => Some((*encoding, *data_slice)),
            _ => None,
        }
    }

    pub fn filter_program(
        &self,
        message: &MessageAccount,
    ) -> Option<(UiAccountEncoding, Option<UiDataSliceConfig>)> {
        match self {
            Self::Program {
                pubkey,
                filters,
                encoding,
                data_slice,
                ..
            } if pubkey == message.owner()
                && filters.iter().all(|filter| match filter {
                    RpcFilterType::DataSize(size) => message.data().len() as u64 == *size,
                    RpcFilterType::Memcmp(memcmp) => memcmp.bytes_match(message.data()),
                    RpcFilterType::TokenAccountState => {
                        SplToken2022Account::valid_account_data(message.data())
                    }
                }) =>
            {
                Some((*encoding, *data_slice))
            }
            _ => None,
        }
    }

    pub fn filter_logs(
        &self,
        message: &MessageTransaction,
    ) -> Option<(Option<TransactionError>, Vec<String>)> {
        if let Self::Logs { kind, .. } = self {
            let filtered = match kind {
                LogsSubscriptionKind::All => !message.vote(),
                LogsSubscriptionKind::AllWithVotes => true,
                LogsSubscriptionKind::Single(pubkey) => {
                    message.account_keys().iter().any(|key| key == pubkey)
                }
            };
            if filtered {
                if let (Ok(error), Ok(tx_meta)) = (
                    convert_from::create_tx_error(message.error().as_ref()),
                    message.transaction_meta(),
                ) {
                    return Some((error, tx_meta.log_messages.clone()));
                }
            }
        }
        None
    }

    pub fn filter_signature(
        &self,
        message: &MessageTransaction,
    ) -> Option<Option<TransactionError>> {
        match self {
            Self::Signature { signature, .. } if signature == message.signature() => {
                convert_from::create_tx_error(message.error().as_ref()).ok()
            }
            _ => None,
        }
    }

    pub fn filter_block(
        &self,
        message: &MessageBlock,
    ) -> Option<(UiTransactionEncoding, BlockEncodingOptions)> {
        if let Self::Block {
            encoding,
            kind,
            transaction_details,
            show_rewards,
            max_supported_transaction_version,
            ..
        } = self
        {
            let filtered = match kind {
                BlockSubscriptionKind::All => true,
                BlockSubscriptionKind::MentionsAccountOrProgram(pubkey) => message
                    .transactions
                    .iter()
                    .any(|tx| tx.account_keys().contains(pubkey)),
            };
            if filtered {
                return Some((
                    *encoding,
                    BlockEncodingOptions {
                        transaction_details: *transaction_details,
                        show_rewards: *show_rewards,
                        max_supported_transaction_version: *max_supported_transaction_version,
                    },
                ));
            }
        }
        None
    }

    pub fn filter_transaction(
        &self,
        message: &MessageTransaction,
    ) -> Option<(UiTransactionEncoding, TransactionDetails, bool, Option<u8>)> {
        match self {
            Self::Transaction {
                filter,
                encoding,
                transaction_details,
                show_rewards,
                max_supported_transaction_version,
                ..
            } if filter.matches(message) => Some((
                *encoding,
                *transaction_details,
                *show_rewards,
                *max_supported_transaction_version,
            )),
            _ => None,
        }
    }

    pub fn filter_transaction_token_owner(
        &self,
        message: &MessageTransaction,
    ) -> Option<(UiTransactionEncoding, TransactionDetails, bool, Option<u8>)> {
        match self {
            Self::TokenOwner {
                pubkey,
                transaction_encoding,
                transaction_details,
                transaction_show_rewards,
                transaction_max_supported_transaction_version,
                ..
            } => {
                let mut selected = false;

                if let Ok(tx_with_meta) = message.as_versioned_transaction_with_status_meta() {
                    let account_keys = tx_with_meta.account_keys();

                    for inner_ixs in tx_with_meta
                        .meta
                        .inner_instructions
                        .as_deref()
                        .unwrap_or_default()
                    {
                        for ix in &inner_ixs.instructions {
                            let program_id =
                                account_keys.get(ix.instruction.program_id_index as usize);

                            if program_id == Some(&spl_token::ID)
                                || program_id == Some(&spl_token_2022::ID)
                            {
                                match SplToken2022Instruction::unpack(&ix.instruction.data) {
                                    Ok(SplToken2022Instruction::InitializeAccount)
                                        if ix
                                            .instruction
                                            .accounts
                                            .get(2)
                                            .and_then(|ix| account_keys.get(*ix as usize))
                                            == Some(pubkey) =>
                                    {
                                        selected = true;
                                    }
                                    Ok(SplToken2022Instruction::InitializeAccount2 { owner })
                                        if &owner == pubkey =>
                                    {
                                        selected = true;
                                    }
                                    Ok(SplToken2022Instruction::InitializeAccount3 { owner })
                                        if &owner == pubkey =>
                                    {
                                        selected = true;
                                    }
                                    Ok(SplToken2022Instruction::InitializeImmutableOwner)
                                        if ix
                                            .instruction
                                            .accounts
                                            .first()
                                            .and_then(|ix| account_keys.get(*ix as usize))
                                            == Some(pubkey) =>
                                    {
                                        selected = true;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }

                selected.then_some((
                    *transaction_encoding,
                    *transaction_details,
                    *transaction_show_rewards,
                    *transaction_max_supported_transaction_version,
                ))
            }
            _ => None,
        }
    }
}

fn check_is_at_least_confirmed(commitment: CommitmentConfig) -> Result<(), ErrorObjectOwned> {
    if !commitment.is_at_least_confirmed() {
        Err(invalid_params(
            "Method does not support commitment below `confirmed`",
        ))
    } else {
        Ok(())
    }
}

fn param<T: FromStr>(param_str: &str, thing: &str) -> Result<T, ErrorObjectOwned> {
    param_str
        .parse::<T>()
        .map_err(|_e| invalid_params(format!("Invalid Request: Invalid {thing} provided")))
}

fn param_set_pubkey(params: &[String]) -> Result<HashSet<Pubkey>, ErrorObjectOwned> {
    params
        .iter()
        .map(|value| param(value, "pubkey"))
        .collect::<Result<HashSet<_>, _>>()
}

fn param_filters(
    filters: Vec<RpcFilterType>,
) -> Result<ArrayVec<RpcFilterType, MAX_FILTERS>, ErrorObjectOwned> {
    if filters.len() > MAX_FILTERS {
        return Err(invalid_params(format!(
            "Too much filters provided; max: {MAX_FILTERS}"
        )));
    }

    let mut verified_filters = ArrayVec::new();
    for mut filter in filters {
        if let Err(error) = filter.verify() {
            return Err(invalid_params(error.to_string()));
        }
        if let RpcFilterType::Memcmp(memcmp) = &mut filter {
            if let Err(error) = memcmp.convert_to_raw_bytes() {
                return Err(invalid_params(format!(
                    "Invalid Request: failed to decode memcmp filter: {error}"
                )));
            }
        }
        verified_filters.push(filter);
    }
    Ok(verified_filters)
}

fn invalid_params(message: impl Into<String>) -> ErrorObjectOwned {
    ErrorObject::owned::<()>(ErrorCode::InvalidParams.code(), message, None)
}

fn parse_params<'a, T>(raw_params: Option<Cow<'a, RawValue>>) -> Result<T, ErrorObjectOwned>
where
    T: for<'de> de::Deserialize<'de>,
{
    let params = Params::new(raw_params.as_ref().map(|p| p.get()));
    params.parse()
}

fn expect_no_params(params: Option<Cow<'_, RawValue>>) -> Result<(), ErrorObjectOwned> {
    if let Some(params) = params {
        if params.get().trim() != "[]" {
            return Err(ErrorObject::owned(
                ErrorCode::InvalidParams.code(),
                "Invalid parameters: No parameters were expected",
                Some(format!("{params:?}")),
            ));
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use {
        super::parse_params, serde::Deserialize, serde_json::value::RawValue,
        solana_rpc_client_api::config::RpcAccountInfoConfig, std::borrow::Cow,
    };

    #[test]
    fn test_parse() {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            pubkey: String,
            #[serde(default)]
            config: Option<RpcAccountInfoConfig>,
        }

        let value = RawValue::from_string(r#"["pubkey"]"#.to_owned()).unwrap();
        let parsed: ReqParams = parse_params(Some(Cow::Borrowed(&value))).unwrap();
        assert_eq!(parsed.pubkey, "pubkey");
        assert_eq!(parsed.config, None);

        let value =
            RawValue::from_string(r#"["pubkey", {"minContextSlot":0}]"#.to_owned()).unwrap();
        let parsed: ReqParams = parse_params(Some(Cow::Borrowed(&value))).unwrap();
        assert_eq!(parsed.pubkey, "pubkey");
        assert_eq!(
            parsed.config,
            Some(RpcAccountInfoConfig {
                encoding: None,
                data_slice: None,
                commitment: None,
                min_context_slot: Some(0)
            })
        );
    }
}

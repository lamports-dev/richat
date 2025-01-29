use {
    crate::pubsub::filter::TransactionFilter,
    arrayvec::ArrayVec,
    jsonrpc_core::types::{
        Call as RpcCall, Error as RpcError, ErrorCode as RpcErrorCode, Failure as RpcFailure,
        Id as RpcId, Params as RpcParams, Version as RpcVersion,
    },
    richat_filter::config::MAX_FILTERS,
    serde::Deserialize,
    solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig},
    solana_rpc::rpc_subscription_tracker::{BlockSubscriptionKind, LogsSubscriptionKind},
    solana_rpc_client_api::{
        config::{
            RpcAccountInfoConfig, RpcBlockSubscribeConfig, RpcBlockSubscribeFilter,
            RpcProgramAccountsConfig, RpcSignatureSubscribeConfig, RpcTransactionLogsConfig,
            RpcTransactionLogsFilter,
        },
        filter::RpcFilterType,
    },
    solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature},
    solana_transaction_status::{TransactionDetails, UiTransactionEncoding},
    std::{collections::HashSet, str::FromStr},
};

pub type SubscriptionId = u64;

#[derive(Debug)]
pub struct SubscribeMessage {
    pub jsonrpc: Option<RpcVersion>,
    pub id: RpcId,
    pub config: SubscribeConfig,
}

impl SubscribeMessage {
    pub fn parse(
        message: &[u8],
        enable_block_subscription: bool,
        enable_vote_subscription: bool,
        enable_transaction_subscription: bool,
    ) -> Result<Option<Self>, RpcFailure> {
        let call: RpcCall = serde_json::from_slice(message).map_err(|_error| RpcFailure {
            jsonrpc: Some(RpcVersion::V2),
            error: RpcError::parse_error(),
            id: RpcId::Null,
        })?;

        let call = match call {
            RpcCall::MethodCall(call) => call,
            RpcCall::Notification(_notification) => return Ok(None),
            RpcCall::Invalid { id } => {
                return Err(RpcFailure {
                    jsonrpc: Some(RpcVersion::V2),
                    error: RpcError::invalid_request(),
                    id,
                })
            }
        };

        let config = SubscribeConfig::parse(
            &call.method,
            call.params,
            enable_block_subscription,
            enable_vote_subscription,
            enable_transaction_subscription,
        )
        .map_err(|error| RpcFailure {
            jsonrpc: call.jsonrpc,
            error,
            id: call.id.clone(),
        })?;

        Ok(Some(Self {
            jsonrpc: call.jsonrpc,
            id: call.id,
            config,
        }))
    }
}

#[derive(Debug)]
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
        with_context: bool,
    },
    Logs {
        kind: LogsSubscriptionKind,
        commitment: CommitmentConfig,
    },
    Signature {
        signature: Signature,
        commitment: CommitmentConfig,
        enable_received_notification: bool,
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
    Vote,
    Root,
    Transaction {
        filter: TransactionFilter,
        encoding: UiTransactionEncoding,
        transaction_details: TransactionDetails,
        show_rewards: bool,
        max_supported_transaction_version: Option<u8>,
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
        params: RpcParams,
        enable_block_subscription: bool,
        enable_vote_subscription: bool,
        enable_transaction_subscription: bool,
    ) -> Result<Self, RpcError> {
        match method {
            "accountSubscribe" => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    pubkey: String,
                    #[serde(default)]
                    config: Option<RpcAccountInfoConfig>,
                }

                let ReqParams { pubkey, config } = params.parse()?;
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

                let ReqParams { pubkey, config } = params.parse()?;
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
                    with_context: config.with_context.unwrap_or_default(),
                })
            }
            "logsSubscribe" => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    filter: RpcTransactionLogsFilter,
                    #[serde(default)]
                    config: Option<RpcTransactionLogsConfig>,
                }

                let ReqParams { filter, config } = params.parse()?;
                Ok(SubscribeConfig::Logs {
                    kind: match filter {
                        RpcTransactionLogsFilter::All => LogsSubscriptionKind::All,
                        RpcTransactionLogsFilter::AllWithVotes => {
                            LogsSubscriptionKind::AllWithVotes
                        }
                        RpcTransactionLogsFilter::Mentions(keys) => {
                            if keys.len() != 1 {
                                return Err(RpcError {
                                    code: RpcErrorCode::InvalidParams,
                                    message: "Invalid Request: Only 1 address supported".into(),
                                    data: None,
                                });
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

                let ReqParams { signature, config } = params.parse()?;
                let config = config.unwrap_or_default();
                Ok(SubscribeConfig::Signature {
                    signature: param::<Signature>(&signature, "signature")?,
                    commitment: config.commitment.unwrap_or_default(),
                    enable_received_notification: config
                        .enable_received_notification
                        .unwrap_or_default(),
                })
            }
            "slotSubscribe" => {
                params.expect_no_params()?;
                Ok(SubscribeConfig::Slot)
            }
            "slotsUpdatesSubscribe" => {
                params.expect_no_params()?;
                Ok(SubscribeConfig::SlotsUpdates)
            }
            "blockSubscribe" => {
                if !enable_block_subscription {
                    return Err(RpcError::method_not_found());
                }

                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    filter: RpcBlockSubscribeFilter,
                    #[serde(default)]
                    config: Option<RpcBlockSubscribeConfig>,
                }

                let ReqParams { filter, config } = params.parse()?;
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
            "voteSubscribe" => {
                if !enable_vote_subscription {
                    return Err(RpcError::method_not_found());
                }

                params.expect_no_params()?;
                Ok(SubscribeConfig::Vote)
            }
            "rootSubscribe" => {
                params.expect_no_params()?;
                Ok(SubscribeConfig::Root)
            }
            "transactionSubscribe" => {
                if !enable_transaction_subscription {
                    return Err(RpcError::method_not_found());
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

                let ReqParams { filter, config } = params.parse()?;
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
            "accountUnsubscribe"
            | "programUnsubscribe"
            | "logsUnsubscribe"
            | "signatureUnsubscribe"
            | "slotUnsubscribe"
            | "slotsUpdatesUnsubscribe"
            | "blockUnsubscribe"
            | "voteUnsubscribe"
            | "rootUnsubscribe"
            | "transactionUnsubscribe" => {
                if (method == "blockUnsubscribe" && !enable_block_subscription)
                    || (method == "voteUnsubscribe" && !enable_vote_subscription)
                    || (method == "transactionUnsubscribe" && !enable_transaction_subscription)
                {
                    return Err(RpcError::method_not_found());
                }

                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    id: SubscriptionId,
                }

                let ReqParams { id } = params.parse()?;
                Ok(SubscribeConfig::Unsubscribe { id })
            }
            "getVersion" => Ok(SubscribeConfig::GetVersion),
            "getVersionRichat" => Ok(SubscribeConfig::GetVersionRichat),
            _ => Err(RpcError::method_not_found()),
        }
    }
}

fn check_is_at_least_confirmed(commitment: CommitmentConfig) -> Result<(), RpcError> {
    if !commitment.is_at_least_confirmed() {
        return Err(RpcError::invalid_params(
            "Method does not support commitment below `confirmed`",
        ));
    }
    Ok(())
}

fn param<T: FromStr>(param_str: &str, thing: &str) -> Result<T, RpcError> {
    param_str.parse::<T>().map_err(|_e| RpcError {
        code: RpcErrorCode::InvalidParams,
        message: format!("Invalid Request: Invalid {thing} provided"),
        data: None,
    })
}

fn param_set_pubkey(params: &[String]) -> Result<HashSet<Pubkey>, RpcError> {
    params
        .iter()
        .map(|value| param(value, "pubkey"))
        .collect::<Result<HashSet<_>, _>>()
}

fn param_filters(
    filters: Vec<RpcFilterType>,
) -> Result<ArrayVec<RpcFilterType, MAX_FILTERS>, RpcError> {
    if filters.len() > MAX_FILTERS {
        return Err(RpcError {
            code: RpcErrorCode::InvalidParams,
            message: format!("Too much filters provided; max: {MAX_FILTERS}"),
            data: None,
        });
    }

    let mut verified_filters = ArrayVec::new();
    for mut filter in filters {
        if let Err(error) = filter.verify() {
            return Err(RpcError {
                code: RpcErrorCode::InvalidParams,
                message: error.to_string(),
                data: None,
            });
        }
        if let RpcFilterType::Memcmp(memcmp) = &mut filter {
            if let Err(error) = memcmp.convert_to_raw_bytes() {
                return Err(RpcError {
                    code: RpcErrorCode::InvalidParams,
                    message: format!("Invalid Request: failed to decode memcmp filter: {error}"),
                    data: None,
                });
            }
        }
        verified_filters.push(filter);
    }
    Ok(verified_filters)
}

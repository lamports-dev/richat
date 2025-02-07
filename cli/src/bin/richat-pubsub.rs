use {
    anyhow::Context,
    clap::{Parser, Subcommand, ValueEnum},
    futures::{sink::SinkExt, stream::StreamExt},
    indicatif::{ProgressBar, ProgressStyle},
    jsonrpsee_types::{Response, ResponsePayload},
    richat_shared::version::GrpcVersionInfoExtra,
    serde::Deserialize,
    serde_json::{json, Value},
    solana_account_decoder::{UiAccountEncoding, UiDataSliceConfig},
    solana_client::nonblocking::pubsub_client::PubsubClient,
    solana_rpc_client_api::{
        config::{
            RpcAccountInfoConfig, RpcBlockSubscribeConfig, RpcBlockSubscribeFilter,
            RpcProgramAccountsConfig, RpcSignatureSubscribeConfig, RpcTransactionLogsConfig,
            RpcTransactionLogsFilter,
        },
        filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
        response::RpcVersionInfo,
    },
    solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature},
    solana_transaction_status::{TransactionDetails, UiTransactionEncoding},
    std::{
        collections::HashMap,
        env, fmt,
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
    },
    tokio::{
        net::TcpStream,
        signal::unix::{signal, SignalKind},
    },
    tokio_tungstenite::{
        connect_async,
        tungstenite::protocol::{
            frame::{coding::CloseCode, Utf8Bytes},
            CloseFrame, Message,
        },
        MaybeTlsStream, WebSocketStream,
    },
    tracing::info,
};

#[derive(Debug, Parser)]
#[clap(author, version, about = "Richat PubSub Tool")]
struct Args {
    /// Richat Geyser plugin Quic Server endpoint
    #[clap(default_value_t = String::from("ws://127.0.0.1:8000"))]
    endpoint: String,

    #[command(subcommand)]
    action: ArgsAction,
}

#[derive(Debug, Clone, Subcommand)]
enum ArgsAction {
    /// Subscribe on updates
    Subscribe {
        /// Type of subscription
        #[command(subcommand)]
        action: ArgsActionSubscribe,
        /// Commitment level of subscritpion
        #[clap(short, long, default_value_t = ArgsActionSubscribeCommitment::default())]
        commitment: ArgsActionSubscribeCommitment,
        /// Show only progress bar with received messages
        #[clap(long, default_value_t = false)]
        stats: bool,
    },
    /// Get node version
    GetVersion,
    /// Get Richat PubSub version
    GetVersionRichat,
}

#[derive(Debug, Clone, Subcommand)]
enum ArgsActionSubscribe {
    /// Subscribe on account updates
    Account {
        /// Account key
        #[clap(short, long)]
        pubkey: String,
        /// Encoding format
        #[clap(long, short)]
        encoding: Option<ArgsUiAccountEncoding>,
        /// Apply slice to data in updated account, format: `offset,length`
        #[clap(long, short)]
        data_slice: Option<String>,
    },
    /// Subscribe on accounts updates owned by program
    Program {
        /// Program account key
        #[clap(short, long)]
        pubkey: String,
        /// Filter by data size
        #[clap(long)]
        filter_data_size: Vec<u64>,
        /// Filter by memcmp, format: `offset,data in base58`
        #[clap(long)]
        filter_memcmp: Vec<String>,
        /// Encoding format
        #[clap(long, short)]
        encoding: Option<ArgsUiAccountEncoding>,
        /// Apply slice to data in updated accounts, format: `offset,length`
        #[clap(long, short)]
        data_slice: Option<String>,
    },
    /// Subscribe on transactions log updates
    Logs {
        /// All transactions
        #[clap(long, default_value_t = false)]
        all: bool,
        /// All transactions with votes
        #[clap(long, default_value_t = false)]
        all_with_votes: bool,
        /// Only transactions with mentions
        #[clap(long)]
        mentions: Vec<String>,
    },
    /// Subscribe on transaction confirmation events
    Signature {
        /// Transaction signature
        #[clap(short, long)]
        signature: String,
    },
    /// Subscribe on processed slot updates
    Slot,
    /// Subscribe on slots updates
    SlotsUpdates,
    /// Subscribe on block updates
    Block {
        /// Program account key
        #[clap(short, long)]
        pubkey: Option<String>,
        /// Encoding format
        #[clap(long, short)]
        encoding: Option<ArgsUiTransactionEncoding>,
        /// Transaction details
        #[clap(long, short)]
        transaction_details: Option<ArgsTransactionDetails>,
        /// Show rewards
        #[clap(long, short)]
        show_rewards: Option<bool>,
        /// Maximum supported transaction version
        #[clap(long, short)]
        max_supported_transaction_version: Option<u8>,
    },
    /// Subscribe on finalized slot updates
    Root,
    /// Subscribe on transaction updates
    Transaction {
        /// Include vote transactions
        #[clap(long)]
        vote: Option<bool>,
        /// Include failed transactions
        #[clap(long)]
        failed: Option<bool>,
        /// Filter transactions by signature
        #[clap(long)]
        signature: Option<String>,
        /// Transaction should include any of these accounts
        #[clap(long)]
        account_include: Vec<String>,
        /// Transaction should not contain any of these accounts
        #[clap(long)]
        account_exclude: Vec<String>,
        /// Transaction should contain all these accounts
        #[clap(long)]
        account_required: Vec<String>,
        /// Encoding format
        #[clap(long, short)]
        encoding: Option<ArgsUiTransactionEncoding>,
        /// Transaction details
        #[clap(long, short)]
        transaction_details: Option<ArgsTransactionDetails>,
        /// Show rewards
        #[clap(long, short)]
        show_rewards: Option<bool>,
        /// Maximum supported transaction version
        #[clap(long, short)]
        max_supported_transaction_version: Option<u8>,
    },
}

impl ArgsActionSubscribe {
    fn parse_data_slice(data_slice: &Option<String>) -> anyhow::Result<Option<UiDataSliceConfig>> {
        Ok(if let Some(data_slice) = data_slice {
            match data_slice.split_once(',') {
                Some((offset, length)) => match (offset.parse(), length.parse()) {
                    (Ok(offset), Ok(length)) => Some(UiDataSliceConfig { offset, length }),
                    _ => anyhow::bail!("invalid data_slice: {data_slice}"),
                },
                _ => anyhow::bail!("invalid data_slice: {data_slice}"),
            }
        } else {
            None
        })
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ArgsUiAccountEncoding {
    Binary,
    Base58,
    Base64,
    JsonParsed,
    Base64Zstd,
}

impl From<ArgsUiAccountEncoding> for UiAccountEncoding {
    fn from(encoding: ArgsUiAccountEncoding) -> Self {
        match encoding {
            ArgsUiAccountEncoding::Binary => Self::Binary,
            ArgsUiAccountEncoding::Base58 => Self::Base58,
            ArgsUiAccountEncoding::Base64 => Self::Base64,
            ArgsUiAccountEncoding::JsonParsed => Self::JsonParsed,
            ArgsUiAccountEncoding::Base64Zstd => Self::Base64Zstd,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ArgsUiTransactionEncoding {
    Binary,
    Base64,
    Base58,
    Json,
    JsonParsed,
}

impl From<ArgsUiTransactionEncoding> for UiTransactionEncoding {
    fn from(encoding: ArgsUiTransactionEncoding) -> Self {
        match encoding {
            ArgsUiTransactionEncoding::Binary => Self::Binary,
            ArgsUiTransactionEncoding::Base64 => Self::Base64,
            ArgsUiTransactionEncoding::Base58 => Self::Base58,
            ArgsUiTransactionEncoding::Json => Self::Json,
            ArgsUiTransactionEncoding::JsonParsed => Self::JsonParsed,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ArgsTransactionDetails {
    Full,
    Signatures,
    None,
    Accounts,
}

impl From<ArgsTransactionDetails> for TransactionDetails {
    fn from(encoding: ArgsTransactionDetails) -> Self {
        match encoding {
            ArgsTransactionDetails::Full => TransactionDetails::Full,
            ArgsTransactionDetails::Signatures => TransactionDetails::Signatures,
            ArgsTransactionDetails::None => TransactionDetails::None,
            ArgsTransactionDetails::Accounts => TransactionDetails::Accounts,
        }
    }
}

#[derive(Clone, Copy, Default, ValueEnum)]
enum ArgsActionSubscribeCommitment {
    Processed,
    Confirmed,
    #[default]
    Finalized,
}

impl fmt::Debug for ArgsActionSubscribeCommitment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Processed => write!(f, "processed"),
            Self::Confirmed => write!(f, "confirmed"),
            Self::Finalized => write!(f, "finalized"),
        }
    }
}

impl fmt::Display for ArgsActionSubscribeCommitment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl From<ArgsActionSubscribeCommitment> for CommitmentConfig {
    fn from(commitment: ArgsActionSubscribeCommitment) -> Self {
        match commitment {
            ArgsActionSubscribeCommitment::Processed => Self::processed(),
            ArgsActionSubscribeCommitment::Confirmed => Self::confirmed(),
            ArgsActionSubscribeCommitment::Finalized => Self::finalized(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum SubscribeResponse {
    Subscribe(usize),
    GetVersion(RpcVersionInfo),
    GetVersionRichat(RichatVersionInfo),
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
struct RichatVersionInfo {
    version: HashMap<String, String>,
    extra: GrpcVersionInfoExtra,
}

async fn subscribe_plain(
    endpoint: &str,
    method: &str,
    params: Value,
) -> anyhow::Result<(
    SubscribeResponse,
    WebSocketStream<MaybeTlsStream<TcpStream>>,
)> {
    let (mut stream, _response) = connect_async(endpoint).await?;
    stream
        .send(Message::Binary(
            serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }))?
            .into(),
        ))
        .await?;

    match stream.next().await {
        Some(Ok(Message::Text(data))) => {
            let response: Response<SubscribeResponse> = serde_json::from_str(&data)?;
            let payload = match response.payload {
                ResponsePayload::Success(data) => data.into_owned(),
                ResponsePayload::Error(error) => anyhow::bail!("subscribe error: {error:?}"),
            };
            Ok((payload, stream))
        }
        Some(Ok(_)) => anyhow::bail!("invalid message type"),
        Some(Err(error)) => anyhow::bail!(error),
        None => anyhow::bail!("no messages"),
    }
}

const fn create_close_frame() -> CloseFrame {
    CloseFrame {
        code: CloseCode::Normal,
        reason: Utf8Bytes::from_static(""),
    }
}

async fn create_shutdown() -> anyhow::Result<()> {
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = sigint.recv() => {},
        _ = sigterm.recv() => {}
    };
    Ok(())
}

async fn main2() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();

    let client = PubsubClient::new(&args.endpoint).await?;
    match args.action {
        ArgsAction::Subscribe {
            action,
            commitment,
            stats,
        } => {
            let pb = stats
                .then(|| {
                    let pb = ProgressBar::new(u64::MAX);
                    pb.set_style(ProgressStyle::with_template(
                        "{spinner:.green} +{pos} messages",
                    )?);
                    Ok::<_, anyhow::Error>(pb)
                })
                .transpose()?;
            let on_new_item = |f: &dyn Fn()| {
                if let Some(pb) = &pb {
                    pb.inc(1)
                } else {
                    f();
                }
            };

            match action {
                ArgsActionSubscribe::Account {
                    pubkey,
                    encoding,
                    data_slice,
                } => {
                    let pubkey = Pubkey::from_str(&pubkey)
                        .with_context(|| format!("invalid pubkey: {pubkey}"))?;

                    let (mut stream, _unsubscribe) = client
                        .account_subscribe(
                            &pubkey,
                            Some(RpcAccountInfoConfig {
                                encoding: encoding.map(Into::into),
                                data_slice: ArgsActionSubscribe::parse_data_slice(&data_slice)?,
                                commitment: Some(commitment.into()),
                                min_context_slot: None,
                            }),
                        )
                        .await?;

                    tokio::select! {
                        result = create_shutdown() => result?,
                        () = async {
                            while let Some(item) = stream.next().await {
                                on_new_item(&|| info!("account, new item: {item:?}"));
                            }
                        } => {},
                    }
                }
                ArgsActionSubscribe::Program {
                    pubkey,
                    filter_data_size,
                    filter_memcmp,
                    encoding,
                    data_slice,
                } => {
                    let pubkey = Pubkey::from_str(&pubkey)
                        .with_context(|| format!("invalid pubkey: {pubkey}"))?;

                    let mut filters = vec![];
                    for data_size in filter_data_size {
                        filters.push(RpcFilterType::DataSize(data_size));
                    }
                    for memcmp in filter_memcmp {
                        match memcmp.split_once(',') {
                            Some((offset, data)) => {
                                filters.push(RpcFilterType::Memcmp(Memcmp::new(
                                    offset.parse().with_context(|| {
                                        format!("invalid offset in memcmp: {offset}")
                                    })?,
                                    MemcmpEncodedBytes::Base58(data.to_owned()),
                                )))
                            }
                            _ => anyhow::bail!("invalid memcmp: {memcmp}"),
                        }
                    }

                    let (mut stream, _unsubscribe) = client
                        .program_subscribe(
                            &pubkey,
                            Some(RpcProgramAccountsConfig {
                                filters: Some(filters),
                                account_config: RpcAccountInfoConfig {
                                    encoding: encoding.map(|e| e.into()),
                                    data_slice: ArgsActionSubscribe::parse_data_slice(&data_slice)?,
                                    commitment: Some(commitment.into()),
                                    min_context_slot: None,
                                },
                                with_context: None,
                                sort_results: None,
                            }),
                        )
                        .await?;

                    tokio::select! {
                        result = create_shutdown() => result?,
                        () = async {
                            while let Some(item) = stream.next().await {
                                on_new_item(&|| info!("program, new item: {item:?}"));
                            }
                        } => {},
                    }
                }
                ArgsActionSubscribe::Logs {
                    all,
                    all_with_votes,
                    mentions,
                } => {
                    let filter = match (all, all_with_votes, !mentions.is_empty()) {
                        (true, false, false) => RpcTransactionLogsFilter::All,
                        (false, true, false) => RpcTransactionLogsFilter::AllWithVotes,
                        (false, false, true) => RpcTransactionLogsFilter::Mentions(mentions),
                        _ => anyhow::bail!(
                            "conflicts between `all`, `all-with-votes` and `mentions`"
                        ),
                    };

                    let (mut stream, _unsubscribe) = client
                        .logs_subscribe(
                            filter,
                            RpcTransactionLogsConfig {
                                commitment: Some(commitment.into()),
                            },
                        )
                        .await?;

                    tokio::select! {
                        result = create_shutdown() => result?,
                        () = async {
                            while let Some(item) = stream.next().await {
                                on_new_item(&|| info!("logs, new item: {item:?}"));
                            }
                        } => {},
                    }
                }
                ArgsActionSubscribe::Signature { signature } => {
                    let signature = Signature::from_str(&signature)
                        .with_context(|| format!("invalid signature: {signature}"))?;

                    let (mut stream, _unsubscribe) = client
                        .signature_subscribe(
                            &signature,
                            Some(RpcSignatureSubscribeConfig {
                                commitment: Some(commitment.into()),
                                enable_received_notification: None,
                            }),
                        )
                        .await?;

                    tokio::select! {
                        result = create_shutdown() => result?,
                        () = async {
                            while let Some(item) = stream.next().await {
                                on_new_item(&|| info!("signature, new item: {item:?}"));
                            }
                        } => {},
                    }
                }
                ArgsActionSubscribe::Slot => {
                    let (mut stream, _unsubscribe) = client.slot_subscribe().await?;

                    tokio::select! {
                        result = create_shutdown() => result?,
                        () = async {
                            while let Some(item) = stream.next().await {
                                on_new_item(&|| info!("slot, new item: {item:?}"));
                            }
                        } => {},
                    }
                }
                ArgsActionSubscribe::SlotsUpdates => {
                    let (mut stream, _unsubscribe) = client.slot_updates_subscribe().await?;

                    tokio::select! {
                        result = create_shutdown() => result?,
                        () = async {
                            while let Some(item) = stream.next().await {
                                on_new_item(&|| info!("slot update, new item: {item:?}"));
                            }
                        } => {},
                    }
                }
                ArgsActionSubscribe::Block {
                    pubkey,
                    encoding,
                    transaction_details,
                    show_rewards,
                    max_supported_transaction_version,
                } => {
                    let filter = if let Some(pubkey) = pubkey {
                        RpcBlockSubscribeFilter::MentionsAccountOrProgram(
                            Pubkey::from_str(&pubkey)
                                .with_context(|| format!("invalid pubkey: {pubkey}"))?
                                .to_string(),
                        )
                    } else {
                        RpcBlockSubscribeFilter::All
                    };

                    let (mut stream, _unsubscribe) = client
                        .block_subscribe(
                            filter,
                            Some(RpcBlockSubscribeConfig {
                                commitment: Some(commitment.into()),
                                encoding: encoding.map(Into::into),
                                transaction_details: transaction_details.map(Into::into),
                                show_rewards,
                                max_supported_transaction_version,
                            }),
                        )
                        .await?;

                    tokio::select! {
                        result = create_shutdown() => result?,
                        () = async {
                            while let Some(item) = stream.next().await {
                                on_new_item(&|| info!("block, new item: {item:?}"));
                            }
                        } => {},
                    }
                }
                ArgsActionSubscribe::Root => {
                    let (mut stream, _unsubscribe) = client.root_subscribe().await?;

                    tokio::select! {
                        result = create_shutdown() => result?,
                        () = async {
                            while let Some(item) = stream.next().await {
                                on_new_item(&|| info!("root, new item: {item:?}"));
                            }
                        } => {},
                    }
                }
                ArgsActionSubscribe::Transaction {
                    vote,
                    failed,
                    signature,
                    account_include,
                    account_exclude,
                    account_required,
                    encoding,
                    transaction_details,
                    show_rewards,
                    max_supported_transaction_version,
                } => {
                    let (SubscribeResponse::Subscribe(_id), mut stream) = subscribe_plain(
                        &args.endpoint,
                        "transactionSubscribe",
                        json!([{
                            "vote": vote,
                            "failed": failed,
                            "signature": signature,
                            "accounts": {
                                "include": account_include,
                                "exclude": account_exclude,
                                "required": account_required,
                            }
                        }, {
                            "commitment": CommitmentConfig::from(commitment),
                            "encoding": encoding.map(UiTransactionEncoding::from),
                            "transactionDetails": transaction_details.map(TransactionDetails::from),
                            "showRewards": show_rewards,
                            "maxSupportedTransactionVersion": max_supported_transaction_version,
                        }]),
                    )
                    .await?
                    else {
                        anyhow::bail!("invalid response");
                    };
                    while let Some(item) = stream.next().await {
                        on_new_item(&|| info!("transaction, new item: {item:?}"));
                    }
                    stream.close(Some(create_close_frame())).await?;
                }
            }
        }
        ArgsAction::GetVersion => {
            let (SubscribeResponse::GetVersion(version), mut stream) =
                subscribe_plain(&args.endpoint, "getVersion", json!([])).await?
            else {
                anyhow::bail!("invalid response");
            };
            info!(
                "solana_core: {}, feature_set: {:?}",
                version.solana_core, version.feature_set
            );
            stream.close(Some(create_close_frame())).await?;
        }
        ArgsAction::GetVersionRichat => {
            let (SubscribeResponse::GetVersionRichat(version), mut stream) =
                subscribe_plain(&args.endpoint, "getVersionRichat", json!([])).await?
            else {
                anyhow::bail!("invalid response");
            };
            info!("richat version: {version:?}");
            stream.close(Some(create_close_frame())).await?;
        }
    }
    client.shutdown().await?;

    Ok(())
}

fn main() -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name_fn(move || {
            static ATOMIC_ID: AtomicU64 = AtomicU64::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
            format!("richatPubSub{id:02}")
        })
        .enable_all()
        .build()?
        .block_on(main2())
}

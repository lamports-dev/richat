use {
    anyhow::Context,
    clap::{Parser, Subcommand},
    futures::stream::{BoxStream, StreamExt, TryStreamExt},
    indicatif::{MultiProgress, ProgressBar, ProgressStyle},
    prost::Message,
    richat_client::{
        error::ReceiveError,
        grpc::GrpcClient,
        quic::{QuicClient, QuicClientBuilder},
        tcp::TcpClient,
    },
    richat_shared::transports::{
        grpc::ConfigGrpcServer, quic::ConfigQuicServer, tcp::ConfigTcpServer,
    },
    serde_json::{json, Value},
    solana_sdk::{clock::Slot, hash::Hash, pubkey::Pubkey, signature::Signature},
    solana_transaction_status::UiTransactionEncoding,
    std::{
        env,
        net::SocketAddr,
        path::PathBuf,
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::fs,
    tonic::{
        service::Interceptor,
        transport::{channel::ClientTlsConfig, Certificate},
    },
    tracing::{error, info},
    yellowstone_grpc_proto::{
        convert_from,
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest, SubscribeUpdate,
            SubscribeUpdateAccountInfo, SubscribeUpdateEntry, SubscribeUpdateTransactionInfo,
        },
    },
};

type SubscribeStream = BoxStream<'static, Result<SubscribeUpdate, ReceiveError>>;

#[derive(Debug, Parser)]
#[clap(author, version, about = "Richat Cli Tool")]
struct Args {
    #[command(subcommand)]
    action: ArgsAppSelect,

    /// Subscribe on stream from slot
    #[clap(long)]
    replay_from_slot: Option<Slot>,

    /// Show total stat instead of messages
    #[clap(long, default_value_t = false)]
    stats: bool,
}

#[derive(Debug, Subcommand)]
enum ArgsAppSelect {
    /// Stream data directly from the plugin
    Stream(ArgsAppStream),
}

#[derive(Debug, clap::Args)]
struct ArgsAppStream {
    #[command(subcommand)]
    action: ArgsAppStreamSelect,
}

impl ArgsAppStream {
    async fn subscribe(self, replay_from_slot: Option<Slot>) -> anyhow::Result<SubscribeStream> {
        match self.action {
            ArgsAppStreamSelect::Quic(args) => args.subscribe(replay_from_slot).await,
            ArgsAppStreamSelect::Tcp(args) => args.subscribe(replay_from_slot).await,
            ArgsAppStreamSelect::Grpc(args) => args.subscribe(replay_from_slot).await,
        }
    }
}

#[derive(Debug, Subcommand)]
enum ArgsAppStreamSelect {
    /// Stream over Quic
    Quic(ArgsAppStreamQuic),
    /// Stream over Tcp
    Tcp(ArgsAppStreamTcp),
    /// Stream over gRPC
    Grpc(ArgsAppStreamGrpc),
}

#[derive(Debug, clap::Args)]
struct ArgsAppStreamQuic {
    /// Richat Geyser plugin Quic Server endpoint
    #[clap(default_value_t = ConfigQuicServer::default_endpoint().to_string())]
    endpoint: String,

    #[clap(long, default_value_t = QuicClientBuilder::default().local_addr)]
    local_addr: SocketAddr,

    #[clap(long, default_value_t = QuicClientBuilder::default().expected_rtt)]
    expected_rtt: u32,

    #[clap(long, default_value_t = QuicClientBuilder::default().max_stream_bandwidth)]
    max_stream_bandwidth: u32,

    #[clap(long, default_value_t = QuicClientBuilder::default().max_idle_timeout.unwrap())]
    max_idle_timeout: u32,

    #[clap(long)]
    server_name: Option<String>,

    #[clap(long, default_value_t = QuicClientBuilder::default().recv_streams)]
    recv_streams: u32,

    #[clap(long)]
    max_backlog: Option<u32>,

    #[clap(long)]
    insecure: bool,

    #[clap(long)]
    cert: Option<PathBuf>,
}

impl ArgsAppStreamQuic {
    async fn subscribe(self, replay_from_slot: Option<Slot>) -> anyhow::Result<SubscribeStream> {
        let builder = QuicClient::builder()
            .set_local_addr(Some(self.local_addr))
            .set_expected_rtt(self.expected_rtt)
            .set_max_stream_bandwidth(self.max_stream_bandwidth)
            .set_max_idle_timeout(Some(self.max_idle_timeout))
            .set_server_name(self.server_name.clone())
            .set_recv_streams(self.recv_streams)
            .set_max_backlog(self.max_backlog);

        let client = if self.insecure {
            builder.insecure().connect(self.endpoint.clone()).await
        } else {
            builder
                .secure(self.cert)
                .connect(self.endpoint.clone())
                .await
        }
        .context("failed to connect")?;
        info!("connected to {} over Quic", self.endpoint);

        let stream = client
            .subscribe(replay_from_slot)
            .await
            .context("failed to subscribe")?;
        info!("subscribed");

        Ok(stream.into_parsed().boxed())
    }
}

#[derive(Debug, clap::Args)]
struct ArgsAppStreamTcp {
    /// Richat Geyser plugin Tcp Server endpoint
    #[clap(default_value_t = ConfigTcpServer::default().endpoint.to_string())]
    endpoint: String,
}

impl ArgsAppStreamTcp {
    async fn subscribe(self, replay_from_slot: Option<Slot>) -> anyhow::Result<SubscribeStream> {
        let client = TcpClient::build()
            .connect(&self.endpoint)
            .await
            .context("failed to connect")?;
        info!("connected to {} over Tcp", self.endpoint);

        let stream = client
            .subscribe(replay_from_slot)
            .await
            .context("failed to subscribe")?;
        info!("subscribed");

        Ok(stream.into_parsed().boxed())
    }
}

#[derive(Debug, clap::Args)]
struct ArgsAppStreamGrpc {
    /// Richat Geyser plugin gRPC Server endpoint
    #[clap(default_value_t = format!("http://{}", ConfigGrpcServer::default().endpoint))]
    endpoint: String,

    /// Path of a certificate authority file
    #[clap(long)]
    ca_certificate: Option<PathBuf>,

    /// Apply a timeout to connecting to the uri.
    #[clap(long)]
    connect_timeout_ms: Option<u64>,

    /// Sets the tower service default internal buffer size, default is 1024
    #[clap(long)]
    buffer_size: Option<usize>,

    /// Sets whether to use an adaptive flow control. Uses hyper’s default otherwise.
    #[clap(long)]
    http2_adaptive_window: Option<bool>,

    /// Set http2 KEEP_ALIVE_TIMEOUT. Uses hyper’s default otherwise.
    #[clap(long)]
    http2_keep_alive_interval_ms: Option<u64>,

    /// Sets the max connection-level flow control for HTTP2, default is 65,535
    #[clap(long)]
    initial_connection_window_size: Option<u32>,

    ///Sets the SETTINGS_INITIAL_WINDOW_SIZE option for HTTP2 stream-level flow control, default is 65,535
    #[clap(long)]
    initial_stream_window_size: Option<u32>,

    ///Set http2 KEEP_ALIVE_TIMEOUT. Uses hyper’s default otherwise.
    #[clap(long)]
    keep_alive_timeout_ms: Option<u64>,

    /// Set http2 KEEP_ALIVE_WHILE_IDLE. Uses hyper’s default otherwise.
    #[clap(long)]
    keep_alive_while_idle: Option<bool>,

    /// Set whether TCP keepalive messages are enabled on accepted connections.
    #[clap(long)]
    tcp_keepalive_ms: Option<u64>,

    /// Set the value of TCP_NODELAY option for accepted connections. Enabled by default.
    #[clap(long)]
    tcp_nodelay: Option<bool>,

    /// Apply a timeout to each request.
    #[clap(long)]
    timeout_ms: Option<u64>,

    /// Max message size before decoding, full blocks can be super large, default is 1GiB
    #[clap(long, default_value_t = 1024 * 1024 * 1024)]
    max_decoding_message_size: usize,

    #[clap(long)]
    x_token: Option<String>,
}

impl ArgsAppStreamGrpc {
    async fn connect(self) -> anyhow::Result<GrpcClient<impl Interceptor>> {
        let mut tls_config = ClientTlsConfig::new().with_native_roots();
        if let Some(path) = &self.ca_certificate {
            let bytes = fs::read(path).await?;
            tls_config = tls_config.ca_certificate(Certificate::from_pem(bytes));
        }
        let mut builder = GrpcClient::build_from_shared(self.endpoint)?
            .x_token(self.x_token)?
            .tls_config(tls_config)?
            .max_decoding_message_size(self.max_decoding_message_size);

        if let Some(duration) = self.connect_timeout_ms {
            builder = builder.connect_timeout(Duration::from_millis(duration));
        }
        if let Some(sz) = self.buffer_size {
            builder = builder.buffer_size(sz);
        }
        if let Some(enabled) = self.http2_adaptive_window {
            builder = builder.http2_adaptive_window(enabled);
        }
        if let Some(duration) = self.http2_keep_alive_interval_ms {
            builder = builder.http2_keep_alive_interval(Duration::from_millis(duration));
        }
        if let Some(sz) = self.initial_connection_window_size {
            builder = builder.initial_connection_window_size(sz);
        }
        if let Some(sz) = self.initial_stream_window_size {
            builder = builder.initial_stream_window_size(sz);
        }
        if let Some(duration) = self.keep_alive_timeout_ms {
            builder = builder.keep_alive_timeout(Duration::from_millis(duration));
        }
        if let Some(enabled) = self.keep_alive_while_idle {
            builder = builder.keep_alive_while_idle(enabled);
        }
        if let Some(duration) = self.tcp_keepalive_ms {
            builder = builder.tcp_keepalive(Some(Duration::from_millis(duration)));
        }
        if let Some(enabled) = self.tcp_nodelay {
            builder = builder.tcp_nodelay(enabled);
        }
        if let Some(duration) = self.timeout_ms {
            builder = builder.timeout(Duration::from_millis(duration));
        }

        builder.connect().await.map_err(Into::into)
    }

    async fn subscribe(self, replay_from_slot: Option<Slot>) -> anyhow::Result<SubscribeStream> {
        let endpoint = self.endpoint.clone();
        let mut client = self.connect().await.context("failed to connect")?;
        info!("connected to {endpoint} over gRPC");

        let stream = client
            .subscribe_once(SubscribeRequest {
                from_slot: replay_from_slot,
                ..Default::default()
            })
            .await
            .context("failed to subscribe")?;
        info!("subscribed");

        Ok(stream.into_parsed().map_err(Into::into).boxed())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProgressBarTpl {
    Msg(&'static str),
    Total,
}

fn crate_progress_bar(
    pb: &MultiProgress,
    pb_t: ProgressBarTpl,
) -> Result<ProgressBar, indicatif::style::TemplateError> {
    let pb = pb.add(ProgressBar::no_length());
    let tpl = match pb_t {
        ProgressBarTpl::Msg(kind) => {
            format!("{{spinner}} {kind}: {{msg}} / ~{{bytes}} (~{{bytes_per_sec}})")
        }
        ProgressBarTpl::Total => {
            "{spinner} total: {msg} / ~{bytes} (~{bytes_per_sec}) in {elapsed_precise}".to_owned()
        }
    };
    pb.set_style(ProgressStyle::with_template(&tpl)?);
    Ok(pb)
}

fn format_thousands(value: u64) -> String {
    value
        .to_string()
        .as_bytes()
        .rchunks(3)
        .rev()
        .map(std::str::from_utf8)
        .collect::<Result<Vec<&str>, _>>()
        .expect("invalid number")
        .join(",")
}

fn create_pretty_account(account: SubscribeUpdateAccountInfo) -> anyhow::Result<Value> {
    Ok(json!({
        "pubkey": Pubkey::try_from(account.pubkey).map_err(|_| anyhow::anyhow!("invalid account pubkey"))?.to_string(),
        "lamports": account.lamports,
        "owner": Pubkey::try_from(account.owner).map_err(|_| anyhow::anyhow!("invalid account owner"))?.to_string(),
        "executable": account.executable,
        "rentEpoch": account.rent_epoch,
        "data": const_hex::encode(account.data),
        "writeVersion": account.write_version,
        "txnSignature": account.txn_signature.map(|sig| bs58::encode(sig).into_string()),
    }))
}

fn create_pretty_transaction(tx: SubscribeUpdateTransactionInfo) -> anyhow::Result<Value> {
    Ok(json!({
        "signature": Signature::try_from(tx.signature.as_slice()).context("invalid signature")?.to_string(),
        "isVote": tx.is_vote,
        "tx": convert_from::create_tx_with_meta(tx)
            .map_err(|error| anyhow::anyhow!(error))
            .context("invalid tx with meta")?
            .encode(UiTransactionEncoding::Base64, Some(u8::MAX), true)
            .context("failed to encode transaction")?,
    }))
}

fn create_pretty_entry(msg: SubscribeUpdateEntry) -> anyhow::Result<Value> {
    Ok(json!({
        "slot": msg.slot,
        "index": msg.index,
        "numHashes": msg.num_hashes,
        "hash": Hash::new_from_array(<[u8; 32]>::try_from(msg.hash.as_slice()).context("invalid entry hash")?).to_string(),
        "executedTransactionCount": msg.executed_transaction_count,
        "startingTransactionIndex": msg.starting_transaction_index,
    }))
}

fn print_update(kind: &str, created_at: SystemTime, filters: &[String], value: Value) {
    let unix_since = created_at
        .duration_since(UNIX_EPOCH)
        .expect("valid system time");
    info!(
        "{kind} ({}) at {}.{:0>6}: {}",
        filters.join(","),
        unix_since.as_secs(),
        unix_since.subsec_micros(),
        serde_json::to_string(&value).expect("json serialization failed")
    );
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();
    let replay_from_slot = args.replay_from_slot;
    let Some(mut stream) = match args.action {
        ArgsAppSelect::Stream(args) => args.subscribe(replay_from_slot).await.map(Some),
    }?
    else {
        return Ok(());
    };

    let pb_multi = MultiProgress::new();
    let mut pb_accounts_c = 0;
    let pb_accounts = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("accounts"))?;
    let mut pb_slots_c = 0;
    let pb_slots = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("slots"))?;
    let mut pb_txs_c = 0;
    let pb_txs = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("transactions"))?;
    let mut pb_txs_st_c = 0;
    let pb_txs_st = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("transactions statuses"))?;
    let mut pb_entries_c = 0;
    let pb_entries = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("entries"))?;
    let mut pb_blocks_mt_c = 0;
    let pb_blocks_mt = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("blocks meta"))?;
    let mut pb_blocks_c = 0;
    let pb_blocks = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("blocks"))?;
    let mut pb_pp_c = 0;
    let pb_pp = crate_progress_bar(&pb_multi, ProgressBarTpl::Msg("ping/pong"))?;
    let mut pb_total_c = 0;
    let pb_total = crate_progress_bar(&pb_multi, ProgressBarTpl::Total)?;

    while let Some(message) = stream.next().await {
        match message {
            Ok(msg) => {
                if args.stats {
                    let encoded_len = msg.encoded_len() as u64;
                    let (pb_c, pb) = match msg.update_oneof {
                        Some(UpdateOneof::Account(_)) => (&mut pb_accounts_c, &pb_accounts),
                        Some(UpdateOneof::Slot(_)) => (&mut pb_slots_c, &pb_slots),
                        Some(UpdateOneof::Transaction(_)) => (&mut pb_txs_c, &pb_txs),
                        Some(UpdateOneof::TransactionStatus(_)) => (&mut pb_txs_st_c, &pb_txs_st),
                        Some(UpdateOneof::Entry(_)) => (&mut pb_entries_c, &pb_entries),
                        Some(UpdateOneof::BlockMeta(_)) => (&mut pb_blocks_mt_c, &pb_blocks_mt),
                        Some(UpdateOneof::Block(_)) => (&mut pb_blocks_c, &pb_blocks),
                        Some(UpdateOneof::Ping(_)) => (&mut pb_pp_c, &pb_pp),
                        Some(UpdateOneof::Pong(_)) => (&mut pb_pp_c, &pb_pp),
                        None => {
                            pb_multi.println("update not found in the message")?;
                            break;
                        }
                    };
                    *pb_c += 1;
                    pb.set_message(format_thousands(*pb_c));
                    pb.inc(encoded_len);
                    pb_total_c += 1;
                    pb_total.set_message(format_thousands(pb_total_c));
                    pb_total.inc(encoded_len);

                    continue;
                }

                let filters = msg.filters;
                let created_at: SystemTime = msg
                    .created_at
                    .ok_or(anyhow::anyhow!("no created_at in the message"))?
                    .try_into()
                    .context("failed to parse created_at")?;
                match msg.update_oneof {
                    Some(UpdateOneof::Account(msg)) => {
                        let account = msg
                            .account
                            .ok_or(anyhow::anyhow!("no account in the message"))?;
                        let mut value = create_pretty_account(account)?;
                        value["isStartup"] = json!(msg.is_startup);
                        value["slot"] = json!(msg.slot);
                        print_update("account", created_at, &filters, value);
                    }
                    Some(UpdateOneof::Slot(msg)) => {
                        let status = CommitmentLevel::try_from(msg.status)
                            .context("failed to decode commitment")?;
                        print_update(
                            "slot",
                            created_at,
                            &filters,
                            json!({
                                "slot": msg.slot,
                                "parent": msg.parent,
                                "status": status.as_str_name(),
                                "deadError": msg.dead_error,
                            }),
                        );
                    }
                    Some(UpdateOneof::Transaction(msg)) => {
                        let tx = msg
                            .transaction
                            .ok_or(anyhow::anyhow!("no transaction in the message"))?;
                        let mut value = create_pretty_transaction(tx)?;
                        value["slot"] = json!(msg.slot);
                        print_update("transaction", created_at, &filters, value);
                    }
                    Some(UpdateOneof::TransactionStatus(msg)) => {
                        print_update(
                            "transactionStatus",
                            created_at,
                            &filters,
                            json!({
                                "slot": msg.slot,
                                "signature": Signature::try_from(msg.signature.as_slice()).context("invalid signature")?.to_string(),
                                "isVote": msg.is_vote,
                                "index": msg.index,
                                "err": convert_from::create_tx_error(msg.err.as_ref())
                                    .map_err(|error| anyhow::anyhow!(error))
                                    .context("invalid error")?,
                            }),
                        );
                    }
                    Some(UpdateOneof::Entry(msg)) => {
                        print_update("entry", created_at, &filters, create_pretty_entry(msg)?);
                    }
                    Some(UpdateOneof::BlockMeta(msg)) => {
                        print_update(
                            "blockmeta",
                            created_at,
                            &filters,
                            json!({
                                "slot": msg.slot,
                                "blockhash": msg.blockhash,
                                "rewards": if let Some(rewards) = msg.rewards {
                                    Some(convert_from::create_rewards_obj(rewards).map_err(|error| anyhow::anyhow!(error))?)
                                } else {
                                    None
                                },
                                "blockTime": msg.block_time.map(|obj| obj.timestamp),
                                "blockHeight": msg.block_height.map(|obj| obj.block_height),
                                "parentSlot": msg.parent_slot,
                                "parentBlockhash": msg.parent_blockhash,
                                "executedTransactionCount": msg.executed_transaction_count,
                                "entriesCount": msg.entries_count,
                            }),
                        );
                    }
                    Some(UpdateOneof::Block(msg)) => {
                        print_update(
                            "block",
                            created_at,
                            &filters,
                            json!({
                                "slot": msg.slot,
                                "blockhash": msg.blockhash,
                                "rewards": if let Some(rewards) = msg.rewards {
                                    Some(convert_from::create_rewards_obj(rewards).map_err(|error| anyhow::anyhow!(error))?)
                                } else {
                                    None
                                },
                                "blockTime": msg.block_time.map(|obj| obj.timestamp),
                                "blockHeight": msg.block_height.map(|obj| obj.block_height),
                                "parentSlot": msg.parent_slot,
                                "parentBlockhash": msg.parent_blockhash,
                                "executedTransactionCount": msg.executed_transaction_count,
                                "transactions": msg.transactions.into_iter().map(create_pretty_transaction).collect::<Result<Value, _>>()?,
                                "updatedAccountCount": msg.updated_account_count,
                                "accounts": msg.accounts.into_iter().map(create_pretty_account).collect::<Result<Value, _>>()?,
                                "entriesCount": msg.entries_count,
                                "entries": msg.entries.into_iter().map(create_pretty_entry).collect::<Result<Value, _>>()?,
                            }),
                        );
                    }
                    Some(UpdateOneof::Ping(_)) => {}
                    Some(UpdateOneof::Pong(_)) => {}
                    None => {
                        error!("update not found in the message");
                        break;
                    }
                }
            }
            Err(error) => {
                error!("error: {error:?}");
                break;
            }
        }
    }
    info!("stream closed");
    Ok(())
}

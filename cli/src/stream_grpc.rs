use {
    crate::stream::handle_stream,
    clap::{Args, Subcommand, ValueEnum},
    futures::stream::{StreamExt, TryStreamExt},
    indicatif::MultiProgress,
    prost::Message,
    richat_client::grpc::GrpcClient,
    richat_proto::geyser::{
        subscribe_request_filter_accounts_filter::Filter as AccountsFilterOneof,
        subscribe_request_filter_accounts_filter_lamports::Cmp as AccountsFilterLamports,
        subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
        CommitmentLevel, SubscribeRequest, SubscribeRequestAccountsDataSlice,
        SubscribeRequestFilterAccounts, SubscribeRequestFilterAccountsFilter,
        SubscribeRequestFilterAccountsFilterLamports, SubscribeRequestFilterAccountsFilterMemcmp,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
        SubscribeRequestFilterTransactions, SubscribeRequestPing, SubscribeUpdate,
    },
    std::{collections::HashMap, fs::File, path::PathBuf, sync::Arc, time::Duration},
    tonic::service::Interceptor,
    tracing::info,
};

type SlotsFilterMap = HashMap<String, SubscribeRequestFilterSlots>;
type AccountFilterMap = HashMap<String, SubscribeRequestFilterAccounts>;
type TransactionsFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;
type TransactionsStatusFilterMap = HashMap<String, SubscribeRequestFilterTransactions>;
type EntryFilterMap = HashMap<String, SubscribeRequestFilterEntry>;
type BlocksFilterMap = HashMap<String, SubscribeRequestFilterBlocks>;
type BlocksMetaFilterMap = HashMap<String, SubscribeRequestFilterBlocksMeta>;

#[derive(Debug, Args)]
pub struct ArgsAppStreamGrpc {
    #[clap(short, long, default_value_t = String::from("http://127.0.0.1:10000"))]
    /// Service endpoint
    endpoint: String,

    /// Path of a certificate authority file
    #[clap(long)]
    ca_certificate: Option<PathBuf>,

    #[clap(long)]
    x_token: Option<String>,

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

    /// Max message size before decoding
    #[clap(long, default_value_t = 64 * 1024 * 1024)]
    max_decoding_message_size: usize,

    /// Commitment level: processed, confirmed or finalized
    #[clap(long)]
    commitment: Option<ArgsCommitment>,

    #[command(subcommand)]
    action: Action,
}

impl ArgsAppStreamGrpc {
    fn get_commitment(&self) -> Option<CommitmentLevel> {
        Some(self.commitment.unwrap_or_default().into())
    }

    async fn connect(self) -> anyhow::Result<(GrpcClient<impl Interceptor>, Action)> {
        let mut builder = GrpcClient::build_from_shared(self.endpoint)?
            .x_token(self.x_token.map(|xt| xt.into_bytes()))?
            .tls_config_native_roots(self.ca_certificate.as_ref())
            .await?
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

        let client = builder.connect().await?;
        Ok((client, self.action))
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let commitment = self.get_commitment();
        let (mut client, action) = self.connect().await?;
        info!("connected");

        match action {
            Action::Subscribe(action_subscribe) => {
                let (request, stats, verify_encoding) = action_subscribe
                    .get_subscribe_request(commitment)
                    .await?
                    .expect("subscribe action");

                return geyser_subscribe(client, request, stats, verify_encoding).await;
            }
            Action::Ping { count } => client
                .ping(count)
                .await
                .map(|response| info!("response: {response:?}")),
            Action::GetLatestBlockhash => client
                .get_latest_blockhash(commitment)
                .await
                .map(|response| info!("response: {response:?}")),
            Action::GetBlockHeight => client
                .get_block_height(commitment)
                .await
                .map(|response| info!("response: {response:?}")),
            Action::GetSlot => client
                .get_slot(commitment)
                .await
                .map(|response| info!("response: {response:?}")),
            Action::IsBlockhashValid { blockhash } => client
                .is_blockhash_valid(blockhash, commitment)
                .await
                .map(|response| info!("response: {response:?}")),
            Action::GetVersion => client
                .get_version()
                .await
                .map(|response| info!("response: {response:?}")),
        }
        .map_err(Into::into)
    }
}

#[derive(Debug, Clone, Copy, Default, ValueEnum)]
enum ArgsCommitment {
    #[default]
    Processed,
    Confirmed,
    Finalized,
}

impl From<ArgsCommitment> for CommitmentLevel {
    fn from(commitment: ArgsCommitment) -> Self {
        match commitment {
            ArgsCommitment::Processed => CommitmentLevel::Processed,
            ArgsCommitment::Confirmed => CommitmentLevel::Confirmed,
            ArgsCommitment::Finalized => CommitmentLevel::Finalized,
        }
    }
}

#[derive(Debug, Clone, Subcommand)]
enum Action {
    Subscribe(Box<ActionSubscribe>),
    Ping {
        #[clap(long, short, default_value_t = 0)]
        count: i32,
    },
    GetLatestBlockhash,
    GetBlockHeight,
    GetSlot,
    IsBlockhashValid {
        #[clap(long, short)]
        blockhash: String,
    },
    GetVersion,
}

#[derive(Debug, Clone, clap::Args)]
struct ActionSubscribe {
    /// Subscribe on accounts updates
    #[clap(long)]
    accounts: bool,

    /// Filter by presence of field txn_signature
    accounts_nonempty_txn_signature: Option<bool>,

    /// Filter by Account Pubkey
    #[clap(long)]
    accounts_account: Vec<String>,

    /// Path to a JSON array of account addresses
    #[clap(long)]
    accounts_account_path: Option<String>,

    /// Filter by Owner Pubkey
    #[clap(long)]
    accounts_owner: Vec<String>,

    /// Filter by Offset and Data, format: `offset,data in base58`
    #[clap(long)]
    accounts_memcmp: Vec<String>,

    /// Filter by Data size
    #[clap(long)]
    accounts_datasize: Option<u64>,

    /// Filter valid token accounts
    #[clap(long)]
    accounts_token_account_state: bool,

    /// Filter by lamports, format: `eq:42` / `ne:42` / `lt:42` / `gt:42`
    #[clap(long)]
    accounts_lamports: Vec<String>,

    /// Receive only part of updated data account, format: `offset,size`
    #[clap(long)]
    accounts_data_slice: Vec<String>,

    /// Subscribe on slots updates
    #[clap(long)]
    slots: bool,

    /// Filter slots by commitment
    #[clap(long)]
    slots_filter_by_commitment: bool,

    /// Subscribe on interslot slot updates
    #[clap(long)]
    slots_interslot_updates: bool,

    /// Subscribe on transactions updates
    #[clap(long)]
    transactions: bool,

    /// Filter vote transactions
    #[clap(long)]
    transactions_vote: Option<bool>,

    /// Filter failed transactions
    #[clap(long)]
    transactions_failed: Option<bool>,

    /// Filter by transaction signature
    #[clap(long)]
    transactions_signature: Option<String>,

    /// Filter included account in transactions
    #[clap(long)]
    transactions_account_include: Vec<String>,

    /// Filter excluded account in transactions
    #[clap(long)]
    transactions_account_exclude: Vec<String>,

    /// Filter required account in transactions
    #[clap(long)]
    transactions_account_required: Vec<String>,

    /// Subscribe on transactions_status updates
    #[clap(long)]
    transactions_status: bool,

    /// Filter vote transactions for transactions_status
    #[clap(long)]
    transactions_status_vote: Option<bool>,

    /// Filter failed transactions for transactions_status
    #[clap(long)]
    transactions_status_failed: Option<bool>,

    /// Filter by transaction signature for transactions_status
    #[clap(long)]
    transactions_status_signature: Option<String>,

    /// Filter included account in transactions for transactions_status
    #[clap(long)]
    transactions_status_account_include: Vec<String>,

    /// Filter excluded account in transactions for transactions_status
    #[clap(long)]
    transactions_status_account_exclude: Vec<String>,

    /// Filter required account in transactions for transactions_status
    #[clap(long)]
    transactions_status_account_required: Vec<String>,

    #[clap(long)]
    entries: bool,

    /// Subscribe on block updates
    #[clap(long)]
    blocks: bool,

    /// Filter included account in transactions
    #[clap(long)]
    blocks_account_include: Vec<String>,

    /// Include transactions to block message
    #[clap(long)]
    blocks_include_transactions: Option<bool>,

    /// Include accounts to block message
    #[clap(long)]
    blocks_include_accounts: Option<bool>,

    /// Include entries to block message
    #[clap(long)]
    blocks_include_entries: Option<bool>,

    /// Subscribe on block meta updates (without transactions)
    #[clap(long)]
    blocks_meta: bool,

    /// Re-send message from slot
    #[clap(long)]
    from_slot: Option<u64>,

    /// Send ping in subscribe request
    #[clap(long)]
    ping: Option<i32>,

    /// Show total stat instead of messages
    #[clap(long, default_value_t = false)]
    stats: bool,

    /// Verify manually implemented encoding against prost
    #[clap(long, default_value_t = false)]
    verify_encoding: bool,
}

impl ActionSubscribe {
    async fn get_subscribe_request(
        self,
        commitment: Option<CommitmentLevel>,
    ) -> anyhow::Result<Option<(SubscribeRequest, bool, bool)>> {
        let mut accounts: AccountFilterMap = HashMap::new();
        if self.accounts {
            let mut accounts_account = self.accounts_account;
            if let Some(path) = self.accounts_account_path {
                let accounts = tokio::task::block_in_place(move || {
                    let file = File::open(path)?;
                    Ok::<Vec<String>, anyhow::Error>(serde_json::from_reader(file)?)
                })?;
                accounts_account.extend(accounts);
            }

            let mut filters = vec![];
            for filter in self.accounts_memcmp.iter() {
                match filter.split_once(',') {
                    Some((offset, data)) => {
                        filters.push(SubscribeRequestFilterAccountsFilter {
                            filter: Some(AccountsFilterOneof::Memcmp(
                                SubscribeRequestFilterAccountsFilterMemcmp {
                                    offset: offset
                                        .parse()
                                        .map_err(|_| anyhow::anyhow!("invalid offset"))?,
                                    data: Some(AccountsFilterMemcmpOneof::Base58(
                                        data.trim().to_string(),
                                    )),
                                },
                            )),
                        });
                    }
                    _ => anyhow::bail!("invalid memcmp"),
                }
            }
            if let Some(datasize) = self.accounts_datasize {
                filters.push(SubscribeRequestFilterAccountsFilter {
                    filter: Some(AccountsFilterOneof::Datasize(datasize)),
                });
            }
            if self.accounts_token_account_state {
                filters.push(SubscribeRequestFilterAccountsFilter {
                    filter: Some(AccountsFilterOneof::TokenAccountState(true)),
                });
            }
            for filter in self.accounts_lamports.iter() {
                match filter.split_once(':') {
                    Some((cmp, value)) => {
                        let Ok(value) = value.parse() else {
                            anyhow::bail!("invalid lamports value: {value}");
                        };
                        filters.push(SubscribeRequestFilterAccountsFilter {
                            filter: Some(AccountsFilterOneof::Lamports(
                                SubscribeRequestFilterAccountsFilterLamports {
                                    cmp: Some(match cmp {
                                        "eq" => AccountsFilterLamports::Eq(value),
                                        "ne" => AccountsFilterLamports::Ne(value),
                                        "lt" => AccountsFilterLamports::Lt(value),
                                        "gt" => AccountsFilterLamports::Gt(value),
                                        _ => {
                                            anyhow::bail!("invalid lamports filter: {cmp}")
                                        }
                                    }),
                                },
                            )),
                        });
                    }
                    _ => anyhow::bail!("invalid lamports"),
                }
            }

            accounts.insert(
                "client".to_owned(),
                SubscribeRequestFilterAccounts {
                    nonempty_txn_signature: self.accounts_nonempty_txn_signature,
                    account: accounts_account,
                    owner: self.accounts_owner,
                    filters,
                },
            );
        }

        let mut slots: SlotsFilterMap = HashMap::new();
        if self.slots {
            slots.insert(
                "client".to_owned(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: Some(self.slots_filter_by_commitment),
                    interslot_updates: Some(self.slots_interslot_updates),
                },
            );
        }

        let mut transactions: TransactionsFilterMap = HashMap::new();
        if self.transactions {
            transactions.insert(
                "client".to_string(),
                SubscribeRequestFilterTransactions {
                    vote: self.transactions_vote,
                    failed: self.transactions_failed,
                    signature: self.transactions_signature,
                    account_include: self.transactions_account_include,
                    account_exclude: self.transactions_account_exclude,
                    account_required: self.transactions_account_required,
                },
            );
        }

        let mut transactions_status: TransactionsStatusFilterMap = HashMap::new();
        if self.transactions_status {
            transactions_status.insert(
                "client".to_string(),
                SubscribeRequestFilterTransactions {
                    vote: self.transactions_status_vote,
                    failed: self.transactions_status_failed,
                    signature: self.transactions_status_signature,
                    account_include: self.transactions_status_account_include,
                    account_exclude: self.transactions_status_account_exclude,
                    account_required: self.transactions_status_account_required,
                },
            );
        }

        let mut entries: EntryFilterMap = HashMap::new();
        if self.entries {
            entries.insert("client".to_owned(), SubscribeRequestFilterEntry {});
        }

        let mut blocks: BlocksFilterMap = HashMap::new();
        if self.blocks {
            blocks.insert(
                "client".to_owned(),
                SubscribeRequestFilterBlocks {
                    account_include: self.blocks_account_include,
                    include_transactions: self.blocks_include_transactions,
                    include_accounts: self.blocks_include_accounts,
                    include_entries: self.blocks_include_entries,
                },
            );
        }

        let mut blocks_meta: BlocksMetaFilterMap = HashMap::new();
        if self.blocks_meta {
            blocks_meta.insert("client".to_owned(), SubscribeRequestFilterBlocksMeta {});
        }

        let mut accounts_data_slice = Vec::new();
        for data_slice in self.accounts_data_slice.iter() {
            match data_slice.split_once(',') {
                Some((offset, length)) => match (offset.parse(), length.parse()) {
                    (Ok(offset), Ok(length)) => {
                        accounts_data_slice
                            .push(SubscribeRequestAccountsDataSlice { offset, length });
                    }
                    _ => anyhow::bail!("invalid data_slice"),
                },
                _ => anyhow::bail!("invalid data_slice"),
            }
        }

        let ping = self.ping.map(|id| SubscribeRequestPing { id });

        Ok(Some((
            SubscribeRequest {
                slots,
                accounts,
                transactions,
                transactions_status,
                entry: entries,
                blocks,
                blocks_meta,
                commitment: commitment.map(|x| x as i32),
                accounts_data_slice,
                ping,
                from_slot: self.from_slot,
            },
            self.stats,
            self.verify_encoding,
        )))
    }
}

async fn geyser_subscribe(
    mut client: GrpcClient<impl Interceptor>,
    request: SubscribeRequest,
    stats: bool,
    verify_encoding: bool,
) -> anyhow::Result<()> {
    let pb_multi = Arc::new(MultiProgress::new());

    let pb_multi_stream = Arc::clone(&pb_multi);
    let stream = client
        .subscribe_dragons_mouth_once(request)
        .await?
        .and_then(move |vec| {
            let pb_multi_stream = Arc::clone(&pb_multi_stream);
            async move {
                let msg = SubscribeUpdate::decode(vec.as_slice())?;
                if verify_encoding && vec != msg.encode_to_vec() {
                    pb_multi_stream
                        .println(format!(
                            "encoding doesn't match: {}",
                            const_hex::encode(&vec)
                        ))
                        .unwrap();
                }
                Ok(msg)
            }
        })
        .boxed();

    handle_stream(stream, pb_multi, stats).await
}

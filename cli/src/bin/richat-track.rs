use {
    clap::Parser,
    futures::{
        future::{try_join_all, TryFutureExt},
        stream::{BoxStream, StreamExt},
    },
    maplit::hashmap,
    richat_client::{
        grpc::{ConfigGrpcClient, GrpcClient},
        quic::ConfigQuicClient,
        tcp::ConfigTcpClient,
    },
    richat_proto::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
            SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions, SubscribeUpdateBlockMeta, SubscribeUpdateSlot,
            SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
        },
        richat::GrpcSubscribeRequest,
    },
    serde::Deserialize,
    solana_sdk::clock::Slot,
    std::{
        collections::{BTreeMap, HashMap},
        env,
        sync::Arc,
        time::{Duration, SystemTime},
    },
    tokio::{fs, sync::Mutex, time::sleep},
    tracing::{error, info, warn},
};

#[derive(Debug, Parser)]
#[clap(author, version, about = "Richat Cli Events Tracker")]
struct Args {
    /// Path to config
    #[clap(short, long, default_value_t = String::from("config.json"))]
    config: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    sources: HashMap<String, ConfigSource>,
    tracks: Vec<ConfigTrack>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, tag = "source")]
enum ConfigSource {
    #[serde(rename = "richat-plugin-agave")]
    RichatPluginAgave(ConfigSourceRichatPluginAgave),
    #[serde(rename = "richat-grpc")]
    RichatGrpc(ConfigYellowstoneGrpc),
    #[serde(rename = "yellowstone-grpc")]
    YellowstoneGrpc(ConfigYellowstoneGrpc),
}

impl ConfigSource {
    async fn subscribe(
        self,
        storage: Arc<Mutex<TrackStorage>>,
        tracks: Vec<ConfigTrack>,
        name: String,
    ) -> anyhow::Result<()> {
        let mut stream = match self {
            Self::RichatPluginAgave(config) => config.subscribe().await,
            Self::RichatGrpc(config) => config.subscribe().await,
            Self::YellowstoneGrpc(config) => config.subscribe().await,
        }?;

        loop {
            let update = stream
                .next()
                .await
                .ok_or(anyhow::anyhow!("stream finished"))??;

            let ts = SystemTime::now();

            let mut storage = storage.lock().await;

            for track in tracks.iter() {
                if let Some(slot) = track.matches(&update) {
                    storage.add(slot, track, name.clone(), ts);
                }
            }

            if let UpdateOneof::Slot(SubscribeUpdateSlot { slot, status, .. }) = update {
                if status == CommitmentLevel::Finalized as i32 {
                    storage.clear(slot);
                }
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, tag = "transport")]
enum ConfigSourceRichatPluginAgave {
    #[serde(rename = "quic")]
    Quic(ConfigQuicClient),
    #[serde(rename = "tcp")]
    Tcp(ConfigTcpClient),
    #[serde(rename = "grpc")]
    Grpc(ConfigGrpcClient),
}

impl ConfigSourceRichatPluginAgave {
    async fn subscribe(self) -> anyhow::Result<BoxStream<'static, anyhow::Result<UpdateOneof>>> {
        let stream = match self {
            Self::Quic(config) => {
                let stream = config.connect().await?.subscribe(None, None).await?;
                stream.into_parsed()
            }
            Self::Tcp(config) => {
                let stream = config.connect().await?.subscribe(None, None).await?;
                stream.into_parsed()
            }
            Self::Grpc(config) => {
                let request = GrpcSubscribeRequest {
                    replay_from_slot: None,
                    filter: None,
                };

                let stream = config.connect().await?.subscribe_richat(request).await?;
                stream.into_parsed()
            }
        }
        .map(|message| {
            message?
                .update_oneof
                .ok_or(anyhow::anyhow!("failed to get update message"))
        });

        Ok(stream.boxed())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfigYellowstoneGrpc {
    endpoint: String,
}

impl ConfigYellowstoneGrpc {
    async fn subscribe(self) -> anyhow::Result<BoxStream<'static, anyhow::Result<UpdateOneof>>> {
        let request = SubscribeRequest {
            accounts: hashmap! { "".to_owned() => SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: None,
            } },
            slots: hashmap! { "".to_owned() => SubscribeRequestFilterSlots {
                filter_by_commitment: None
            } },
            transactions: hashmap! { "".to_owned() => SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            } },
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: hashmap! { "".to_owned() => SubscribeRequestFilterBlocksMeta {} },
            entry: hashmap! { "".to_owned() => SubscribeRequestFilterEntry {} },
            commitment: Some(CommitmentLevel::Processed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        };

        let builder = GrpcClient::build_from_shared(self.endpoint)?;
        let mut client = builder.connect().await?;
        let stream = client.subscribe_dragons_mouth_once(request).await?;
        let parsed = stream.into_parsed().map(|message| {
            message?
                .update_oneof
                .ok_or(anyhow::anyhow!("failed to get update message"))
        });
        Ok(parsed.boxed())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Deserialize)]
#[serde(deny_unknown_fields, tag = "event")]
enum ConfigTrack {
    BlockMeta,
    Transaction { index: u64 },
}

impl ConfigTrack {
    fn matches(&self, update: &UpdateOneof) -> Option<Slot> {
        match (self, update) {
            (Self::BlockMeta, UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta { slot, .. })) => {
                Some(*slot)
            }
            (
                Self::Transaction { index },
                UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    slot,
                    transaction:
                        Some(SubscribeUpdateTransactionInfo {
                            index: tx_index, ..
                        }),
                }),
            ) if index == tx_index => Some(*slot),
            _ => None,
        }
    }

    fn to_info(&self, slot: Slot) -> String {
        match self {
            Self::BlockMeta => format!("Slot {slot} | BlockMeta"),
            Self::Transaction { index } => format!("Slot {slot} | Transaction#{index}"),
        }
    }
}

type TrackStorageSlot = HashMap<ConfigTrack, BTreeMap<String, SystemTime>>;

#[derive(Debug, Default)]
struct TrackStat {
    win: usize,
    delay: Duration,
    delay_count: u32,
}

#[derive(Debug)]
struct TrackStorage {
    map: HashMap<Slot, TrackStorageSlot>,
    total: usize,
    stats: BTreeMap<String, TrackStat>,
}

impl TrackStorage {
    fn new(total: usize) -> Self {
        Self {
            map: Default::default(),
            total,
            stats: Default::default(),
        }
    }

    fn add(&mut self, slot: Slot, track: &ConfigTrack, name: String, ts: SystemTime) {
        let storage = self.map.entry(slot).or_default();

        let map = storage.entry(track.clone()).or_default();
        map.insert(name, ts);

        if map.len() == self.total {
            warn!("{}", track.to_info(slot));

            let (best_name, best_ts) = map
                .iter()
                .min_by_key(|(_k, v)| *v)
                .map(|(k, v)| (k.clone(), *v))
                .unwrap();
            for (name, ts) in map.iter() {
                if name == &best_name {
                    info!("+000.000000s {name}");
                } else {
                    let elapsed = ts.duration_since(best_ts).unwrap();
                    info!(
                        "+{:03}.{:06}s {name}",
                        elapsed.as_secs(),
                        elapsed.subsec_micros()
                    );
                    let entry = self.stats.entry(name.clone()).or_default();
                    entry.delay += elapsed;
                    entry.delay_count += 1;
                }
            }
            self.stats.entry(best_name).or_default().win += 1;
        }
    }

    fn clear(&mut self, finalized: Slot) {
        self.map.retain(|k, _v| *k >= finalized);
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

    let args = Args::parse();
    let config = fs::read(&args.config).await?;
    let config: Config = serde_yaml::from_slice(&config)?;

    let mut storage = TrackStorage::new(config.sources.len());
    for name in config.sources.keys() {
        storage.stats.insert(name.clone(), TrackStat::default());
    }
    let storage = Arc::new(Mutex::new(storage));
    try_join_all(
        std::iter::once(
            tokio::spawn({
                let storage = Arc::clone(&storage);
                async move {
                    loop {
                        sleep(Duration::from_secs(10)).await;
                        let storage = storage.lock().await;
                        error!("Stats");
                        for (name, stat) in storage.stats.iter() {
                            let avg = stat.delay.checked_div(stat.delay_count).unwrap_or_default();
                            error!(
                                "win {:010} | avg delay +{:03}.{:06}s | {name}",
                                stat.win,
                                avg.as_secs(),
                                avg.subsec_micros()
                            );
                        }
                    }
                }
            })
            .map_err(anyhow::Error::new),
        )
        .chain(config.sources.into_iter().map(|(name, source)| {
            tokio::spawn(source.subscribe(Arc::clone(&storage), config.tracks.clone(), name))
                .map_err(anyhow::Error::new)
        })),
    )
    .await
    .map(|_| ())
}

use {
    clap::Parser,
    futures::{
        future::{try_join_all, TryFutureExt},
        stream::{BoxStream, StreamExt},
    },
    indicatif::{MultiProgress, ProgressBar, ProgressStyle},
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
        sync::Arc,
        time::{Duration, SystemTime},
    },
    tokio::{fs, sync::Mutex},
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
                    storage.add(slot, track, name.clone(), ts)?;
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

#[derive(Debug)]
struct TrackStat {
    pb: ProgressBar,
    win: usize,
    delay: Duration,
    delay_count: u32,
}

#[derive(Debug)]
struct TrackStorage {
    map: HashMap<Slot, TrackStorageSlot>,
    total: usize,
    stats: BTreeMap<String, TrackStat>,
    pb_multi: MultiProgress,
}

impl TrackStorage {
    fn new(names: impl Iterator<Item = String>) -> anyhow::Result<Self> {
        let mut total = 0;
        let mut stats = BTreeMap::new();

        let pb_multi = MultiProgress::new();
        for name in names {
            let pb = pb_multi.add(ProgressBar::no_length());
            pb.set_style(ProgressStyle::with_template(&format!(
                "{{spinner}} {{msg}} | {name}"
            ))?);

            total += 1;
            stats.insert(
                name,
                TrackStat {
                    pb,
                    win: 0,
                    delay: Duration::ZERO,
                    delay_count: 0,
                },
            );
        }

        Ok(Self {
            map: Default::default(),
            total,
            stats,
            pb_multi,
        })
    }

    fn add(
        &mut self,
        slot: Slot,
        track: &ConfigTrack,
        name: String,
        ts: SystemTime,
    ) -> anyhow::Result<()> {
        let storage = self.map.entry(slot).or_default();

        let map = storage.entry(track.clone()).or_default();
        map.insert(name, ts);

        if map.len() == self.total {
            self.pb_multi.println(track.to_info(slot))?;

            let (best_name, best_ts) = map
                .iter()
                .min_by_key(|(_k, v)| *v)
                .map(|(k, v)| (k.clone(), *v))
                .unwrap();
            for (name, ts) in map.iter() {
                if name == &best_name {
                    self.pb_multi.println(format!("+000.000000s {name}"))?;
                    self.stats.get_mut(&best_name).unwrap().win += 1;
                } else {
                    let elapsed = ts.duration_since(best_ts).unwrap();
                    self.pb_multi.println(format!(
                        "+{:03}.{:06}s {name}",
                        elapsed.as_secs(),
                        elapsed.subsec_micros()
                    ))?;
                    let entry = self.stats.get_mut(name).unwrap();
                    entry.delay += elapsed;
                    entry.delay_count += 1;
                }
            }

            self.update_pb();
        }

        Ok(())
    }

    fn clear(&mut self, finalized: Slot) {
        self.map.retain(|k, _v| *k >= finalized);
    }

    fn update_pb(&self) {
        for stat in self.stats.values() {
            let avg = stat.delay.checked_div(stat.delay_count).unwrap_or_default();
            stat.pb.set_message(format!(
                "win {:010} | avg delay +{:03}.{:06}s",
                stat.win,
                avg.as_secs(),
                avg.subsec_micros()
            ));
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = fs::read(&args.config).await?;
    let config: Config = serde_yaml::from_slice(&config)?;

    let storage = TrackStorage::new(config.sources.keys().cloned())?;
    let storage = Arc::new(Mutex::new(storage));
    try_join_all(config.sources.into_iter().map(|(name, source)| {
        tokio::spawn(source.subscribe(Arc::clone(&storage), config.tracks.clone(), name))
            .map_err(anyhow::Error::new)
    }))
    .await
    .map(|_| ())
}

use {
    crate::{
        grpc::config::ConfigAppsGrpc, pubsub::config::ConfigAppsPubsub,
        richat::config::ConfigAppsRichat,
    },
    futures::future::{ready, try_join_all, TryFutureExt},
    richat_client::{grpc::ConfigGrpcClient, quic::ConfigQuicClient},
    richat_filter::message::MessageParserEncoding,
    richat_metrics::ConfigMetrics,
    richat_shared::{
        config::{deserialize_affinity, deserialize_num_str, ConfigTokio},
        shutdown::Shutdown,
    },
    rocksdb::DBCompressionType,
    serde::Deserialize,
    std::{
        collections::HashSet,
        fs,
        path::{Path, PathBuf},
        thread::Builder,
    },
    tokio::time::{sleep, Duration},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub logs: ConfigLogs,
    #[serde(default)]
    pub metrics: Option<ConfigMetrics>,
    pub channel: ConfigChannel,
    #[serde(default)]
    pub apps: ConfigApps,
}

impl Config {
    pub fn load_from_file<P: AsRef<Path>>(file: P) -> anyhow::Result<Self> {
        let config = fs::read_to_string(&file)?;
        if matches!(
            file.as_ref().extension().and_then(|e| e.to_str()),
            Some("yml") | Some("yaml")
        ) {
            serde_yaml::from_str(&config).map_err(Into::into)
        } else {
            json5::from_str(&config).map_err(Into::into)
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigLogs {
    pub json: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigChannel {
    /// Runtime for receiving plugin messages
    #[serde(default)]
    pub tokio: ConfigTokio,
    pub sources: Vec<ConfigChannelSource>,
    #[serde(default)]
    pub config: ConfigChannelInner,
}

impl ConfigChannel {
    pub fn get_messages_parser(&self) -> anyhow::Result<MessageParserEncoding> {
        let mut set = HashSet::new();
        for source in self.sources.iter() {
            set.insert(match source {
                ConfigChannelSource::Quic { general, .. } => general.parser,
                ConfigChannelSource::Grpc { general, .. } => general.parser,
            });
        }
        anyhow::ensure!(
            set.len() == 1,
            "multiple messages parsers: {set:?} (only same parser can be used)"
        );
        Ok(set.into_iter().next().unwrap())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, tag = "transport")]
pub enum ConfigChannelSource {
    #[serde(rename = "quic")]
    Quic {
        #[serde(flatten)]
        general: ConfigChannelSourceGeneral,
        #[serde(flatten)]
        config: ConfigQuicClient,
    },
    #[serde(rename = "grpc")]
    Grpc {
        #[serde(flatten)]
        general: ConfigChannelSourceGeneral,
        source: ConfigGrpcClientSource,
        #[serde(flatten)]
        config: ConfigGrpcClient,
    },
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigChannelSourceGeneral {
    pub name: String,
    /// Messages parser: `prost` or `limited`
    pub parser: MessageParserEncoding,
    #[serde(default)]
    pub disable_accounts: bool,
    #[serde(default)]
    pub reconnect: Option<ConfigChannelSourceReconnect>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigChannelSourceReconnect {
    #[serde(
        with = "humantime_serde",
        default = "ConfigChannelSourceReconnect::default_initial_interval"
    )]
    pub initial_interval: Duration,
    #[serde(
        with = "humantime_serde",
        default = "ConfigChannelSourceReconnect::default_max_interval"
    )]
    pub max_interval: Duration,
    #[serde(default = "ConfigChannelSourceReconnect::default_multiplier")]
    pub multiplier: f64,
}

impl ConfigChannelSourceReconnect {
    const fn default_initial_interval() -> Duration {
        Duration::from_secs(1)
    }

    const fn default_max_interval() -> Duration {
        Duration::from_secs(15)
    }

    const fn default_multiplier() -> f64 {
        2.0
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum ConfigGrpcClientSource {
    DragonsMouth,
    #[default]
    Richat,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigChannelInner {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max_messages: usize,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max_bytes: usize,
    pub storage: Option<ConfigStorage>,
}

impl Default for ConfigChannelInner {
    fn default() -> Self {
        Self {
            max_messages: 2_097_152, // aligned to power of 2, ~20k/slot should give us ~100 slots
            max_bytes: 15 * 1024 * 1024 * 1024, // 15GiB with ~150MiB/slot should give us ~100 slots
            storage: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStorage {
    pub path: PathBuf,
    #[serde(
        default = "ConfigStorage::default_max_slots",
        deserialize_with = "deserialize_num_str"
    )]
    pub max_slots: usize,
    #[serde(default, deserialize_with = "deserialize_affinity")]
    pub serialize_affinity: Option<Vec<usize>>,
    #[serde(default, deserialize_with = "deserialize_affinity")]
    pub write_affinity: Option<Vec<usize>>,
    #[serde(default)]
    pub messages_compression: ConfigStorageRocksdbCompression,
    #[serde(
        default = "ConfigStorage::default_replay_channel_capacity",
        deserialize_with = "deserialize_num_str"
    )]
    pub replay_inflight_max: usize,
    #[serde(
        default = "ConfigStorage::default_replay_threads",
        deserialize_with = "deserialize_num_str"
    )]
    pub replay_threads: usize,
    #[serde(default, deserialize_with = "deserialize_affinity")]
    pub replay_affinity: Option<Vec<usize>>,
    #[serde(
        default = "ConfigStorage::default_replay_decode_per_tick",
        deserialize_with = "deserialize_num_str"
    )]
    pub replay_decode_per_tick: usize,
}

impl ConfigStorage {
    const fn default_max_slots() -> usize {
        1024
    }

    const fn default_replay_channel_capacity() -> usize {
        1024
    }

    const fn default_replay_threads() -> usize {
        4
    }

    const fn default_replay_decode_per_tick() -> usize {
        256
    }
}

#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum ConfigStorageRocksdbCompression {
    #[default]
    None,
    Snappy,
    Zlib,
    Bz2,
    Lz4,
    Lz4hc,
    Zstd,
}

impl From<ConfigStorageRocksdbCompression> for DBCompressionType {
    fn from(value: ConfigStorageRocksdbCompression) -> Self {
        match value {
            ConfigStorageRocksdbCompression::None => Self::None,
            ConfigStorageRocksdbCompression::Snappy => Self::Snappy,
            ConfigStorageRocksdbCompression::Zlib => Self::Zlib,
            ConfigStorageRocksdbCompression::Bz2 => Self::Bz2,
            ConfigStorageRocksdbCompression::Lz4 => Self::Lz4,
            ConfigStorageRocksdbCompression::Lz4hc => Self::Lz4hc,
            ConfigStorageRocksdbCompression::Zstd => Self::Zstd,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigApps {
    /// Runtime for incoming connections
    pub tokio: ConfigTokio,
    /// downstream richat
    pub richat: Option<ConfigAppsRichat>,
    /// gRPC app (fully compatible with Yellowstone Dragon's Mouth)
    pub grpc: Option<ConfigAppsGrpc>,
    /// WebSocket app (fully compatible with Solana PubSub)
    pub pubsub: Option<ConfigAppsPubsub>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsWorkers {
    /// Number of worker threads
    pub threads: usize,
    /// Threads affinity
    #[serde(deserialize_with = "deserialize_affinity")]
    pub affinity: Option<Vec<usize>>,
}

impl Default for ConfigAppsWorkers {
    fn default() -> Self {
        Self {
            threads: 1,
            affinity: None,
        }
    }
}

impl ConfigAppsWorkers {
    pub async fn run(
        self,
        get_name: impl Fn(usize) -> String,
        spawn_fn: impl FnOnce(usize) -> anyhow::Result<()> + Clone + Send + 'static,
        shutdown: Shutdown,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(self.threads > 0, "number of threads can be zero");

        let mut jhs = Vec::with_capacity(self.threads);
        for index in 0..self.threads {
            let cpus = self.affinity.as_ref().map(|affinity| {
                if self.threads == affinity.len() {
                    vec![affinity[index]]
                } else {
                    affinity.clone()
                }
            });

            jhs.push(Self::run_once(
                index,
                get_name(index),
                cpus,
                spawn_fn.clone(),
                shutdown.clone(),
            )?);
        }

        try_join_all(jhs).await.map(|_| ())
    }

    pub fn run_once(
        index: usize,
        name: String,
        cpus: Option<Vec<usize>>,
        spawn_fn: impl FnOnce(usize) -> anyhow::Result<()> + Send + 'static,
        shutdown: Shutdown,
    ) -> anyhow::Result<impl std::future::Future<Output = anyhow::Result<()>>> {
        let th = Builder::new().name(name).spawn(move || {
            if let Some(cpus) = cpus {
                affinity_linux::set_thread_affinity(cpus.into_iter())
                    .expect("failed to set affinity");
            }
            spawn_fn(index)
        })?;

        let jh = tokio::spawn(async move {
            while !th.is_finished() {
                let ms = if shutdown.is_set() { 10 } else { 2_000 };
                sleep(Duration::from_millis(ms)).await;
            }
            th.join().expect("failed to join thread")
        });

        Ok(jh.map_err(anyhow::Error::new).and_then(ready))
    }
}

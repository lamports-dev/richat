use {
    crate::grpc::config::ConfigAppsGrpc,
    futures::future::{ready, try_join_all, TryFutureExt},
    richat_client::{grpc::ConfigGrpcClient, quic::ConfigQuicClient, tcp::ConfigTcpClient},
    richat_filter::message::MessageParserEncoding,
    richat_shared::{
        config::{deserialize_num_str, parse_taskset, ConfigPrometheus, ConfigTokio},
        shutdown::Shutdown,
    },
    serde::{
        de::{self, Deserializer},
        Deserialize,
    },
    std::{fs, path::Path, thread::Builder},
    tokio::time::{sleep, Duration},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub log: ConfigLog,
    pub channel: ConfigChannel,
    #[serde(default)]
    pub apps: ConfigApps,
    #[serde(default)]
    pub prometheus: Option<ConfigPrometheus>,
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
pub struct ConfigLog {
    pub json: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigChannel {
    /// Runtime for receiving plugin messages
    #[serde(default)]
    pub tokio: ConfigTokio,
    pub source: ConfigChannelSource,
    #[serde(default)]
    pub config: ConfigChannelInner,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, tag = "transport")]
pub enum ConfigChannelSource {
    #[serde(rename = "quic")]
    Quic(ConfigQuicClient),
    #[serde(rename = "tcp")]
    Tcp(ConfigTcpClient),
    #[serde(rename = "grpc")]
    Grpc(ConfigGrpcClient),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigChannelInner {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max_messages: usize,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max_slots: usize,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max_bytes: usize,
    pub parser: MessageParserEncoding,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub parser_channel_size: usize,
    pub parser_affinity: usize,
}

impl Default for ConfigChannelInner {
    fn default() -> Self {
        Self {
            max_messages: 2_097_152, // assume 20k messages per slot, aligned to power of 2
            max_slots: 100,
            max_bytes: 10 * 1024 * 1024 * 1024, // 10GiB, assume 100MiB per slot
            parser: MessageParserEncoding::Prost,
            parser_channel_size: 65_536,
            parser_affinity: 0,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigApps {
    /// Runtime for incoming connections
    pub tokio: ConfigTokio,
    /// gRPC app (fully compatible with Yellowstone Dragon's Mouth)
    pub grpc: Option<ConfigAppsGrpc>,
    // TODO: pubsub
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsWorkers {
    /// Number of worker threads
    pub threads: usize,
    /// Threads affinity
    #[serde(deserialize_with = "ConfigAppsWorkers::deserialize_affinity")]
    pub affinity: Vec<usize>,
}

impl Default for ConfigAppsWorkers {
    fn default() -> Self {
        Self {
            threads: 1,
            affinity: (0..affinity::get_core_num()).collect(),
        }
    }
}

impl ConfigAppsWorkers {
    pub fn deserialize_affinity<'de, D>(deserializer: D) -> Result<Vec<usize>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let taskset: &str = Deserialize::deserialize(deserializer)?;
        parse_taskset(taskset).map_err(de::Error::custom)
    }

    pub async fn run(
        self,
        get_name: impl Fn(usize) -> String,
        spawn_fn: impl FnOnce(usize) -> anyhow::Result<()> + Clone + Send + 'static,
        shutdown: Shutdown,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(self.threads > 0, "number of threads can be zero");

        let mut jhs = Vec::with_capacity(self.threads);
        for index in 0..self.threads {
            let cpus = if self.threads == self.affinity.len() {
                vec![self.affinity[index]]
            } else {
                self.affinity.clone()
            };

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
        cpus: Vec<usize>,
        spawn_fn: impl FnOnce(usize) -> anyhow::Result<()> + Clone + Send + 'static,
        shutdown: Shutdown,
    ) -> anyhow::Result<impl std::future::Future<Output = anyhow::Result<()>>> {
        let th = Builder::new().name(name).spawn(move || {
            affinity::set_thread_affinity(cpus).expect("failed to set affinity");
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

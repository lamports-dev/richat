use {
    crate::grpc::config::ConfigAppsGrpc,
    futures::future::{ready, try_join_all, TryFutureExt},
    richat_client::{grpc::ConfigGrpcClient, quic::ConfigQuicClient, tcp::ConfigTcpClient},
    richat_shared::{
        config::{deserialize_num_str, ConfigPrometheus, ConfigTokio},
        shutdown::Shutdown,
    },
    serde::{
        de::{self, Deserializer},
        Deserialize,
    },
    solana_sdk::pubkey::Pubkey,
    std::{collections::HashSet, fs, path::Path, thread::Builder},
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
}

impl Default for ConfigChannelInner {
    fn default() -> Self {
        Self {
            max_messages: 2_097_152, // assume 20k messages per slot, aligned to power of 2
            max_slots: 100,
            max_bytes: 10 * 1024 * 1024 * 1024, // 10GiB, assume 100MiB per slot
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
    #[serde(deserialize_with = "ConfigTokio::deserialize_affinity")]
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
        spawn_fn: impl Fn(usize) -> anyhow::Result<()> + Clone + Send + 'static,
        shutdown: Shutdown,
    ) -> anyhow::Result<()> {
        let mut jhs = Vec::with_capacity(self.threads);
        for index in 0..self.threads {
            let cpus = self.affinity.clone();
            let spawn_fn = spawn_fn.clone();
            let shutdown = shutdown.clone();
            let th = Builder::new().name(get_name(index)).spawn(move || {
                if let Some(cpus) = cpus {
                    affinity::set_thread_affinity(&cpus).expect("failed to set affinity");
                }
                spawn_fn(index)
            })?;

            let jh = tokio::spawn(async move {
                while !th.is_finished() {
                    let ms = if shutdown.is_set() { 10 } else { 3_000 };
                    sleep(Duration::from_millis(ms)).await;
                }
                th.join().expect("failed to join thread")
            });

            jhs.push(jh.map_err(anyhow::Error::new).and_then(ready));
        }

        try_join_all(jhs).await.map(|_| ())
    }
}

pub fn deserialize_pubkey_set<'de, D>(deserializer: D) -> Result<HashSet<Pubkey>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<&str>::deserialize(deserializer)?
        .into_iter()
        .map(|value| {
            value
                .parse()
                .map_err(|error| de::Error::custom(format!("Invalid pubkey: {value} ({error:?})")))
        })
        .collect::<Result<_, _>>()
}

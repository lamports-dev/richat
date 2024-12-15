use {
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    serde::{
        de::{self, Deserializer},
        Deserialize,
    },
    std::{collections::HashSet, fs::read_to_string, net::SocketAddr, path::Path, time::Duration},
    tonic::codec::CompressionEncoding,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub libpath: String,
    #[serde(default)]
    pub log: ConfigLog,

    #[serde(default)]
    pub tokio: ConfigTokio,

    #[serde(default)]
    pub channel: ConfigChannel,

    #[serde(default)]
    pub grpc: Option<ConfigGrpc>,

    #[serde(default)]
    pub prometheus: Option<ConfigPrometheus>,
}

impl Config {
    fn load_from_str(config: &str) -> PluginResult<Self> {
        serde_json::from_str(config).map_err(|error| GeyserPluginError::ConfigFileReadError {
            msg: error.to_string(),
        })
    }

    pub fn load_from_file<P: AsRef<Path>>(file: P) -> PluginResult<Self> {
        let config = read_to_string(file).map_err(GeyserPluginError::ConfigFileOpenError)?;
        Self::load_from_str(&config)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigLog {
    /// Log level
    #[serde(default = "ConfigLog::default_level")]
    pub level: String,
}

impl Default for ConfigLog {
    fn default() -> Self {
        Self {
            level: Self::default_level(),
        }
    }
}

impl ConfigLog {
    fn default_level() -> String {
        "info".to_owned()
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigTokio {
    /// Number of worker threads in Tokio runtime
    pub worker_threads: Option<usize>,
    /// Threads affinity
    #[serde(deserialize_with = "ConfigTokio::deserialize_affinity")]
    pub affinity: Option<Vec<usize>>,
}

impl ConfigTokio {
    fn deserialize_affinity<'de, D>(deserializer: D) -> Result<Option<Vec<usize>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<&str>::deserialize(deserializer)? {
            Some(taskset) => parse_taskset(taskset).map(Some).map_err(de::Error::custom),
            None => Ok(None),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigChannel {
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max_messages: usize,
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max_slots: usize,
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max_bytes: usize,
}

impl Default for ConfigChannel {
    fn default() -> Self {
        Self {
            max_messages: 2_097_152, // assume 20k messages per slot, aligned to power of 2
            max_slots: 100,
            max_bytes: 10 * 1024 * 1024 * 1024, // 10GiB, assume 100MiB per slot
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ConfigGrpc {
    pub endpoint: SocketAddr,
    /// TLS config
    #[serde(default)]
    pub tls_config: Option<ConfigGrpcTls>,
    #[serde(default)]
    pub compression: ConfigGrpcCompression,
    /// Limits the maximum size of a decoded message, default is 4MiB
    #[serde(
        default = "ConfigGrpc::max_decoding_message_size_default",
        deserialize_with = "deserialize_usize_str"
    )]
    pub max_decoding_message_size: usize,
    #[serde(default)]
    pub server_http2_adaptive_window: Option<bool>,
    #[serde(with = "humantime_serde")]
    pub server_http2_keepalive_interval: Option<Duration>,
    #[serde(with = "humantime_serde")]
    pub server_http2_keepalive_timeout: Option<Duration>,
    #[serde(default)]
    pub server_initial_connection_window_size: Option<u32>,
    #[serde(default)]
    pub server_initial_stream_window_size: Option<u32>,
}

impl ConfigGrpc {
    const fn max_decoding_message_size_default() -> usize {
        4 * 1024 * 1024
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcTls {
    pub cert: String,
    pub key: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigGrpcCompression {
    #[serde(
        deserialize_with = "ConfigGrpcCompression::deserialize_compression",
        default = "ConfigGrpcCompression::default_compression"
    )]
    pub accept: Vec<CompressionEncoding>,
    #[serde(
        deserialize_with = "ConfigGrpcCompression::deserialize_compression",
        default = "ConfigGrpcCompression::default_compression"
    )]
    pub send: Vec<CompressionEncoding>,
}

impl Default for ConfigGrpcCompression {
    fn default() -> Self {
        Self {
            accept: Self::default_compression(),
            send: Self::default_compression(),
        }
    }
}

impl ConfigGrpcCompression {
    fn deserialize_compression<'de, D>(
        deserializer: D,
    ) -> Result<Vec<CompressionEncoding>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<&str>::deserialize(deserializer)?
            .into_iter()
            .map(|value| match value {
                "gzip" => Ok(CompressionEncoding::Gzip),
                "zstd" => Ok(CompressionEncoding::Zstd),
                value => Err(de::Error::custom(format!(
                    "Unknown compression format: {value}"
                ))),
            })
            .collect::<Result<_, _>>()
    }

    fn default_compression() -> Vec<CompressionEncoding> {
        vec![CompressionEncoding::Gzip, CompressionEncoding::Zstd]
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigPrometheus {
    /// Endpoint of Prometheus service
    pub endpoint: SocketAddr,
}

fn parse_taskset(taskset: &str) -> Result<Vec<usize>, String> {
    let mut set = HashSet::new();
    for taskset2 in taskset.split(',') {
        match taskset2.split_once('-') {
            Some((start, end)) => {
                let start: usize = start
                    .parse()
                    .map_err(|_error| format!("failed to parse {start:?} from {taskset:?}"))?;
                let end: usize = end
                    .parse()
                    .map_err(|_error| format!("failed to parse {end:?} from {taskset:?}"))?;
                if start > end {
                    return Err(format!("invalid interval {taskset2:?} in {taskset:?}"));
                }
                for idx in start..=end {
                    set.insert(idx);
                }
            }
            None => {
                set.insert(
                    taskset2.parse().map_err(|_error| {
                        format!("failed to parse {taskset2:?} from {taskset:?}")
                    })?,
                );
            }
        }
    }

    let mut vec = set.into_iter().collect::<Vec<usize>>();
    vec.sort();

    if let Some(set_max_index) = vec.last().copied() {
        let max_index = affinity::get_thread_affinity()
            .map_err(|_err| "failed to get affinity".to_owned())?
            .into_iter()
            .max()
            .unwrap_or(0);

        if set_max_index > max_index {
            return Err(format!("core index must be in the range [0, {max_index}]"));
        }
    }

    Ok(vec)
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ValueIntStr<'a> {
    Int(usize),
    Str(&'a str),
}

fn deserialize_usize_str<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    match ValueIntStr::deserialize(deserializer)? {
        ValueIntStr::Int(value) => Ok(value),
        ValueIntStr::Str(value) => value
            .replace('_', "")
            .parse::<usize>()
            .map_err(de::Error::custom),
    }
}

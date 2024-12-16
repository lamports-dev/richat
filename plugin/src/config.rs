use {
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    richat_shared::{config::deserialize_usize_str, transports::grpc::ConfigGrpcServer},
    rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    serde::{
        de::{self, Deserializer},
        Deserialize,
    },
    std::{
        collections::HashSet,
        fs,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::{Path, PathBuf},
    },
};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub libpath: String,
    pub log: ConfigLog,
    pub tokio: ConfigTokio,
    pub channel: ConfigChannel,
    pub quic: Option<ConfigQuic>,
    pub grpc: Option<ConfigGrpcServer>,
    pub prometheus: Option<ConfigPrometheus>,
}

impl Config {
    fn load_from_str(config: &str) -> PluginResult<Self> {
        serde_json::from_str(config).map_err(|error| GeyserPluginError::ConfigFileReadError {
            msg: error.to_string(),
        })
    }

    pub fn load_from_file<P: AsRef<Path>>(file: P) -> PluginResult<Self> {
        let config = fs::read_to_string(file).map_err(GeyserPluginError::ConfigFileOpenError)?;
        Self::load_from_str(&config)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigLog {
    /// Log level
    pub level: String,
}

impl Default for ConfigLog {
    fn default() -> Self {
        Self {
            level: "info".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
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
#[serde(default, deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct ConfigQuic {
    #[serde(default = "ConfigQuic::default_endpoint")]
    pub endpoint: SocketAddr,
    #[serde(deserialize_with = "ConfigQuic::deserialize_tls_config")]
    pub tls_config: rustls::ServerConfig,
    /// Value in ms
    #[serde(default = "ConfigQuic::default_expected_rtt")]
    pub expected_rtt: u32,
    /// Value in bytes/s, default with expected rtt 100 is 100Mbps
    #[serde(default = "ConfigQuic::default_max_stream_bandwidth")]
    pub max_stream_bandwidth: u32,
    /// Max number of outgoing streams
    #[serde(default = "ConfigQuic::default_max_uni_streams")]
    pub max_uni_streams: u32,
}

impl ConfigQuic {
    const fn default_endpoint() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 10100)
    }

    fn deserialize_tls_config<'de, D>(deserializer: D) -> Result<rustls::ServerConfig, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields, untagged)]
        enum Config<'a> {
            Signed { cert: &'a str, key: &'a str },
            SelfSigned { self_signed_alt_names: Vec<String> },
        }

        let (certs, key) = match Config::deserialize(deserializer)? {
            Config::Signed { cert, key } => {
                let cert_path = PathBuf::from(cert);
                let cert_bytes = fs::read(&cert_path).map_err(|error| {
                    de::Error::custom(format!("failed to read cert {cert_path:?}: {error:?}"))
                })?;
                let cert_chain = if cert_path.extension().is_some_and(|x| x == "der") {
                    vec![CertificateDer::from(cert_bytes)]
                } else {
                    rustls_pemfile::certs(&mut &*cert_bytes)
                        .collect::<Result<_, _>>()
                        .map_err(|error| {
                            de::Error::custom(format!("invalid PEM-encoded certificate: {error:?}"))
                        })?
                };

                let key_path = PathBuf::from(key);
                let key_bytes = fs::read(&key_path).map_err(|error| {
                    de::Error::custom(format!("failed to read key {key_path:?}: {error:?}"))
                })?;
                let key = if key_path.extension().is_some_and(|x| x == "der") {
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_bytes))
                } else {
                    rustls_pemfile::private_key(&mut &*key_bytes)
                        .map_err(|error| {
                            de::Error::custom(format!("malformed PKCS #1 private key: {error:?}"))
                        })?
                        .ok_or_else(|| de::Error::custom("no private keys found"))?
                };

                (cert_chain, key)
            }
            Config::SelfSigned {
                self_signed_alt_names,
            } => {
                let cert =
                    rcgen::generate_simple_self_signed(self_signed_alt_names).map_err(|error| {
                        de::Error::custom(format!("failed to generate self-signed cert: {error:?}"))
                    })?;
                let cert_der = CertificateDer::from(cert.cert);
                let priv_key = PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der());
                (vec![cert_der], priv_key.into())
            }
        };

        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|error| de::Error::custom(format!("failed to use cert: {error:?}")))
    }

    const fn default_expected_rtt() -> u32 {
        100
    }

    const fn default_max_stream_bandwidth() -> u32 {
        12_500 * 1000
    }

    const fn default_max_uni_streams() -> u32 {
        16
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigPrometheus {
    /// Endpoint of Prometheus service
    pub endpoint: SocketAddr,
}

impl Default for ConfigPrometheus {
    fn default() -> Self {
        Self {
            endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 10123),
        }
    }
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

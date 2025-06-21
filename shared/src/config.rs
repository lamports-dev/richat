use {
    crate::five8::{pubkey_decode, signature_decode},
    base64::{engine::general_purpose::STANDARD as base64_engine, Engine},
    regex::Regex,
    rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    serde::{
        de::{self, Deserializer},
        Deserialize,
    },
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    std::{
        collections::HashSet,
        fmt::Display,
        fs, io,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::PathBuf,
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
    },
    thiserror::Error,
};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigTokio {
    /// Number of worker threads in Tokio runtime
    pub worker_threads: Option<usize>,
    /// Threads affinity
    #[serde(deserialize_with = "deserialize_affinity")]
    pub affinity: Option<Vec<usize>>,
}

impl ConfigTokio {
    pub fn build_runtime<T>(self, thread_name_prefix: T) -> io::Result<tokio::runtime::Runtime>
    where
        T: AsRef<str> + Send + Sync + 'static,
    {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(worker_threads) = self.worker_threads {
            builder.worker_threads(worker_threads);
        }
        if let Some(cpus) = self.affinity.clone() {
            builder.on_thread_start(move || {
                affinity_linux::set_thread_affinity(cpus.iter().copied())
                    .expect("failed to set affinity")
            });
        }
        builder
            .thread_name_fn(move || {
                static ATOMIC_ID: AtomicU64 = AtomicU64::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
                format!("{}{id:02}", thread_name_prefix.as_ref())
            })
            .enable_all()
            .build()
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigMetrics {
    /// Endpoint of Prometheus service
    pub endpoint: SocketAddr,
}

impl Default for ConfigMetrics {
    fn default() -> Self {
        Self {
            endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10123),
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ValueNumStr<'a, T> {
    Num(T),
    Str(&'a str),
}

pub fn deserialize_num_str<'de, T, D>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + FromStr,
    <T as FromStr>::Err: Display,
{
    match ValueNumStr::deserialize(deserializer)? {
        ValueNumStr::Num(value) => Ok(value),
        ValueNumStr::Str(value) => value
            .replace('_', "")
            .parse::<T>()
            .map_err(de::Error::custom),
    }
}

pub fn deserialize_maybe_num_str<'de, T, D>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + FromStr,
    <T as FromStr>::Err: Display,
{
    match Option::<ValueNumStr<T>>::deserialize(deserializer)? {
        Some(ValueNumStr::Num(value)) => Ok(Some(value)),
        Some(ValueNumStr::Str(value)) => value
            .replace('_', "")
            .parse::<T>()
            .map_err(de::Error::custom)
            .map(Some),
        None => Ok(None),
    }
}

#[derive(Debug, Error)]
enum DecodeXTokenError {
    #[error(transparent)]
    Base64(#[from] base64::DecodeError),
    #[error(transparent)]
    Base58(#[from] bs58::decode::Error),
}

fn decode_x_token(x_token: &str) -> Result<Vec<u8>, DecodeXTokenError> {
    Ok(match &x_token[0..7] {
        "base64:" => base64_engine.decode(x_token)?,
        "base58:" => bs58::decode(x_token).into_vec()?,
        _ => x_token.as_bytes().to_vec(),
    })
}

pub fn deserialize_maybe_x_token<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    let x_token: Option<&str> = Deserialize::deserialize(deserializer)?;
    x_token
        .map(|x_token| decode_x_token(x_token).map_err(de::Error::custom))
        .transpose()
}

pub fn deserialize_x_token_set<'de, D>(deserializer: D) -> Result<HashSet<Vec<u8>>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<&str>::deserialize(deserializer).and_then(|vec| {
        vec.into_iter()
            .map(|x_token| decode_x_token(x_token).map_err(de::Error::custom))
            .collect::<Result<_, _>>()
    })
}

pub fn deserialize_pubkey_set<'de, D>(deserializer: D) -> Result<HashSet<Pubkey>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<&str>::deserialize(deserializer)?
        .into_iter()
        .map(|value| {
            pubkey_decode(value)
                .map_err(|error| de::Error::custom(format!("Invalid pubkey: {value} ({error:?})")))
        })
        .collect::<Result<_, _>>()
}

pub fn deserialize_pubkey_vec<'de, D>(deserializer: D) -> Result<Vec<Pubkey>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_pubkey_set(deserializer).map(|set| set.into_iter().collect())
}

pub fn deserialize_maybe_signature<'de, D>(deserializer: D) -> Result<Option<Signature>, D::Error>
where
    D: Deserializer<'de>,
{
    let sig: Option<&str> = Deserialize::deserialize(deserializer)?;
    sig.map(|sig| signature_decode(sig).map_err(de::Error::custom))
        .transpose()
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, untagged)]
enum RustlsServerConfigSignedSelfSigned<'a> {
    Signed { cert: &'a str, key: &'a str },
    SelfSigned { self_signed_alt_names: Vec<String> },
}

impl<'a> RustlsServerConfigSignedSelfSigned<'a> {
    fn parse<D>(self) -> Result<rustls::ServerConfig, D::Error>
    where
        D: Deserializer<'a>,
    {
        let (certs, key) = match self {
            Self::Signed { cert, key } => {
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
            Self::SelfSigned {
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
}

pub fn deserialize_maybe_rustls_server_config<'de, D>(
    deserializer: D,
) -> Result<Option<rustls::ServerConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    let config: Option<RustlsServerConfigSignedSelfSigned> =
        Deserialize::deserialize(deserializer)?;
    if let Some(config) = config {
        config.parse::<D>().map(Some)
    } else {
        Ok(None)
    }
}

pub fn deserialize_rustls_server_config<'de, D>(
    deserializer: D,
) -> Result<rustls::ServerConfig, D::Error>
where
    D: Deserializer<'de>,
{
    let config: RustlsServerConfigSignedSelfSigned = Deserialize::deserialize(deserializer)?;
    config.parse::<D>()
}

pub fn deserialize_affinity<'de, D>(deserializer: D) -> Result<Option<Vec<usize>>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<&str>::deserialize(deserializer)? {
        Some(taskset) => parse_taskset(taskset).map(Some).map_err(de::Error::custom),
        None => Ok(None),
    }
}

pub fn parse_taskset(taskset: &str) -> Result<Vec<usize>, String> {
    let re = Regex::new(r"^(\d+)(?:-(\d+)(?::(\d+))?)?$").expect("valid regex");
    let mut set = HashSet::new();
    for cpulist in taskset.split(',') {
        let Some(caps) = re.captures(cpulist) else {
            return Err(format!("invalid cpulist: {cpulist}"));
        };

        let start = caps
            .get(1)
            .and_then(|m| m.as_str().parse().ok())
            .expect("valid regex");
        let end = caps
            .get(2)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(start);
        let step = caps
            .get(3)
            .and_then(|m| m.as_str().parse().ok())
            .unwrap_or(1);

        for cpu in (start..=end).step_by(step) {
            set.insert(cpu);
        }
    }

    let mut vec = set.into_iter().collect::<Vec<usize>>();
    vec.sort();

    if !vec.is_empty() {
        if let Some(cores) = affinity_linux::get_thread_affinity()
            .map_err(|error| format!("failed to get allowed cpus: {error:?}"))?
        {
            let mut cores = cores.into_iter().collect::<Vec<_>>();
            cores.sort();

            for core in vec.iter_mut() {
                if let Some(actual_core) = cores.get(*core) {
                    *core = *actual_core;
                } else {
                    return Err(format!(
                        "we don't have core {core}, available cores: {:?}",
                        (0..cores.len()).collect::<Vec<_>>()
                    ));
                }
            }
        }
    }

    Ok(vec)
}

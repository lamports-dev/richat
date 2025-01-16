use {
    base64::{engine::general_purpose::STANDARD as base64_engine, Engine},
    serde::{
        de::{self, Deserializer},
        Deserialize,
    },
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    std::{
        collections::HashSet,
        fmt::Display,
        io,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        str::FromStr,
        sync::atomic::{AtomicU64, Ordering},
    },
    thiserror::Error,
};

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
    pub fn deserialize_affinity<'de, D>(deserializer: D) -> Result<Option<Vec<usize>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Option::<&str>::deserialize(deserializer)? {
            Some(taskset) => parse_taskset(taskset).map(Some).map_err(de::Error::custom),
            None => Ok(None),
        }
    }

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
                affinity::set_thread_affinity(&cpus).expect("failed to set affinity")
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

pub fn parse_taskset(taskset: &str) -> Result<Vec<usize>, String> {
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

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigPrometheus {
    /// Endpoint of Prometheus service
    pub endpoint: SocketAddr,
}

impl Default for ConfigPrometheus {
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
            value
                .parse()
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
    sig.map(|sig| sig.parse().map_err(de::Error::custom))
        .transpose()
}

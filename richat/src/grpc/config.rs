use {
    crate::config::ConfigAppsWorkers,
    richat_filter::config::ConfigLimits as ConfigFilterLimits,
    richat_shared::{
        config::{deserialize_affinity, deserialize_num_str, deserialize_x_token_set},
        transports::grpc::ConfigGrpcServer as ConfigAppGrpcServer,
    },
    serde::Deserialize,
    std::{collections::HashSet, time::Duration},
};

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpc {
    pub server: ConfigAppGrpcServer,
    pub workers: ConfigAppsGrpcWorkers,
    pub stream: ConfigAppsGrpcStream,
    pub unary: ConfigAppsGrpcUnary,
    pub filter_limits: ConfigFilterLimits,
    #[serde(deserialize_with = "deserialize_x_token_set")]
    pub x_token: HashSet<Vec<u8>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpcWorkers {
    #[serde(flatten)]
    pub threads: ConfigAppsWorkers,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub messages_cached_max: usize,
}

impl Default for ConfigAppsGrpcWorkers {
    fn default() -> Self {
        Self {
            threads: ConfigAppsWorkers::default(),
            messages_cached_max: 1_024,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpcStream {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub messages_len_max: usize,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub messages_max_per_tick: usize,
    #[serde(with = "humantime_serde")]
    pub ping_iterval: Duration,
}

impl Default for ConfigAppsGrpcStream {
    fn default() -> Self {
        Self {
            messages_len_max: 16 * 1024 * 1024,
            messages_max_per_tick: 100,
            ping_iterval: Duration::from_secs(15),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpcUnary {
    pub enabled: bool,
    #[serde(deserialize_with = "deserialize_affinity")]
    pub affinity: Option<Vec<usize>>,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub requests_queue_size: usize,
}

impl Default for ConfigAppsGrpcUnary {
    fn default() -> Self {
        Self {
            enabled: true,
            affinity: None,
            requests_queue_size: 100,
        }
    }
}

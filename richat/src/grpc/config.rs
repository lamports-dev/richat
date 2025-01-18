use {
    crate::config::ConfigAppsWorkers,
    richat_filter::config::ConfigLimits as ConfigFilterLimits,
    richat_shared::{
        config::{deserialize_num_str, deserialize_x_token_set},
        transports::grpc::ConfigGrpcServer as ConfigAppGrpcServer,
    },
    serde::Deserialize,
    std::{collections::HashSet, time::Duration},
};

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpc {
    pub server: ConfigAppGrpcServer,
    pub workers: ConfigAppsWorkers,
    pub stream: ConfigAppsGrpcStream,
    pub unary: ConfigAppsGrpcUnary,
    pub filter_limits: ConfigFilterLimits,
    #[serde(deserialize_with = "deserialize_x_token_set")]
    pub x_token: HashSet<Vec<u8>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpcStream {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub messages_len_max: usize,
    #[serde(with = "humantime_serde")]
    pub ping_iterval: Duration,
}

impl Default for ConfigAppsGrpcStream {
    fn default() -> Self {
        Self {
            messages_len_max: 16 * 1024 * 1024,
            ping_iterval: Duration::from_secs(15),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpcUnary {
    pub enabled: bool,
    pub affinity: usize,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub requests_queue_size: usize,
}

impl Default for ConfigAppsGrpcUnary {
    fn default() -> Self {
        Self {
            enabled: true,
            affinity: 0,
            requests_queue_size: 100,
        }
    }
}

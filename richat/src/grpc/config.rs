use {
    crate::config::ConfigAppsWorkers,
    richat_filter::config::ConfigLimits as ConfigFilterLimits,
    richat_shared::{
        config::{deserialize_num_str, deserialize_x_token_set},
        transports::grpc::ConfigGrpcServer as ConfigAppGrpcServer,
    },
    serde::Deserialize,
    std::collections::HashSet,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpc {
    pub server: ConfigAppGrpcServer,
    pub workers: ConfigAppsWorkers,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub stream_channel_capacity: usize,
    pub unary: ConfigAppsGrpcUnary,
    pub filter_limits: ConfigFilterLimits,
    #[serde(deserialize_with = "deserialize_x_token_set")]
    pub x_token: HashSet<Vec<u8>>,
}

impl Default for ConfigAppsGrpc {
    fn default() -> Self {
        Self {
            server: ConfigAppGrpcServer::default(),
            workers: ConfigAppsWorkers::default(),
            stream_channel_capacity: 100_000,
            unary: ConfigAppsGrpcUnary::default(),
            filter_limits: ConfigFilterLimits::default(),
            x_token: HashSet::default(),
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

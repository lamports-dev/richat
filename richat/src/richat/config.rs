use {
    richat_shared::transports::{grpc::ConfigGrpcServer, quic::ConfigQuicServer},
    serde::Deserialize,
};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsRichat {
    pub quic: Option<ConfigQuicServer>,
    pub grpc: Option<ConfigGrpcServer>,
}

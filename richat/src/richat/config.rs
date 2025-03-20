use {
    richat_shared::transports::{
        grpc::ConfigGrpcServer, quic::ConfigQuicServer, tcp::ConfigTcpServer,
    },
    serde::Deserialize,
};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsRichat {
    pub quic: Option<ConfigQuicServer>,
    pub tcp: Option<ConfigTcpServer>,
    pub grpc: Option<ConfigGrpcServer>,
}

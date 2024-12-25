use {
    richat_client::{grpc::ConfigGrpcClient, quic::ConfigQuicClient, tcp::ConfigTcpClient},
    richat_shared::config::{ConfigPrometheus, ConfigTokio},
    serde::Deserialize,
    std::{fs, path::Path},
};

#[derive(Debug, Deserialize)]
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

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigLog {
    pub json: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigChannel {
    /// Runtime for receiving plugin messages
    #[serde(default)]
    pub tokio: ConfigTokio,
    pub source: ConfigChannelSource,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, tag = "transport")]
pub enum ConfigChannelSource {
    #[serde(rename = "quic")]
    Quic(ConfigQuicClient),
    #[serde(rename = "tcp")]
    Tcp(ConfigTcpClient),
    #[serde(rename = "grpc")]
    Grpc(ConfigGrpcClient),
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigApps {
    /// Runtime for incoming connections
    pub tokio: ConfigTokio,
    // grpc, pubsub
}

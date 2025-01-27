use {
    crate::protobuf::ProtobufEncoder,
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    richat_shared::{
        config::{deserialize_num_str, ConfigPrometheus, ConfigTokio},
        transports::{grpc::ConfigGrpcServer, quic::ConfigQuicServer, tcp::ConfigTcpServer},
    },
    serde::{
        de::{self, Deserializer},
        Deserialize,
    },
    std::{fs, path::Path},
};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub libpath: String,
    pub log: ConfigLog,
    pub tokio: ConfigTokio,
    pub channel: ConfigChannel,
    pub quic: Option<ConfigQuicServer>,
    pub tcp: Option<ConfigTcpServer>,
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
#[serde(deny_unknown_fields, default)]
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

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigChannel {
    #[serde(deserialize_with = "ConfigChannel::deserialize_encoder")]
    pub encoder: ProtobufEncoder,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max_messages: usize,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max_slots: usize,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max_bytes: usize,
}

impl Default for ConfigChannel {
    fn default() -> Self {
        Self {
            encoder: ProtobufEncoder::Raw,
            max_messages: 2_097_152, // assume 20k messages per slot, aligned to power of 2
            max_slots: 100,
            max_bytes: 10 * 1024 * 1024 * 1024, // 10GiB, assume 100MiB per slot
        }
    }
}

impl ConfigChannel {
    pub fn deserialize_encoder<'de, D>(deserializer: D) -> Result<ProtobufEncoder, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Deserialize::deserialize(deserializer)? {
            "prost" => Ok(ProtobufEncoder::Prost),
            "raw" => Ok(ProtobufEncoder::Raw),
            value => Err(de::Error::custom(format!(
                "failed to decode encoder: {value}"
            ))),
        }
    }
}

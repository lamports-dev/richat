pub use crate::transports::proto::GrpcSubscribeRequest;
use {
    crate::config::deserialize_usize_str,
    serde::{
        de::{self, Deserializer},
        Deserialize,
    },
    std::{
        fs,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        time::Duration,
    },
    thiserror::Error,
    tonic::{
        codec::CompressionEncoding,
        transport::{
            server::{Server, TcpIncoming},
            Identity, ServerTlsConfig,
        },
    },
};

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigGrpcCompression {
    #[serde(deserialize_with = "ConfigGrpcCompression::deserialize_compression")]
    pub accept: Vec<CompressionEncoding>,
    #[serde(deserialize_with = "ConfigGrpcCompression::deserialize_compression")]
    pub send: Vec<CompressionEncoding>,
}

impl Default for ConfigGrpcCompression {
    fn default() -> Self {
        Self {
            accept: Self::default_compression(),
            send: Self::default_compression(),
        }
    }
}

impl ConfigGrpcCompression {
    fn deserialize_compression<'de, D>(
        deserializer: D,
    ) -> Result<Vec<CompressionEncoding>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<&str>::deserialize(deserializer)?
            .into_iter()
            .map(|value| match value {
                "gzip" => Ok(CompressionEncoding::Gzip),
                "zstd" => Ok(CompressionEncoding::Zstd),
                value => Err(de::Error::custom(format!(
                    "Unknown compression format: {value}"
                ))),
            })
            .collect::<Result<_, _>>()
    }

    fn default_compression() -> Vec<CompressionEncoding> {
        vec![CompressionEncoding::Gzip, CompressionEncoding::Zstd]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigGrpcServer {
    pub endpoint: SocketAddr,
    #[serde(deserialize_with = "ConfigGrpcServer::deserialize_tls_config")]
    pub tls_config: Option<ServerTlsConfig>,
    pub compression: ConfigGrpcCompression,
    /// Limits the maximum size of a decoded message, default is 4MiB
    #[serde(deserialize_with = "deserialize_usize_str")]
    pub max_decoding_message_size: usize,
    #[serde(with = "humantime_serde")]
    pub server_tcp_keepalive: Option<Duration>,
    pub server_tcp_nodelay: bool,
    pub server_http2_adaptive_window: Option<bool>,
    #[serde(with = "humantime_serde")]
    pub server_http2_keepalive_interval: Option<Duration>,
    #[serde(with = "humantime_serde")]
    pub server_http2_keepalive_timeout: Option<Duration>,
    pub server_initial_connection_window_size: Option<u32>,
    pub server_initial_stream_window_size: Option<u32>,
}

impl Default for ConfigGrpcServer {
    fn default() -> Self {
        Self {
            endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10102),
            tls_config: Default::default(),
            compression: Default::default(),
            max_decoding_message_size: 4 * 1024 * 1024, // 4MiB
            server_tcp_keepalive: Some(Duration::from_secs(15)),
            server_tcp_nodelay: true,
            server_http2_adaptive_window: Default::default(),
            server_http2_keepalive_interval: Default::default(),
            server_http2_keepalive_timeout: Default::default(),
            server_initial_connection_window_size: Default::default(),
            server_initial_stream_window_size: Default::default(),
        }
    }
}

impl ConfigGrpcServer {
    fn deserialize_tls_config<'de, D>(deserializer: D) -> Result<Option<ServerTlsConfig>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ConfigTls<'a> {
            cert: &'a str,
            key: &'a str,
        }

        Option::<ConfigTls>::deserialize(deserializer)?
            .map(|config| {
                let cert = fs::read(config.cert).map_err(|error| {
                    de::Error::custom(format!("failed to read cert {}: {error:?}", config.cert))
                })?;
                let key = fs::read(config.key).map_err(|error| {
                    de::Error::custom(format!("failed to read key {}: {error:?}", config.key))
                })?;

                Ok(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))
            })
            .transpose()
    }

    pub fn create_server(&self) -> Result<(TcpIncoming, Server), CreateServerError> {
        // Bind service address
        let incoming = TcpIncoming::new(
            self.endpoint,
            self.server_tcp_nodelay,
            self.server_tcp_keepalive,
        )
        .map_err(|error| CreateServerError::Bind {
            error,
            endpoint: self.endpoint,
        })?;

        // Create service
        let mut server_builder = Server::builder();
        if let Some(tls_config) = self.tls_config.clone() {
            server_builder = server_builder.tls_config(tls_config)?;
        }
        if let Some(enabled) = self.server_http2_adaptive_window {
            server_builder = server_builder.http2_adaptive_window(Some(enabled));
        }
        if let Some(http2_keepalive_interval) = self.server_http2_keepalive_interval {
            server_builder =
                server_builder.http2_keepalive_interval(Some(http2_keepalive_interval));
        }
        if let Some(http2_keepalive_timeout) = self.server_http2_keepalive_timeout {
            server_builder = server_builder.http2_keepalive_timeout(Some(http2_keepalive_timeout));
        }
        if let Some(sz) = self.server_initial_connection_window_size {
            server_builder = server_builder.initial_connection_window_size(sz);
        }
        if let Some(sz) = self.server_initial_stream_window_size {
            server_builder = server_builder.initial_stream_window_size(sz);
        }

        Ok((incoming, server_builder))
    }
}

#[derive(Debug, Error)]
pub enum CreateServerError {
    #[error("failed to bind {endpoint}: {error}")]
    Bind {
        error: Box<dyn std::error::Error + Send + Sync>,
        endpoint: SocketAddr,
    },
    #[error("failed to apply tls_config: {0}")]
    Tls(#[from] tonic::transport::Error),
}

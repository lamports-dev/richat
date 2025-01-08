pub use crate::transports::proto::{
    QuicSubscribeClose, QuicSubscribeCloseError, QuicSubscribeRequest, QuicSubscribeResponse,
    QuicSubscribeResponseError,
};
use {
    crate::config::deserialize_x_token_set,
    quinn::{
        crypto::rustls::{NoInitialCipherSuite, QuicServerConfig},
        Endpoint, VarInt,
    },
    rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    serde::{
        de::{self, Deserializer},
        Deserialize,
    },
    std::{
        collections::HashSet,
        fs, io,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::PathBuf,
        sync::Arc,
    },
    thiserror::Error,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigQuicServer {
    #[serde(default = "ConfigQuicServer::default_endpoint")]
    pub endpoint: SocketAddr,
    #[serde(deserialize_with = "ConfigQuicServer::deserialize_tls_config")]
    pub tls_config: rustls::ServerConfig,
    /// Value in ms
    #[serde(default = "ConfigQuicServer::default_expected_rtt")]
    pub expected_rtt: u32,
    /// Value in bytes/s, default with expected rtt 100 is 100Mbps
    #[serde(default = "ConfigQuicServer::default_max_stream_bandwidth")]
    pub max_stream_bandwidth: u32,
    /// Maximum duration of inactivity to accept before timing out the connection
    #[serde(default = "ConfigQuicServer::default_max_idle_timeout")]
    pub max_idle_timeout: Option<u32>,
    /// Max number of outgoing streams
    #[serde(default = "ConfigQuicServer::default_max_recv_streams")]
    pub max_recv_streams: u32,
    #[serde(deserialize_with = "deserialize_x_token_set")]
    pub x_tokens: HashSet<Vec<u8>>,
}

impl ConfigQuicServer {
    pub const fn default_endpoint() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10100)
    }

    fn deserialize_tls_config<'de, D>(deserializer: D) -> Result<rustls::ServerConfig, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields, untagged)]
        enum Config<'a> {
            Signed { cert: &'a str, key: &'a str },
            SelfSigned { self_signed_alt_names: Vec<String> },
        }

        let (certs, key) = match Config::deserialize(deserializer)? {
            Config::Signed { cert, key } => {
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
            Config::SelfSigned {
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

    const fn default_expected_rtt() -> u32 {
        100
    }

    const fn default_max_stream_bandwidth() -> u32 {
        12_500 * 1000
    }

    const fn default_max_idle_timeout() -> Option<u32> {
        Some(30_000)
    }

    const fn default_max_recv_streams() -> u32 {
        16
    }

    pub fn create_server(&self) -> Result<Endpoint, CreateServerError> {
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(self.tls_config.clone())?,
        ));

        // disallow incoming uni streams
        let transport_config =
            Arc::get_mut(&mut server_config.transport).ok_or(CreateServerError::TransportConfig)?;
        transport_config.max_concurrent_bidi_streams(1u8.into());
        transport_config.max_concurrent_uni_streams(0u8.into());

        // set window size
        let stream_rwnd = self.max_stream_bandwidth / 1_000 * self.expected_rtt;
        transport_config.stream_receive_window(stream_rwnd.into());
        transport_config.send_window(8 * stream_rwnd as u64);
        transport_config.datagram_receive_buffer_size(Some(stream_rwnd as usize));

        // set idle timeout
        transport_config
            .max_idle_timeout(self.max_idle_timeout.map(|ms| VarInt::from_u32(ms).into()));

        Endpoint::server(server_config, self.endpoint).map_err(|error| CreateServerError::Bind {
            error,
            endpoint: self.endpoint,
        })
    }
}

#[derive(Debug, Error)]
pub enum CreateServerError {
    #[error("failed to crate QuicServerConfig")]
    ServerConfig(#[from] NoInitialCipherSuite),
    #[error("failed to modify TransportConfig")]
    TransportConfig,
    #[error("failed to bind {endpoint}: {error}")]
    Bind {
        error: io::Error,
        endpoint: SocketAddr,
    },
}

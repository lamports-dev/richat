use {
    richat_shared::config::{deserialize_maybe_rustls_server_config, deserialize_num_str},
    serde::Deserialize,
    std::{
        io,
        net::{IpAddr, Ipv4Addr, SocketAddr},
    },
    tokio::net::TcpStream,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsPubsub {
    pub endpoint: SocketAddr,
    pub tcp_nodelay: Option<bool>,
    #[serde(deserialize_with = "deserialize_maybe_rustls_server_config")]
    pub tls_config: Option<rustls::ServerConfig>,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub recv_max_message_size: usize,
    pub enable_block_subscription: bool,
    pub enable_vote_subscription: bool,
    pub enable_transaction_subscription: bool,
}

impl Default for ConfigAppsPubsub {
    fn default() -> Self {
        Self {
            endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000),
            tcp_nodelay: None,
            tls_config: None,
            recv_max_message_size: 8_192, // 8KiB
            enable_block_subscription: false,
            enable_vote_subscription: false,
            enable_transaction_subscription: false,
        }
    }
}

impl ConfigAppsPubsub {
    pub fn set_accepted_socket_options(&self, stream: &TcpStream) -> io::Result<()> {
        if let Some(nodelay) = self.tcp_nodelay {
            stream.set_nodelay(nodelay)?;
        }
        Ok(())
    }
}

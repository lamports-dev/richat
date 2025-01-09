pub use crate::transports::proto::TcpSubscribeRequest;
use {
    crate::config::deserialize_x_token_set,
    serde::Deserialize,
    std::{
        collections::HashSet,
        io,
        net::{IpAddr, Ipv4Addr, SocketAddr},
    },
    tokio::net::{TcpListener, TcpSocket},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigTcpServer {
    pub endpoint: SocketAddr,
    pub backlog: u32,
    pub keepalive: Option<bool>,
    pub nodelay: Option<bool>,
    pub send_buffer_size: Option<u32>,
    #[serde(deserialize_with = "deserialize_x_token_set")]
    pub x_tokens: HashSet<Vec<u8>>,
}

impl Default for ConfigTcpServer {
    fn default() -> Self {
        Self {
            endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10101),
            backlog: 1024,
            keepalive: None,
            nodelay: None,
            send_buffer_size: None,
            x_tokens: HashSet::new(),
        }
    }
}

impl ConfigTcpServer {
    pub fn create_server(&self) -> io::Result<TcpListener> {
        let socket = match self.endpoint {
            SocketAddr::V4(_) => TcpSocket::new_v4(),
            SocketAddr::V6(_) => TcpSocket::new_v6(),
        }?;
        socket.bind(self.endpoint)?;

        if let Some(keepalive) = self.keepalive {
            socket.set_keepalive(keepalive)?;
        }
        if let Some(nodelay) = self.nodelay {
            socket.set_nodelay(nodelay)?;
        }
        if let Some(send_buffer_size) = self.send_buffer_size {
            socket.set_send_buffer_size(send_buffer_size)?;
        }

        socket.listen(self.backlog)
    }
}

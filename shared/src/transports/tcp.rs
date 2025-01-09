pub use crate::transports::proto::TcpSubscribeRequest;
use {
    crate::{
        config::deserialize_x_token_set,
        shutdown::Shutdown,
        transports::{
            quic::{
                QuicSubscribeClose, QuicSubscribeCloseError, QuicSubscribeResponse,
                QuicSubscribeResponseError,
            },
            RecvError, RecvStream, Subscribe, SubscribeError,
        },
    },
    futures::stream::StreamExt,
    prost::Message,
    serde::Deserialize,
    std::{
        borrow::Cow,
        collections::HashSet,
        future::Future,
        io,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
    },
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpSocket, TcpStream},
        task::JoinError,
    },
    tracing::{error, info},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigTcpServer {
    pub endpoint: SocketAddr,
    pub backlog: u32,
    pub keepalive: Option<bool>,
    pub nodelay: Option<bool>,
    pub send_buffer_size: Option<u32>,
    /// Max request size in bytes
    pub max_request_size: usize,
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
            max_request_size: 1024,
            x_tokens: HashSet::new(),
        }
    }
}

impl ConfigTcpServer {
    pub fn listen(&self) -> io::Result<TcpListener> {
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

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Prost(#[from] prost::DecodeError),
}

#[derive(Debug)]
pub struct TcpServer;

impl TcpServer {
    pub async fn spawn(
        config: ConfigTcpServer,
        messages: impl Subscribe + Clone + Send + 'static,
        on_conn_new_cb: impl Fn() + Copy + Send + 'static,
        on_conn_drop_cb: impl Fn() + Copy + Send + 'static,
        shutdown: Shutdown,
    ) -> io::Result<impl Future<Output = Result<(), JoinError>>> {
        let listener = config.listen()?;
        info!("start server at {}", config.endpoint);

        Ok(tokio::spawn(async move {
            let max_request_size = config.max_request_size as u64;
            let x_tokens = Arc::new(config.x_tokens);

            let mut id = 0;
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    incoming = listener.accept() => {
                        let socket = match incoming {
                            Ok((socket, addr)) => {
                                info!("#{id}: new connection from {addr:?}");
                                socket
                            }
                            Err(error) => {
                                error!("failed to accept new connection: {error}");
                                break;
                            }
                        };

                        let messages = messages.clone();
                        let x_tokens = Arc::clone(&x_tokens);
                        tokio::spawn(async move {
                            on_conn_new_cb();
                            if let Err(error) = Self::handle_incoming(id, socket, messages, max_request_size, x_tokens).await {
                                error!("#{id}: connection failed: {error}");
                            } else {
                                info!("#{id}: connection closed");
                            }
                            on_conn_drop_cb();
                        });
                        id += 1;
                    }
                    () = &mut shutdown => {
                        info!("shutdown");
                        break
                    },
                }
            }
        }))
    }

    async fn handle_incoming(
        id: u64,
        mut stream: TcpStream,
        messages: impl Subscribe,
        max_request_size: u64,
        x_tokens: Arc<HashSet<Vec<u8>>>,
    ) -> Result<(), ConnectionError> {
        // Read request and subscribe
        let (response, maybe_rx) =
            Self::handle_request(id, &mut stream, messages, max_request_size, x_tokens).await?;

        // Send response
        let buf = response.encode_to_vec();
        stream.write_u64(buf.len() as u64).await?;
        stream.write_all(&buf).await?;

        let Some(mut rx) = maybe_rx else {
            return Ok(());
        };

        // Send loop
        loop {
            match rx.next().await {
                Some(Ok(message)) => {
                    stream.write_u64(message.len() as u64).await?;
                    stream.write_all(&message).await?;
                }
                Some(Err(error)) => {
                    error!("#{id}: failed to get message: {error}");
                    let msg = QuicSubscribeClose {
                        error: match error {
                            RecvError::Lagged => QuicSubscribeCloseError::Lagged,
                            RecvError::Closed => QuicSubscribeCloseError::Closed,
                        } as i32,
                    };
                    let message = msg.encode_to_vec();

                    stream.write_u64(u64::MAX).await?;
                    stream.write_u64(message.len() as u64).await?;
                    stream.write_all(&message).await?;
                }
                None => break,
            }
        }

        Ok(())
    }

    async fn handle_request(
        id: u64,
        stream: &mut TcpStream,
        messages: impl Subscribe,
        max_request_size: u64,
        x_tokens: Arc<HashSet<Vec<u8>>>,
    ) -> Result<(QuicSubscribeResponse, Option<RecvStream>), ConnectionError> {
        // Read request
        let size = stream.read_u64().await?;
        if size > max_request_size {
            let msg = QuicSubscribeResponse {
                error: Some(QuicSubscribeResponseError::RequestSizeTooLarge as i32),
                ..Default::default()
            };
            return Ok((msg, None));
        }
        let mut buf = vec![0; size as usize]; // TODO: use MaybeUninit
        stream.read_exact(buf.as_mut_slice()).await?;

        // Decode request
        let TcpSubscribeRequest { request, x_token } = Message::decode(buf.as_slice())?;

        // verify access token
        if !x_tokens.is_empty() {
            if let Some(error) = match x_token {
                Some(x_token) if !x_tokens.contains(&x_token) => {
                    Some(QuicSubscribeResponseError::XTokenInvalid as i32)
                }
                None => Some(QuicSubscribeResponseError::XTokenRequired as i32),
                _ => None,
            } {
                let msg = QuicSubscribeResponse {
                    error: Some(error),
                    ..Default::default()
                };
                return Ok((msg, None));
            }
        }

        let replay_from_slot = request.and_then(|req| req.replay_from_slot);
        Ok(match messages.subscribe(replay_from_slot) {
            Ok(rx) => {
                let pos = replay_from_slot
                    .map(|slot| format!("slot {slot}").into())
                    .unwrap_or(Cow::Borrowed("latest"));
                info!("#{id}: subscribed from {pos}");
                (QuicSubscribeResponse::default(), Some(rx))
            }
            Err(SubscribeError::NotInitialized) => {
                let msg = QuicSubscribeResponse {
                    error: Some(QuicSubscribeResponseError::NotInitialized as i32),
                    ..Default::default()
                };
                (msg, None)
            }
            Err(SubscribeError::SlotNotAvailable { first_available }) => {
                let msg = QuicSubscribeResponse {
                    error: Some(QuicSubscribeResponseError::SlotNotAvailable as i32),
                    first_available_slot: Some(first_available),
                    ..Default::default()
                };
                (msg, None)
            }
        })
    }
}

use {
    crate::{
        config::{deserialize_rustls_server_config, deserialize_x_token_set},
        shutdown::Shutdown,
        transports::{RecvError, RecvItem, RecvStream, Subscribe, SubscribeError, WriteVectored},
    },
    futures::{
        future::{pending, FutureExt},
        stream::StreamExt,
    },
    prost::Message,
    quinn::{
        crypto::rustls::{NoInitialCipherSuite, QuicServerConfig},
        Connection, Endpoint, Incoming, SendStream, VarInt,
    },
    richat_proto::richat::{
        QuicSubscribeClose, QuicSubscribeCloseError, QuicSubscribeRequest, QuicSubscribeResponse,
        QuicSubscribeResponseError,
    },
    serde::Deserialize,
    std::{
        borrow::Cow,
        collections::{BTreeSet, HashSet, VecDeque},
        future::Future,
        io::{self, IoSlice},
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
    },
    thiserror::Error,
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        task::{JoinError, JoinSet},
    },
    tracing::{error, info},
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigQuicServer {
    #[serde(default = "ConfigQuicServer::default_endpoint")]
    pub endpoint: SocketAddr,
    #[serde(deserialize_with = "deserialize_rustls_server_config")]
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
    /// Max request size in bytes
    #[serde(default = "ConfigQuicServer::default_max_request_size")]
    pub max_request_size: usize,
    #[serde(default, deserialize_with = "deserialize_x_token_set")]
    pub x_tokens: HashSet<Vec<u8>>,
}

impl ConfigQuicServer {
    pub const fn default_endpoint() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10100)
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

    const fn default_max_request_size() -> usize {
        1024
    }

    pub fn create_endpoint(&self) -> Result<Endpoint, CreateEndpointError> {
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(self.tls_config.clone())?,
        ));

        // disallow incoming uni streams
        let transport_config = Arc::get_mut(&mut server_config.transport)
            .ok_or(CreateEndpointError::TransportConfig)?;
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

        Endpoint::server(server_config, self.endpoint).map_err(|error| CreateEndpointError::Bind {
            error,
            endpoint: self.endpoint,
        })
    }
}

#[derive(Debug, Error)]
pub enum CreateEndpointError {
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

#[derive(Debug, Error)]
enum ConnectionError {
    #[error(transparent)]
    QuinnConnection(#[from] quinn::ConnectionError),
    #[error(transparent)]
    QuinnReadExact(#[from] quinn::ReadExactError),
    #[error(transparent)]
    QuinnWrite(#[from] quinn::WriteError),
    #[error(transparent)]
    QuinnClosedStream(#[from] quinn::ClosedStream),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Prost(#[from] prost::DecodeError),
    #[error(transparent)]
    Join(#[from] JoinError),
    #[error("stream is not available")]
    StreamNotAvailable,
}

#[derive(Debug)]
pub struct QuicServer;

impl QuicServer {
    pub async fn spawn(
        config: ConfigQuicServer,
        messages: impl Subscribe + Clone + Send + 'static,
        on_conn_new_cb: impl Fn() + Clone + Send + 'static,
        on_conn_drop_cb: impl Fn() + Clone + Send + 'static,
        shutdown: Shutdown,
    ) -> Result<impl Future<Output = Result<(), JoinError>>, CreateEndpointError> {
        let endpoint = config.create_endpoint()?;
        info!("start server at {}", config.endpoint);

        Ok(tokio::spawn(async move {
            let max_recv_streams = config.max_recv_streams;
            let max_request_size = config.max_request_size as u64;
            let x_tokens = Arc::new(config.x_tokens);

            let mut id = 0;
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    incoming = endpoint.accept() => {
                        let Some(incoming) = incoming else {
                            error!("quic connection closed");
                            break;
                        };

                        let messages = messages.clone();
                        let on_conn_new_cb = on_conn_new_cb.clone();
                        let on_conn_drop_cb = on_conn_drop_cb.clone();
                        let x_tokens = Arc::clone(&x_tokens);
                        tokio::spawn(async move {
                            on_conn_new_cb();
                            if let Err(error) = Self::handle_incoming(
                                id, incoming, messages, max_recv_streams, max_request_size, x_tokens
                            ).await {
                                error!("#{id}: connection failed: {error}");
                            } else {
                                info!("#{id}: connection closed");
                            }
                            on_conn_drop_cb();
                        });
                        id += 1;
                    }
                    () = &mut shutdown => {
                        endpoint.close(0u32.into(), b"shutdown");
                        info!("shutdown");
                        break
                    },
                };
            }
        }))
    }

    async fn handle_incoming(
        id: u64,
        incoming: Incoming,
        messages: impl Subscribe,
        max_recv_streams: u32,
        max_request_size: u64,
        x_tokens: Arc<HashSet<Vec<u8>>>,
    ) -> Result<(), ConnectionError> {
        let conn = incoming.await?;
        info!("#{id}: new connection from {:?}", conn.remote_address());

        // Read request and subscribe
        let (mut send, response, maybe_rx) = Self::handle_request(
            id,
            &conn,
            messages,
            max_recv_streams,
            max_request_size,
            x_tokens,
        )
        .await?;

        // Send response
        let buf = response.encode_to_vec();
        send.write_u64(buf.len() as u64).await?;
        send.write_all(&buf).await?;
        send.flush().await?;

        let Some((recv_streams, max_backlog, mut rx)) = maybe_rx else {
            return Ok(());
        };

        // Open connections
        let mut streams = VecDeque::with_capacity(recv_streams as usize);
        while streams.len() < recv_streams as usize {
            streams.push_back(conn.open_uni().await?);
        }

        // Send loop
        let mut msg_id = 0;
        let mut msg_ids = BTreeSet::new();
        let mut next_message: Option<RecvItem> = None;
        let mut set = JoinSet::new();
        loop {
            if msg_id - msg_ids.first().copied().unwrap_or(msg_id) < max_backlog {
                if let Some(message) = next_message.take() {
                    if let Some(mut stream) = streams.pop_front() {
                        msg_ids.insert(msg_id);
                        set.spawn(async move {
                            WriteVectored::new(
                                &mut stream,
                                &mut [
                                    IoSlice::new(&msg_id.to_be_bytes()),
                                    IoSlice::new(&(message.len() as u64).to_be_bytes()),
                                    IoSlice::new(&message),
                                ],
                            )
                            .await?;
                            Ok::<_, ConnectionError>((msg_id, stream))
                        });
                        msg_id += 1;
                    } else {
                        next_message = Some(message);
                    }
                }
            }

            let rx_recv = if next_message.is_none() {
                rx.next().boxed()
            } else {
                pending().boxed()
            };
            let set_join_next = if !set.is_empty() {
                set.join_next().boxed()
            } else {
                pending().boxed()
            };

            tokio::select! {
                message = rx_recv => {
                    match message {
                        Some(Ok(message)) => next_message = Some(message),
                        Some(Err(error)) => {
                            error!("#{id}: failed to get message: {error}");
                            if streams.is_empty() {
                                let (msg_id, stream) = set.join_next().await.expect("already verified")??;
                                msg_ids.remove(&msg_id);
                                streams.push_back(stream);
                            }
                            let Some(mut stream) = streams.pop_front() else {
                                return Err(ConnectionError::StreamNotAvailable);
                            };

                            let msg = QuicSubscribeClose {
                                error: match error {
                                    RecvError::Lagged => QuicSubscribeCloseError::Lagged,
                                    RecvError::Closed => QuicSubscribeCloseError::Closed,
                                } as i32
                            };
                            let message = msg.encode_to_vec();

                            set.spawn(async move {
                                stream.write_u64(u64::MAX).await?;
                                stream.write_u64(message.len() as u64).await?;
                                stream.write_all(&message).await?;
                                Ok::<_, ConnectionError>((msg_id, stream))
                            });
                        },
                        None => break,
                    }
                },
                result = set_join_next => {
                    let (msg_id, stream) = result.expect("already verified")??;
                    msg_ids.remove(&msg_id);
                    streams.push_back(stream);
                }
            }
        }

        for (_, mut stream) in set.join_all().await.into_iter().flatten() {
            stream.finish()?;
        }
        for mut stream in streams {
            stream.finish()?;
        }
        drop(conn);

        Ok(())
    }

    async fn handle_request(
        id: u64,
        conn: &Connection,
        messages: impl Subscribe,
        max_recv_streams: u32,
        max_request_size: u64,
        x_tokens: Arc<HashSet<Vec<u8>>>,
    ) -> Result<
        (
            SendStream,
            QuicSubscribeResponse,
            Option<(u32, u64, RecvStream)>,
        ),
        ConnectionError,
    > {
        let (send, mut recv) = conn.accept_bi().await?;

        // Read request
        let size = recv.read_u64().await?;
        if size > max_request_size {
            let msg = QuicSubscribeResponse {
                error: Some(QuicSubscribeResponseError::RequestSizeTooLarge as i32),
                ..Default::default()
            };
            return Ok((send, msg, None));
        }
        let mut buf = vec![0; size as usize]; // TODO: use MaybeUninit
        recv.read_exact(buf.as_mut_slice()).await?;

        // Decode request
        let QuicSubscribeRequest {
            x_token,
            recv_streams,
            max_backlog,
            replay_from_slot,
            filter,
        } = Message::decode(buf.as_slice())?;

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
                return Ok((send, msg, None));
            }
        }

        // validate number of streams
        if recv_streams == 0 || recv_streams > max_recv_streams {
            let code = if recv_streams == 0 {
                QuicSubscribeResponseError::ZeroRecvStreams
            } else {
                QuicSubscribeResponseError::ExceedRecvStreams
            };
            let msg = QuicSubscribeResponse {
                error: Some(code as i32),
                max_recv_streams: Some(max_recv_streams),
                ..Default::default()
            };
            return Ok((send, msg, None));
        }

        Ok(match messages.subscribe(replay_from_slot, filter) {
            Ok(rx) => {
                let pos = replay_from_slot
                    .map(|slot| format!("slot {slot}").into())
                    .unwrap_or(Cow::Borrowed("latest"));
                info!("#{id}: subscribed from {pos}");
                (
                    send,
                    QuicSubscribeResponse::default(),
                    Some((
                        recv_streams,
                        max_backlog.map(|x| x as u64).unwrap_or(u64::MAX),
                        rx,
                    )),
                )
            }
            Err(SubscribeError::NotInitialized) => {
                let msg = QuicSubscribeResponse {
                    error: Some(QuicSubscribeResponseError::NotInitialized as i32),
                    ..Default::default()
                };
                (send, msg, None)
            }
            Err(SubscribeError::SlotNotAvailable { first_available }) => {
                let msg = QuicSubscribeResponse {
                    error: Some(QuicSubscribeResponseError::SlotNotAvailable as i32),
                    first_available_slot: Some(first_available),
                    ..Default::default()
                };
                (send, msg, None)
            }
        })
    }
}

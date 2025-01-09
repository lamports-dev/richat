use {
    crate::{
        channel::{Receiver, ReceiverItem, RecvError, Sender, SubscribeError},
        metrics,
    },
    futures::future::{pending, FutureExt},
    log::{error, info},
    prost::Message,
    quinn::{Connection, Incoming, SendStream},
    richat_shared::transports::quic::{
        ConfigQuicServer, QuicSubscribeClose, QuicSubscribeCloseError, QuicSubscribeRequest,
        QuicSubscribeResponse, QuicSubscribeResponseError,
    },
    std::{
        borrow::Cow,
        collections::{BTreeSet, HashSet, VecDeque},
        future::Future,
        io,
        sync::Arc,
    },
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        task::{JoinError, JoinSet},
    },
};

#[derive(Debug)]
pub struct QuicServer;

impl QuicServer {
    pub async fn spawn(
        config: ConfigQuicServer,
        messages: Sender,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> anyhow::Result<impl Future<Output = Result<(), JoinError>>> {
        let endpoint = config.create_server()?;
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
                        let x_tokens = Arc::clone(&x_tokens);
                        tokio::spawn(async move {
                            metrics::connections_total_add(metrics::ConnectionsTransport::Quic);
                            if let Err(error) = Self::handle_incoming(
                                id, incoming, messages, max_recv_streams, max_request_size, x_tokens
                            ).await {
                                error!("#{id}: connection failed: {error}");
                            } else {
                                info!("#{id}: connection closed");
                            }
                            metrics::connections_total_dec(metrics::ConnectionsTransport::Quic);
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
        messages: Sender,
        max_recv_streams: u32,
        max_request_size: u64,
        x_tokens: Arc<HashSet<Vec<u8>>>,
    ) -> anyhow::Result<()> {
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
        let mut next_message: Option<ReceiverItem> = None;
        let mut set = JoinSet::new();
        loop {
            if msg_id - msg_ids.first().copied().unwrap_or(msg_id) < max_backlog {
                if let Some(message) = next_message.take() {
                    if let Some(mut stream) = streams.pop_front() {
                        msg_ids.insert(msg_id);
                        set.spawn(async move {
                            stream.write_u64(msg_id).await?;
                            stream.write_u64(message.len() as u64).await?;
                            stream.write_all(&message).await?;
                            Ok::<_, io::Error>((msg_id, stream))
                        });
                        msg_id += 1;
                    } else {
                        next_message = Some(message);
                    }
                }
            }

            let rx_recv = if next_message.is_none() {
                rx.recv().boxed()
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
                        Ok(message) => next_message = Some(message),
                        Err(error) => {
                            error!("#{id}: failed to get message: {error}");
                            if streams.is_empty() {
                                match set.join_next().await {
                                    Some(Ok(Ok((msg_id, stream)))) => {
                                        msg_ids.remove(&msg_id);
                                        streams.push_back(stream);
                                    },
                                    Some(Ok(Err(error))) => anyhow::bail!("failed to send data: {error}"),
                                    Some(Err(error)) => anyhow::bail!("failed to join sending task: {error}"),
                                    None => unreachable!(),
                                }
                            }
                            let Some(mut stream) = streams.pop_front() else {
                                anyhow::bail!("failed to get stream to close connection");
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
                                Ok::<_, io::Error>((msg_id, stream))
                            });

                            break;
                        },
                    }
                },
                result = set_join_next => match result {
                    Some(Ok(Ok((msg_id, stream)))) => {
                        msg_ids.remove(&msg_id);
                        streams.push_back(stream);
                    },
                    Some(Ok(Err(error))) => anyhow::bail!("failed to send data: {error}"),
                    Some(Err(error)) => anyhow::bail!("failed to join sending task: {error}"),
                    None => unreachable!(),
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
        messages: Sender,
        max_recv_streams: u32,
        max_request_size: u64,
        x_tokens: Arc<HashSet<Vec<u8>>>,
    ) -> anyhow::Result<(
        SendStream,
        QuicSubscribeResponse,
        Option<(u32, u64, Receiver)>,
    )> {
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
            request,
            recv_streams,
            max_backlog,
            x_token,
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

        let replay_from_slot = request.and_then(|req| req.replay_from_slot);
        Ok(match messages.subscribe(replay_from_slot) {
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

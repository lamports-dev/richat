use {
    crate::{
        channel::{Receiver, RecvError, Sender, SubscribeError},
        metrics,
    },
    log::{error, info},
    prost::Message,
    richat_shared::transports::{
        quic::{
            QuicSubscribeClose, QuicSubscribeCloseError, QuicSubscribeResponse,
            QuicSubscribeResponseError,
        },
        tcp::{ConfigTcpServer, TcpSubscribeRequest},
    },
    std::{borrow::Cow, collections::HashSet, future::Future, sync::Arc},
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        task::JoinError,
    },
};

#[derive(Debug)]
pub struct TcpServer;

impl TcpServer {
    pub async fn spawn(
        config: ConfigTcpServer,
        messages: Sender,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> anyhow::Result<impl Future<Output = Result<(), JoinError>>> {
        let listener = config.create_server()?;
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
                            metrics::connections_total_add(metrics::ConnectionsTransport::Tcp);
                            if let Err(error) = Self::handle_incoming(id, socket, messages, max_request_size, x_tokens).await {
                                error!("#{id}: connection failed: {error}");
                            } else {
                                info!("#{id}: connection closed");
                            }
                            metrics::connections_total_dec(metrics::ConnectionsTransport::Tcp);
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
        messages: Sender,
        max_request_size: u64,
        x_tokens: Arc<HashSet<Vec<u8>>>,
    ) -> anyhow::Result<()> {
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
            match rx.recv().await {
                Ok(message) => {
                    stream.write_u64(message.len() as u64).await?;
                    stream.write_all(&message).await?;
                }
                Err(error) => {
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
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_request(
        id: u64,
        stream: &mut TcpStream,
        messages: Sender,
        max_request_size: u64,
        x_tokens: Arc<HashSet<Vec<u8>>>,
    ) -> anyhow::Result<(QuicSubscribeResponse, Option<Receiver>)> {
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

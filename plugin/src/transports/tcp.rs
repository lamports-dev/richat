use {
    crate::channel::{Receiver, RecvError, Sender, SubscribeError},
    log::{error, info},
    prost::Message,
    richat_shared::transports::{
        grpc::GrpcSubscribeRequest,
        quic::{
            QuicSubscribeClose, QuicSubscribeCloseError, QuicSubscribeResponse,
            QuicSubscribeResponseError,
        },
        tcp::ConfigTcpServer,
    },
    std::{borrow::Cow, future::Future},
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
                                error!("failed to accept new connection: {error:?}");
                                break;
                            }
                        };

                        let messages = messages.clone();
                        tokio::spawn(async move {
                            if let Err(error) = Self::handle_incoming(id, socket, messages).await {
                                error!("#{id}: connection failed: {error:?}");
                            } else {
                                info!("#{id}: connection closed");
                            }
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
    ) -> anyhow::Result<()> {
        let Some(mut rx) = Self::handle_request(id, &mut stream, messages).await? else {
            return Ok(());
        };

        loop {
            match rx.recv().await {
                Ok(message) => {
                    stream.write_u64(message.len() as u64).await?;
                    stream.write_all(&message).await?;
                }
                Err(error) => {
                    error!("#{id}: failed to get message: {error:?}");
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
    ) -> anyhow::Result<Option<Receiver>> {
        let size = stream.read_u64().await?;
        let mut buf = vec![0; size as usize];
        stream.read_exact(buf.as_mut_slice()).await?;

        let GrpcSubscribeRequest { replay_from_slot } =
            GrpcSubscribeRequest::decode(buf.as_slice())?;

        let (msg, result) = match messages.subscribe(replay_from_slot) {
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
        };

        let buf = msg.encode_to_vec();
        stream.write_u64(buf.len() as u64).await?;
        stream.write_all(&buf).await?;

        Ok(result)
    }
}

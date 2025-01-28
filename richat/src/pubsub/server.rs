use {
    crate::{
        channel::Messages,
        pubsub::{config::ConfigAppsPubsub, solana::SubscribeMessage},
    },
    fastwebsockets::{
        upgrade::{is_upgrade_request, upgrade, UpgradeFut},
        CloseCode, FragmentCollectorRead, Frame, OpCode, Payload, WebSocketError,
    },
    futures::future::{try_join_all, FutureExt, TryFutureExt},
    http_body_util::{BodyExt, Empty as BodyEmpty},
    hyper::{body::Incoming as BodyIncoming, service::service_fn, Request, Response, StatusCode},
    hyper_util::{
        rt::tokio::{TokioExecutor, TokioIo},
        server::conn::auto::Builder as ServerBuilder,
    },
    richat_shared::shutdown::Shutdown,
    std::{future::Future, net::TcpListener as StdTcpListener, sync::Arc},
    tokio::net::TcpListener,
    tokio_rustls::TlsAcceptor,
    tracing::{error, info, warn},
};

#[derive(Debug)]
pub struct PubSubServer {
    //
}

impl PubSubServer {
    pub fn spawn(
        mut config: ConfigAppsPubsub,
        messages: Messages,
        shutdown: Shutdown,
    ) -> anyhow::Result<impl Future<Output = anyhow::Result<()>>> {
        let acceptor = config
            .tls_config
            .take()
            .map(Arc::new)
            .map(TlsAcceptor::from);

        let std_listener = StdTcpListener::bind(config.endpoint)?;
        std_listener.set_nonblocking(true)?;

        let listener = TcpListener::from_std(std_listener)?;
        info!("start server at {}", config.endpoint);

        // Spawn server
        let server = tokio::spawn(async move {
            let mut id = 0;
            tokio::pin!(shutdown);
            loop {
                // accept connection
                let stream = tokio::select! {
                    incoming = listener.accept() => match incoming {
                        Ok((stream, addr)) => {
                            if let Err(error) = config.set_accepted_socket_options(&stream) {
                                warn!("#{id}: failed to set socket options {error:?}");
                            }
                            info!("#{id}: new connection from {addr:?}");
                            stream
                        }
                        Err(error) => {
                            error!("failed to accept new connection: {error}");
                            break;
                        }
                    },
                    () = &mut shutdown => break,
                };

                // Create service
                let recv_max_message_size = config.recv_max_message_size;
                let enable_block_subscription = config.enable_block_subscription;
                let enable_vote_subscription = config.enable_vote_subscription;
                let enable_transaction_subscription = config.enable_transaction_subscription;
                let service = service_fn(move |req: Request<BodyIncoming>| async move {
                    match (req.uri().path(), is_upgrade_request(&req)) {
                        ("/", true) => match upgrade(req) {
                            Ok((response, ws_fut)) => {
                                tokio::spawn(async move {
                                    if let Err(error) = Self::handle_client(
                                        id,
                                        ws_fut,
                                        recv_max_message_size,
                                        enable_block_subscription,
                                        enable_vote_subscription,
                                        enable_transaction_subscription,
                                    )
                                    .await
                                    {
                                        error!("Error serving WebSocket connection: {error:?}")
                                    }
                                });

                                let (parts, body) = response.into_parts();
                                Ok(Response::from_parts(parts, body.boxed()))
                            }
                            Err(error) => Response::builder()
                                .status(StatusCode::BAD_REQUEST)
                                .body(format!("upgrade error: {error:?}").boxed()),
                        },
                        _ => Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(BodyEmpty::new().boxed()),
                    }
                });

                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let builder = ServerBuilder::new(TokioExecutor::new());
                    let served_result = if let Some(acceptor) = acceptor {
                        acceptor
                            .accept(stream)
                            .map_err(Into::into)
                            .and_then(|stream| {
                                builder
                                    .serve_connection_with_upgrades(TokioIo::new(stream), service)
                            })
                            .await
                    } else {
                        builder
                            .serve_connection_with_upgrades(TokioIo::new(stream), service)
                            .await
                    };

                    if let Err(error) = served_result {
                        error!("Error serving HTTP connection: {error:?}");
                    }
                });

                id += 1;
            }
            Ok::<(), anyhow::Error>(())
        })
        .map_err(anyhow::Error::new)
        .boxed();

        // Wait spawned features
        Ok(try_join_all([server]).map_ok(|_| ()))
    }

    async fn handle_client(
        id: u64,
        ws_fut: UpgradeFut,
        recv_max_message_size: usize,
        enable_block_subscription: bool,
        enable_vote_subscription: bool,
        enable_transaction_subscription: bool,
    ) -> anyhow::Result<()> {
        let mut ws = ws_fut.await?;
        ws.set_max_message_size(recv_max_message_size);
        ws.set_auto_pong(false);
        ws.set_auto_close(false);
        let (rx, mut tx) = ws.split(tokio::io::split);
        let mut ws = FragmentCollectorRead::new(rx);

        loop {
            // read msg
            let frame = ws
                .read_frame(&mut |_| async { Ok::<(), String>(()) })
                .await?;
            let payload = match frame.opcode {
                OpCode::Close => {
                    let frame = create_frame_close(frame)?;
                    tx.write_frame(frame).await?;
                    break;
                }
                OpCode::Ping => {
                    let frame = Frame::pong(frame.payload);
                    tx.write_frame(frame).await?;
                    continue;
                }
                OpCode::Text | OpCode::Binary => frame.payload,
                OpCode::Continuation | OpCode::Pong => continue,
            };

            // parse msg
            let msg = match SubscribeMessage::parse(
                payload.as_ref(),
                enable_block_subscription,
                enable_vote_subscription,
                enable_transaction_subscription,
            ) {
                Ok(Some(msg)) => msg,
                Ok(None) => continue,
                Err(error) => {
                    let vec = serde_json::to_vec(&error).expect("json serialization never fail");
                    tx.write_frame(Frame::text(Payload::Owned(vec))).await?;
                    continue;
                }
            };

            //
        }

        Ok(())
    }
}

fn create_frame_close(frame: Frame) -> Result<Frame, WebSocketError> {
    match frame.payload.len() {
        0 => {}
        1 => return Err(WebSocketError::InvalidCloseFrame),
        _ => {
            let code = CloseCode::from(u16::from_be_bytes(frame.payload[0..2].try_into().unwrap()));

            if std::str::from_utf8(&frame.payload[2..]).is_err() {
                return Err(WebSocketError::InvalidUTF8);
            };

            if !code.is_allowed() {
                return Ok(Frame::close(1002, &frame.payload[2..]));
            }
        }
    };

    Ok(Frame::close_raw(frame.payload.to_owned().into()))
}

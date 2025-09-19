use {
    crate::{
        channel::Messages,
        config::ConfigAppsWorkers,
        metrics,
        pubsub::{
            config::ConfigAppsPubsub,
            notification::{RpcNotification, RpcNotifications},
            solana::{SubscribeConfig, SubscribeMessage, SubscribeMethod},
            tracker::{subscriptions_worker, ClientRequest},
            ClientId, SubscriptionId,
        },
        version::VERSION,
    },
    ::metrics::{counter, gauge},
    fastwebsockets::{
        upgrade::{is_upgrade_request, upgrade, UpgradeFut},
        CloseCode, FragmentCollectorRead, Frame, OpCode, Payload, WebSocketError,
    },
    futures::future::{ready, try_join_all, FutureExt, TryFutureExt},
    http_body_util::{BodyExt, Empty as BodyEmpty},
    hyper::{body::Incoming as BodyIncoming, service::service_fn, Request, Response, StatusCode},
    hyper_util::{
        rt::tokio::{TokioExecutor, TokioIo},
        server::conn::auto::Builder as ServerBuilder,
    },
    jsonrpsee_types::{ResponsePayload, TwoPointZero},
    richat_shared::{jsonrpc::helpers::get_x_subscription_id, shutdown::Shutdown},
    solana_nohash_hasher::IntMap,
    solana_rpc_client_api::response::RpcVersionInfo,
    std::{future::Future, net::TcpListener as StdTcpListener, sync::Arc},
    tokio::{
        net::TcpListener,
        sync::{broadcast, mpsc, oneshot},
    },
    tokio_rustls::TlsAcceptor,
    tracing::{error, info, warn},
};

#[derive(Debug)]
pub struct PubSubServer;

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

        // Clients requests channel
        let (clients_tx, clients_rx) = mpsc::channel(config.clients_requests_channel_size);

        // Spawn subscription channel
        let (notifications, _) = broadcast::channel(config.notifications_messages_max_count);
        let subscriptions_jh = ConfigAppsWorkers::run_once(
            0,
            "richatPSubWrk".to_owned(),
            config.subscriptions_worker_affinity.take(),
            {
                let subscriptions_workers_affinity = config.subscriptions_workers_affinity.take();
                let notifications = notifications.clone();
                move |_index| {
                    subscriptions_worker(
                        messages,
                        clients_rx,
                        config.subscriptions_workers_count,
                        subscriptions_workers_affinity,
                        config.subscriptions_max_clients_request_per_tick,
                        config.subscriptions_max_messages_per_commitment_per_tick,
                        RpcNotifications::new(
                            config.notifications_messages_max_count,
                            config.notifications_messages_max_bytes,
                            notifications,
                        ),
                        config.signatures_cache_max,
                        config.signatures_cache_slots_max,
                    )
                }
            },
            shutdown.clone(),
        )?
        .boxed();

        // Spawn server
        let server_jh = tokio::spawn(async move {
            let mut client_id = 0;
            tokio::pin!(shutdown);
            loop {
                // accept connection
                let stream = tokio::select! {
                    incoming = listener.accept() => match incoming {
                        Ok((stream, addr)) => {
                            if let Err(error) = config.set_accepted_socket_options(&stream) {
                                warn!("#{client_id}: failed to set socket options {error:?}");
                            }
                            info!("#{client_id}: new connection from {addr:?}");
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
                let enable_transaction_subscription = config.enable_transaction_subscription;
                let service = service_fn({
                    let clients_tx = clients_tx.clone();
                    let notifications = notifications.clone();
                    let shutdown = shutdown.clone();
                    move |req: Request<BodyIncoming>| {
                        let clients_tx = clients_tx.clone();
                        let notifications = notifications.subscribe();
                        let shutdown = shutdown.clone();
                        async move {
                            let x_subscription_id: Arc<str> = get_x_subscription_id(req.headers());
                            let connections_total = gauge!(
                                metrics::PUBSUB_CONNECTIONS_TOTAL,
                                "x_subscription_id" => Arc::clone(&x_subscription_id),
                            );

                            match (req.uri().path(), is_upgrade_request(&req)) {
                                ("/", true) => match upgrade(req) {
                                    Ok((response, ws_fut)) => {
                                        tokio::spawn(async move {
                                            connections_total.increment(1);
                                            if let Err(error) = Self::handle_client(
                                                client_id,
                                                x_subscription_id,
                                                ws_fut,
                                                recv_max_message_size,
                                                enable_block_subscription,
                                                enable_transaction_subscription,
                                                clients_tx,
                                                notifications,
                                                shutdown,
                                            )
                                            .await
                                            {
                                                error!("Error serving WebSocket connection: {error:?}")
                                            }
                                            connections_total.decrement(1);
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
                        }
                    }
                });

                let acceptor = acceptor.clone();
                let clients_tx = clients_tx.clone();
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
                    let _ = clients_tx.send(ClientRequest::Remove { client_id }).await;
                });

                client_id += 1;
            }
            Ok::<(), anyhow::Error>(())
        })
        .map_err(anyhow::Error::new)
        .and_then(ready)
        .boxed();

        // Wait spawned features
        Ok(try_join_all([subscriptions_jh, server_jh]).map_ok(|_| ()))
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_client(
        client_id: ClientId,
        x_subscription_id: Arc<str>,
        ws_fut: UpgradeFut,
        recv_max_message_size: usize,
        enable_block_subscription: bool,
        enable_transaction_subscription: bool,
        clients_tx: mpsc::Sender<ClientRequest>,
        mut notifications: broadcast::Receiver<RpcNotification>,
        shutdown: Shutdown,
    ) -> anyhow::Result<()> {
        let mut ws = ws_fut.await?;
        ws.set_max_message_size(recv_max_message_size);
        ws.set_auto_pong(false);
        ws.set_auto_close(false);
        let (ws_rx, mut ws_tx) = ws.split(tokio::io::split);
        let mut ws_rx = FragmentCollectorRead::new(ws_rx);

        let (read_tx, mut read_rx) = mpsc::channel::<WriteRequest>(1);
        let read_fut = tokio::spawn(async move {
            let mut send_frame = None;
            let mut last_frame = false;
            let mut send_fn = |_| async { Ok::<(), String>(()) };
            tokio::pin!(shutdown);
            loop {
                if let Some(frame) = send_frame.take() {
                    let (tx, rx) = oneshot::channel();
                    let msg = WriteRequest::Frame { frame, tx };
                    if read_tx.send(msg).await.is_err() || rx.await.is_err() {
                        last_frame = true
                    }
                }
                if last_frame {
                    break;
                }

                // read msg
                let frame = tokio::select! {
                    frame = ws_rx.read_frame(&mut send_fn) => frame?,
                    () = &mut shutdown => break,
                };
                let payload = match frame.opcode {
                    OpCode::Close => {
                        send_frame = Some(create_frame_close(frame)?);
                        last_frame = true;
                        continue;
                    }
                    OpCode::Ping => {
                        send_frame = Some(Frame::pong(frame.payload));
                        continue;
                    }
                    OpCode::Text | OpCode::Binary => frame.payload,
                    OpCode::Continuation | OpCode::Pong => continue,
                };

                // parse msg
                let message = match SubscribeMessage::parse(
                    payload.as_ref(),
                    enable_block_subscription,
                    enable_transaction_subscription,
                ) {
                    Ok(Some(msg)) => msg,
                    Ok(None) => continue,
                    Err(error) => {
                        let vec =
                            serde_json::to_vec(&error).expect("json serialization never fail");
                        send_frame = Some(Frame::text(Payload::Owned(vec)));
                        continue;
                    }
                };

                // send msg to write fut
                let (tx, rx) = oneshot::channel();
                let msg = WriteRequest::Message { message, tx };
                if read_tx.send(msg).await.is_err() || rx.await.is_err() {
                    last_frame = true
                }
            }
            Ok::<(), anyhow::Error>(())
        })
        .map_err(anyhow::Error::new)
        .and_then(ready);

        let write_fut = tokio::spawn(async move {
            let mut subscriptions = IntMap::<SubscriptionId, SubscribeMethod>::default();
            let maybe_close_reason = loop {
                tokio::select! {
                    message = read_rx.recv() => match message {
                        Some(WriteRequest::Frame { frame, tx }) => {
                            ws_tx.write_frame(frame).await?;
                            let _ = tx.send(());
                        },
                        Some(WriteRequest::Message { message, tx }) => {
                            let result = match message.config {
                                SubscribeConfig::GetVersion => {
                                    let version = solana_version::Version::default();
                                    serde_json::to_value(&RpcVersionInfo {
                                        solana_core: version.to_string(),
                                        feature_set: Some(version.feature_set),
                                    })
                                    .expect("json serialization never fail")
                                },
                                SubscribeConfig::GetVersionRichat => VERSION.create_grpc_version_info().value(),
                                SubscribeConfig::Unsubscribe { id } => {
                                    let (tx, rx) = oneshot::channel();
                                    if clients_tx.send(ClientRequest::Unsubscribe {
                                        client_id,
                                        subscription_id: id,
                                        tx,
                                    }).await.is_err() {
                                        break Some("shutdown".as_bytes());
                                    }
                                    let removed = rx.await?;
                                    if removed {
                                        if let Some(method) = subscriptions.remove(&id) {
                                            gauge!(
                                                metrics::PUBSUB_SUBSCRIPTIONS_TOTAL,
                                                "x_subscription_id" => Arc::clone(&x_subscription_id),
                                                "method" => method.as_str()
                                            )
                                            .decrement(1);
                                        }
                                    }
                                    removed.into()
                                },
                                config => {
                                    let (tx, rx) = oneshot::channel();
                                    if clients_tx.send(ClientRequest::Subscribe {
                                        client_id,
                                        config,
                                        tx,
                                    }).await.is_err() {
                                        break Some("shutdown".as_bytes());
                                    }
                                    let (id, method) = rx.await?;
                                    subscriptions.insert(id, method);
                                    gauge!(
                                        metrics::PUBSUB_SUBSCRIPTIONS_TOTAL,
                                        "x_subscription_id" => Arc::clone(&x_subscription_id),
                                        "method" => method.as_str()
                                    )
                                    .increment(1);
                                    id.into()
                                },
                            };

                            let vec = serde_json::to_vec(&jsonrpsee_types::Response {
                                jsonrpc: Some(TwoPointZero),
                                payload: ResponsePayload::success(result),
                                id: message.id
                            }).expect("json serialization never fail");
                            let frame = Frame::text(Payload::Owned(vec));
                            ws_tx.write_frame(frame).await?;
                            let _ = tx.send(());
                        },
                        None => break None, // means shutdown
                    },
                    message = notifications.recv() => match message {
                        Ok(notification) if subscriptions.contains_key(&notification.subscription_id) => {
                            if notification.is_final {
                                if let Some(method) = subscriptions.remove(&notification.subscription_id) {
                                    gauge!(
                                        metrics::PUBSUB_SUBSCRIPTIONS_TOTAL,
                                        "x_subscription_id" => Arc::clone(&x_subscription_id),
                                        "method" => method.as_str()
                                    )
                                    .decrement(1);
                                }
                            }

                            match notification.json.upgrade() {
                                Some(json) => {
                                    let size = json.len();
                                    let frame = Frame::text(Payload::Borrowed(json.as_bytes()));
                                    ws_tx.write_frame(frame).await?;

                                    counter!(
                                        metrics::PUBSUB_MESSAGES_SENT_COUNT_TOTAL,
                                        "x_subscription_id" => Arc::clone(&x_subscription_id),
                                        "subscription" => notification.method.as_str(),
                                    )
                                    .increment(1);
                                    counter!(
                                        metrics::PUBSUB_MESSAGES_SENT_BYTES_TOTAL,
                                        "x_subscription_id" => Arc::clone(&x_subscription_id),
                                        "subscription" => notification.method.as_str(),
                                    )
                                    .increment(size as u64);
                                },
                                None => {
                                    break Some("lagged: memory".as_bytes())
                                }
                            }
                        },
                        Ok(_) => {},
                        Err(broadcast::error::RecvError::Closed) => break Some("shutdown".as_bytes()),
                        Err(broadcast::error::RecvError::Lagged(_)) => break Some("lagged: len".as_bytes()),
                    }
                }
            };
            if let Some(close_reason) = maybe_close_reason {
                let frame = Frame::close(CloseCode::Away.into(), close_reason);
                ws_tx.write_frame(frame).await?;
            }
            for (_subscription_id, method) in subscriptions {
                gauge!(
                    metrics::PUBSUB_SUBSCRIPTIONS_TOTAL,
                    "x_subscription_id" => Arc::clone(&x_subscription_id),
                    "method" => method.as_str()
                )
                .decrement(1);
            }
            Ok::<(), anyhow::Error>(())
        })
        .map_err(anyhow::Error::new)
        .and_then(ready);

        tokio::try_join!(read_fut, write_fut).map(|((), ())| ())
    }
}

#[allow(clippy::large_enum_variant)]
enum WriteRequest<'a> {
    Frame {
        frame: Frame<'a>,
        tx: oneshot::Sender<()>,
    },
    Message {
        message: SubscribeMessage,
        tx: oneshot::Sender<()>,
    },
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

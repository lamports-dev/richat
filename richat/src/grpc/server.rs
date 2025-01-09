use {
    crate::{
        channel::{message::Message, Messages},
        grpc::{block_meta::BlockMetaStorage, config::ConfigAppsGrpc},
        version::VERSION,
    },
    futures::{
        future::{ready, try_join_all, FutureExt, TryFutureExt},
        stream::Stream,
    },
    richat_shared::shutdown::Shutdown,
    solana_sdk::clock::MAX_PROCESSING_AGE,
    std::{
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        task::{Context, Poll},
        thread,
        time::{Duration, Instant},
    },
    thiserror::Error,
    tokio::{sync::mpsc, time::sleep},
    tonic::{
        service::interceptor::interceptor, Request, Response, Result as TonicResult, Status,
        Streaming,
    },
    tracing::{error, info},
    yellowstone_grpc_proto::geyser::{
        CommitmentLevel, GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
        GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest,
        GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest,
        PongResponse, SubscribeRequest, SubscribeUpdate,
    },
};

pub mod gen {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/geyser.Geyser.rs"));
}

#[derive(Debug, Clone)]
pub struct GrpcServer {
    messages: Messages,
    block_meta: Option<Arc<BlockMetaStorage>>,
    subscribe_id: Arc<AtomicU64>,
    ping_tx: mpsc::Sender<ReceiverStream>,
}

impl GrpcServer {
    pub fn spawn(
        config: ConfigAppsGrpc,
        messages: Messages,
        shutdown: Shutdown,
    ) -> anyhow::Result<impl Future<Output = anyhow::Result<()>>> {
        // Spawn ping messages sender
        let (ping_tx, ping_rx) = mpsc::channel(4_096);
        let ping_jh = tokio::spawn({
            let th = thread::Builder::new().spawn({
                let shutdown = shutdown.clone();
                move || {
                    if let Some(cpus) = config.worker_ping_affinity {
                        affinity::set_thread_affinity(&cpus).expect("failed to set affinity");
                    }
                    Self::worker_send_ping(ping_rx, shutdown)
                }
            })?;

            let shutdown = shutdown.clone();
            async move {
                while !th.is_finished() {
                    let ms = if shutdown.is_set() { 10 } else { 2_000 };
                    sleep(Duration::from_millis(ms)).await;
                }
                th.join().expect("failed to join thread")
            }
        })
        .map_err(anyhow::Error::new)
        .and_then(ready)
        .boxed();

        // Create gRPC server
        let (incoming, server_builder) = config.server.create_server_builder()?;
        info!("start server at {}", config.server.endpoint);

        let (block_meta, block_meta_jh) = if config.unary.enabled {
            let (meta, jh) = BlockMetaStorage::new(config.unary.requests_queue_size);
            (Some(Arc::new(meta)), jh.boxed())
        } else {
            (None, ready(Ok(())).boxed())
        };

        let grpc_server = Self {
            messages,
            block_meta,
            subscribe_id: Arc::new(AtomicU64::new(0)),
            ping_tx,
        };

        let mut service = gen::geyser_server::GeyserServer::new(grpc_server.clone())
            .max_decoding_message_size(config.server.max_decoding_message_size);
        for encoding in config.server.compression.accept {
            service = service.accept_compressed(encoding);
        }
        for encoding in config.server.compression.send {
            service = service.send_compressed(encoding);
        }

        // Spawn workers pool
        let threads = config
            .workers
            .run(
                |index| format!("grpcWrk{index:02}"),
                move |index| grpc_server.worker_messages(index),
                shutdown.clone(),
            )
            .boxed();

        // Spawn server
        let server = tokio::spawn(async move {
            if let Err(error) = server_builder
                .layer(interceptor(move |request: Request<()>| {
                    if config.x_token.is_empty() {
                        Ok(request)
                    } else {
                        match request.metadata().get("x-token") {
                            Some(token) if config.x_token.contains(token.as_bytes()) => Ok(request),
                            _ => Err(Status::unauthenticated("No valid auth token")),
                        }
                    }
                }))
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await
            {
                error!("server error: {error:?}")
            } else {
                info!("shutdown")
            }
        })
        .map_err(anyhow::Error::new)
        .boxed();

        // Wait spawned features
        Ok(try_join_all([threads, server, block_meta_jh, ping_jh]).map_ok(|_| ()))
    }

    fn parse_commitment(commitment: Option<i32>) -> Result<CommitmentLevel, Status> {
        let commitment = commitment.unwrap_or(CommitmentLevel::Processed as i32);
        CommitmentLevel::try_from(commitment)
            .map(Into::into)
            .map_err(|_error| {
                let msg = format!("failed to create CommitmentLevel from {commitment:?}");
                Status::unknown(msg)
            })
            .and_then(|commitment| {
                if matches!(
                    commitment,
                    CommitmentLevel::Processed
                        | CommitmentLevel::Confirmed
                        | CommitmentLevel::Finalized
                ) {
                    Ok(commitment)
                } else {
                    Err(Status::unknown(
                        "only Processed, Confirmed and Finalized are allowed",
                    ))
                }
            })
    }

    async fn with_block_meta<'a, T, F>(
        &'a self,
        f: impl FnOnce(&'a BlockMetaStorage) -> F,
    ) -> TonicResult<Response<T>>
    where
        F: Future<Output = TonicResult<T>> + 'a,
    {
        if let Some(storage) = &self.block_meta {
            f(storage).await.map(Response::new)
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    fn worker_messages(&self, index: usize) -> anyhow::Result<()> {
        let mut receiver = self.messages.subscribe();
        loop {
            let Some(message) = receiver.try_recv()? else {
                continue;
            };

            // Update block meta only from first thread
            if index == 0 {
                if let Some(block_meta_storage) = &self.block_meta {
                    if matches!(message.as_ref(), Message::Slot(_) | Message::BlockMeta(_)) {
                        block_meta_storage.push(Arc::clone(&message));
                    }
                }
            }

            todo!()
        }
    }

    fn worker_send_ping(
        mut rx: mpsc::Receiver<ReceiverStream>,
        shutdown: Shutdown,
    ) -> anyhow::Result<()> {
        const PING_INTERVAL: Duration = Duration::from_secs(15);

        let mut streams = vec![];
        let mut ts_msg = Instant::now();
        while !shutdown.is_set() {
            match rx.try_recv() {
                Ok(rx) => streams.push(rx),
                Err(mpsc::error::TryRecvError::Disconnected) => break,
                Err(mpsc::error::TryRecvError::Empty) => {
                    if ts_msg.elapsed() > PING_INTERVAL {
                        ts_msg = Instant::now();
                        streams.retain(|rx| rx.send_ping().is_ok());
                    } else {
                        thread::sleep(Duration::from_millis(10));
                    }
                }
            }
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl gen::geyser_server::Geyser for GrpcServer {
    type SubscribeStream = ReceiverStream;

    async fn subscribe(
        &self,
        _request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let _id = self.subscribe_id.fetch_add(1, Ordering::Relaxed);

        let rx = ReceiverStream;
        if self.ping_tx.send(rx.clone()).await.is_err() {
            return Err(Status::unavailable(
                "failed to send receive stream to ping worker",
            ));
        }

        //

        todo!()
    }

    async fn ping(&self, request: Request<PingRequest>) -> TonicResult<Response<PongResponse>> {
        let count = request.get_ref().count;
        let response = PongResponse { count };
        Ok(Response::new(response))
    }

    async fn get_latest_blockhash(
        &self,
        request: Request<GetLatestBlockhashRequest>,
    ) -> TonicResult<Response<GetLatestBlockhashResponse>> {
        let commitment = Self::parse_commitment(request.get_ref().commitment)?;
        self.with_block_meta(|storage| async move {
            let block = storage.get_block(commitment).await?;
            Ok(GetLatestBlockhashResponse {
                slot: block.slot,
                blockhash: block.blockhash.as_ref().clone(),
                last_valid_block_height: block.block_height + MAX_PROCESSING_AGE as u64,
            })
        })
        .await
    }

    async fn get_block_height(
        &self,
        request: Request<GetBlockHeightRequest>,
    ) -> TonicResult<Response<GetBlockHeightResponse>> {
        let commitment = Self::parse_commitment(request.get_ref().commitment)?;
        self.with_block_meta(|storage| async move {
            let block = storage.get_block(commitment).await?;
            Ok(GetBlockHeightResponse {
                block_height: block.block_height,
            })
        })
        .await
    }

    async fn get_slot(
        &self,
        request: Request<GetSlotRequest>,
    ) -> TonicResult<Response<GetSlotResponse>> {
        let commitment = Self::parse_commitment(request.get_ref().commitment)?;
        self.with_block_meta(|storage| async move {
            let block = storage.get_block(commitment).await?;
            Ok(GetSlotResponse { slot: block.slot })
        })
        .await
    }

    async fn is_blockhash_valid(
        &self,
        request: tonic::Request<IsBlockhashValidRequest>,
    ) -> TonicResult<Response<IsBlockhashValidResponse>> {
        let commitment = Self::parse_commitment(request.get_ref().commitment)?;
        self.with_block_meta(|storage| async move {
            let (valid, slot) = storage
                .is_blockhash_valid(request.into_inner().blockhash, commitment)
                .await?;
            Ok(IsBlockhashValidResponse { valid, slot })
        })
        .await
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> TonicResult<Response<GetVersionResponse>> {
        Ok(Response::new(GetVersionResponse {
            version: VERSION.create_grpc_version_info().json(),
        }))
    }
}

#[derive(Debug, Error)]
enum SendError {
    // #[error("channel is full")]
    // Full,
    // #[error("channel closed")]
    // Closed,
}

#[derive(Debug, Clone)]
pub struct ReceiverStream;

impl ReceiverStream {
    // fn send_update(&self) -> Result<(), SendError> {
    //     Ok(())
    // }

    const fn send_ping(&self) -> Result<(), SendError> {
        Ok(())
    }

    // fn send_pong(&self) -> Result<(), SendError> {
    //     Ok(())
    // }
}

impl Stream for ReceiverStream {
    type Item = TonicResult<SubscribeUpdate>;

    #[allow(unused)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

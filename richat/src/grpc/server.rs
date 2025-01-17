use {
    crate::{
        channel::{Messages, ParsedMessage},
        grpc::{block_meta::BlockMetaStorage, config::ConfigAppsGrpc},
        version::VERSION,
    },
    futures::{
        future::{ready, try_join_all, FutureExt, TryFutureExt},
        stream::Stream,
    },
    richat_filter::{
        config::{ConfigFilter, ConfigLimits as ConfigFilterLimits},
        filter::Filter,
    },
    richat_proto::geyser::{
        CommitmentLevel, GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
        GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest,
        GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest,
        PongResponse, SubscribeRequest,
    },
    richat_shared::shutdown::Shutdown,
    solana_sdk::clock::MAX_PROCESSING_AGE,
    std::{
        collections::{LinkedList, VecDeque},
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex, MutexGuard,
        },
        task::{Context, Poll},
    },
    tonic::{
        service::interceptor::interceptor, Request, Response, Result as TonicResult, Status,
        Streaming,
    },
    tracing::{debug, error, info},
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
    filter_limits: Arc<ConfigFilterLimits>,
    subscribe_id: Arc<AtomicU64>,
    subscribe_clients: Arc<Mutex<VecDeque<SubscribeClient>>>,
}

impl GrpcServer {
    pub fn spawn(
        config: ConfigAppsGrpc,
        messages: Messages,
        shutdown: Shutdown,
    ) -> anyhow::Result<impl Future<Output = anyhow::Result<()>>> {
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
            filter_limits: Arc::new(config.filter_limits),
            subscribe_id: Arc::new(AtomicU64::new(0)),
            subscribe_clients: Arc::new(Mutex::new(VecDeque::new())),
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
        Ok(try_join_all([block_meta_jh, threads, server]).map_ok(|_| ()))
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

    #[inline]
    fn subscribe_clients_lock(&self) -> MutexGuard<'_, VecDeque<SubscribeClient>> {
        match self.subscribe_clients.lock() {
            Ok(guard) => guard,
            Err(error) => error.into_inner(),
        }
    }

    #[inline]
    fn push_client(&self, client: SubscribeClient) {
        self.subscribe_clients_lock().push_back(client);
    }

    #[inline]
    fn pop_client(&self) -> Option<SubscribeClient> {
        self.subscribe_clients_lock().pop_front()
    }

    fn worker_messages(&self, index: usize) -> anyhow::Result<()> {
        let mut receiver = self.messages.to_receiver();
        let mut head = self.messages.get_current_tail();
        loop {
            let Some(message) = receiver.try_recv(head)? else {
                continue;
            };
            head += 1;

            // Update block meta only from first thread
            if index == 0 {
                if let Some(block_meta_storage) = &self.block_meta {
                    if matches!(
                        message,
                        ParsedMessage::Slot(_) | ParsedMessage::BlockMeta(_)
                    ) {
                        block_meta_storage.push(message.clone());
                    }
                }
            }

            // todo!()
        }
    }
}

#[tonic::async_trait]
impl gen::geyser_server::Geyser for GrpcServer {
    type SubscribeStream = ReceiverStream;

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let id = self.subscribe_id.fetch_add(1, Ordering::Relaxed);
        info!(id, "new client");

        let client = SubscribeClient::new(id);
        self.push_client(client.clone());

        tokio::spawn({
            let mut stream = request.into_inner();
            let client = client.clone();
            let limits = Arc::clone(&self.filter_limits);
            async move {
                loop {
                    match stream.message().await {
                        Ok(Some(message)) => {
                            let new_filter = ConfigFilter::try_from(message)
                                .map_err(|error| {
                                    Status::invalid_argument(format!(
                                        "failed to create filter: {error:?}"
                                    ))
                                })
                                .and_then(|config| {
                                    limits
                                        .check_filter(&config)
                                        .map(|()| Filter::new(&config))
                                        .map_err(|error| {
                                            Status::invalid_argument(format!(
                                                "failed to check filter: {error:?}"
                                            ))
                                        })
                                });

                            let mut state = client.state_lock();
                            match new_filter {
                                Ok(filter) => {
                                    // TODO: update id
                                    state.filter = Some(filter);
                                }
                                Err(error) => {
                                    state.messages.push_back(Err(error));
                                    break;
                                }
                            }
                        }
                        Ok(None) => debug!(id, "incoming stream finished"),
                        Err(error) => {
                            error!(id, %error, "error to receive new filter");
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream { client }))
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

#[derive(Debug, Clone)]
struct SubscribeClient {
    id: u64,
    state: Arc<Mutex<SubscribeClientState>>,
}

impl Drop for SubscribeClient {
    fn drop(&mut self) {
        info!(id = self.id, "drop client rx stream");
    }
}

impl SubscribeClient {
    fn new(id: u64) -> Self {
        Self {
            id,
            state: Arc::new(Mutex::new(SubscribeClientState::default())),
        }
    }

    #[inline]
    fn state_lock(&self) -> MutexGuard<'_, SubscribeClientState> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(error) => error.into_inner(),
        }
    }
}

#[derive(Debug, Default)]
struct SubscribeClientState {
    head: u64,
    filter: Option<Filter>,
    messages: LinkedList<TonicResult<Vec<u8>>>,
    messages_total_len: usize,
}

// #[derive(Debug, Error)]
// enum SendError {
//     // #[error("channel is full")]
//     // Full,
//     // #[error("channel closed")]
//     // Closed,
// }

#[derive(Debug)]
pub struct ReceiverStream {
    client: SubscribeClient,
}

impl ReceiverStream {
    // fn send_update(&self) -> Result<(), SendError> {
    //     Ok(())
    // }

    // fn send_pong(&self) -> Result<(), SendError> {
    //     Ok(())
    // }
}

impl Stream for ReceiverStream {
    type Item = TonicResult<Vec<u8>>;

    #[allow(unused)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

use {
    crate::{
        channel::{Messages, ParsedMessage, RecvError},
        config::ConfigAppsWorkers,
        grpc::{block_meta::BlockMetaStorage, config::ConfigAppsGrpc},
        version::VERSION,
    },
    futures::{
        future::{ready, try_join_all, FutureExt, TryFutureExt},
        stream::Stream,
    },
    prost::Message,
    richat_filter::{
        config::{ConfigFilter, ConfigLimits as ConfigFilterLimits},
        filter::Filter,
    },
    richat_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel as CommitmentLevelProto,
        GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
        GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest,
        GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest,
        PongResponse, SubscribeRequest, SubscribeRequestPing, SubscribeUpdate, SubscribeUpdatePong,
    },
    richat_shared::shutdown::Shutdown,
    solana_sdk::{clock::MAX_PROCESSING_AGE, commitment_config::CommitmentLevel},
    std::{
        collections::{LinkedList, VecDeque},
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex, MutexGuard,
        },
        task::{Context, Poll, Waker},
        thread,
        time::{Duration, SystemTime},
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
    subscribe_messages_len_max: usize,
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

        // BlockMeta thread & task
        let (block_meta, block_meta_jh, block_meta_task_jh) = if config.unary.enabled {
            let (meta, task_jh) = BlockMetaStorage::new(config.unary.requests_queue_size);

            let jh = ConfigAppsWorkers::run_once(
                "grpcWrkBM".to_owned(),
                vec![config.unary.affinity],
                {
                    let messages = messages.clone();
                    let meta = meta.clone();
                    let shutdown = shutdown.clone();
                    move || Self::worker_block_meta(messages, meta, shutdown)
                },
                shutdown.clone(),
            )?;

            (Some(Arc::new(meta)), jh.boxed(), task_jh.boxed())
        } else {
            (None, ready(Ok(())).boxed(), ready(Ok(())).boxed())
        };

        // gRPC service
        let grpc_server = Self {
            messages,
            block_meta,
            filter_limits: Arc::new(config.filter_limits),
            subscribe_id: Arc::new(AtomicU64::new(0)),
            subscribe_clients: Arc::new(Mutex::new(VecDeque::new())),
            subscribe_messages_len_max: config.stream.messages_len_max,
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
        let workers = config
            .workers
            .run(
                |index| format!("grpcWrk{index:02}"),
                move || {
                    grpc_server.worker_messages(
                        config.stream.messages_max_per_tick,
                        config.stream.ping_iterval,
                    )
                },
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
        Ok(try_join_all([block_meta_jh, block_meta_task_jh, workers, server]).map_ok(|_| ()))
    }

    fn parse_commitment(commitment: Option<i32>) -> Result<CommitmentLevelProto, Status> {
        let commitment = commitment.unwrap_or(CommitmentLevelProto::Processed as i32);
        CommitmentLevelProto::try_from(commitment)
            .map(Into::into)
            .map_err(|_error| {
                let msg = format!("failed to create CommitmentLevel from {commitment:?}");
                Status::unknown(msg)
            })
            .and_then(|commitment| {
                if matches!(
                    commitment,
                    CommitmentLevelProto::Processed
                        | CommitmentLevelProto::Confirmed
                        | CommitmentLevelProto::Finalized
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

    fn worker_block_meta(
        messages: Messages,
        block_meta: BlockMetaStorage,
        shutdown: Shutdown,
    ) -> anyhow::Result<()> {
        let receiver = messages.to_receiver();
        let mut head = messages
            .get_current_tail(CommitmentLevel::Processed, None)
            .ok_or(anyhow::anyhow!(
                "failed to get head position for block meta worker"
            ))?;

        let mut counter = 0;
        const COUNTER_LIMIT: i32 = 10_000;
        loop {
            counter += 1;
            if counter > COUNTER_LIMIT {
                counter = 0;
                if shutdown.is_set() {
                    break;
                }
            }

            let Some(message) = receiver.try_recv(CommitmentLevel::Processed, head)? else {
                counter = COUNTER_LIMIT;
                thread::sleep(Duration::from_millis(2));
                continue;
            };
            head += 1;

            if matches!(
                message,
                ParsedMessage::Slot(_) | ParsedMessage::BlockMeta(_)
            ) {
                block_meta.push(message);
            }
        }

        Ok(())
    }

    fn worker_messages(
        &self,
        messages_max_per_tick: usize,
        ping_interval: Duration,
    ) -> anyhow::Result<()> {
        let receiver = self.messages.to_receiver();
        loop {
            // get client and state
            let Some(client) = self.pop_client() else {
                continue;
            };
            let mut state = client.state_lock();

            // send ping
            let ts = SystemTime::now();
            if !state.is_full()
                && ts.duration_since(state.ping_ts_latest).unwrap_or_default() > ping_interval
            {
                state.ping_ts_latest = ts;
                let message = SubscribeClientState::create_ping(state.ping_id);
                state.push_message(message);
                state.ping_id += 1;
            }

            // filter messages
            let Some(filter) = state.filter.as_ref() else {
                continue;
            };
            let mut counter = 0;
            while !state.is_full() && counter < messages_max_per_tick {
                let message = match receiver.try_recv(state.commitment, state.head) {
                    Ok(Some(message)) => {
                        counter += 1;
                        state.head += 1;
                        message
                    }
                    Ok(None) => break,
                    Err(RecvError::Lagged) => {
                        let item = Err(Status::data_loss("lagged"));
                        state.push_message(item);
                        break;
                    }
                };

                //
            }
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

        let client = SubscribeClient::new(id, self.subscribe_messages_len_max);
        self.push_client(client.clone());

        tokio::spawn({
            let mut stream = request.into_inner();
            let limits = Arc::clone(&self.filter_limits);
            let client = client.clone();
            let messages = self.messages.clone();
            async move {
                loop {
                    match stream.message().await {
                        Ok(Some(message)) => {
                            if let Some(SubscribeRequestPing { id }) = message.ping {
                                let message = SubscribeClientState::create_ping(id);
                                let mut state = client.state_lock();
                                state.push_message(message);
                                continue;
                            }

                            let subscribe_from_slot = message.from_slot;
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
                            if let Err(error) = new_filter.map(|filter| {
                                state.commitment = filter.commitment().into();
                                state.head = messages
                                    .get_current_tail(state.commitment, subscribe_from_slot)
                                    .ok_or(Status::invalid_argument(format!(
                                        "failed to get slot {subscribe_from_slot:?}"
                                    )))?;
                                state.filter = Some(filter);
                                Ok::<(), Status>(())
                            }) {
                                state.push_message(Err(error));
                                break;
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

        Ok(Response::new(ReceiverStream::new(client)))
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
    fn new(id: u64, messages_len_max: usize) -> Self {
        Self {
            id,
            state: Arc::new(Mutex::new(SubscribeClientState::new(messages_len_max))),
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

#[derive(Debug)]
struct SubscribeClientState {
    commitment: CommitmentLevel,
    head: u64,
    filter: Option<Filter>,
    messages_len_max: usize,
    messages_len_total: usize,
    messages: LinkedList<TonicResult<Vec<u8>>>,
    messages_waker: Option<Waker>,
    ping_id: i32,
    ping_ts_latest: SystemTime,
}

impl SubscribeClientState {
    fn new(messages_len_max: usize) -> Self {
        Self {
            commitment: CommitmentLevel::default(),
            head: 0,
            filter: None,
            messages_len_max,
            messages_len_total: 0,
            messages: LinkedList::new(),
            messages_waker: None,
            ping_id: 0,
            ping_ts_latest: SystemTime::now(),
        }
    }

    fn create_ping(id: i32) -> TonicResult<Vec<u8>> {
        Ok(SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Pong(SubscribeUpdatePong { id })),
            created_at: Some(SystemTime::now().into()),
        }
        .encode_to_vec())
    }

    const fn is_full(&self) -> bool {
        self.messages_len_max > self.messages_len_total
    }

    fn push_message(&mut self, message: TonicResult<Vec<u8>>) {
        self.messages_len_total += message.as_ref().map(|v| v.len()).unwrap_or_default();
        self.messages.push_back(message);
        if let Some(waker) = self.messages_waker.take() {
            waker.wake();
        }
    }

    fn pop_message(&mut self, cx: &Context) -> Option<TonicResult<Vec<u8>>> {
        if let Some(message) = self.messages.pop_front() {
            self.messages_len_total -= message.as_ref().map(|v| v.len()).unwrap_or_default();
            Some(message)
        } else {
            self.messages_waker = Some(cx.waker().clone());
            None
        }
    }
}

#[derive(Debug)]
pub struct ReceiverStream {
    client: SubscribeClient,
    finished: bool,
}

impl ReceiverStream {
    const fn new(client: SubscribeClient) -> Self {
        Self {
            client,
            finished: false,
        }
    }
}

impl Stream for ReceiverStream {
    type Item = TonicResult<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let mut state = self.client.state_lock();
        if let Some(item) = state.pop_message(cx) {
            drop(state);
            self.finished = item.is_err();
            Poll::Ready(Some(item))
        } else {
            Poll::Pending
        }
    }
}

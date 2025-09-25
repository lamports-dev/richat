use {
    crate::{
        channel::{IndexLocation, Messages, ParsedMessage, ReceiverSync},
        config::ConfigAppsWorkers,
        grpc::{block_meta::BlockMetaStorage, config::ConfigAppsGrpc},
        metrics::{self, GrpcSubscribeMessage},
        version::VERSION,
    },
    ::metrics::{counter, gauge, Gauge},
    futures::{
        future::{ready, try_join_all, FutureExt, TryFutureExt},
        stream::Stream,
    },
    prost::Message,
    quanta::Instant,
    richat_filter::{
        config::{
            ConfigFilter, ConfigFilterAccounts, ConfigFilterSlots,
            ConfigLimits as ConfigFilterLimits,
        },
        filter::Filter,
        message::MessageRef,
    },
    richat_metrics::duration_to_seconds,
    richat_proto::{
        geyser::{
            subscribe_update::UpdateOneof, CommitmentLevel as CommitmentLevelProto,
            GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
            GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest,
            GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest,
            PongResponse, SubscribeReplayInfoRequest, SubscribeReplayInfoResponse,
            SubscribeRequest, SubscribeUpdate, SubscribeUpdatePing, SubscribeUpdatePong,
        },
        richat::SubscribeRequestJup,
    },
    richat_shared::{
        jsonrpc::helpers::X_SUBSCRIPTION_ID, mutex_lock, shutdown::Shutdown, transports::RecvError,
    },
    smallvec::SmallVec,
    solana_sdk::{
        clock::{Slot, MAX_PROCESSING_AGE},
        commitment_config::CommitmentLevel,
        pubkey::Pubkey,
    },
    std::{
        borrow::Cow,
        collections::{HashSet, LinkedList, VecDeque},
        fmt,
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex, MutexGuard,
        },
        task::{Context, Poll, Waker},
        thread::sleep,
        time::{Duration, SystemTime},
    },
    tonic::{
        service::interceptor::InterceptorLayer, Request, Response, Result as TonicResult, Status,
        Streaming,
    },
    tracing::{error, info, warn},
};

pub mod gen {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/geyser.Geyser.rs"));
}

#[derive(Debug, Clone)]
pub struct GrpcServer {
    shutdown: Shutdown,
    messages: Messages,
    block_meta: Option<Arc<BlockMetaStorage>>,
    filter_limits: Arc<ConfigFilterLimits>,
    ping_iterval: Duration,
    subscribe_id: Arc<AtomicU64>,
    subscribe_clients: Arc<Mutex<VecDeque<SubscribeClient>>>,
    subscribe_messages_len_max: usize,
    subscribe_messages_replay_len_max: usize,
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
                0,
                "richatGrpcWrkBM".to_owned(),
                config.unary.affinity,
                {
                    let messages = messages.clone();
                    let meta = meta.clone();
                    let shutdown = shutdown.clone();
                    move |_index| Self::worker_block_meta(messages, meta, shutdown)
                },
                shutdown.clone(),
            )?;

            (Some(Arc::new(meta)), jh.boxed(), task_jh.boxed())
        } else {
            (None, ready(Ok(())).boxed(), ready(Ok(())).boxed())
        };

        // gRPC service
        let grpc_server = Self {
            shutdown: shutdown.clone(),
            messages,
            block_meta,
            filter_limits: Arc::new(config.filter_limits),
            ping_iterval: config.stream.ping_iterval,
            subscribe_id: Arc::new(AtomicU64::new(0)),
            subscribe_clients: Arc::new(Mutex::new(VecDeque::new())),
            subscribe_messages_len_max: config.stream.messages_len_max,
            subscribe_messages_replay_len_max: config.stream.messages_replay_len_max,
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
            .threads
            .run(
                |index| format!("richatGrpcWrk{index:02}"),
                {
                    let shutdown = shutdown.clone();
                    move |index| {
                        grpc_server.worker_messages(
                            index,
                            config.workers.messages_cached_max,
                            config.stream.messages_max_per_tick,
                            shutdown,
                        )
                    }
                },
                shutdown.clone(),
            )
            .boxed();

        // Spawn server
        let server = tokio::spawn(async move {
            if let Err(error) = server_builder
                .layer(InterceptorLayer::new(move |request: Request<()>| {
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
                info!("gRPC server shutdown")
            }
        })
        .map_err(anyhow::Error::new)
        .boxed();

        // Wait spawned features
        Ok(try_join_all([block_meta_jh, block_meta_task_jh, workers, server]).map_ok(|_| ()))
    }

    fn get_x_subscription_id<T>(request: &Request<T>) -> String {
        request
            .metadata()
            .get(X_SUBSCRIPTION_ID)
            .and_then(|value| value.to_str().ok().map(ToOwned::to_owned))
            .unwrap_or_default()
    }

    fn parse_commitment(commitment: Option<i32>) -> Result<CommitmentLevelProto, Status> {
        let commitment = commitment.unwrap_or(CommitmentLevelProto::Processed as i32);
        CommitmentLevelProto::try_from(commitment)
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
        mutex_lock(&self.subscribe_clients)
    }

    #[inline]
    fn push_client(&self, client: SubscribeClient) {
        self.subscribe_clients_lock().push_back(client);
    }

    #[inline]
    fn pop_client(&self, prev_client: Option<SubscribeClient>) -> Option<SubscribeClient> {
        let mut state = self.subscribe_clients_lock();
        if let Some(client) = prev_client {
            state.push_back(client);
        }
        state.pop_front()
    }

    fn worker_block_meta(
        messages: Messages,
        block_meta: BlockMetaStorage,
        shutdown: Shutdown,
    ) -> anyhow::Result<()> {
        let receiver = messages.to_receiver();
        let mut head = messages.get_current_tail(CommitmentLevel::Processed) + 1;

        const COUNTER_LIMIT: i32 = 10_000;
        let mut counter = 0;
        loop {
            counter += 1;
            if counter > COUNTER_LIMIT {
                counter = 0;
                if shutdown.is_set() {
                    info!("gRPC block meta thread shutdown");
                    return Ok(());
                }
            }

            let Some(message) = receiver.try_recv(CommitmentLevel::Processed, head)? else {
                counter = COUNTER_LIMIT;
                sleep(Duration::from_micros(100));
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
    }

    fn worker_messages(
        &self,
        index: usize,
        messages_cached_max: usize,
        messages_max_per_tick: usize,
        shutdown: Shutdown,
    ) -> anyhow::Result<()> {
        let messages_cached_max = messages_cached_max.next_power_of_two();
        let mut messages_cache_processed = MessagesCache::new(messages_cached_max);
        let mut messages_cache_confirmed = MessagesCache::new(messages_cached_max);
        let mut messages_cache_finalized = MessagesCache::new(messages_cached_max);

        let receiver = self.messages.to_receiver();
        const SHUTDOWN_COUNTER_LIMIT: i32 = 50_000;
        let mut shutdown_counter = 0;
        let mut prev_client = None;
        loop {
            shutdown_counter += 1;
            if shutdown_counter > SHUTDOWN_COUNTER_LIMIT {
                shutdown_counter = 0;
                if shutdown.is_set() {
                    while self.pop_client(None).is_some() {}
                    info!("gRPC worker#{index:02} shutdown");
                    return Ok(());
                }
            }

            // get client and state
            let Some(client) = self.pop_client(prev_client.take()) else {
                shutdown_counter += 9;
                sleep(Duration::from_micros(1));
                continue;
            };
            let mut state = client.state_lock();
            if state.finished {
                continue;
            }
            let ts = Instant::now();

            // filter messages
            let mut head = match (state.filter.is_some(), state.head) {
                (true, IndexLocation::Memory(head)) => head,
                _ => {
                    drop(state);
                    prev_client = Some(client);
                    continue;
                }
            };

            let messages_cache = match state.commitment {
                CommitmentLevel::Processed => &mut messages_cache_processed,
                CommitmentLevel::Confirmed => &mut messages_cache_confirmed,
                CommitmentLevel::Finalized => &mut messages_cache_finalized,
            };
            let mut errored = false;
            let mut messages_counter = 0;
            while !state.is_full() && messages_counter < messages_max_per_tick {
                let message = match messages_cache.try_recv(&receiver, state.commitment, head) {
                    Ok(Some(message)) => {
                        messages_counter += 1;
                        head += 1;
                        state.head = IndexLocation::Memory(head);
                        message
                    }
                    Ok(None) => break,
                    Err(RecvError::Lagged) => {
                        state.push_error(Status::data_loss("lagged"));
                        errored = true;
                        break;
                    }
                    Err(RecvError::Closed) => {
                        state.push_error(Status::data_loss("closed"));
                        errored = true;
                        break;
                    }
                };

                let message_ref: MessageRef = message.as_ref().into();
                if let Some(filter) = state.filter.as_ref() {
                    let items = filter
                        .get_updates_ref(message_ref, state.commitment)
                        .iter()
                        .map(|msg| ((&msg.filtered_update).into(), msg.encode_to_vec()))
                        .collect::<SmallVec<[(GrpcSubscribeMessage, Vec<u8>); 2]>>();

                    for (message, data) in items {
                        state.push_message(message, data);
                    }
                }
            }
            if messages_counter > 0 {
                state
                    .metric_cpu_usage
                    .increment(duration_to_seconds(ts.elapsed()));
            }
            drop(state);
            if !errored {
                prev_client = Some(client);
            }
        }
    }

    fn subscribe2<T: 'static>(
        &self,
        request: Request<Streaming<T>>,
        method: &'static str,
        get_ping: impl Fn(&T) -> Option<i32> + Send + 'static,
        mut get_filter: impl FnMut(&ConfigFilterLimits, T) -> (Option<Slot>, Result<Filter, Status>)
            + Send
            + 'static,
    ) -> TonicResult<Response<ReceiverStream>> {
        let x_subscription_id: Arc<str> = Self::get_x_subscription_id(&request).into();
        counter!(
            metrics::GRPC_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => method
        )
        .increment(1);

        let id = self.subscribe_id.fetch_add(1, Ordering::Relaxed);
        let client = SubscribeClient::new(
            id,
            self.subscribe_messages_len_max,
            self.subscribe_messages_replay_len_max,
            Arc::clone(&x_subscription_id),
        );
        self.push_client(client.clone());

        tokio::spawn({
            let shutdown = self.shutdown.clone();
            let ping_interval = self.ping_iterval;
            let client = client.clone();
            async move {
                tokio::pin!(shutdown);
                let mut ts_latest = Instant::now();
                loop {
                    tokio::select! {
                        () = &mut shutdown => {
                            tracing::error!("push error");
                            let mut state = client.state_lock();
                            state.push_error(Status::internal("shutdown"));
                            break
                        }
                        () = tokio::time::sleep(Duration::from_millis(500)) => {
                            let mut state = client.state_lock();
                            if state.finished {
                                break
                            }

                            let ts = Instant::now();
                            if ts.duration_since(ts_latest) > ping_interval {
                                ts_latest = ts;
                                let message = SubscribeClientState::create_ping();
                                state.push_message(GrpcSubscribeMessage::Ping, message);
                            }
                        }
                    }
                }
            }
        });

        tokio::spawn({
            let mut stream = request.into_inner();
            let limits = Arc::clone(&self.filter_limits);
            let client = client.clone();
            let messages = self.messages.clone();
            async move {
                loop {
                    match stream.message().await {
                        Ok(Some(message)) => {
                            if let Some(id) = get_ping(&message) {
                                let message = SubscribeClientState::create_pong(id);
                                let mut state = client.state_lock();
                                state.push_message(GrpcSubscribeMessage::Pong, message);
                                continue;
                            }

                            let (subscribe_from_slot, new_filter) = get_filter(&limits, message);
                            let mut state = client.state_lock();
                            if let Err(error) = new_filter.and_then(|filter| {
                                if filter.contains_blocks() && subscribe_from_slot.is_some() {
                                    return Err(Status::invalid_argument(
                                        "blocks are not possible to replay",
                                    ));
                                }

                                let commitment_prev = state.commitment;
                                state.commitment = filter.commitment().into();
                                if state.filter.is_none() || state.commitment != commitment_prev {
                                    let current_head = state.head;
                                    state.head = messages
                                        .get_current_tail_with_replay(
                                            state.commitment,
                                            subscribe_from_slot,
                                        )
                                        .map_err(Status::invalid_argument)?;
                                    if !matches!(current_head, IndexLocation::Storage(_))
                                        && matches!(state.head, IndexLocation::Storage(_))
                                    {
                                        let metric_cpu_usage = gauge!(
                                            metrics::GRPC_SUBSCRIBE_REPLAY_DISK_SECONDS_TOTAL,
                                            "x_subscription_id" => Arc::clone(&x_subscription_id)
                                        );
                                        messages
                                            .replay_from_storage(client.clone(), metric_cpu_usage)
                                            .map_err(Status::internal)?;
                                    }
                                }
                                state.filter = Some(filter);
                                Ok::<(), Status>(())
                            }) {
                                warn!(id, %error, "failed to handle request");
                                state.push_error(error);
                            } else {
                                info!(id, "set new filter");
                                continue;
                            }
                        }
                        Ok(None) => info!(id, "tx stream finished"),
                        Err(error) => warn!(id, %error, "error to receive new filter"),
                    };
                    break;
                }
                info!(id, "drop client tx stream");
            }
        });

        Ok(Response::new(ReceiverStream::new(client)))
    }
}

#[tonic::async_trait]
impl gen::geyser_server::Geyser for GrpcServer {
    type SubscribeStream = ReceiverStream;
    type SubscribeJupStream = ReceiverStream;

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        self.subscribe2(
            request,
            "subscribe",
            |message| message.ping.map(|msg| msg.id),
            |limits, message| {
                let subscribe_from_slot = message.from_slot;
                let new_filter = ConfigFilter::try_from(message)
                    .map_err(|error| {
                        Status::invalid_argument(format!("failed to create filter: {error:?}"))
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
                (subscribe_from_slot, new_filter)
            },
        )
    }

    async fn subscribe_jup(
        &self,
        request: Request<Streaming<SubscribeRequestJup>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let mut pubkeys = HashSet::new();
        self.subscribe2(
            request,
            "subscribe_jup",
            |message| message.ping,
            move |limits, message| {
                fn try_conv(pubkeys: Vec<Vec<u8>>) -> impl Iterator<Item = Result<Pubkey, String>> {
                    pubkeys.into_iter().map(|bytes| {
                        let slice: [u8; 32] = bytes
                            .try_into()
                            .map_err(|_| "invalid pubkey len".to_owned())?;
                        Ok(Pubkey::from(slice))
                    })
                }

                let new_filter = Ok(&mut pubkeys)
                    .and_then(|pubkeys| {
                        for item in try_conv(message.add) {
                            pubkeys.insert(item?);
                        }
                        for item in try_conv(message.remove) {
                            pubkeys.remove(&item?);
                        }
                        Ok(pubkeys)
                    })
                    .and_then(|pubkeys| {
                        let config = ConfigFilter {
                            slots: [("".to_owned(), ConfigFilterSlots::default())]
                                .into_iter()
                                .collect(),
                            accounts: [(
                                "".to_owned(),
                                ConfigFilterAccounts {
                                    account: pubkeys.iter().cloned().collect(),
                                    ..Default::default()
                                },
                            )]
                            .into_iter()
                            .collect(),
                            ..Default::default()
                        };
                        limits
                            .check_filter(&config)
                            .map(|()| Filter::new(&config))
                            .map_err(|error| format!("failed to check filter: {error:?}"))
                    })
                    .map_err(Status::invalid_argument);

                (message.from_slot, new_filter)
            },
        )
    }

    async fn subscribe_replay_info(
        &self,
        _request: Request<SubscribeReplayInfoRequest>,
    ) -> TonicResult<Response<SubscribeReplayInfoResponse>> {
        let response = SubscribeReplayInfoResponse {
            first_available: self.messages.get_first_available_slot(),
        };
        Ok(Response::new(response))
    }

    async fn ping(&self, request: Request<PingRequest>) -> TonicResult<Response<PongResponse>> {
        counter!(
            metrics::GRPC_REQUESTS_TOTAL,
            "x_subscription_id" => Self::get_x_subscription_id(&request),
            "method" => "ping"
        )
        .increment(1);

        let count = request.get_ref().count;
        let response = PongResponse { count };
        Ok(Response::new(response))
    }

    async fn get_latest_blockhash(
        &self,
        request: Request<GetLatestBlockhashRequest>,
    ) -> TonicResult<Response<GetLatestBlockhashResponse>> {
        counter!(
            metrics::GRPC_REQUESTS_TOTAL,
            "x_subscription_id" => Self::get_x_subscription_id(&request),
            "method" => "get_latest_blockhash"
        )
        .increment(1);

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
        counter!(
            metrics::GRPC_REQUESTS_TOTAL,
            "x_subscription_id" => Self::get_x_subscription_id(&request),
            "method" => "get_block_height"
        )
        .increment(1);

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
        counter!(
            metrics::GRPC_REQUESTS_TOTAL,
            "x_subscription_id" => Self::get_x_subscription_id(&request),
            "method" => "get_slot"
        )
        .increment(1);

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
        counter!(
            metrics::GRPC_REQUESTS_TOTAL,
            "x_subscription_id" => Self::get_x_subscription_id(&request),
            "method" => "is_blockhash_valid"
        )
        .increment(1);

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
        request: Request<GetVersionRequest>,
    ) -> TonicResult<Response<GetVersionResponse>> {
        counter!(
            metrics::GRPC_REQUESTS_TOTAL,
            "x_subscription_id" => Self::get_x_subscription_id(&request),
            "method" => "get_version"
        )
        .increment(1);

        Ok(Response::new(GetVersionResponse {
            version: VERSION.create_grpc_version_info().json(),
        }))
    }
}

#[derive(Debug, Clone)]
pub struct SubscribeClient {
    state: Arc<Mutex<SubscribeClientState>>,
}

impl SubscribeClient {
    fn new(
        id: u64,
        messages_len_max: usize,
        messages_replay_len_max: usize,
        x_subscription_id: Arc<str>,
    ) -> Self {
        let state = SubscribeClientState::new(
            id,
            messages_len_max,
            messages_replay_len_max,
            x_subscription_id,
        );
        Self {
            state: Arc::new(Mutex::new(state)),
        }
    }

    #[inline]
    pub fn state_lock(&self) -> MutexGuard<'_, SubscribeClientState> {
        mutex_lock(&self.state)
    }
}

#[derive(Debug)]
pub struct SubscribeClientState {
    pub finished: bool, // check in workers with acquired mutex
    id: u64,
    x_subscription_id: Arc<str>,
    commitment: CommitmentLevel,
    pub head: IndexLocation,
    pub filter: Option<Filter>,
    messages_error: Option<Status>,
    messages_len_total: usize,
    messages_len_max: usize,
    messages_replay_len_max: usize,
    messages: LinkedList<(GrpcSubscribeMessage, Vec<u8>)>,
    messages_waker: Option<Waker>,
    metric_cpu_usage: Gauge,
}

impl Drop for SubscribeClientState {
    fn drop(&mut self) {
        info!(
            id = self.id,
            x_subscription_id = self.x_subscription_id.as_ref(),
            "drop client state"
        );
        gauge!(metrics::GRPC_SUBSCRIBE_TOTAL, "x_subscription_id" => Arc::clone(&self.x_subscription_id))
            .decrement(1);
    }
}

impl SubscribeClientState {
    fn new(
        id: u64,
        messages_len_max: usize,
        messages_replay_len_max: usize,
        x_subscription_id: Arc<str>,
    ) -> Self {
        info!(
            id,
            x_subscription_id = x_subscription_id.as_ref(),
            "new client"
        );
        gauge!(metrics::GRPC_SUBSCRIBE_TOTAL, "x_subscription_id" => Arc::clone(&x_subscription_id))
            .increment(1);

        let metric_cpu_usage = gauge!(
            metrics::GRPC_SUBSCRIBE_CPU_SECONDS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id)
        );

        Self {
            finished: false,
            id,
            x_subscription_id,
            commitment: CommitmentLevel::default(),
            head: IndexLocation::Unknown,
            filter: None,
            messages_error: None,
            messages_len_total: 0,
            messages_len_max,
            messages_replay_len_max,
            messages: LinkedList::new(),
            messages_waker: None,
            metric_cpu_usage,
        }
    }

    #[inline]
    fn serialize_ping_pong(oneof: UpdateOneof) -> Vec<u8> {
        SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(oneof),
            created_at: Some(SystemTime::now().into()),
        }
        .encode_to_vec()
    }

    #[inline]
    fn create_ping() -> Vec<u8> {
        Self::serialize_ping_pong(UpdateOneof::Ping(SubscribeUpdatePing {}))
    }

    #[inline]
    fn create_pong(id: i32) -> Vec<u8> {
        Self::serialize_ping_pong(UpdateOneof::Pong(SubscribeUpdatePong { id }))
    }

    const fn is_full(&self) -> bool {
        self.messages_len_total > self.messages_len_max
    }

    pub const fn is_full_replay(&self) -> bool {
        self.messages_len_total > self.messages_replay_len_max
    }

    pub fn push_error(&mut self, error: Status) {
        self.messages_error = Some(error);
        if let Some(waker) = self.messages_waker.take() {
            waker.wake();
        }
    }

    pub fn push_message(&mut self, message: GrpcSubscribeMessage, data: Vec<u8>) {
        self.messages_len_total += data.len();
        self.messages.push_back((message, data));
        if let Some(waker) = self.messages_waker.take() {
            waker.wake();
        }
    }

    fn pop_message(
        &mut self,
        cx: &Context,
    ) -> Option<TonicResult<(GrpcSubscribeMessage, Vec<u8>)>> {
        if let Some(error) = self.messages_error.take() {
            return Some(Err(error));
        }

        if let Some(message) = self.messages.pop_front() {
            self.messages_len_total -= message.1.len();
            Some(Ok(message))
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

impl Drop for ReceiverStream {
    fn drop(&mut self) {
        let mut state = self.client.state_lock();
        state.finished = true;
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
            let item = match item {
                Ok((message, data)) => {
                    counter!(
                        metrics::GRPC_SUBSCRIBE_MESSAGES_COUNT_TOTAL,
                        "x_subscription_id" => Arc::clone(&state.x_subscription_id),
                        "message" => message.as_str(),
                    )
                    .increment(1);
                    counter!(
                        metrics::GRPC_SUBSCRIBE_MESSAGES_BYTES_TOTAL,
                        "x_subscription_id" => Arc::clone(&state.x_subscription_id),
                        "message" => message.as_str(),
                    )
                    .increment(data.len() as u64);

                    Ok(data)
                }
                Err(error) => Err(error),
            };
            drop(state);

            self.finished = item.is_err();
            Poll::Ready(Some(item))
        } else {
            Poll::Pending
        }
    }
}

struct MessagesCache {
    head: u64,
    mask: u64,
    buffer: Box<[MessagesCacheItem]>,
}

impl fmt::Debug for MessagesCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessagesCache")
            .field("head", &self.head)
            .field("mask", &self.mask)
            .finish()
    }
}

impl MessagesCache {
    fn new(max_messages: usize) -> Self {
        let buffer = (0..max_messages)
            .map(|_| MessagesCacheItem {
                pos: u64::MAX,
                msg: None,
            })
            .collect::<Vec<_>>();

        Self {
            head: 0,
            mask: (max_messages - 1) as u64,
            buffer: buffer.into_boxed_slice(),
        }
    }

    #[inline]
    const fn get_idx(&self, pos: u64) -> usize {
        (pos & self.mask) as usize
    }

    fn try_recv(
        &mut self,
        receiver: &ReceiverSync,
        commitment: CommitmentLevel,
        head: u64,
    ) -> Result<Option<Cow<'_, ParsedMessage>>, RecvError> {
        if head > self.head {
            self.head = head;
        }
        let inrange = head >= self.head - self.mask;

        // return if item cached
        let idx = self.get_idx(head);
        if inrange && self.buffer[idx].pos == head {
            return Ok(self.buffer[idx].msg.as_ref().map(Cow::Borrowed));
        }

        // try to get from the channel
        let Some(item) = receiver.try_recv(commitment, head)? else {
            return Ok(None);
        };

        // save item if in range
        if inrange {
            self.buffer[idx] = MessagesCacheItem {
                pos: head,
                msg: Some(item.clone()),
            };
        }
        Ok(Some(Cow::Owned(item)))
    }
}

struct MessagesCacheItem {
    pos: u64,
    msg: Option<ParsedMessage>,
}

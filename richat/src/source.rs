use {
    crate::{
        channel::GlobalReplayFromSlot,
        config::{
            ConfigChannelSource, ConfigChannelSourceGeneral, ConfigChannelSourceReconnect,
            ConfigGrpcClientSource,
        },
    },
    anyhow::Context as _,
    futures::{
        future::try_join_all,
        stream::{BoxStream, Stream, StreamExt, try_unfold},
    },
    maplit::hashmap,
    richat_client::{
        grpc::{ConfigGrpcClient, GrpcClientBuilderError},
        quic::{ConfigQuicClient, QuicConnectError},
    },
    richat_filter::message::{Message, MessageParseError, MessageParserEncoding},
    richat_proto::{
        geyser::{
            CommitmentLevel as CommitmentLevelProto, SubscribeRequest,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
            SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions,
        },
        richat::{GrpcSubscribeRequest, RichatFilter},
    },
    solana_clock::Slot,
    std::{
        collections::{HashMap, HashSet},
        fmt,
        pin::Pin,
        sync::{LazyLock, Mutex},
        task::{Context, Poll},
    },
    thiserror::Error,
    tokio::time::{Duration, sleep},
    tonic::Code,
    tracing::{error, info},
};

#[derive(Debug, Error)]
enum ConnectError {
    #[error(transparent)]
    Quic(QuicConnectError),
    #[error(transparent)]
    Grpc(GrpcClientBuilderError),
}

#[derive(Debug, Error)]
enum SubscribeError {
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error(transparent)]
    Subscribe(#[from] richat_client::error::SubscribeError),
    #[error(transparent)]
    SubscribeGrpc(#[from] tonic::Status),
}

#[derive(Debug, Error)]
pub enum ReceiveError {
    #[error(transparent)]
    Receive(#[from] richat_client::error::ReceiveError),
    #[error(transparent)]
    Parse(#[from] MessageParseError),
    #[error("replay from the requested slot is not available from any source")]
    ReplayFailed,
}

#[derive(Debug, Clone)]
enum SubscriptionConfig {
    Quic {
        config: ConfigQuicClient,
    },
    Grpc {
        source: ConfigGrpcClientSource,
        config: ConfigGrpcClient,
    },
}

impl SubscriptionConfig {
    fn new(config: ConfigChannelSource) -> (Self, ConfigChannelSourceGeneral) {
        match config {
            ConfigChannelSource::Quic { general, config } => (Self::Quic { config }, general),
            ConfigChannelSource::Grpc {
                general,
                source,
                config,
            } => (Self::Grpc { source, config }, general),
        }
    }
}

#[derive(Debug)]
struct Backoff {
    current_interval: Duration,
    initial_interval: Duration,
    max_interval: Duration,
    multiplier: f64,
}

impl Backoff {
    const fn new(config: ConfigChannelSourceReconnect) -> Self {
        Self {
            current_interval: config.initial_interval,
            initial_interval: config.initial_interval,
            max_interval: config.max_interval,
            multiplier: config.multiplier,
        }
    }

    async fn sleep(&mut self) {
        sleep(self.current_interval).await;
        self.current_interval = self
            .current_interval
            .mul_f64(self.multiplier)
            .min(self.max_interval);
    }

    const fn reset(&mut self) {
        self.current_interval = self.initial_interval;
    }
}

type SubscriptionMessage = Result<(&'static str, Message), ReceiveError>;

pub type PreparedReloadResult = anyhow::Result<(Vec<&'static str>, Vec<Subscription>)>;

pub struct Subscription {
    name: &'static str,
    config: ConfigChannelSource,
    stream: BoxStream<'static, SubscriptionMessage>,
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("name", &self.name)
            .finish()
    }
}

impl Subscription {
    async fn new(
        source_config: ConfigChannelSource,
        global_replay_from_slot: GlobalReplayFromSlot,
    ) -> anyhow::Result<Self> {
        let (subscription_config, mut config) = SubscriptionConfig::new(source_config.clone());
        let name = Self::get_static_name(&config.name);

        let stream = if let Some(reconnect) = config.reconnect.take() {
            let backoff = Backoff::new(reconnect);
            try_unfold(
                (
                    backoff,
                    subscription_config,
                    config,
                    global_replay_from_slot,
                    None,
                ),
                move |mut state: (
                    Backoff,
                    SubscriptionConfig,
                    ConfigChannelSourceGeneral,
                    GlobalReplayFromSlot,
                    Option<kanal::AsyncReceiver<SubscriptionMessage>>,
                )| async move {
                    loop {
                        if let Some(stream) = state.4.as_mut() {
                            match stream.recv().await {
                                Ok(Ok((name, message))) => {
                                    return Ok(Some(((name, message), state)));
                                }
                                Ok(Err(ReceiveError::ReplayFailed)) => {
                                    if state.3.report_replay_failed(name) {
                                        return Err(ReceiveError::ReplayFailed);
                                    }
                                    error!(name, "failed to replay, waiting for other sources");
                                }
                                Ok(Err(error)) => {
                                    error!(name, ?error, "failed to receive")
                                }
                                Err(_) => {
                                    error!(name, "stream is finished")
                                }
                            }
                            state.4 = None;
                            state.0.sleep().await;
                        } else {
                            match Subscription::subscribe(
                                name,
                                state.1.clone(),
                                state.2.disable_accounts,
                                state.2.parser,
                                state.2.channel_size,
                                state.3.load(),
                            )
                            .await
                            {
                                Ok(stream) => {
                                    state.4 = Some(stream);
                                    state.0.reset();
                                }
                                Err(error) => {
                                    error!(name, ?error, "failed to connect");
                                    state.0.sleep().await;
                                }
                            }
                        }
                    }
                },
            )
            .boxed()
        } else {
            let rx = Self::subscribe(
                name,
                subscription_config,
                config.disable_accounts,
                config.parser,
                config.channel_size,
                global_replay_from_slot.load(),
            )
            .await?;
            futures::stream::unfold(
                rx,
                |rx| async move { rx.recv().await.ok().map(|msg| (msg, rx)) },
            )
            .boxed()
        };

        Ok(Self {
            name,
            config: source_config,
            stream,
        })
    }

    fn get_static_name(name: &str) -> &'static str {
        static NAMES: LazyLock<Mutex<HashSet<&'static str>>> =
            LazyLock::new(|| Mutex::new(HashSet::new()));
        let mut set = NAMES.lock().expect("poisoned");
        if let Some(&name) = set.get(name) {
            name
        } else {
            let name: &'static str = name.to_owned().leak();
            set.insert(name);
            name
        }
    }

    async fn subscribe(
        name: &'static str,
        config: SubscriptionConfig,
        disable_accounts: bool,
        parser: MessageParserEncoding,
        channel_size: usize,
        replay_from_slot: Option<Slot>,
    ) -> Result<kanal::AsyncReceiver<SubscriptionMessage>, SubscribeError> {
        let (tx, rx) = kanal::bounded_async(channel_size);

        let mut stream = match config {
            SubscriptionConfig::Quic { config } => {
                let connection = config.connect().await.map_err(ConnectError::Quic)?;
                let filter = Self::create_richat_filter(disable_accounts);
                match connection.subscribe(replay_from_slot, filter).await {
                    Ok(stream) => {
                        info!(name, version = stream.get_version(), "connected");
                        stream.boxed()
                    }
                    Err(richat_client::error::SubscribeError::ReplayFromSlotNotAvailable(_)) => {
                        let _ = tx.send(Err(ReceiveError::ReplayFailed)).await;
                        return Ok(rx);
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            SubscriptionConfig::Grpc { source, config } => {
                let mut connection = config.connect().await.map_err(ConnectError::Grpc)?;
                match source {
                    ConfigGrpcClientSource::DragonsMouth => {
                        let version = connection
                            .get_version()
                            .await
                            .map_err(|error| ConnectError::Grpc(error.into()))?;
                        info!(name, version = version.version, "connected");
                        connection
                            .subscribe_dragons_mouth_once(Self::create_dragons_mouth_filter(
                                disable_accounts,
                                replay_from_slot,
                            ))
                            .await?
                            .boxed()
                    }
                    ConfigGrpcClientSource::Richat => connection
                        .subscribe_richat(GrpcSubscribeRequest {
                            replay_from_slot,
                            filter: Self::create_richat_filter(disable_accounts),
                        })
                        .await?
                        .boxed(),
                }
            }
        };
        info!(name, "subscribed");

        tokio::spawn(async move {
            loop {
                let message = match stream.next().await {
                    Some(Ok(data)) => match Message::parse(data.into(), parser) {
                        Ok(message) => Ok((name, message)),
                        Err(MessageParseError::InvalidUpdateMessage("Ping")) => continue,
                        Err(error) => Err(error.into()),
                    },
                    Some(Err(error)) => {
                        if matches!(
                            &error,
                            richat_client::error::ReceiveError::Status(status)
                                if (status.code() == Code::InvalidArgument && status.message().contains("replay")) || status.code() == Code::DataLoss
                        ) {
                            Err(ReceiveError::ReplayFailed)
                        } else {
                            Err(error.into())
                        }
                    }
                    None => break,
                };

                if tx.send(message).await.is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }

    const fn create_richat_filter(disable_accounts: bool) -> Option<RichatFilter> {
        Some(RichatFilter {
            disable_accounts,
            disable_transactions: false,
            disable_entries: false,
        })
    }

    fn create_dragons_mouth_filter(
        disable_accounts: bool,
        from_slot: Option<Slot>,
    ) -> SubscribeRequest {
        SubscribeRequest {
            accounts: if disable_accounts {
                HashMap::new()
            } else {
                hashmap! { "".to_owned() => SubscribeRequestFilterAccounts::default() }
            },
            slots: hashmap! { "".to_owned() => SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            } },
            transactions: hashmap! { "".to_owned() => SubscribeRequestFilterTransactions::default() },
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: hashmap! { "".to_owned() => SubscribeRequestFilterBlocksMeta::default() },
            entry: hashmap! { "".to_owned() => SubscribeRequestFilterEntry::default() },
            commitment: Some(CommitmentLevelProto::Processed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot,
        }
    }
}

pub struct Subscriptions {
    global_replay_from_slot: GlobalReplayFromSlot,
    streams: Vec<Subscription>,
    last_polled: usize,
}

impl fmt::Debug for Subscriptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscriptions")
            .field("streams", &self.streams.len())
            .field("last_polled", &self.last_polled)
            .finish()
    }
}

impl Subscriptions {
    pub async fn new(
        sources: Vec<ConfigChannelSource>,
        global_replay_from_slot: GlobalReplayFromSlot,
    ) -> anyhow::Result<Self> {
        let streams = Self::create_subscriptions(sources, &global_replay_from_slot).await?;

        Ok(Self {
            global_replay_from_slot,
            streams,
            last_polled: 0,
        })
    }

    async fn create_subscriptions(
        sources: impl IntoIterator<Item = ConfigChannelSource>,
        global_replay_from_slot: &GlobalReplayFromSlot,
    ) -> anyhow::Result<Vec<Subscription>> {
        try_join_all(sources.into_iter().map(|config| {
            let global_replay_from_slot = global_replay_from_slot.clone();
            async move {
                Subscription::new(config, global_replay_from_slot)
                    .await
                    .context("failed to subscribe")
            }
        }))
        .await
    }

    pub fn get_last_polled_name(&self) -> &'static str {
        self.streams[self.last_polled].name
    }

    /// Prepare reload by computing changes and creating subscriptions.
    /// Returns a future that resolves to names to remove and new streams to add.
    pub fn prepare_reload(
        &self,
        new_sources: Vec<ConfigChannelSource>,
    ) -> impl Future<Output = PreparedReloadResult> + use<> {
        // collect streams for removal
        let to_remove: Vec<&'static str> = self
            .streams
            .iter()
            .filter(|stream| {
                let matching = new_sources.iter().find(|new_source_config| {
                    let name = match new_source_config {
                        ConfigChannelSource::Quic { general, .. } => general.name.as_str(),
                        ConfigChannelSource::Grpc { general, .. } => general.name.as_str(),
                    };
                    stream.name == name
                });
                match matching {
                    None => true,
                    Some(new_source_config) if &stream.config != new_source_config => true,
                    Some(_) => false,
                }
            })
            .map(|stream| stream.name)
            .collect();

        // collect sources to add
        let sources_to_add: Vec<ConfigChannelSource> = new_sources
            .into_iter()
            .filter(|new_source_config| {
                let name = Subscription::get_static_name(match new_source_config {
                    ConfigChannelSource::Quic { general, .. } => &general.name,
                    ConfigChannelSource::Grpc { general, .. } => &general.name,
                });
                match self.streams.iter().find(|s| s.name == name) {
                    Some(stream) => &stream.config != new_source_config,
                    None => true,
                }
            })
            .collect();

        let global_replay_from_slot = self.global_replay_from_slot.clone();
        async move {
            Self::create_subscriptions(sources_to_add, &global_replay_from_slot)
                .await
                .map(|new_streams| (to_remove, new_streams))
        }
    }

    /// Apply the result of `prepare_reload` to update subscriptions.
    pub fn apply_reload(&mut self, to_remove: Vec<&'static str>, new_streams: Vec<Subscription>) {
        for name in to_remove {
            info!(name, "removing subscription");
            self.streams.retain(|stream| stream.name != name);
        }

        for stream in new_streams {
            info!(name = stream.name, "adding subscription");
            self.streams.push(stream);
        }

        self.global_replay_from_slot
            .update_sources(self.streams.len());

        self.last_polled = 0;
    }
}

impl Stream for Subscriptions {
    type Item = SubscriptionMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let init_index = self.last_polled;
        loop {
            self.last_polled = (self.last_polled + 1) % self.streams.len();
            let index = self.last_polled;

            let result = self.streams[index].stream.poll_next_unpin(cx);
            if let Poll::Ready(value) = result {
                return Poll::Ready(value);
            }

            if index == init_index {
                return Poll::Pending;
            }
        }
    }
}

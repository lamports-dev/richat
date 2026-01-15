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
        collections::HashMap,
        fmt,
        pin::Pin,
        task::{Context, Poll},
    },
    thiserror::Error,
    tokio::{
        sync::mpsc,
        time::{Duration, sleep},
    },
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

struct Subscription {
    rx: mpsc::Receiver<Result<(usize, &'static str, Message), ReceiveError>>,
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription").finish()
    }
}

impl Stream for Subscription {
    type Item = Result<(usize, &'static str, Message), ReceiveError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl Subscription {
    async fn new(
        name: &'static str,
        config: SubscriptionConfig,
        disable_accounts: bool,
        parser: MessageParserEncoding,
        channel_size: usize,
        replay_from_slot: Option<Slot>,
        index: usize,
    ) -> Result<Self, SubscribeError> {
        let mut stream = match config {
            SubscriptionConfig::Quic { config } => {
                let connection = config.connect().await.map_err(ConnectError::Quic)?;
                let filter = Self::create_richat_filter(disable_accounts);
                let stream = connection.subscribe(replay_from_slot, filter).await?;
                info!(name, version = stream.get_version(), "connected");
                stream.boxed()
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

        let (tx, rx) = mpsc::channel(channel_size);
        tokio::spawn(async move {
            loop {
                let message = match stream.next().await {
                    Some(Ok(data)) => match Message::parse(data.into(), parser) {
                        Ok(message) => Ok((index, name, message)),
                        Err(MessageParseError::InvalidUpdateMessage("Ping")) => continue,
                        Err(error) => Err(error.into()),
                    },
                    Some(Err(error)) => Err(error.into()),
                    None => break,
                };

                if tx.send(message).await.is_err() {
                    break;
                }
            }
        });

        Ok(Self { rx })
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

struct BoxedSubscription {
    name: &'static str,
    stream: BoxStream<'static, Result<(usize, &'static str, Message), ReceiveError>>,
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

async fn subscribe(
    config: ConfigChannelSource,
    global_replay_from_slot: GlobalReplayFromSlot,
    index: usize,
) -> anyhow::Result<BoxedSubscription> {
    let (subscription_config, mut config) = SubscriptionConfig::new(config);
    let name: &'static str = config.name.clone().leak();

    let Some(reconnect) = config.reconnect.take() else {
        let stream = Subscription::new(
            name,
            subscription_config,
            config.disable_accounts,
            config.parser,
            config.channel_size,
            global_replay_from_slot.load(),
            index,
        )
        .await?;
        return Ok(BoxedSubscription {
            name,
            stream: stream.boxed(),
        });
    };

    let backoff = Backoff::new(reconnect);
    let stream = try_unfold(
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
            Option<Subscription>,
        )| async move {
            loop {
                if let Some(stream) = state.4.as_mut() {
                    match stream.next().await {
                        Some(Ok((index, name, message))) => {
                            return Ok(Some(((index, name, message), state)));
                        }
                        Some(Err(error)) => {
                            error!(name, ?error, "failed to receive")
                        }
                        None => {
                            error!(name, "stream is finished")
                        }
                    }
                    state.4 = None;
                    state.0.sleep().await;
                } else {
                    match Subscription::new(
                        name,
                        state.1.clone(),
                        state.2.disable_accounts,
                        state.2.parser,
                        state.2.channel_size,
                        state.3.load(),
                        index,
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
    );
    Ok(BoxedSubscription {
        name,
        stream: stream.boxed(),
    })
}

pub struct Subscriptions {
    #[allow(clippy::type_complexity)]
    streams: Vec<BoxedSubscription>,
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
        let streams = try_join_all(sources.into_iter().enumerate().map(|(index, config)| {
            let global_replay_from_slot = global_replay_from_slot.clone();
            async move {
                subscribe(config, global_replay_from_slot, index)
                    .await
                    .context("failed to subscribe")
            }
        }))
        .await?;

        Ok(Self {
            streams,
            last_polled: 0,
        })
    }

    pub fn get_last_polled_name(&self) -> &'static str {
        self.streams[self.last_polled].name
    }
}

impl Stream for Subscriptions {
    type Item = Result<(usize, &'static str, Message), ReceiveError>;

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

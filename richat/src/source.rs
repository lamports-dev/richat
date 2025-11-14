use {
    crate::config::{
        ConfigChannelSource, ConfigChannelSourceGeneral, ConfigChannelSourceReconnect,
        ConfigGrpcClientSource,
    },
    anyhow::Context as _,
    futures::{
        future::try_join_all,
        ready,
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
            CommitmentLevel as CommitmentLevelProto, SlotStatus, SubscribeRequest,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
            SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions,
        },
        richat::{GrpcSubscribeRequest, RichatFilter},
    },
    solana_sdk::clock::Slot,
    std::{
        collections::HashMap,
        fmt,
        pin::Pin,
        task::{Context, Poll},
    },
    thiserror::Error,
    tokio::time::{Duration, sleep},
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
    index: usize,
    name: &'static str,
    parser: MessageParserEncoding,
    stream: BoxStream<'static, Result<Vec<u8>, richat_client::error::ReceiveError>>,
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription").finish()
    }
}

impl Stream for Subscription {
    type Item = Result<(usize, &'static str, Message), ReceiveError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let value = ready!(self.stream.poll_next_unpin(cx));
            return Poll::Ready(match value {
                Some(Ok(data)) => match Message::parse(data.into(), self.parser) {
                    Ok(message) => Some(Ok((self.index, self.name, message))),
                    Err(MessageParseError::InvalidUpdateMessage("Ping")) => continue,
                    Err(error) => Some(Err(error.into())),
                },
                Some(Err(error)) => Some(Err(error.into())),
                None => None,
            });
        }
    }
}

impl Subscription {
    async fn new(
        name: &'static str,
        config: SubscriptionConfig,
        disable_accounts: bool,
        parser: MessageParserEncoding,
        replay_from_slot: Option<Slot>,
        index: usize,
    ) -> Result<Self, SubscribeError> {
        let stream = match config {
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

        Ok(Self {
            index,
            name,
            parser,
            stream,
        })
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
    replay_for_storage: bool,
    replay_from_slot: Option<Slot>,
    index: usize,
) -> anyhow::Result<BoxStream<'static, Result<(usize, &'static str, Message), ReceiveError>>> {
    let (subscription_config, mut config) = SubscriptionConfig::new(config);
    let name: &'static str = config.name.clone().leak();

    let Some(reconnect) = config.reconnect.take() else {
        let stream = Subscription::new(
            name,
            subscription_config,
            config.disable_accounts,
            config.parser,
            replay_from_slot,
            index,
        )
        .await?;
        return Ok(stream.boxed());
    };

    let backoff = Backoff::new(reconnect);
    let stream = try_unfold(
        (
            backoff,
            subscription_config,
            name,
            config,
            None,
            replay_from_slot,
        ),
        move |mut state: (
            Backoff,
            SubscriptionConfig,
            &'static str,
            ConfigChannelSourceGeneral,
            Option<Subscription>,
            Option<Slot>,
        )| async move {
            loop {
                if let Some(stream) = state.4.as_mut() {
                    match stream.next().await {
                        Some(Ok((index, name, message))) => {
                            if replay_for_storage {
                                if let Message::Slot(msg) = &message {
                                    if msg.status() == SlotStatus::SlotFinalized {
                                        state.5 = Some(match state.5 {
                                            Some(slot) => slot.max(msg.slot() + 1),
                                            None => msg.slot() + 1,
                                        });
                                    }
                                }
                            }

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
                        state.3.disable_accounts,
                        state.3.parser,
                        state.5,
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
    Ok(stream.boxed())
}

pub struct Subscriptions {
    #[allow(clippy::type_complexity)]
    streams: Vec<BoxStream<'static, Result<(usize, &'static str, Message), ReceiveError>>>,
    next_stream: usize,
}

impl fmt::Debug for Subscriptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscriptions")
            .field("streams", &self.streams.len())
            .field("next_stream", &self.next_stream)
            .finish()
    }
}

impl Subscriptions {
    pub async fn new(
        sources: Vec<ConfigChannelSource>,
        replay_for_storage: bool,
        replay_from_slot: Option<Slot>,
    ) -> anyhow::Result<Self> {
        let streams = try_join_all(sources.into_iter().enumerate().map(
            |(index, config)| async move {
                subscribe(config, replay_for_storage, replay_from_slot, index)
                    .await
                    .context("failed to subscribe")
            },
        ))
        .await?;

        Ok(Self {
            streams,
            next_stream: 0,
        })
    }
}

impl Stream for Subscriptions {
    type Item = Result<(usize, &'static str, Message), ReceiveError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let init_index = self.next_stream;
        loop {
            let index = self.next_stream;
            self.next_stream = (self.next_stream + 1) % self.streams.len();

            let result = self.streams[index].poll_next_unpin(cx);
            if let Poll::Ready(value) = result {
                return Poll::Ready(value);
            }

            if self.next_stream == init_index {
                return Poll::Pending;
            }
        }
    }
}

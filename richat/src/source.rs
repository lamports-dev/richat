use {
    crate::{
        channel::ParsedMessage,
        config::{
            ConfigChannelSource, ConfigChannelSourceGeneral, ConfigChannelSourceReconnect,
            ConfigGrpcClientSource,
        },
    },
    futures::{
        ready,
        stream::{try_unfold, BoxStream, Stream, StreamExt},
    },
    maplit::hashmap,
    pin_project_lite::pin_project,
    richat_client::{
        grpc::{ConfigGrpcClient, GrpcClientBuilderError},
        quic::{ConfigQuicClient, QuicConnectError},
        tcp::ConfigTcpClient,
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
    std::{
        collections::HashMap,
        fmt, io,
        pin::Pin,
        task::{Context, Poll},
    },
    thiserror::Error,
    tokio::time::{sleep, Duration},
    tracing::{error, info},
};

#[derive(Debug, Error)]
enum ConnectError {
    #[error(transparent)]
    Quic(QuicConnectError),
    #[error(transparent)]
    Tcp(io::Error),
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
    Tcp {
        config: ConfigTcpClient,
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
            ConfigChannelSource::Tcp { general, config } => (Self::Tcp { config }, general),
            ConfigChannelSource::Grpc {
                general,
                source,
                config,
            } => (Self::Grpc { source, config }, general),
        }
    }
}

pin_project! {
    struct Subscription {
        stream: BoxStream<'static, Result<Vec<u8>, richat_client::error::ReceiveError>>,
        parser: MessageParserEncoding,
        index: usize,
    }
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription").finish()
    }
}

impl Stream for Subscription {
    type Item = Result<(usize, ParsedMessage), ReceiveError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let value = ready!(self.stream.poll_next_unpin(cx));
            return Poll::Ready(match value {
                Some(Ok(data)) => match Message::parse(data, self.parser) {
                    Ok(message) => Some(Ok((self.index, message.into()))),
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
        config: SubscriptionConfig,
        disable_accounts: bool,
        parser: MessageParserEncoding,
        index: usize,
    ) -> Result<Self, SubscribeError> {
        let stream = match config {
            SubscriptionConfig::Quic { config } => {
                let connection = config.connect().await.map_err(ConnectError::Quic)?;
                let filter = Self::create_richat_filter(disable_accounts);
                connection.subscribe(None, filter).await?.boxed()
            }
            SubscriptionConfig::Tcp { config } => {
                let connection = config.connect().await.map_err(ConnectError::Tcp)?;
                let filter = Self::create_richat_filter(disable_accounts);
                connection.subscribe(None, filter).await?.boxed()
            }
            SubscriptionConfig::Grpc { source, config } => {
                let mut connection = config.connect().await.map_err(ConnectError::Grpc)?;
                match source {
                    ConfigGrpcClientSource::DragonsMouth => connection
                        .subscribe_dragons_mouth_once(Self::create_dragons_mouth_filter(
                            disable_accounts,
                        ))
                        .await?
                        .boxed(),
                    ConfigGrpcClientSource::Richat => connection
                        .subscribe_richat(GrpcSubscribeRequest {
                            replay_from_slot: None,
                            filter: Self::create_richat_filter(disable_accounts),
                        })
                        .await?
                        .boxed(),
                }
            }
        };

        Ok(Self {
            stream,
            parser,
            index,
        })
    }

    const fn create_richat_filter(disable_accounts: bool) -> Option<RichatFilter> {
        Some(RichatFilter {
            disable_accounts,
            disable_transactions: false,
            disable_entries: false,
        })
    }

    fn create_dragons_mouth_filter(disable_accounts: bool) -> SubscribeRequest {
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
            from_slot: None,
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

    fn reset(&mut self) {
        self.current_interval = self.initial_interval;
    }
}

pub async fn subscribe(
    config: ConfigChannelSource,
    index: usize,
) -> anyhow::Result<BoxStream<'static, Result<(usize, ParsedMessage), ReceiveError>>> {
    let (subscription_config, mut config) = SubscriptionConfig::new(config);

    let Some(reconnect) = config.reconnect.take() else {
        let stream = Subscription::new(
            subscription_config,
            config.disable_accounts,
            config.parser,
            index,
        )
        .await?;
        info!(name = config.name, "connected");
        return Ok(stream.boxed());
    };

    let backoff = Backoff::new(reconnect);
    let stream = try_unfold(
        (backoff, subscription_config, config, None),
        move |mut state: (
            Backoff,
            SubscriptionConfig,
            ConfigChannelSourceGeneral,
            Option<Subscription>,
        )| async move {
            loop {
                if let Some(stream) = state.3.as_mut() {
                    match stream.next().await {
                        Some(Ok(message)) => return Ok(Some((message, state))),
                        Some(Err(error)) => {
                            error!(name = state.2.name, ?error, "failed to receive")
                        }
                        None => {
                            error!(name = state.2.name, "stream is finished")
                        }
                    }
                    state.3 = None;
                    state.0.sleep().await;
                } else {
                    match Subscription::new(
                        state.1.clone(),
                        state.2.disable_accounts,
                        state.2.parser,
                        index,
                    )
                    .await
                    {
                        Ok(stream) => {
                            info!(name = state.2.name, "connected");
                            state.3 = Some(stream);
                            state.0.reset();
                        }
                        Err(error) => {
                            error!(name = state.2.name, ?error, "failed to connect");
                            state.0.sleep().await;
                        }
                    }
                }
            }
        },
    );
    Ok(stream.boxed())
}

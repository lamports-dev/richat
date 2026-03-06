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
                let matching = new_sources
                    .iter()
                    .find(|new_source_config| stream.name == new_source_config.name());
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
                let name = Subscription::get_static_name(new_source_config.name());
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
        if self.streams.is_empty() {
            return Poll::Ready(None);
        }

        let init_index = self.last_polled;
        loop {
            self.last_polled = (self.last_polled + 1) % self.streams.len();
            let index = self.last_polled;

            match self.streams[index].stream.poll_next_unpin(cx) {
                Poll::Ready(Some(value)) => return Poll::Ready(Some(value)),
                Poll::Ready(None) => {
                    let removed = self.streams.remove(index);
                    error!(name = removed.name, "source stream finished, removing");
                    if self.streams.is_empty() {
                        return Poll::Ready(None);
                    }
                    // Adjust last_polled after removal
                    self.last_polled = if index == 0 {
                        self.streams.len() - 1
                    } else {
                        index - 1
                    };
                    // Restart the scan with updated indices
                    return self.poll_next(cx);
                }
                Poll::Pending => {
                    if index == init_index {
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        futures::stream::{self, StreamExt},
    };

    fn mock_subscription(
        name: &'static str,
        stream: BoxStream<'static, SubscriptionMessage>,
    ) -> Subscription {
        Subscription {
            name,
            // config is only used for reload comparison; not relevant in these tests
            config: ConfigChannelSource::Grpc {
                general: ConfigChannelSourceGeneral {
                    name: name.to_owned(),
                    parser: MessageParserEncoding::Prost,
                    disable_accounts: false,
                    reconnect: None,
                    channel_size: 1,
                },
                source: ConfigGrpcClientSource::default(),
                config: serde_json::from_str(r#"{"endpoint":"http://test"}"#).unwrap(),
            },
            stream,
        }
    }

    fn mock_subscriptions(streams: Vec<Subscription>) -> Subscriptions {
        Subscriptions {
            global_replay_from_slot: GlobalReplayFromSlot::new_test(streams.len()),
            last_polled: 0,
            streams,
        }
    }

    /// Helper: create a mock source backed by a futures mpsc channel.
    /// Returns the subscription and a sender that can inject messages.
    fn mock_channel_subscription(
        name: &'static str,
    ) -> (Subscription, futures::channel::mpsc::Sender<SubscriptionMessage>) {
        let (tx, rx) = futures::channel::mpsc::channel::<SubscriptionMessage>(4);
        (mock_subscription(name, rx.boxed()), tx)
    }

    /// Before the fix, when one source's stream finished (returned None),
    /// the entire Subscriptions stream would propagate that None — killing
    /// the whole Richat process even though other sources were still healthy.
    #[tokio::test]
    async fn one_source_finishes_others_continue() {
        // source_a has a message, source_b finishes immediately (simulates disconnect)
        // Round-robin with last_polled=0 polls index 1 first, so source_b (empty)
        // is hit first — this is the exact scenario that triggered the bug.
        let (source_a, mut tx) = mock_channel_subscription("source_a");
        let source_b = mock_subscription("source_b", stream::empty().boxed());

        let mut subs = mock_subscriptions(vec![source_a, source_b]);

        // Send a message on the healthy source
        tx.try_send(Err(ReceiveError::ReplayFailed)).unwrap();

        // Poll: source_b (index 1) is polled first and is empty.
        // Old code: propagated None, killing the instance.
        // Fixed code: removes source_b, continues to poll source_a.
        let result = subs.next().await;
        assert!(
            result.is_some(),
            "Subscriptions should NOT return None when one source finishes"
        );

        // Verify source_b was removed
        assert_eq!(subs.streams.len(), 1, "source_b should have been removed");
        assert_eq!(subs.streams[0].name, "source_a");
    }

    /// When ALL sources finish, the stream should return None.
    #[tokio::test]
    async fn all_sources_finish_returns_none() {
        let source_a = mock_subscription("a", stream::empty().boxed());
        let source_b = mock_subscription("b", stream::empty().boxed());

        let mut subs = mock_subscriptions(vec![source_a, source_b]);

        let result = subs.next().await;
        assert!(
            result.is_none(),
            "Subscriptions should return None when all sources are finished"
        );
        assert!(subs.streams.is_empty());
    }

    /// Messages from multiple active sources should be round-robin polled.
    #[tokio::test]
    async fn round_robin_across_sources() {
        let (source_a, mut tx_a) = mock_channel_subscription("a");
        let (source_b, mut tx_b) = mock_channel_subscription("b");

        let mut subs = mock_subscriptions(vec![source_a, source_b]);

        // Send an error from each (easy to construct, proves delivery from both)
        tx_a.try_send(Err(ReceiveError::ReplayFailed)).unwrap();
        tx_b.try_send(Err(ReceiveError::ReplayFailed)).unwrap();

        // Both messages should be delivered
        let r1 = subs.next().await;
        let r2 = subs.next().await;
        assert!(r1.is_some());
        assert!(r2.is_some());
        assert_eq!(subs.streams.len(), 2);
    }

    /// Verifies the old buggy behavior: a single source returning None
    /// would have terminated the entire Subscriptions stream.
    /// This test documents the bug that was fixed.
    #[tokio::test]
    async fn regression_single_disconnect_does_not_kill_instance() {
        // 3 sources: source_a and source_c (alive), source_b (disconnects)
        // Round-robin polls index 1 first (source_b), which is the dead one.
        let (source_a, mut tx_a) = mock_channel_subscription("source_a");
        let source_b = mock_subscription("source_b", stream::empty().boxed());
        let (source_c, mut tx_c) = mock_channel_subscription("source_c");

        let mut subs = mock_subscriptions(vec![source_a, source_b, source_c]);

        // Send messages on the two healthy sources
        tx_a.try_send(Err(ReceiveError::ReplayFailed)).unwrap();
        tx_c.try_send(Err(ReceiveError::ReplayFailed)).unwrap();

        // Should get both messages despite source_b being dead
        let r1 = subs.next().await;
        let r2 = subs.next().await;
        assert!(r1.is_some(), "first healthy source message should arrive");
        assert!(r2.is_some(), "second healthy source message should arrive");
        assert_eq!(
            subs.streams.len(),
            2,
            "only the two healthy sources should remain"
        );

        let names: Vec<&str> = subs.streams.iter().map(|s| s.name).collect();
        assert!(names.contains(&"source_a"));
        assert!(names.contains(&"source_c"));
        assert!(!names.contains(&"source_b"));
    }
}

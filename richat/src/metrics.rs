use {
    crate::version::VERSION as VERSION_INFO,
    hyper::body::Bytes,
    metrics::{counter, describe_counter, describe_gauge},
    metrics_exporter_prometheus::{BuildError, PrometheusBuilder, PrometheusHandle},
    richat_filter::filter::FilteredUpdateType,
    richat_shared::config::ConfigMetrics,
    solana_sdk::clock::Slot,
    std::future::Future,
    tokio::{
        task::JoinError,
        time::{sleep, Duration},
    },
    tracing::error,
};

pub const BLOCK_MESSAGE_FAILED: &str = "block_message_failed"; // reason
pub const CHANNEL_SLOT: &str = "channel_slot"; // commitment
pub const CHANNEL_MESSAGES_TOTAL: &str = "channel_messages_total";
pub const CHANNEL_SLOTS_TOTAL: &str = "channel_slots_total";
pub const CHANNEL_BYTES_TOTAL: &str = "channel_bytes_total";
pub const GRPC_BLOCK_META_SLOT: &str = "grpc_block_meta_slot"; // commitment
pub const GRPC_BLOCK_META_QUEUE_SIZE: &str = "grpc_block_meta_queue_size";
pub const GRPC_REQUESTS_TOTAL: &str = "grpc_requests_total"; // x_subscription_id, method
pub const GRPC_SUBSCRIBE_TOTAL: &str = "grpc_subscribe_total"; // x_subscription_id
pub const GRPC_SUBSCRIBE_MESSAGES_COUNT_TOTAL: &str = "grpc_subscribe_messages_count_total"; // x_subscription_id, message
pub const GRPC_SUBSCRIBE_MESSAGES_BYTES_TOTAL: &str = "grpc_subscribe_messages_bytes_total"; // x_subscription_id, message
pub const GRPC_SUBSCRIBE_CPU_SECONDS_TOTAL: &str = "grpc_subscribe_cpu_seconds_total"; // x_subscription_id
pub const PUBSUB_SLOT: &str = "pubsub_slot"; // commitment
pub const PUBSUB_CACHED_SIGNATURES_TOTAL: &str = "pubsub_cached_signatures_total";
pub const PUBSUB_STORED_MESSAGES_COUNT_TOTAL: &str = "pubsub_stored_messages_count_total";
pub const PUBSUB_STORED_MESSAGES_BYTES_TOTAL: &str = "pubsub_stored_messages_bytes_total";
pub const PUBSUB_CONNECTIONS_TOTAL: &str = "pubsub_connections_total"; // x_subscription_id
pub const PUBSUB_SUBSCRIPTIONS_TOTAL: &str = "pubsub_subscriptions_total"; // x_subscription_id, subscription
pub const PUBSUB_MESSAGES_SENT_COUNT_TOTAL: &str = "pubsub_messages_sent_count_total"; // x_subscription_id, subscription
pub const PUBSUB_MESSAGES_SENT_BYTES_TOTAL: &str = "pubsub_messages_sent_bytes_total"; // x_subscription_id, subscription
pub const RICHAT_CONNECTIONS_TOTAL: &str = "richat_connections_total"; // transport

pub fn setup() -> Result<PrometheusHandle, BuildError> {
    let handle = PrometheusBuilder::new().install_recorder()?;

    describe_counter!("version", "Richat App version info");
    counter!(
        "version",
        "buildts" => VERSION_INFO.buildts,
        "git" => VERSION_INFO.git,
        "package" => VERSION_INFO.package,
        "proto" => VERSION_INFO.proto,
        "rustc" => VERSION_INFO.rustc,
        "solana" => VERSION_INFO.solana,
        "version" => VERSION_INFO.version,
    )
    .absolute(1);

    describe_counter!(BLOCK_MESSAGE_FAILED, "Block message reconstruction errors");
    describe_gauge!(CHANNEL_SLOT, "Latest slot in channel by commitment");
    describe_gauge!(
        CHANNEL_MESSAGES_TOTAL,
        "Total number of messages in channel"
    );
    describe_gauge!(CHANNEL_SLOTS_TOTAL, "Total number of slots in channel");
    describe_gauge!(CHANNEL_BYTES_TOTAL, "Total size of all messages in channel");
    describe_gauge!(GRPC_BLOCK_META_SLOT, "Latest slot in gRPC block meta");
    describe_gauge!(
        GRPC_BLOCK_META_QUEUE_SIZE,
        "Number of gRPC requests to block meta data"
    );
    describe_counter!(GRPC_REQUESTS_TOTAL, "Number of gRPC requests per method");
    describe_gauge!(GRPC_SUBSCRIBE_TOTAL, "Number of gRPC subscriptions");
    describe_counter!(
        GRPC_SUBSCRIBE_MESSAGES_COUNT_TOTAL,
        "Number of gRPC messages in subscriptions by type"
    );
    describe_counter!(
        GRPC_SUBSCRIBE_MESSAGES_BYTES_TOTAL,
        "Total size of gRPC messages in subscriptions by type"
    );
    describe_gauge!(
        GRPC_SUBSCRIBE_CPU_SECONDS_TOTAL,
        "CPU consumption of gRPC filters in subscriptions"
    );
    describe_gauge!(PUBSUB_SLOT, "Latest slot handled in PubSub by commitment");
    describe_gauge!(
        PUBSUB_CACHED_SIGNATURES_TOTAL,
        "Number of cached signatures"
    );
    describe_gauge!(
        PUBSUB_STORED_MESSAGES_COUNT_TOTAL,
        "Number of stored filtered messages in cache"
    );
    describe_gauge!(
        PUBSUB_STORED_MESSAGES_BYTES_TOTAL,
        "Total size of stored filtered messages in cache"
    );
    describe_gauge!(PUBSUB_CONNECTIONS_TOTAL, "Number of connections to PubSub");
    describe_gauge!(
        PUBSUB_SUBSCRIPTIONS_TOTAL,
        "Number of subscriptions by type"
    );
    describe_counter!(
        PUBSUB_MESSAGES_SENT_COUNT_TOTAL,
        "Number of sent filtered messages by type"
    );
    describe_counter!(
        PUBSUB_MESSAGES_SENT_BYTES_TOTAL,
        "Total size of sent filtered messages by type"
    );
    describe_gauge!(
        RICHAT_CONNECTIONS_TOTAL,
        "Total number of connections to Richat"
    );

    Ok(handle)
}

pub async fn spawn_server(
    config: ConfigMetrics,
    handle: PrometheusHandle,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<impl Future<Output = Result<(), JoinError>>> {
    let recorder_handle = handle.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(1)).await;
            recorder_handle.run_upkeep();
        }
    });

    richat_shared::metrics::spawn_server(
        config,
        move || Bytes::from(handle.render()), // metrics
        || true,                              // health
        || true,                              // ready
        shutdown,
    )
    .await
    .map_err(Into::into)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockMessageFailedReason {
    MissedBlockMeta,
    MismatchTransactions,
    MismatchEntries,
    ExtraAccount,
    ExtraTransaction,
    ExtraEntry,
    ExtraBlockMeta,
}

impl BlockMessageFailedReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::MissedBlockMeta => "MissedBlockMeta",
            Self::MismatchTransactions => "MismatchTransactions",
            Self::MismatchEntries => "MismatchEntries",
            Self::ExtraAccount => "ExtraAccount",
            Self::ExtraTransaction => "ExtraTransaction",
            Self::ExtraEntry => "ExtraEntry",
            Self::ExtraBlockMeta => "ExtraBlockMeta",
        }
    }
}

pub fn block_message_failed_inc(slot: Slot, reasons: &[BlockMessageFailedReason]) {
    if !reasons.is_empty() {
        error!(
            "failed to build block ({slot}): {}",
            reasons
                .iter()
                .map(|r| r.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );

        for reason in reasons {
            counter!(BLOCK_MESSAGE_FAILED, "reason" => reason.as_str()).increment(1);
        }
        counter!(BLOCK_MESSAGE_FAILED, "reason" => "Total").increment(reasons.len() as u64);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcSubscribeMessage {
    Slot,
    Account,
    Transaction,
    TransactionStatus,
    Entry,
    BlockMeta,
    Block,
    Ping,
    Pong,
}

impl<'a> From<&FilteredUpdateType<'a>> for GrpcSubscribeMessage {
    fn from(value: &FilteredUpdateType<'a>) -> Self {
        match value {
            FilteredUpdateType::Slot { .. } => Self::Slot,
            FilteredUpdateType::Account { .. } => Self::Account,
            FilteredUpdateType::Transaction { .. } => Self::Transaction,
            FilteredUpdateType::TransactionStatus { .. } => Self::TransactionStatus,
            FilteredUpdateType::Entry { .. } => Self::Entry,
            FilteredUpdateType::BlockMeta { .. } => Self::BlockMeta,
            FilteredUpdateType::Block { .. } => Self::Block,
        }
    }
}

impl GrpcSubscribeMessage {
    pub const fn as_str(self) -> &'static str {
        match self {
            GrpcSubscribeMessage::Slot => "slot",
            GrpcSubscribeMessage::Account => "account",
            GrpcSubscribeMessage::Transaction => "transaction",
            GrpcSubscribeMessage::TransactionStatus => "transactionstatus",
            GrpcSubscribeMessage::Entry => "entry",
            GrpcSubscribeMessage::BlockMeta => "blockmeta",
            GrpcSubscribeMessage::Block => "block",
            GrpcSubscribeMessage::Ping => "ping",
            GrpcSubscribeMessage::Pong => "pong",
        }
    }
}

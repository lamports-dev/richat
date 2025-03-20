use {
    crate::{pubsub::solana::SubscribeMethod, version::VERSION as VERSION_INFO},
    prometheus::{GaugeVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry},
    richat_filter::{filter::FilteredUpdateType, message::MessageSlot},
    richat_proto::geyser::SlotStatus,
    richat_shared::config::ConfigMetrics,
    solana_sdk::{clock::Slot, commitment_config::CommitmentLevel},
    std::{future::Future, sync::Once, time::Duration},
    tokio::task::JoinError,
    tracing::error,
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Richat App version info"),
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
    ).unwrap();

    // Block build
    static ref BLOCK_MESSAGE_FAILED: IntGaugeVec = IntGaugeVec::new(
        Opts::new("block_message_failed", "Block message reconstruction errors"),
        &["reason"]
    ).unwrap();

    // Channel
    static ref CHANNEL_SLOT: IntGaugeVec = IntGaugeVec::new(
        Opts::new("channel_slot", "Latest slot in channel by commitment"),
        &["commitment"]
    ).unwrap();

    static ref CHANNEL_MESSAGES_TOTAL: IntGauge = IntGauge::new(
        "channel_messages_total", "Total number of messages in channel"
    ).unwrap();

    static ref CHANNEL_SLOTS_TOTAL: IntGauge = IntGauge::new(
        "channel_slots_total", "Total number of slots in channel"
    ).unwrap();

    static ref CHANNEL_BYTES_TOTAL: IntGauge = IntGauge::new(
        "channel_bytes_total", "Total size of all messages in channel"
    ).unwrap();

    // gRPC block meta
    static ref GRPC_BLOCK_META_SLOT: IntGaugeVec = IntGaugeVec::new(
        Opts::new("grpc_block_meta_slot", "Latest slot in gRPC block meta"),
        &["commitment"]
    ).unwrap();

    static ref GRPC_BLOCK_META_QUEUE_SIZE: IntGauge = IntGauge::new(
        "grpc_block_meta_queue_size", "Number of gRPC requests to block meta data"
    ).unwrap();

    // gRPC
    static ref GRPC_REQUESTS_TOTAL: IntGaugeVec = IntGaugeVec::new(
        Opts::new("grpc_requests_total", "Number of gRPC requests per method"),
        &["x_subscription_id", "method"]
    ).unwrap();

    static ref GRPC_SUBSCRIBE_TOTAL: IntGaugeVec = IntGaugeVec::new(
        Opts::new("grpc_subscribe_total", "Number of gRPC subscriptions"),
        &["x_subscription_id"]
    ).unwrap();

    static ref GRPC_SUBSCRIBE_MESSAGES_COUNT_TOTAL: IntGaugeVec = IntGaugeVec::new(
        Opts::new("grpc_subscribe_messages_count_total", "Number of gRPC messages in subscriptions by type"),
        &["x_subscription_id", "message"]
    ).unwrap();

    static ref GRPC_SUBSCRIBE_MESSAGES_BYTES_TOTAL: IntGaugeVec = IntGaugeVec::new(
        Opts::new("grpc_subscribe_messages_bytes_total", "Total size of gRPC messages in subscriptions by type"),
        &["x_subscription_id", "message"]
    ).unwrap();

    static ref GRPC_SUBSCRIBE_CPU_SECONDS_TOTAL: GaugeVec = GaugeVec::new(
        Opts::new("grpc_subscribe_cpu_seconds_total", "CPU consumption of gRPC filters in subscriptions"),
        &["x_subscription_id"]
    ).unwrap();

    // PubSub
    static ref PUBSUB_SLOT: IntGaugeVec = IntGaugeVec::new(
        Opts::new("pubsub_slot", "Latest slot handled in PubSub by commitment"),
        &["commitment"]
    ).unwrap();

    static ref PUBSUB_CACHED_SIGNATURES_TOTAL: IntGauge = IntGauge::new(
        "pubsub_cached_signatures_total", "Number of cached signatures"
    ).unwrap();

    static ref PUBSUB_STORED_MESSAGES_COUNT_TOTAL: IntGauge = IntGauge::new(
        "pubsub_stored_messages_count_total", "Number of stored filtered messages in cache"
    ).unwrap();

    static ref PUBSUB_STORED_MESSAGES_BYTES_TOTAL: IntGauge = IntGauge::new(
        "pubsub_stored_messages_bytes_total", "Total size of stored filtered messages in cache"
    ).unwrap();

    static ref PUBSUB_CONNECTIONS_TOTAL: IntGaugeVec = IntGaugeVec::new(
        Opts::new("pubsub_connections_total", "Number of connections to PubSub"),
        &["x_subscription_id"]
    ).unwrap();

    static ref PUBSUB_SUBSCRIPTIONS_TOTAL: IntGaugeVec = IntGaugeVec::new(
        Opts::new("pubsub_subscriptions_total", "Number of subscriptions by type"),
        &["x_subscription_id", "subscription"]
    ).unwrap();

    static ref PUBSUB_MESSAGES_SENT_COUNT_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("pubsub_messages_sent_count_total", "Number of sent filtered messages by type"),
        &["x_subscription_id", "subscription"]
    ).unwrap();

    static ref PUBSUB_MESSAGES_SENT_BYTES_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("pubsub_messages_sent_bytes_total", "Total size of sent filtered messages by type"),
        &["x_subscription_id", "subscription"]
    ).unwrap();

    // Richat
    static ref RICHAT_CONNECTIONS_TOTAL: IntGaugeVec = IntGaugeVec::new(
        Opts::new("richat_connections_total", "Total number of connections to Richat"),
        &["transport"]
    ).unwrap();
}

pub async fn spawn_server(
    config: ConfigMetrics,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<impl Future<Output = Result<(), JoinError>>> {
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        macro_rules! register {
            ($collector:ident) => {
                REGISTRY
                    .register(Box::new($collector.clone()))
                    .expect("collector can't be registered");
            };
        }
        register!(VERSION);
        register!(BLOCK_MESSAGE_FAILED);
        register!(CHANNEL_SLOT);
        register!(CHANNEL_MESSAGES_TOTAL);
        register!(CHANNEL_SLOTS_TOTAL);
        register!(CHANNEL_BYTES_TOTAL);
        register!(GRPC_BLOCK_META_SLOT);
        register!(GRPC_BLOCK_META_QUEUE_SIZE);
        register!(GRPC_REQUESTS_TOTAL);
        register!(GRPC_SUBSCRIBE_TOTAL);
        register!(GRPC_SUBSCRIBE_MESSAGES_COUNT_TOTAL);
        register!(GRPC_SUBSCRIBE_MESSAGES_BYTES_TOTAL);
        register!(GRPC_SUBSCRIBE_CPU_SECONDS_TOTAL);
        register!(PUBSUB_SLOT);
        register!(PUBSUB_CACHED_SIGNATURES_TOTAL);
        register!(PUBSUB_STORED_MESSAGES_COUNT_TOTAL);
        register!(PUBSUB_STORED_MESSAGES_BYTES_TOTAL);
        register!(PUBSUB_CONNECTIONS_TOTAL);
        register!(PUBSUB_SUBSCRIPTIONS_TOTAL);
        register!(PUBSUB_MESSAGES_SENT_COUNT_TOTAL);
        register!(PUBSUB_MESSAGES_SENT_BYTES_TOTAL);
        register!(RICHAT_CONNECTIONS_TOTAL);

        VERSION
            .with_label_values(&[
                VERSION_INFO.buildts,
                VERSION_INFO.git,
                VERSION_INFO.package,
                VERSION_INFO.proto,
                VERSION_INFO.rustc,
                VERSION_INFO.solana,
                VERSION_INFO.version,
            ])
            .inc();
    });

    richat_shared::metrics::spawn_server(config, || REGISTRY.gather(), shutdown)
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
            BLOCK_MESSAGE_FAILED
                .with_label_values(&[reason.as_str()])
                .inc();
        }
        BLOCK_MESSAGE_FAILED
            .with_label_values(&["Total"])
            .add(reasons.len() as i64);
    }
}

pub fn channel_slot_set(message: &MessageSlot) {
    if let Some(commitment) = match message.status() {
        SlotStatus::SlotProcessed => Some("processed"),
        SlotStatus::SlotConfirmed => Some("confirmed"),
        SlotStatus::SlotFinalized => Some("finalized"),
        _ => None,
    } {
        CHANNEL_SLOT
            .with_label_values(&[commitment])
            .set(message.slot() as i64)
    }
}

pub fn channel_messages_set(count: usize) {
    CHANNEL_MESSAGES_TOTAL.set(count as i64)
}

pub fn channel_slots_set(count: usize) {
    CHANNEL_SLOTS_TOTAL.set(count as i64)
}

pub fn channel_bytes_set(bytes: usize) {
    CHANNEL_BYTES_TOTAL.set(bytes as i64)
}

pub fn grpc_block_meta_slot_set(commitment: CommitmentLevel, slot: Slot) {
    if let Some(commitment) = match commitment {
        CommitmentLevel::Processed => Some("processed"),
        CommitmentLevel::Confirmed => Some("confirmed"),
        CommitmentLevel::Finalized => Some("finalized"),
    } {
        GRPC_BLOCK_META_SLOT
            .with_label_values(&[commitment])
            .set(slot as i64)
    }
}

pub fn grpc_block_meta_queue_inc() {
    GRPC_BLOCK_META_QUEUE_SIZE.inc()
}

pub fn grpc_block_meta_queue_dec() {
    GRPC_BLOCK_META_QUEUE_SIZE.dec()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrpcRequestMethod {
    Subscribe,
    Ping,
    GetLatestBlockhash,
    GetBlockHeight,
    GetSlot,
    IsBlockhashValid,
    GetVersion,
}

impl GrpcRequestMethod {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Subscribe => "subscribe",
            Self::Ping => "ping",
            Self::GetLatestBlockhash => "get_latest_blockhash",
            Self::GetBlockHeight => "get_block_height",
            Self::GetSlot => "get_slot",
            Self::IsBlockhashValid => "is_blockhash_valid",
            Self::GetVersion => "get_version",
        }
    }
}

pub fn grpc_requests_inc(x_subscription_id: &str, method: GrpcRequestMethod) {
    GRPC_REQUESTS_TOTAL
        .with_label_values(&[x_subscription_id, method.as_str()])
        .inc()
}

pub fn grpc_subscribe_inc(x_subscription_id: &str) {
    GRPC_SUBSCRIBE_TOTAL
        .with_label_values(&[x_subscription_id])
        .inc()
}

pub fn grpc_subscribe_dec(x_subscription_id: &str) {
    GRPC_SUBSCRIBE_TOTAL
        .with_label_values(&[x_subscription_id])
        .dec()
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

pub fn grpc_subscribe_messages_inc(
    x_subscription_id: &str,
    message: GrpcSubscribeMessage,
    bytes: usize,
) {
    GRPC_SUBSCRIBE_MESSAGES_COUNT_TOTAL
        .with_label_values(&[x_subscription_id, message.as_str()])
        .inc();
    GRPC_SUBSCRIBE_MESSAGES_BYTES_TOTAL
        .with_label_values(&[x_subscription_id, message.as_str()])
        .add(bytes as i64)
}

pub fn grpc_subscribe_cpu_inc(x_subscription_id: &str, elapsed: Duration) {
    GRPC_SUBSCRIBE_CPU_SECONDS_TOTAL
        .with_label_values(&[x_subscription_id])
        .add(elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1e9)
}

pub fn pubsub_slot_set(commitment: CommitmentLevel, slot: Slot) {
    if let Some(commitment) = match commitment {
        CommitmentLevel::Processed => Some("processed"),
        CommitmentLevel::Confirmed => Some("confirmed"),
        CommitmentLevel::Finalized => Some("finalized"),
    } {
        PUBSUB_SLOT
            .with_label_values(&[commitment])
            .set(slot as i64)
    }
}

pub fn pubsub_cached_signatures_set_count(count: usize) {
    PUBSUB_CACHED_SIGNATURES_TOTAL.set(count as i64)
}

pub fn pubsub_stored_messages_set(count: usize, bytes: usize) {
    PUBSUB_STORED_MESSAGES_COUNT_TOTAL.set(count as i64);
    PUBSUB_STORED_MESSAGES_BYTES_TOTAL.set(bytes as i64);
}

pub fn pubsub_connections_inc(x_subscription_id: &str) {
    PUBSUB_CONNECTIONS_TOTAL
        .with_label_values(&[x_subscription_id])
        .inc()
}

pub fn pubsub_connections_dec(x_subscription_id: &str) {
    PUBSUB_CONNECTIONS_TOTAL
        .with_label_values(&[x_subscription_id])
        .dec()
}

pub fn pubsub_subscriptions_inc(x_subscription_id: &str, subscription: SubscribeMethod) {
    PUBSUB_SUBSCRIPTIONS_TOTAL
        .with_label_values(&[x_subscription_id, subscription.as_str()])
        .inc()
}

pub fn pubsub_subscriptions_dec(x_subscription_id: &str, subscription: SubscribeMethod) {
    PUBSUB_SUBSCRIPTIONS_TOTAL
        .with_label_values(&[x_subscription_id, subscription.as_str()])
        .dec()
}

pub fn pubsub_messages_sent_inc(
    x_subscription_id: &str,
    subscription: SubscribeMethod,
    bytes: usize,
) {
    PUBSUB_MESSAGES_SENT_COUNT_TOTAL
        .with_label_values(&[x_subscription_id, subscription.as_str()])
        .inc();
    PUBSUB_MESSAGES_SENT_BYTES_TOTAL
        .with_label_values(&[x_subscription_id, subscription.as_str()])
        .inc_by(bytes as u64);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RichatConnectionsTransport {
    Grpc,
    Quic,
    Tcp,
}

impl RichatConnectionsTransport {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::Quic => "quic",
            Self::Tcp => "tcp",
        }
    }
}

pub fn richat_connections_add(transport: RichatConnectionsTransport) {
    RICHAT_CONNECTIONS_TOTAL
        .with_label_values(&[transport.as_str()])
        .inc();
}

pub fn richat_connections_dec(transport: RichatConnectionsTransport) {
    RICHAT_CONNECTIONS_TOTAL
        .with_label_values(&[transport.as_str()])
        .dec();
}

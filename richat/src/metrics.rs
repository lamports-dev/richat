use {
    crate::{channel::message::MessageSlot, version::VERSION as VERSION_INFO},
    prometheus::{IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry},
    richat_proto::geyser::CommitmentLevel,
    richat_shared::config::ConfigPrometheus,
    solana_sdk::clock::Slot,
    std::{future::Future, sync::Once},
    tokio::task::JoinError,
    tracing::error,
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Richat App version info"),
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
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

    // Block build
    static ref BLOCK_MESSAGE_FAILED: IntGaugeVec = IntGaugeVec::new(
        Opts::new("block_message_failed", "Block message reconstruction errors"),
        &["reason"]
    ).unwrap();
}

pub async fn spawn_server(
    config: ConfigPrometheus,
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
        register!(CHANNEL_SLOT);
        register!(CHANNEL_MESSAGES_TOTAL);
        register!(CHANNEL_SLOTS_TOTAL);
        register!(CHANNEL_BYTES_TOTAL);
        register!(BLOCK_MESSAGE_FAILED);

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

pub fn channel_slot_set(message: &MessageSlot) {
    if let Some(commitment) = match message.commitment {
        CommitmentLevel::Processed => Some("processed"),
        CommitmentLevel::Confirmed => Some("confirmed"),
        CommitmentLevel::Finalized => Some("finalized"),
        _ => None,
    } {
        CHANNEL_SLOT
            .with_label_values(&[commitment])
            .set(message.slot as i64)
    }
}

pub fn channel_messages_set(count: usize) {
    CHANNEL_MESSAGES_TOTAL.set(count as i64)
}

pub fn channel_slots_set(count: usize) {
    CHANNEL_SLOTS_TOTAL.set(count as i64)
}

pub fn channel_bytes_set(count: usize) {
    CHANNEL_BYTES_TOTAL.set(count as i64)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockMessageFailedReason {
    MissedBlockMeta,
    MissedAccountUpdate,
    TransactionsMismatch,
    EntriesMismatch,
}

impl BlockMessageFailedReason {
    const fn as_str(self) -> &'static str {
        match self {
            Self::MissedBlockMeta => "MissedBlockMeta",
            Self::MissedAccountUpdate => "MissedAccountUpdate",
            Self::TransactionsMismatch => "TransactionsMismatch",
            Self::EntriesMismatch => "EntriesMismatch",
        }
    }
}

pub fn block_message_failed_inc(slot: Slot, reasons: &[BlockMessageFailedReason]) {
    error!(
        "full block failed ({slot}): {}",
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

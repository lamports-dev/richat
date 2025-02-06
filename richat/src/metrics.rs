use {
    crate::version::VERSION as VERSION_INFO,
    prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry},
    richat_shared::config::ConfigMetrics,
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

    // Block build
    static ref BLOCK_MESSAGE_FAILED: IntGaugeVec = IntGaugeVec::new(
        Opts::new("block_message_failed", "Block message reconstruction errors"),
        &["reason"]
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

use {
    crate::version::VERSION as VERSION_INFO,
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus,
    prometheus::{IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry},
    richat_shared::config::ConfigPrometheus,
    solana_sdk::clock::Slot,
    std::{future::Future, sync::Once},
    tokio::task::JoinHandle,
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Richat Plugin version info"),
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
    ).unwrap();

    // Geyser
    static ref GEYSER_SLOT_STATUS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("geyser_slot_status", "Latest slot received from Geyser"),
        &["status"]
    ).unwrap();

    static ref GEYSER_MISSED_SLOT_STATUS: IntCounterVec = IntCounterVec::new(
        Opts::new("geyser_missed_slot_status_total", "Number of missed slot status updates"),
        &["status"]
    ).unwrap();

    // Channel
    static ref CHANNEL_MESSAGES_TOTAL: IntGauge = IntGauge::new(
        "channel_messages_total", "Total number of messages in channel"
    ).unwrap();

    static ref CHANNEL_SLOTS_TOTAL: IntGauge = IntGauge::new(
        "channel_slots_total", "Total number of slots in channel"
    ).unwrap();

    static ref CHANNEL_BYTES_TOTAL: IntGauge = IntGauge::new(
        "channel_bytes_total", "Total size of all messages in channel"
    ).unwrap();

    // gRPC
    static ref GRPC_CONNECTIONS_TOTAL: IntGauge = IntGauge::new(
        "grpc_connections_total", "Total number of connections"
    ).unwrap();
}

pub async fn spawn_server(
    config: ConfigPrometheus,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<JoinHandle<()>> {
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
        register!(GEYSER_SLOT_STATUS);
        register!(GEYSER_MISSED_SLOT_STATUS);
        register!(CHANNEL_MESSAGES_TOTAL);
        register!(CHANNEL_SLOTS_TOTAL);
        register!(CHANNEL_BYTES_TOTAL);
        register!(GRPC_CONNECTIONS_TOTAL);

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

pub fn geyser_slot_status_set(slot: Slot, status: &SlotStatus) {
    if let Some(status) = match status {
        SlotStatus::Processed => Some("processed"),
        SlotStatus::Rooted => Some("finalized"),
        SlotStatus::Confirmed => Some("confirmed"),
        SlotStatus::FirstShredReceived => Some("first_shred_received"),
        SlotStatus::Completed => Some("completed"),
        SlotStatus::CreatedBank => Some("created_bank"),
        SlotStatus::Dead(_) => None,
    } {
        GEYSER_SLOT_STATUS
            .with_label_values(&[status])
            .set(slot as i64);
    }
}

pub fn geyser_missed_slot_status_inc(status: &SlotStatus) {
    if let Some(status) = match status {
        SlotStatus::Confirmed => Some("confirmed"),
        SlotStatus::Rooted => Some("finalized"),
        _ => None,
    } {
        GEYSER_MISSED_SLOT_STATUS.with_label_values(&[status]).inc()
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

pub fn grpc_connection_new() {
    GRPC_CONNECTIONS_TOTAL.inc();
}

pub fn grpc_connection_drop() {
    GRPC_CONNECTIONS_TOTAL.dec();
}

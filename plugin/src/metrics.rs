use {
    crate::version::VERSION as VERSION_INFO,
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus,
    futures::future::{try_join, TryFutureExt},
    prometheus::{
        HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
    },
    richat_shared::{config::ConfigPrometheus, shutdown::Shutdown},
    solana_sdk::clock::Slot,
    std::{
        future::Future,
        sync::{Once, OnceLock},
    },
    tokio::{sync::mpsc, task::JoinError},
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

    // Connections
    static ref CONNECTIONS_TOTAL: IntGaugeVec = IntGaugeVec::new(
        Opts::new("connections_total", "Total number of connections"),
        &["transport"]
    ).unwrap();

    static ref CONNECTIONS_LAGGED_TOTAL: IntCounterVec = IntCounterVec::new(
        Opts::new("connections_lagged_total", "Total number of lagged connections"),
        &["transport"]
    ).unwrap();

    static ref CONNECTIONS_SLOT_LAG: HistogramVec = HistogramVec::new(
        HistogramOpts {
            common_opts: Opts::new("connections_slot_lag", "Connection lag in slots"),
            buckets: vec![0.0, 1.0, 2.0, 3.0, 5.0, 10.0, 15.0, 20.0, 25.0, 50.0, 100.0]
        },
        &["transport"]
    ).unwrap();
}

static CONNECTIONS_SLOT_LAG_TX: OnceLock<mpsc::Sender<ConnectionsSlotLagMessage>> = OnceLock::new();

pub async fn spawn_server(
    config: ConfigPrometheus,
    shutdown: Shutdown,
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
        register!(GEYSER_SLOT_STATUS);
        register!(GEYSER_MISSED_SLOT_STATUS);
        register!(CHANNEL_MESSAGES_TOTAL);
        register!(CHANNEL_SLOTS_TOTAL);
        register!(CHANNEL_BYTES_TOTAL);
        register!(CONNECTIONS_TOTAL);
        register!(CONNECTIONS_LAGGED_TOTAL);
        register!(CONNECTIONS_SLOT_LAG);

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

    let (tx, rx) = mpsc::channel(8_192);
    anyhow::ensure!(
        CONNECTIONS_SLOT_LAG_TX.set(tx).is_ok(),
        "failed to set connections slot lag tx channel"
    );
    let connections_slot_lag_jh = tokio::spawn({
        let shutdown = shutdown.clone();
        async move {
            tokio::select! {
                () = shutdown => {},
                () = connections_slot_lag_loop(rx) => {}
            }
        }
    });

    let metrics_jh =
        richat_shared::metrics::spawn_server(config, || REGISTRY.gather(), shutdown).await?;

    Ok(try_join(connections_slot_lag_jh, metrics_jh).map_ok(|_| ()))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionsTransport {
    Grpc,
    Quic,
    Tcp,
}

impl ConnectionsTransport {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::Quic => "quic",
            Self::Tcp => "tcp",
        }
    }
}

pub fn connections_total_add(transport: ConnectionsTransport) {
    CONNECTIONS_TOTAL
        .with_label_values(&[transport.as_str()])
        .inc();
}

pub fn connections_total_dec(transport: ConnectionsTransport) {
    CONNECTIONS_TOTAL
        .with_label_values(&[transport.as_str()])
        .dec();
}

pub fn connections_lagged_inc(transport: ConnectionsTransport) {
    CONNECTIONS_LAGGED_TOTAL
        .with_label_values(&[transport.as_str()])
        .inc();
}

#[derive(Debug)]
enum ConnectionsSlotLagMessage {
    NewTip(Slot),
    ObserveSlot(ConnectionsTransport, Slot),
}

#[derive(Debug)]
pub struct ConnectionSlotLag {
    id: ConnectionsTransport,
    tx: mpsc::Sender<ConnectionsSlotLagMessage>,
    latest: Slot,
}

impl ConnectionSlotLag {
    pub fn observe(&mut self, slot: Slot) {
        if slot > self.latest {
            self.latest = slot;

            let msg = ConnectionsSlotLagMessage::ObserveSlot(self.id, slot);
            let _ = self.tx.try_send(msg);
        }
    }
}

pub fn connections_slot_lag_start(transport: ConnectionsTransport) -> Option<ConnectionSlotLag> {
    CONNECTIONS_SLOT_LAG_TX
        .get()
        .cloned()
        .map(|tx| ConnectionSlotLag {
            id: transport,
            tx,
            latest: 0,
        })
}

pub fn connections_slot_lag_new_tip(slot: Slot) {
    if let Some(tx) = CONNECTIONS_SLOT_LAG_TX.get() {
        let _ = tx.try_send(ConnectionsSlotLagMessage::NewTip(slot));
    }
}

async fn connections_slot_lag_loop(mut rx: mpsc::Receiver<ConnectionsSlotLagMessage>) {
    let mut latest = 0;

    while let Some(message) = rx.recv().await {
        match message {
            ConnectionsSlotLagMessage::NewTip(slot) => {
                latest = latest.max(slot);
            }
            ConnectionsSlotLagMessage::ObserveSlot(transport, slot) => {
                CONNECTIONS_SLOT_LAG
                    .with_label_values(&[transport.as_str()])
                    .observe((latest - slot) as f64);
            }
        }
    }
}

use {
    crate::version::VERSION as VERSION_INFO,
    metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle, PrometheusRecorder},
    richat_metrics::{counter, describe_counter, describe_gauge, ConfigMetrics},
    std::{future::Future, io},
    tokio::{
        task::JoinError,
        time::{sleep, Duration},
    },
};

pub const GEYSER_SLOT_STATUS: &str = "geyser_slot_status"; // status
pub const GEYSER_MISSED_SLOT_STATUS: &str = "geyser_missed_slot_status_total"; // status
pub const CHANNEL_MESSAGES_TOTAL: &str = "channel_messages_total";
pub const CHANNEL_SLOTS_TOTAL: &str = "channel_slots_total";
pub const CHANNEL_BYTES_TOTAL: &str = "channel_bytes_total";
pub const CONNECTIONS_TOTAL: &str = "connections_total"; // transport

#[rustfmt::skip]
pub fn setup() -> PrometheusRecorder {
    let recorder = PrometheusBuilder::new().build_recorder();

    describe_counter!(recorder, "version", "Richat Plugin version info");
    counter!(
        recorder,
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

    describe_gauge!(recorder, GEYSER_SLOT_STATUS, "Latest slot received from Geyser");
    describe_counter!(recorder, GEYSER_MISSED_SLOT_STATUS, "Number of missed slot status updates");
    describe_gauge!(recorder, CHANNEL_MESSAGES_TOTAL, "Total number of messages in channel");
    describe_gauge!(recorder, CHANNEL_SLOTS_TOTAL, "Total number of slots in channel");
    describe_gauge!(recorder, CHANNEL_BYTES_TOTAL, "Total size of all messages in channel");
    describe_gauge!(recorder, CONNECTIONS_TOTAL, "Total number of connections");

    recorder
}

pub async fn spawn_server(
    config: ConfigMetrics,
    handle: PrometheusHandle,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> io::Result<impl Future<Output = Result<(), JoinError>>> {
    let recorder_handle = handle.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(1)).await;
            recorder_handle.run_upkeep();
        }
    });

    richat_metrics::spawn_server(
        config,
        move || handle.render().into_bytes(), // metrics
        || true,                              // health
        || true,                              // ready
        shutdown,
    )
    .await
}

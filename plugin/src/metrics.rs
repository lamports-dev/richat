use {
    crate::{config::ConfigPrometheus, version::VERSION as VERSION_INFO},
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus,
    http_body_util::{combinators::BoxBody, BodyExt, Empty as BodyEmpty, Full as BodyFull},
    hyper::{
        body::{Bytes, Incoming as BodyIncoming},
        service::service_fn,
        Request, Response, StatusCode,
    },
    hyper_util::{
        rt::tokio::{TokioExecutor, TokioIo},
        server::conn::auto::Builder as ServerBuilder,
    },
    log::{error, info},
    prometheus::{IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder},
    solana_sdk::clock::Slot,
    std::{
        convert::Infallible,
        sync::{Arc, Once},
    },
    tokio::{net::TcpListener, sync::Notify},
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Plugin version info"),
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
    ).unwrap();

    // Geyser
    static ref GEYSER_SLOT_STATUS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("geyser_slot_status", "Latest slot received from Geyser"),
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
}

#[derive(Debug)]
pub struct PrometheusService {
    shutdown: Arc<Notify>,
}

impl PrometheusService {
    pub async fn new(config: Option<ConfigPrometheus>) -> std::io::Result<Self> {
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
            register!(CHANNEL_MESSAGES_TOTAL);
            register!(CHANNEL_SLOTS_TOTAL);
            register!(CHANNEL_BYTES_TOTAL);

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

        let shutdown = Arc::new(Notify::new());
        if let Some(ConfigPrometheus { endpoint: addr }) = config {
            let shutdown = Arc::clone(&shutdown);
            let listener = TcpListener::bind(&addr).await?;
            info!("start prometheus server: {addr}");
            tokio::spawn(async move {
                loop {
                    let stream = tokio::select! {
                        () = shutdown.notified() => break,
                        maybe_conn = listener.accept() => {
                            match maybe_conn {
                                Ok((stream, _addr)) => stream,
                                Err(error) => {
                                    error!("failed to accept new connection: {error}");
                                    break;
                                }
                            }
                        }
                    };
                    tokio::spawn(async move {
                        if let Err(error) = ServerBuilder::new(TokioExecutor::new())
                            .serve_connection(
                                TokioIo::new(stream),
                                service_fn(move |req: Request<BodyIncoming>| async move {
                                    match req.uri().path() {
                                        "/metrics" => metrics_handler(),
                                        _ => not_found_handler(),
                                    }
                                }),
                            )
                            .await
                        {
                            error!("failed to handle request: {error}");
                        }
                    });
                }
            });
        }

        Ok(PrometheusService { shutdown })
    }

    pub fn shutdown(self) {
        self.shutdown.notify_one();
    }
}

fn metrics_handler() -> http::Result<Response<BoxBody<Bytes, Infallible>>> {
    let metrics = TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {}", error);
            String::new()
        });
    Response::builder()
        .status(StatusCode::OK)
        .body(BodyFull::new(Bytes::from(metrics)).boxed())
}

fn not_found_handler() -> http::Result<Response<BoxBody<Bytes, Infallible>>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(BodyEmpty::new().boxed())
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

pub fn channel_messages_set(count: usize) {
    CHANNEL_MESSAGES_TOTAL.set(count as i64)
}

pub fn channel_slots_set(count: usize) {
    CHANNEL_SLOTS_TOTAL.set(count as i64)
}

pub fn channel_bytes_set(count: usize) {
    CHANNEL_BYTES_TOTAL.set(count as i64)
}

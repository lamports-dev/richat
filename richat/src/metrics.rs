use {
    crate::version::VERSION as VERSION_INFO,
    prometheus::{IntCounterVec, Opts, Registry},
    richat_shared::config::ConfigPrometheus,
    std::{future::Future, sync::Once},
    tokio::task::JoinHandle,
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Richat App version info"),
        &["buildts", "git", "package", "proto", "rustc", "solana", "version"]
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

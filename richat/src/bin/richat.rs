use {
    anyhow::Context,
    clap::Parser,
    futures::future::{pending, try_join_all, FutureExt, TryFutureExt},
    richat::config::Config,
    std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    tokio::{
        signal::unix::{signal, SignalKind},
        sync::broadcast,
    },
    tracing::{info, warn},
};

#[derive(Debug, Parser)]
#[clap(author, version, about = "Richat App")]
struct Args {
    #[clap(short, long, default_value_t = String::from("config.json"))]
    /// Path to config
    pub config: String,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = Config::load_from_file(&args.config)
        .with_context(|| format!("failed to load config from {}", args.config))?;

    // Setup logs
    richat::log::setup(config.log.json)?;

    // Shutdown channel/flag
    let shutdown_flag = Arc::new(AtomicBool::new(false));
    let (shutdown_tx, _rx) = broadcast::channel(1);
    let create_shutdown_rx = {
        let shutdown_tx = shutdown_tx.clone();
        move || {
            let mut rx = shutdown_tx.subscribe();
            async move {
                let _ = rx.recv().await;
            }
        }
    };

    // TODO: create channel

    // Create runtime for incoming connections
    let runtime = config.apps.tokio.build_runtime("richatApp")?;
    runtime.block_on(async move {
        let prometheus_fut = if let Some(config) = config.prometheus {
            richat::metrics::spawn_server(config, create_shutdown_rx())
                .await?
                .map_err(anyhow::Error::from)
                .boxed()
        } else {
            pending().boxed()
        };

        let tasks = try_join_all(vec![prometheus_fut]);
        tokio::pin!(tasks);

        let mut sigint = signal(SignalKind::interrupt())?;
        tokio::select! {
            result = &mut tasks => {
                result?;
            }
            _ = sigint.recv() => {
                info!("SIGINT received...");
                shutdown_flag.store(true, Ordering::Relaxed);
                let _ = shutdown_tx.send(());

                tokio::select! {
                    result = &mut tasks => {
                        result?;
                    }
                    _ = sigint.recv() => {
                        warn!("SIGINT received again, shutdown");
                    }
                }
            }
        }
        Ok(())
    })
}

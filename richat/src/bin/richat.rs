use {
    anyhow::Context,
    clap::Parser,
    futures::{
        future::{ready, try_join_all, FutureExt, TryFutureExt},
        stream::StreamExt,
    },
    richat::{
        channel::Messages, config::Config, grpc::server::GrpcServer, pubsub::server::PubSubServer,
        richat::server::RichatServer, source::Subscriptions,
    },
    richat_shared::shutdown::Shutdown,
    signal_hook::{consts::SIGINT, iterator::Signals},
    std::{
        thread::{self, sleep},
        time::Duration,
    },
    tracing::{info, warn},
};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Debug, Parser)]
#[clap(author, version, about = "Richat App")]
struct Args {
    /// Path to config
    #[clap(short, long, default_value_t = String::from("config.json"))]
    pub config: String,

    /// Only check config and exit
    #[clap(long, default_value_t = false)]
    pub check: bool,
}

fn main() -> anyhow::Result<()> {
    anyhow::ensure!(
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .is_ok(),
        "failed to call CryptoProvider::install_default()"
    );

    let args = Args::parse();
    let config = Config::load_from_file(&args.config)
        .with_context(|| format!("failed to load config from {}", args.config))?;
    if args.check {
        info!("Config is OK!");
        return Ok(());
    }

    let metrics_handle = if config.metrics.is_some() {
        Some(richat::metrics::setup().context("failed to setup metrics")?)
    } else {
        None
    };

    // Setup logs
    richat::log::setup(config.logs.json)?;

    // Shutdown channel/flag
    let shutdown = Shutdown::new();

    // Create channel runtime (receive messages from solana node / richat)
    let messages = Messages::new(
        config.channel.config,
        config.apps.richat.is_some(),
        config.apps.grpc.is_some(),
        config.apps.pubsub.is_some(),
    );
    let source_jh = thread::Builder::new()
        .name("richatSource".to_owned())
        .spawn({
            let shutdown = shutdown.clone();
            let mut messages = messages.to_sender();
            || {
                let runtime = config.channel.tokio.build_runtime("richatSource")?;
                runtime.block_on(async move {
                    let streams_total = config.channel.sources.len();
                    let mut stream = Subscriptions::new(config.channel.sources).await?;
                    tokio::pin!(shutdown);
                    loop {
                        let (index, message) = tokio::select! {
                            biased;
                            message = stream.next() => match message {
                                Some(Ok(value)) => value,
                                Some(Err(error)) => return Err(anyhow::Error::new(error)),
                                None => anyhow::bail!("source stream finished"),
                            },
                            () = &mut shutdown => return Ok(()),
                        };

                        let index_info = if streams_total == 1 {
                            None
                        } else {
                            Some((index, streams_total))
                        };
                        messages.push(message, index_info);
                    }
                })
            }
        })?;

    // Create runtime for incoming connections
    let apps_jh = thread::Builder::new().name("richatApp".to_owned()).spawn({
        let shutdown = shutdown.clone();
        move || {
            let runtime = config.apps.tokio.build_runtime("richatApp")?;
            runtime.block_on(async move {
                let richat_fut = if let Some(config) = config.apps.richat {
                    RichatServer::spawn(config, messages.clone(), shutdown.clone())
                        .await?
                        .boxed()
                } else {
                    ready(Ok(())).boxed()
                };

                let grpc_fut = if let Some(config) = config.apps.grpc {
                    GrpcServer::spawn(config, messages.clone(), shutdown.clone())?.boxed()
                } else {
                    ready(Ok(())).boxed()
                };

                let pubsub_fut = if let Some(config) = config.apps.pubsub {
                    PubSubServer::spawn(config, messages, shutdown.clone())?.boxed()
                } else {
                    ready(Ok(())).boxed()
                };

                let metrics_fut = if let (Some(config), Some(metrics_handle)) =
                    (config.metrics, metrics_handle)
                {
                    richat::metrics::spawn_server(config, metrics_handle, shutdown)
                        .await?
                        .map_err(anyhow::Error::from)
                        .boxed()
                } else {
                    ready(Ok(())).boxed()
                };

                try_join_all(vec![richat_fut, grpc_fut, pubsub_fut, metrics_fut])
                    .await
                    .map(|_| ())
            })
        }
    })?;

    let mut signals = Signals::new([SIGINT])?;
    let mut threads = [("source", Some(source_jh)), ("apps", Some(apps_jh))];
    'outer: while threads.iter().any(|th| th.1.is_some()) {
        for signal in signals.pending() {
            match signal {
                SIGINT => {
                    if shutdown.is_set() {
                        warn!("SIGINT received again, shutdown now");
                        break 'outer;
                    }
                    info!("SIGINT received...");
                    shutdown.shutdown();
                }
                _ => unreachable!(),
            }
        }

        for (name, tjh) in threads.iter_mut() {
            if let Some(jh) = tjh.take() {
                if jh.is_finished() {
                    jh.join()
                        .unwrap_or_else(|_| panic!("{name} thread join failed"))?;
                    info!("thread {name} finished");
                } else {
                    *tjh = Some(jh);
                }
            }
        }

        sleep(Duration::from_millis(25));
    }

    Ok(())
}

use {
    anyhow::Context,
    clap::Parser,
    futures::{
        future::{FutureExt, TryFutureExt, ready, try_join_all},
        stream::StreamExt,
    },
    richat::{
        channel::Messages,
        config::{Config, ConfigChannelSource},
        grpc::server::GrpcServer,
        pubsub::server::PubSubServer,
        richat::server::RichatServer,
        source::{PreparedReloadResult, ReceiveError, Subscriptions},
        version::VERSION,
    },
    richat_filter::message::MessageParserEncoding,
    signal_hook::{
        consts::{SIGINT, SIGUSR1},
        iterator::Signals,
    },
    std::{
        future::{Future, pending},
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, sleep},
        time::Duration,
    },
    tokio::sync::Notify,
    tokio_util::sync::CancellationToken,
    tracing::{error, info, warn},
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
    let config: Config = richat_shared::config::load_from_file_sync(&args.config)
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
    richat_shared::tracing::setup(config.logs.json)?;
    info!("version: {} / {}", VERSION.version, VERSION.git);

    // Shutdown channel/flag
    let shutdown = CancellationToken::new();
    let is_ready = Arc::new(AtomicBool::new(false));

    // Create channel runtime (receive messages from solana node / richat)
    let config_path = args.config.clone();
    let sources_parser = config.channel.get_messages_parser();
    let sources_sigusr1_reload = config.channel.sources_sigusr1_reload;
    if sources_sigusr1_reload {
        for source in &config.channel.sources {
            let (name, has_reconnect) = match source {
                ConfigChannelSource::Quic { general, .. } => {
                    (&general.name, general.reconnect.is_some())
                }
                ConfigChannelSource::Grpc { general, .. } => {
                    (&general.name, general.reconnect.is_some())
                }
            };
            anyhow::ensure!(
                has_reconnect,
                "source '{name}' must have reconnect configured when sources_sigusr1_reload is enabled"
            );
        }
    }
    let streams_total = config.channel.sources.len();
    let dedup_required = sources_sigusr1_reload || streams_total > 1;
    let reload_notify = Arc::new(Notify::new());

    let (messages, mut threads) = Messages::new(
        sources_parser,
        config.channel.config,
        config.apps.richat.is_some(),
        config.apps.grpc.is_some(),
        config.apps.pubsub.is_some(),
        shutdown.clone(),
    )?;
    let source_jh = thread::Builder::new()
        .name("richatSource".to_owned())
        .spawn({
            let shutdown = shutdown.clone();
            let mut messages = messages.clone();
            let is_ready = Arc::clone(&is_ready);
            let reload_notify = Arc::clone(&reload_notify);
            move || {
                let (mut sender, replay_from_slot) = messages.to_sender(config.channel.sources.len())?;
                let runtime = config.channel.tokio.build_runtime("richatSource")?;
                runtime.block_on(async move {
                    let mut stream = Subscriptions::new(
                        replay_from_slot,
                        config.channel.sources,
                    )
                    .await?;
                    is_ready.store(true, Ordering::Relaxed);

                    let shutdown = shutdown.cancelled();
                    tokio::pin!(shutdown);

                    let mut reload_in_progress = false;
                    let mut reload_prepare_task: Pin<Box<dyn Future<Output = PreparedReloadResult> + Send>> =
                        Box::pin(pending());

                    loop {
                        let (source_name, message) = tokio::select! {
                            biased;
                            message = stream.next() => match message {
                                Some(Ok(value)) => value,
                                Some(Err(error @ ReceiveError::ReplayFailed)) => {
                                    eprintln!("Error: {error:?}");
                                    std::process::exit(2);
                                }
                                Some(Err(error)) => return Err(
                                    anyhow::Error::new(error).context(format!("source: {}", stream.get_last_polled_name()))
                                ),
                                None => anyhow::bail!("{:?} source stream finished", stream.get_last_polled_name()),
                            },
                            _ = reload_notify.notified(), if sources_sigusr1_reload && !reload_in_progress => {
                                info!("SIGUSR1: reloading sources...");
                                match load_config_for_reloading(&config_path, sources_parser).await {
                                    Ok(config) => {
                                        reload_in_progress = true;
                                        reload_prepare_task = Box::pin(stream.prepare_reload(config.channel.sources));
                                    }
                                    Err(error) => error!("SIGUSR1: failed to load config: {error:?}"),
                                }
                                continue;
                            },
                            result = &mut reload_prepare_task, if reload_in_progress => {
                                reload_in_progress = false;
                                reload_prepare_task = Box::pin(pending());
                                match result {
                                    Ok((to_remove, new_streams)) => {
                                        stream.apply_reload(to_remove, new_streams);
                                        info!("SIGUSR1: sources reloaded");
                                    }
                                    Err(error) => error!("SIGUSR1: failed to reload sources: {error:?}"),
                                }
                                continue;
                            },
                            () = &mut shutdown => return Ok(()),
                        };
                        sender.push(dedup_required, source_name, message);
                    }
                })
            }
        })?;
    threads.push(("richatSource".to_owned(), Some(source_jh)));

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
                    richat::metrics::spawn_server(
                        config,
                        metrics_handle,
                        is_ready,
                        shutdown.cancelled_owned(),
                    )
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
    threads.push(("richatApp".to_owned(), Some(apps_jh)));

    let mut signals = Signals::new([SIGINT, SIGUSR1])?;
    'outer: while threads.iter().any(|th| th.1.is_some()) {
        for signal in signals.pending() {
            match signal {
                SIGINT => {
                    if shutdown.is_cancelled() {
                        warn!("SIGINT received again, shutdown now");
                        break 'outer;
                    }
                    info!("SIGINT received...");
                    shutdown.cancel();
                }
                SIGUSR1 => {
                    if sources_sigusr1_reload {
                        info!("SIGUSR1 received, triggering source reload...");
                        reload_notify.notify_one();
                    } else {
                        warn!("SIGUSR1 received but sources_sigusr1_reload is disabled");
                    }
                }
                _ => unreachable!(),
            }
        }

        for (name, tjh) in threads.iter_mut() {
            if let Some(jh) = tjh.take() {
                if jh.is_finished() {
                    let _: () = jh
                        .join()
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

async fn load_config_for_reloading(
    config_path: &str,
    current_parser: MessageParserEncoding,
) -> anyhow::Result<Config> {
    let config: Config = richat_shared::config::load_from_file(config_path)
        .await
        .with_context(|| format!("failed to load config from {config_path}"))?;

    // Validate parser hasn't changed
    let new_parser = config.channel.get_messages_parser();
    anyhow::ensure!(
        new_parser == current_parser,
        "MessageParserEncoding cannot be changed (current: {current_parser:?}, new: {new_parser:?})"
    );

    // Validate all sources have reconnect configured
    for source in &config.channel.sources {
        let (name, has_reconnect) = match source {
            ConfigChannelSource::Quic { general, .. } => {
                (&general.name, general.reconnect.is_some())
            }
            ConfigChannelSource::Grpc { general, .. } => {
                (&general.name, general.reconnect.is_some())
            }
        };
        anyhow::ensure!(
            has_reconnect,
            "source '{name}' must have reconnect configured for SIGUSR1 reload"
        );
    }

    Ok(config)
}

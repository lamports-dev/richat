use {
    anyhow::Context,
    clap::Parser,
    futures::{
        future::{pending, try_join_all, FutureExt, TryFutureExt},
        stream::StreamExt,
    },
    richat::{channel, config::Config, grpc::server::GrpcServer},
    richat_shared::shutdown::Shutdown,
    signal_hook::{consts::SIGINT, iterator::Signals},
    std::{
        thread::{self, sleep},
        time::Duration,
    },
    tracing::{info, warn},
};

#[derive(Debug, Parser)]
#[clap(author, version, about = "Richat App")]
struct Args {
    #[clap(short, long, default_value_t = String::from("config.json"))]
    /// Path to config
    pub config: String,

    /// Only check config and exit
    #[clap(long, default_value_t = false)]
    pub check: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = Config::load_from_file(&args.config)
        .with_context(|| format!("failed to load config from {}", args.config))?;
    if args.check {
        info!("Config is OK!");
        return Ok(());
    }

    // Setup logs
    richat::log::setup(config.log.json)?;

    // Shutdown channel/flag
    let shutdown = Shutdown::new();

    // Create channel runtime (receive messages from solana node / richat)
    let (mut msg_tx, mut msg_rx) =
        channel::binary::channel(config.channel.config.parser_channel_size);
    let source_jh = thread::Builder::new()
        .name("richatSource".to_owned())
        .spawn({
            let shutdown = shutdown.clone();
            || {
                let runtime = config.channel.tokio.build_runtime("richatSource")?;
                runtime.block_on(async move {
                    let mut stream = channel::Messages::subscribe_source(config.channel.source)
                        .await
                        .context("failed to subscribe")?;
                    tokio::pin!(shutdown);

                    loop {
                        tokio::select! {
                            message = stream.next() => match message {
                                Some(Ok(message)) => {
                                    let mut maybe_message = Some(message);
                                    loop {
                                        let Some(message) = maybe_message.take() else {
                                            break;
                                        };
                                        maybe_message = msg_tx.send(message);
                                        if maybe_message.is_some() && shutdown.is_set() {
                                            break;
                                        }
                                    }
                                },
                                Some(Err(error)) => return Err(anyhow::Error::new(error)),
                                None => anyhow::bail!("source stream finished"),
                            },
                            () = &mut shutdown => return Ok(()),
                        }
                    }
                })
            }
        })?;

    // Create parser channel
    let messages = channel::Messages::new(config.channel.config, config.apps.grpc.is_some());
    let parser_jh = thread::Builder::new()
        .name("richatParser".to_owned())
        .spawn({
            let shutdown = shutdown.clone();
            let mut messages = messages.to_sender();
            move || {
                const COUNTER_LIMIT: i32 = 10_000;
                let mut counter = 0;
                loop {
                    counter += 1;
                    if counter > COUNTER_LIMIT {
                        counter = 0;
                        if shutdown.is_set() {
                            break;
                        }
                    }

                    if let Some(message) = msg_rx.recv() {
                        messages.push(message)?;
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
        })?;

    // Create runtime for incoming connections
    let apps_jh = thread::Builder::new().name("richatApp".to_owned()).spawn({
        let shutdown = shutdown.clone();
        move || {
            let runtime = config.apps.tokio.build_runtime("richatApp")?;
            runtime.block_on(async move {
                let grpc_fut = if let Some(config) = config.apps.grpc {
                    GrpcServer::spawn(config, messages, shutdown.clone())?.boxed()
                } else {
                    pending().boxed()
                };

                let prometheus_fut = if let Some(config) = config.prometheus {
                    richat::metrics::spawn_server(config, shutdown)
                        .await?
                        .map_err(anyhow::Error::from)
                        .boxed()
                } else {
                    pending().boxed()
                };

                try_join_all(vec![grpc_fut, prometheus_fut])
                    .await
                    .map(|_| ())
            })
        }
    })?;

    let mut signals = Signals::new([SIGINT])?;
    let mut threads = [
        ("source", Some(source_jh)),
        ("parser", Some(parser_jh)),
        ("apps", Some(apps_jh)),
    ];
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

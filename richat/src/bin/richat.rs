use {
    anyhow::Context,
    clap::Parser,
    futures::{
        future::{pending, try_join_all, FutureExt, TryFutureExt},
        stream::StreamExt,
    },
    richat::{channel, config::Config},
    signal_hook::{consts::SIGINT, iterator::Signals},
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::sleep,
        time::Duration,
    },
    tokio::sync::broadcast,
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
        return Ok(());
    }

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

    // Create channel runtime (receive messages from solana node / richat)
    let messages = channel::Messages::new(config.channel.config);
    let mut chan_jh = std::thread::Builder::new()
        .name("richatChan".to_owned())
        .spawn({
            let shutdown = create_shutdown_rx();
            let mut messages = messages.clone().to_sender();
            || {
                let runtime = config.channel.tokio.build_runtime("richatChan")?;
                runtime.block_on(async move {
                    let mut stream = channel::source::subscribe(config.channel.source)
                        .await
                        .context("failed to subscribe")?;
                    tokio::pin!(shutdown);

                    loop {
                        tokio::select! {
                            message = stream.next() => match message {
                                Some(Ok(message)) => messages.push(message)?,
                                Some(Err(error)) => return Err(anyhow::Error::new(error)),
                                None => anyhow::bail!("source stream finished"),
                            },
                            () = &mut shutdown => return Ok(()),
                        }
                    }
                })
            }
        })
        .map(Some)?;

    // Create runtime for incoming connections
    let mut app_jh = std::thread::Builder::new()
        .name("richatApp".to_owned())
        .spawn(move || {
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

                try_join_all(vec![prometheus_fut]).await.map(|_| ())
            })
        })
        .map(Some)?;

    let mut signals = Signals::new([SIGINT])?;
    let mut shutdown_inprogress = false;
    'outer: while chan_jh.is_some() || app_jh.is_some() {
        for signal in signals.pending() {
            match signal {
                SIGINT => {
                    if shutdown_inprogress {
                        warn!("SIGINT received again, shutdown now");
                        break 'outer;
                    }
                    shutdown_inprogress = true;
                    info!("SIGINT received...");
                    shutdown_flag.store(true, Ordering::Relaxed);
                    let _ = shutdown_tx.send(());
                }
                _ => unreachable!(),
            }
        }

        if let Some(jh) = chan_jh.take() {
            if jh.is_finished() {
                jh.join().expect("chan thread join failed")?;
            } else {
                chan_jh = Some(jh);
            }
        }

        if let Some(jh) = app_jh.take() {
            if jh.is_finished() {
                jh.join().expect("app thread join failed")?;
            } else {
                app_jh = Some(jh);
            }
        }

        sleep(Duration::from_millis(10));
    }

    Ok(())
}

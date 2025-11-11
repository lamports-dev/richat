use {
    clap::{Parser, Subcommand},
    richat_cli::{
        pubsub::ArgsAppPubSub, stream_grpc::ArgsAppStreamGrpc, stream_richat::ArgsAppStreamRichat,
        track::ArgsAppTrack,
    },
    std::{
        io::{self, IsTerminal},
        sync::atomic::{AtomicU64, Ordering},
    },
    tracing_subscriber::{
        filter::{EnvFilter, LevelFilter},
        fmt::layer,
        layer::SubscriberExt,
        util::SubscriberInitExt,
    },
};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Debug, Parser)]
#[clap(author, version, about = "Richat Cli Tool: pubsub, stream, track")]
struct Args {
    #[command(subcommand)]
    action: ArgsAppSelect,
}

#[derive(Debug, Subcommand)]
enum ArgsAppSelect {
    /// Subscribe on updates over WebSocket (Solana PubSub)
    Pubsub(ArgsAppPubSub),

    /// Stream data from Yellowstone gRPC / Dragon's Mouth
    StreamGrpc(ArgsAppStreamGrpc),

    /// Stream data directly from the richat-plugin
    StreamRichat(ArgsAppStreamRichat),

    /// Events tracker
    Track(ArgsAppTrack),
}

fn setup_logs() -> anyhow::Result<()> {
    let env = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env()?;

    let is_atty = io::stdout().is_terminal() && io::stderr().is_terminal();
    let io_layer = layer().with_ansi(is_atty).with_line_number(true);

    tracing_subscriber::registry()
        .with(env)
        .with(io_layer)
        .try_init()
        .map_err(Into::into)
}

async fn main2() -> anyhow::Result<()> {
    anyhow::ensure!(
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .is_ok(),
        "failed to call CryptoProvider::install_default()"
    );

    setup_logs()?;

    let args = Args::parse();
    match args.action {
        ArgsAppSelect::Pubsub(action) => action.run().await,
        ArgsAppSelect::StreamGrpc(action) => action.run().await,
        ArgsAppSelect::StreamRichat(action) => action.run().await,
        ArgsAppSelect::Track(action) => action.run().await,
    }
}

fn main() -> anyhow::Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name_fn(move || {
            static ATOMIC_ID: AtomicU64 = AtomicU64::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
            format!("richatCli{id:02}")
        })
        .enable_all()
        .build()?
        .block_on(main2())
}

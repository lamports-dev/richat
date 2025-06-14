use {
    clap::{Parser, Subcommand},
    richat_cli::{
        pubsub::ArgsAppPubSub, stream_grpc::ArgsAppStreamGrpc, stream_richat::ArgsAppStreamRichat,
        track::ArgsAppTrack,
    },
    std::{
        env,
        sync::atomic::{AtomicU64, Ordering},
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

async fn main2() -> anyhow::Result<()> {
    anyhow::ensure!(
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .is_ok(),
        "failed to call CryptoProvider::install_default()"
    );

    env::set_var(
        env_logger::DEFAULT_FILTER_ENV,
        env::var_os(env_logger::DEFAULT_FILTER_ENV).unwrap_or_else(|| "info".into()),
    );
    env_logger::init();

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

pub mod channel;
pub mod config;
pub mod grpc;
pub mod log;
pub mod metrics;
pub mod pubsub;
pub mod richat;
pub mod source;
pub mod storage;
pub mod version;

pub type SpawnedThreads = Vec<(
    &'static str,
    Option<std::thread::JoinHandle<anyhow::Result<()>>>,
)>;

pub type SpawnedThread = (String, Option<std::thread::JoinHandle<anyhow::Result<()>>>);
pub type SpawnedThreads = Vec<SpawnedThread>;

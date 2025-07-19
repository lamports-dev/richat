pub type SpawnedThreads = Vec<(String, Option<std::thread::JoinHandle<anyhow::Result<()>>>)>;

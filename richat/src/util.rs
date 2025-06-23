use std::sync::{Mutex, MutexGuard};

pub type SpawnedThreads = Vec<(String, Option<std::thread::JoinHandle<anyhow::Result<()>>>)>;

#[inline]
pub fn mutex_lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(lock) => lock,
        Err(p_err) => p_err.into_inner(),
    }
}

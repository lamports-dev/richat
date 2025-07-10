#[cfg(feature = "config")]
pub mod config;
#[cfg(feature = "five8")]
pub mod five8;
#[cfg(feature = "jsonrpc")]
pub mod jsonrpc;
#[cfg(feature = "metrics")]
pub mod metrics;
#[cfg(feature = "shutdown")]
pub mod shutdown;
#[cfg(feature = "transports")]
pub mod transports;
#[cfg(feature = "version")]
pub mod version;

#[inline]
pub fn mutex_lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(lock) => lock,
        Err(p_err) => p_err.into_inner(),
    }
}

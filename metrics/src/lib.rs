pub use ::metrics::*;

mod config;
pub use config::ConfigMetrics;

mod macros;

mod recorder;
pub use recorder::MaybeRecorder;

mod server;
pub use server::spawn_server;

#[inline]
pub fn duration_to_seconds(d: std::time::Duration) -> f64 {
    d.as_secs() as f64 + d.subsec_nanos() as f64 / 1e9
}

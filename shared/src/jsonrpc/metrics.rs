use metrics::{describe_counter, describe_histogram};

pub const RPC_REQUESTS_TOTAL: &str = "rpc_requests_total"; // x_subscription_id, method
pub const RPC_REQUESTS_DURATION_SECONDS: &str = "rpc_requests_duration_seconds"; // x_subscription_id, method
pub const RPC_REQUESTS_GENERATED_BYTES_TOTAL: &str = "rpc_requests_generated_bytes_total"; // x_subscription_id

pub fn describe() {
    describe_counter!(
        RPC_REQUESTS_TOTAL,
        "Number of RPC requests by x-subscription-id and method"
    );
    describe_histogram!(
        RPC_REQUESTS_DURATION_SECONDS,
        "RPC request time by x-subscription-id and method"
    );
    describe_counter!(
        RPC_REQUESTS_GENERATED_BYTES_TOTAL,
        "Number of bytes generated by RPC requests by x-subscription-id"
    );
}

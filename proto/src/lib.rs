#[cfg(feature = "yellowstone-grpc-plugin")]
pub use yellowstone_grpc_proto::plugin;
pub use yellowstone_grpc_proto::{convert_from, convert_to, geyser, solana};

pub mod richat {
    #![allow(clippy::missing_const_for_fn)]
    include!(concat!(env!("OUT_DIR"), "/richat.rs"));
}

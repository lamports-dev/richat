pub mod grpc;
pub mod quic;

pub(crate) mod proto {
    include!(concat!(env!("OUT_DIR"), "/transport.rs"));
}

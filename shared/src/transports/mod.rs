pub mod grpc;
pub mod quic;
pub mod tcp;

pub(crate) mod proto {
    #![allow(clippy::missing_const_for_fn)]
    include!(concat!(env!("OUT_DIR"), "/transport.rs"));
}

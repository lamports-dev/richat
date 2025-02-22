#[cfg(not(feature = "transports"))]
fn main() {}

#[cfg(feature = "transports")]
fn main() -> anyhow::Result<()> {
    // build protos
    std::env::set_var("PROTOC", protobuf_src::protoc());
    generate_grpc_geyser()
}

#[cfg(feature = "transports")]
fn generate_grpc_geyser() -> anyhow::Result<()> {
    use tonic_build::manual::{Builder, Method, Service};

    let geyser_service = Service::builder()
        .name("Geyser")
        .package("geyser")
        .method(
            Method::builder()
                .name("subscribe")
                .route_name("Subscribe")
                .input_type("crate::transports::grpc::GrpcSubscribeRequest")
                .output_type("Arc<Vec<u8>>")
                .codec_path("crate::transports::grpc::SubscribeCodec")
                .client_streaming()
                .server_streaming()
                .build(),
        )
        .method(
            Method::builder()
                .name("get_version")
                .route_name("GetVersion")
                .input_type("richat_proto::geyser::GetVersionRequest")
                .output_type("richat_proto::geyser::GetVersionResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .build();

    Builder::new()
        .build_client(false)
        .compile(&[geyser_service]);

    Ok(())
}

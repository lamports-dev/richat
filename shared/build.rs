use tonic_build::manual::{Builder, Method, Service};

fn main() -> anyhow::Result<()> {
    // build protos
    std::env::set_var("PROTOC", protobuf_src::protoc());
    generate_transport()?;
    generate_grpc_geyser()
}

fn generate_transport() -> anyhow::Result<()> {
    tonic_build::configure()
        .build_client(false)
        .build_server(false)
        .compile_protos(&["proto/transport.proto"], &["proto"])?;

    Ok(())
}

fn generate_grpc_geyser() -> anyhow::Result<()> {
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
                .input_type("yellowstone_grpc_proto::geyser::GetVersionRequest")
                .output_type("yellowstone_grpc_proto::geyser::GetVersionResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .build();

    Builder::new()
        .build_client(false)
        .compile(&[geyser_service]);

    Ok(())
}

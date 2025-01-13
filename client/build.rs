use tonic_build::manual::{Builder, Method, Service};

fn main() -> anyhow::Result<()> {
    generate_grpc_geyser()
}

fn generate_grpc_geyser() -> anyhow::Result<()> {
    let geyser_service = Service::builder()
        .name("Geyser")
        .package("geyser")
        .method(
            Method::builder()
                .name("subscribe")
                .route_name("Subscribe")
                .input_type("richat_proto::geyser::SubscribeRequest")
                .output_type("Vec<u8>")
                .codec_path("crate::grpc::SubscribeCodec")
                .client_streaming()
                .server_streaming()
                .build(),
        )
        .method(
            Method::builder()
                .name("ping")
                .route_name("Ping")
                .input_type("richat_proto::geyser::PingRequest")
                .output_type("richat_proto::geyser::PongResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_latest_blockhash")
                .route_name("GetLatestBlockhash")
                .input_type("richat_proto::geyser::GetLatestBlockhashRequest")
                .output_type("richat_proto::geyser::GetLatestBlockhashResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_block_height")
                .route_name("GetBlockHeight")
                .input_type("richat_proto::geyser::GetBlockHeightRequest")
                .output_type("richat_proto::geyser::GetBlockHeightResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_slot")
                .route_name("GetSlot")
                .input_type("richat_proto::geyser::GetSlotRequest")
                .output_type("richat_proto::geyser::GetSlotResponse")
                .codec_path("tonic::codec::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("is_blockhash_valid")
                .route_name("IsBlockhashValid")
                .input_type("richat_proto::geyser::IsBlockhashValidRequest")
                .output_type("richat_proto::geyser::IsBlockhashValidResponse")
                .codec_path("tonic::codec::ProstCodec")
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
        .build_server(false)
        .compile(&[geyser_service]);

    Ok(())
}

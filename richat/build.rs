use {
    cargo_lock::Lockfile,
    std::collections::HashSet,
    tonic_build::manual::{Builder, Method, Service},
};

fn main() -> anyhow::Result<()> {
    emit_version()?;
    generate_grpc_geyser()
}

fn emit_version() -> anyhow::Result<()> {
    vergen::Emitter::default()
        .add_instructions(&vergen::BuildBuilder::all_build()?)?
        .add_instructions(&vergen::RustcBuilder::all_rustc()?)?
        .emit()?;

    // vergen git version does not looks cool
    println!(
        "cargo:rustc-env=GIT_VERSION={}",
        git_version::git_version!()
    );

    // Extract packages version
    let lockfile = Lockfile::load("../Cargo.lock")?;
    println!(
        "cargo:rustc-env=SOLANA_SDK_VERSION={}",
        get_pkg_version(&lockfile, "solana-sdk")
    );
    println!(
        "cargo:rustc-env=YELLOWSTONE_GRPC_PROTO_VERSION={}",
        get_pkg_version(&lockfile, "yellowstone-grpc-proto")
    );
    println!(
        "cargo:rustc-env=RICHAT_PROTO_VERSION={}",
        get_pkg_version(&lockfile, "richat-proto")
    );

    Ok(())
}

fn get_pkg_version(lockfile: &Lockfile, pkg_name: &str) -> String {
    lockfile
        .packages
        .iter()
        .filter(|pkg| pkg.name.as_str() == pkg_name)
        .map(|pkg| pkg.version.to_string())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(",")
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
                .codec_path("richat_shared::transports::grpc::SubscribeCodec")
                .client_streaming()
                .server_streaming()
                .build(),
        )
        .method(
            Method::builder()
                .name("subscribe_jup")
                .route_name("SubscribeJup")
                .input_type("richat_proto::richat::SubscribeRequestJup")
                .output_type("Vec<u8>")
                .codec_path("richat_shared::transports::grpc::SubscribeCodec")
                .client_streaming()
                .server_streaming()
                .build(),
        )
        .method(
            Method::builder()
                .name("subscribe_replay_info")
                .route_name("SubscribeReplayInfo")
                .input_type("richat_proto::geyser::SubscribeReplayInfoRequest")
                .output_type("richat_proto::geyser::SubscribeReplayInfoResponse")
                .codec_path("tonic_prost::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("ping")
                .route_name("Ping")
                .input_type("richat_proto::geyser::PingRequest")
                .output_type("richat_proto::geyser::PongResponse")
                .codec_path("tonic_prost::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_latest_blockhash")
                .route_name("GetLatestBlockhash")
                .input_type("richat_proto::geyser::GetLatestBlockhashRequest")
                .output_type("richat_proto::geyser::GetLatestBlockhashResponse")
                .codec_path("tonic_prost::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_block_height")
                .route_name("GetBlockHeight")
                .input_type("richat_proto::geyser::GetBlockHeightRequest")
                .output_type("richat_proto::geyser::GetBlockHeightResponse")
                .codec_path("tonic_prost::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_slot")
                .route_name("GetSlot")
                .input_type("richat_proto::geyser::GetSlotRequest")
                .output_type("richat_proto::geyser::GetSlotResponse")
                .codec_path("tonic_prost::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("is_blockhash_valid")
                .route_name("IsBlockhashValid")
                .input_type("richat_proto::geyser::IsBlockhashValidRequest")
                .output_type("richat_proto::geyser::IsBlockhashValidResponse")
                .codec_path("tonic_prost::ProstCodec")
                .build(),
        )
        .method(
            Method::builder()
                .name("get_version")
                .route_name("GetVersion")
                .input_type("richat_proto::geyser::GetVersionRequest")
                .output_type("richat_proto::geyser::GetVersionResponse")
                .codec_path("tonic_prost::ProstCodec")
                .build(),
        )
        .build();

    Builder::new()
        .build_client(false)
        .compile(&[geyser_service]);

    Ok(())
}

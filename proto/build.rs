fn main() -> anyhow::Result<()> {
    // Use vendored protoc to avoid building C++ protobuf via autotools
    let protoc_path = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc_path);

    // build protos
    generate_transport()
}

fn generate_transport() -> anyhow::Result<()> {
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(false)
        .compile_protos(&["proto/richat.proto"], &["proto"])?;

    Ok(())
}

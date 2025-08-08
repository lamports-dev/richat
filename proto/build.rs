fn main() -> anyhow::Result<()> {
    // build protos
    std::env::set_var("PROTOC", protobuf_src::protoc());
    generate_transport()
}

fn generate_transport() -> anyhow::Result<()> {
    tonic_prost_build::configure()
        .build_client(false)
        .build_server(false)
        .compile_protos(&["proto/richat.proto"], &["proto"])?;

    Ok(())
}

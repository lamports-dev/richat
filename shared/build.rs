fn main() -> anyhow::Result<()> {
    // build protos
    std::env::set_var("PROTOC", protobuf_src::protoc());
    tonic_build::configure()
        .build_client(false)
        .build_server(false)
        .compile_protos(&["proto/transport.proto"], &["proto"])?;

    Ok(())
}

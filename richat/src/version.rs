use {richat_shared::version::Version, std::env};

pub const VERSION: Version = Version {
    package: env!("CARGO_PKG_NAME"),
    version: env!("CARGO_PKG_VERSION"),
    proto: env!("YELLOWSTONE_GRPC_PROTO_VERSION"),
    proto_richat: env!("RICHAT_PROTO_VERSION"),
    solana: env!("SOLANA_SDK_VERSION"),
    git: env!("GIT_VERSION"),
    rustc: env!("VERGEN_RUSTC_SEMVER"),
    buildts: env!("VERGEN_BUILD_TIMESTAMP"),
};

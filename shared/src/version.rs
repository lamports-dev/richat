use serde::Serialize;

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Version {
    pub package: &'static str,
    pub version: &'static str,
    pub proto: &'static str,
    pub proto_richat: &'static str,
    pub solana: &'static str,
    pub git: &'static str,
    pub rustc: &'static str,
    pub buildts: &'static str,
}

impl Version {
    pub fn create_grpc_version_info(self) -> GrpcVersionInfo {
        GrpcVersionInfo::new(self)
    }
}

#[derive(Debug, Serialize)]
pub struct GrpcVersionInfoExtra {
    hostname: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct GrpcVersionInfo {
    version: Version,
    extra: GrpcVersionInfoExtra,
}

impl GrpcVersionInfo {
    pub fn new(version: Version) -> Self {
        Self {
            version,
            extra: GrpcVersionInfoExtra {
                hostname: hostname::get()
                    .ok()
                    .and_then(|name| name.into_string().ok()),
            },
        }
    }

    pub fn json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

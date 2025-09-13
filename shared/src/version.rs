use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Version<'a> {
    pub package: &'a str,
    pub version: &'a str,
    pub proto: &'a str,
    pub proto_richat: &'a str,
    pub solana: &'a str,
    pub git: &'a str,
    pub rustc: &'a str,
    pub buildts: &'a str,
}

impl<'a> Version<'a> {
    pub fn create_grpc_version_info(self) -> GrpcVersionInfo<'a> {
        GrpcVersionInfo::new(self)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrpcVersionInfoExtra {
    pub hostname: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GrpcVersionInfo<'a> {
    #[serde(borrow)]
    pub version: Version<'a>,
    pub extra: GrpcVersionInfoExtra,
}

impl<'a> GrpcVersionInfo<'a> {
    pub fn new(version: Version<'a>) -> Self {
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
        serde_json::to_string(self).expect("json serialization never fail")
    }

    pub fn value(&self) -> serde_json::Value {
        serde_json::to_value(self).expect("json serialization never fail")
    }
}

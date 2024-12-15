use {
    crate::{config::ConfigGrpc, version::GrpcVersionInfo},
    tonic::{Request, Response, Status},
    yellowstone_grpc_proto::geyser::{GetVersionRequest, GetVersionResponse},
};

pub mod gen {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/geyser.Geyser.rs"));
}

#[derive(Debug)]
pub struct GrpcServer {
    //
}

impl GrpcServer {
    pub async fn new(config: ConfigGrpc) -> Self {
        todo!()
    }
}

#[tonic::async_trait]
impl gen::geyser_server::Geyser for GrpcServer {
    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: serde_json::to_string(&GrpcVersionInfo::default()).unwrap(),
        }))
    }
}

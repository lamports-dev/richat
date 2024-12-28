use {
    crate::{channel::Messages, grpc::config::ConfigAppsGrpc},
    futures::{
        future::{try_join_all, FutureExt, TryFutureExt},
        stream::Stream,
    },
    richat_shared::shutdown::Shutdown,
    std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll},
    },
    tonic::{Request, Response, Result as TonicResult, Streaming},
    tracing::{error, info},
    yellowstone_grpc_proto::geyser::{
        GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
        GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse, GetVersionRequest,
        GetVersionResponse, IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest,
        PongResponse, SubscribeRequest, SubscribeUpdate,
    },
};

pub mod gen {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/geyser.Geyser.rs"));
}

#[derive(Debug, Clone)]
pub struct GrpcServer {
    messages: Messages,
}

impl GrpcServer {
    pub fn spawn(
        config: ConfigAppsGrpc,
        messages: Messages,
        shutdown: Shutdown,
    ) -> anyhow::Result<impl Future<Output = anyhow::Result<()>>> {
        // create gRPC server
        let (incoming, mut server_builder) = config.server.create_server()?;
        info!("start server at {}", config.server.endpoint);

        let grpc_server = Self { messages };

        let mut service = gen::geyser_server::GeyserServer::new(grpc_server.clone())
            .max_decoding_message_size(config.server.max_decoding_message_size);
        for encoding in config.server.compression.accept {
            service = service.accept_compressed(encoding);
        }
        for encoding in config.server.compression.send {
            service = service.send_compressed(encoding);
        }

        // Spawn workers pool
        let threads = config
            .workers
            .run(
                |index| format!("grpcWrk{index:02}"),
                move |index| grpc_server.work(index),
                shutdown.clone(),
            )
            .boxed();

        // Spawn server
        let server = tokio::spawn(async move {
            if let Err(error) = server_builder
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await
            {
                error!("server error: {error:?}")
            } else {
                info!("shutdown")
            }
        })
        .map_err(anyhow::Error::new)
        .boxed();

        Ok(try_join_all([threads, server]).map_ok(|_| ()))
    }

    fn work(&self, index: usize) -> anyhow::Result<()> {
        todo!()
    }
}

#[tonic::async_trait]
impl gen::geyser_server::Geyser for GrpcServer {
    type SubscribeStream = ReceiverStream;

    async fn subscribe(
        &self,
        _request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        todo!()
    }

    async fn ping(&self, _request: Request<PingRequest>) -> TonicResult<Response<PongResponse>> {
        todo!()
    }

    async fn get_latest_blockhash(
        &self,
        _request: Request<GetLatestBlockhashRequest>,
    ) -> TonicResult<Response<GetLatestBlockhashResponse>> {
        todo!()
    }

    async fn get_block_height(
        &self,
        _request: Request<GetBlockHeightRequest>,
    ) -> TonicResult<Response<GetBlockHeightResponse>> {
        todo!()
    }

    async fn get_slot(
        &self,
        _request: Request<GetSlotRequest>,
    ) -> TonicResult<Response<GetSlotResponse>> {
        todo!()
    }

    async fn is_blockhash_valid(
        &self,
        _request: tonic::Request<IsBlockhashValidRequest>,
    ) -> TonicResult<Response<IsBlockhashValidResponse>> {
        todo!()
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> TonicResult<Response<GetVersionResponse>> {
        todo!()
    }
}

#[derive(Debug)]
pub struct ReceiverStream;

impl Stream for ReceiverStream {
    type Item = TonicResult<SubscribeUpdate>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

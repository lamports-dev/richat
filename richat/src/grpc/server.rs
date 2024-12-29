use {
    crate::{
        channel::{message::Message, Messages},
        grpc::{block_meta::BlockMetaStorage, config::ConfigAppsGrpc},
        version::VERSION,
    },
    futures::{
        future::{ready, try_join_all, FutureExt, TryFutureExt},
        stream::Stream,
    },
    richat_shared::shutdown::Shutdown,
    solana_sdk::clock::MAX_PROCESSING_AGE,
    std::{
        future::Future,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    },
    tonic::{Request, Response, Result as TonicResult, Status, Streaming},
    tracing::{error, info},
    yellowstone_grpc_proto::geyser::{
        CommitmentLevel, GetBlockHeightRequest, GetBlockHeightResponse, GetLatestBlockhashRequest,
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
    block_meta: Option<Arc<BlockMetaStorage>>,
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

        let (block_meta, block_meta_jh) = if config.unary.enabled {
            let (meta, jh) = BlockMetaStorage::new(config.unary.requests_queue_size);
            (Some(Arc::new(meta)), jh.boxed())
        } else {
            (None, ready(Ok(())).boxed())
        };

        let grpc_server = Self {
            messages,
            block_meta,
        };

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

        Ok(try_join_all([threads, server, block_meta_jh]).map_ok(|_| ()))
    }

    fn parse_commitment(commitment: Option<i32>) -> Result<CommitmentLevel, Status> {
        let commitment = commitment.unwrap_or(CommitmentLevel::Processed as i32);
        CommitmentLevel::try_from(commitment)
            .map(Into::into)
            .map_err(|_error| {
                let msg = format!("failed to create CommitmentLevel from {commitment:?}");
                Status::unknown(msg)
            })
            .and_then(|commitment| {
                if matches!(
                    commitment,
                    CommitmentLevel::Processed
                        | CommitmentLevel::Confirmed
                        | CommitmentLevel::Finalized
                ) {
                    Ok(commitment)
                } else {
                    Err(Status::unknown(
                        "only Processed, Confirmed and Finalized are allowed",
                    ))
                }
            })
    }

    async fn with_block_meta<'a, T, F>(
        &'a self,
        f: impl FnOnce(&'a BlockMetaStorage) -> F,
    ) -> TonicResult<Response<T>>
    where
        F: Future<Output = TonicResult<T>> + 'a,
    {
        if let Some(storage) = &self.block_meta {
            f(storage).await.map(Response::new)
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    fn work(&self, index: usize) -> anyhow::Result<()> {
        let mut receiver = self.messages.subscribe();
        loop {
            let Some(message) = receiver.try_recv()? else {
                continue;
            };

            // Update block meta only from first thread
            if index == 0 {
                if let Some(block_meta_storage) = &self.block_meta {
                    if matches!(&message, Message::Slot(_) | Message::BlockMeta(_)) {
                        block_meta_storage.push(message.clone());
                    }
                }
            }

            todo!()
        }
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

    async fn ping(&self, request: Request<PingRequest>) -> TonicResult<Response<PongResponse>> {
        let count = request.get_ref().count;
        let response = PongResponse { count };
        Ok(Response::new(response))
    }

    async fn get_latest_blockhash(
        &self,
        request: Request<GetLatestBlockhashRequest>,
    ) -> TonicResult<Response<GetLatestBlockhashResponse>> {
        let commitment = Self::parse_commitment(request.get_ref().commitment)?;
        self.with_block_meta(|storage| async move {
            let block = storage.get_block(commitment).await?;
            Ok(GetLatestBlockhashResponse {
                slot: block.slot,
                blockhash: block.blockhash.as_ref().clone(),
                last_valid_block_height: block.block_height + MAX_PROCESSING_AGE as u64,
            })
        })
        .await
    }

    async fn get_block_height(
        &self,
        request: Request<GetBlockHeightRequest>,
    ) -> TonicResult<Response<GetBlockHeightResponse>> {
        let commitment = Self::parse_commitment(request.get_ref().commitment)?;
        self.with_block_meta(|storage| async move {
            let block = storage.get_block(commitment).await?;
            Ok(GetBlockHeightResponse {
                block_height: block.block_height,
            })
        })
        .await
    }

    async fn get_slot(
        &self,
        request: Request<GetSlotRequest>,
    ) -> TonicResult<Response<GetSlotResponse>> {
        let commitment = Self::parse_commitment(request.get_ref().commitment)?;
        self.with_block_meta(|storage| async move {
            let block = storage.get_block(commitment).await?;
            Ok(GetSlotResponse { slot: block.slot })
        })
        .await
    }

    async fn is_blockhash_valid(
        &self,
        request: tonic::Request<IsBlockhashValidRequest>,
    ) -> TonicResult<Response<IsBlockhashValidResponse>> {
        let commitment = Self::parse_commitment(request.get_ref().commitment)?;
        self.with_block_meta(|storage| async move {
            let (valid, slot) = storage
                .is_blockhash_valid(request.into_inner().blockhash, commitment)
                .await?;
            Ok(IsBlockhashValidResponse { valid, slot })
        })
        .await
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> TonicResult<Response<GetVersionResponse>> {
        Ok(Response::new(GetVersionResponse {
            version: VERSION.create_grpc_version_info().json(),
        }))
    }
}

#[derive(Debug)]
pub struct ReceiverStream;

impl Stream for ReceiverStream {
    type Item = TonicResult<SubscribeUpdate>;

    #[allow(unused)]
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}

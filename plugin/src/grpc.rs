use {
    crate::{channel::Sender, config::ConfigGrpc, version::GrpcVersionInfo},
    anyhow::Context,
    log::{error, info},
    std::sync::Arc,
    tokio::sync::Notify,
    tonic::{
        transport::server::{Server, TcpIncoming},
        Request, Response, Status,
    },
    yellowstone_grpc_proto::geyser::{GetVersionRequest, GetVersionResponse},
};

pub mod gen {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/geyser.Geyser.rs"));
}

#[derive(Debug)]
pub struct GrpcServer {
    messages: Sender,
}

impl GrpcServer {
    pub async fn spawn(config: ConfigGrpc, messages: Sender) -> anyhow::Result<Arc<Notify>> {
        // Bind service address
        let incoming = TcpIncoming::new(
            config.endpoint,
            config.server_tcp_nodelay,
            config.server_tcp_keepalive,
        )
        .map_err(|error| anyhow::anyhow!(error))
        .context(format!("failed to bind {}", config.endpoint))?;
        info!("start server at {}", config.endpoint);

        // Create service
        let mut server_builder = Server::builder();
        if let Some(tls_config) = config.tls_config {
            server_builder = server_builder
                .tls_config(tls_config)
                .context("failed to apply tls_config")?;
        }
        if let Some(enabled) = config.server_http2_adaptive_window {
            server_builder = server_builder.http2_adaptive_window(Some(enabled));
        }
        if let Some(http2_keepalive_interval) = config.server_http2_keepalive_interval {
            server_builder =
                server_builder.http2_keepalive_interval(Some(http2_keepalive_interval));
        }
        if let Some(http2_keepalive_timeout) = config.server_http2_keepalive_timeout {
            server_builder = server_builder.http2_keepalive_timeout(Some(http2_keepalive_timeout));
        }
        if let Some(sz) = config.server_initial_connection_window_size {
            server_builder = server_builder.initial_connection_window_size(sz);
        }
        if let Some(sz) = config.server_initial_stream_window_size {
            server_builder = server_builder.initial_stream_window_size(sz);
        }

        let mut service = gen::geyser_server::GeyserServer::new(Self { messages })
            .max_decoding_message_size(config.max_decoding_message_size);
        for encoding in config.compression.accept {
            service = service.accept_compressed(encoding);
        }
        for encoding in config.compression.send {
            service = service.send_compressed(encoding);
        }

        // Spawn server
        let shutdown = Arc::new(Notify::new());
        let shutdown2 = Arc::clone(&shutdown);
        tokio::spawn(async move {
            if let Err(error) = server_builder
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, shutdown2.notified())
                .await
            {
                error!("server error: {error:?}")
            } else {
                info!("shutdown")
            }
        });

        Ok(shutdown)
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

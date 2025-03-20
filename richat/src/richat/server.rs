use {
    crate::{channel::Messages, metrics, richat::config::ConfigAppsRichat, version::VERSION},
    futures::future::{try_join_all, FutureExt, TryFutureExt},
    richat_shared::{
        shutdown::Shutdown,
        transports::{grpc::GrpcServer, quic::QuicServer, tcp::TcpServer},
    },
    std::future::Future,
};

#[derive(Debug)]
pub struct RichatServer;

impl RichatServer {
    pub async fn spawn(
        config: ConfigAppsRichat,
        messages: Messages,
        shutdown: Shutdown,
    ) -> anyhow::Result<impl Future<Output = anyhow::Result<()>>> {
        let mut tasks = Vec::with_capacity(3);

        use metrics::{richat_connections_add, richat_connections_dec, RichatConnectionsTransport};

        // Start Quic
        if let Some(config) = config.quic {
            tasks.push(
                QuicServer::spawn(
                    config,
                    messages.clone(),
                    || richat_connections_add(RichatConnectionsTransport::Quic), // on_conn_new_cb
                    || richat_connections_dec(RichatConnectionsTransport::Quic), // on_conn_drop_cb
                    shutdown.clone(),
                )
                .await?
                .boxed(),
            );
        }

        // Start Tcp
        if let Some(config) = config.tcp {
            tasks.push(
                TcpServer::spawn(
                    config,
                    messages.clone(),
                    || richat_connections_add(RichatConnectionsTransport::Tcp), // on_conn_new_cb
                    || richat_connections_dec(RichatConnectionsTransport::Tcp), // on_conn_drop_cb
                    shutdown.clone(),
                )
                .await?
                .boxed(),
            );
        }

        // Start gRPC
        if let Some(config) = config.grpc {
            tasks.push(
                GrpcServer::spawn(
                    config,
                    messages.clone(),
                    || richat_connections_add(RichatConnectionsTransport::Grpc), // on_conn_new_cb
                    || richat_connections_dec(RichatConnectionsTransport::Grpc), // on_conn_drop_cb
                    VERSION,
                    shutdown.clone(),
                )
                .await?
                .boxed(),
            );
        }

        Ok(try_join_all(tasks).map_ok(|_| ()).map_err(Into::into))
    }
}

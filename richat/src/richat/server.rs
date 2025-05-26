use {
    crate::{channel::Messages, metrics, richat::config::ConfigAppsRichat, version::VERSION},
    ::metrics::gauge,
    futures::future::{try_join_all, FutureExt, TryFutureExt},
    richat_shared::{
        shutdown::Shutdown,
        transports::{grpc::GrpcServer, quic::QuicServer},
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

        // Start Quic
        if let Some(config) = config.quic {
            let connections_inc = gauge!(metrics::RICHAT_CONNECTIONS_TOTAL, "transport" => "quic");
            let connections_dec = connections_inc.clone();
            tasks.push(
                QuicServer::spawn(
                    config,
                    messages.clone(),
                    move || connections_inc.increment(1), // on_conn_new_cb
                    move || connections_dec.decrement(1), // on_conn_drop_cb
                    shutdown.clone(),
                )
                .await?
                .boxed(),
            );
        }

        // Start gRPC
        if let Some(config) = config.grpc {
            let connections_inc = gauge!(metrics::RICHAT_CONNECTIONS_TOTAL, "transport" => "grpc");
            let connections_dec = connections_inc.clone();
            tasks.push(
                GrpcServer::spawn(
                    config,
                    messages.clone(),
                    move || connections_inc.increment(1), // on_conn_new_cb
                    move || connections_dec.decrement(1), // on_conn_drop_cb
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

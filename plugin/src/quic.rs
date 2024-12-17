use {
    crate::channel::Sender, richat_shared::transports::quic::ConfigQuicServer, std::future::Future,
    tokio::task::JoinHandle,
};

#[derive(Debug)]
pub struct QuicServer;

impl QuicServer {
    pub async fn spawn(
        config: ConfigQuicServer,
        messages: Sender,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> anyhow::Result<JoinHandle<()>> {
        let endpoint = config.create_server()?;

        Ok(tokio::spawn(async move {
            tokio::pin!(shutdown);
            loop {
                tokio::select! {
                    incoming = endpoint.accept() => {
                        todo!()
                    }
                    () = &mut shutdown => {
                        // info!("shutdown");
                        break
                    },
                };
            }
        }))
    }
}

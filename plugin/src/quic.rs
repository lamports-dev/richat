use {
    crate::channel::Sender,
    anyhow::Context,
    quinn::{crypto::rustls::QuicServerConfig, Endpoint},
    richat_shared::transports::quic::ConfigQuicServer,
    std::{future::Future, sync::Arc},
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
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
            QuicServerConfig::try_from(config.tls_config)?,
        ));

        // disallow incoming uni streams
        let transport_config = Arc::get_mut(&mut server_config.transport)
            .context("failed to get QUIC transport config")?;
        transport_config.max_concurrent_bidi_streams(1u8.into());
        transport_config.max_concurrent_uni_streams(0u8.into());

        // set window size
        let stream_rwnd = config.max_stream_bandwidth / 1_000 * config.expected_rtt;
        transport_config.stream_receive_window(stream_rwnd.into());
        transport_config.send_window(8 * stream_rwnd as u64);
        transport_config.datagram_receive_buffer_size(Some(stream_rwnd as usize));

        let endpoint = Endpoint::server(server_config, config.endpoint)
            .context(format!("failed to bind {}", config.endpoint))?;

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

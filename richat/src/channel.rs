use {
    crate::config::{ConfigChannelInner, ConfigChannelSource},
    futures::stream::{BoxStream, StreamExt},
    richat_client::error::ReceiveError,
    std::{fmt, sync::Arc},
    yellowstone_grpc_proto::geyser::SubscribeRequest,
};

pub async fn subscribe(
    config: ConfigChannelSource,
) -> anyhow::Result<BoxStream<'static, Result<Vec<u8>, ReceiveError>>> {
    Ok(match config {
        ConfigChannelSource::Quic(config) => config.connect().await?.subscribe(None).await?.boxed(),
        ConfigChannelSource::Tcp(config) => config.connect().await?.subscribe(None).await?.boxed(),
        ConfigChannelSource::Grpc(config) => config
            .connect()
            .await?
            .subscribe_once(SubscribeRequest {
                from_slot: None,
                ..Default::default()
            })
            .await?
            .boxed(),
    })
}

#[derive(Debug, Clone)]
pub struct Sender {
    shared: Arc<Shared>,
}

impl Sender {
    pub fn new(config: ConfigChannelInner) -> Self {
        Self {
            shared: Arc::new(Shared { mask: 0 }),
        }
    }

    pub fn push(&self, message: Vec<u8>) -> anyhow::Result<()> {
        tracing::info!("push msg with len {}", message.len());
        Ok(())
    }
}

struct Shared {
    mask: u64,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared").field("mask", &self.mask).finish()
    }
}

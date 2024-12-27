use {
    crate::config::ConfigChannelSource,
    futures::stream::{BoxStream, StreamExt},
    richat_client::error::ReceiveError,
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

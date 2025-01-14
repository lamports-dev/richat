use {
    crate::config::ConfigChannelSource,
    futures::stream::{BoxStream, StreamExt},
    richat_client::error::ReceiveError,
    richat_proto::richat::GrpcSubscribeRequest,
};

pub async fn subscribe(
    config: ConfigChannelSource,
) -> anyhow::Result<BoxStream<'static, Result<Vec<u8>, ReceiveError>>> {
    Ok(match config {
        ConfigChannelSource::Quic(config) => config
            .connect()
            .await?
            .subscribe(None, None, None)
            .await?
            .boxed(),
        ConfigChannelSource::Tcp(config) => config
            .connect()
            .await?
            .subscribe(None, None, None)
            .await?
            .boxed(),
        ConfigChannelSource::Grpc(config) => config
            .connect()
            .await?
            .subscribe_richat_once(GrpcSubscribeRequest {
                replay_from_slot: None,
                filter: None,
            })
            .await?
            .boxed(),
    })
}

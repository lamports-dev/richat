use {
    crate::{
        channel::ParsedMessage,
        config::{ConfigChannelSource, ConfigChannelSourceShared, ConfigGrpcClientSource},
    },
    futures::stream::{BoxStream, StreamExt},
    maplit::hashmap,
    richat_client::{error::ReceiveError, grpc::GrpcClientBuilderError, quic::QuicConnectError},
    richat_filter::message::{Message, MessageParseError},
    richat_proto::{
        geyser::{
            CommitmentLevel as CommitmentLevelProto, SubscribeRequest,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
            SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions,
        },
        richat::{GrpcSubscribeRequest, RichatFilter},
    },
    std::{collections::HashMap, io},
    thiserror::Error,
};

#[derive(Debug, Error)]
enum ConnectError {
    #[error(transparent)]
    Quic(QuicConnectError),
    #[error(transparent)]
    Tcp(io::Error),
    #[error(transparent)]
    Grpc(GrpcClientBuilderError),
}

#[derive(Debug, Error)]
enum SubscribePlainError {
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error(transparent)]
    Subscribe(#[from] richat_client::error::SubscribeError),
    #[error(transparent)]
    SubscribeGrpc(#[from] tonic::Status),
}

async fn subscribe_plain(
    config: ConfigChannelSource,
) -> Result<
    (
        ConfigChannelSourceShared,
        BoxStream<'static, Result<Vec<u8>, ReceiveError>>,
    ),
    SubscribePlainError,
> {
    let create_filter = |disable_accounts: bool| {
        Some(RichatFilter {
            disable_accounts,
            disable_transactions: false,
            disable_entries: false,
        })
    };

    Ok(match config {
        ConfigChannelSource::Quic { general, config } => {
            let connection = config.connect().await.map_err(ConnectError::Quic)?;
            let filter = create_filter(general.disable_accounts);
            (general, connection.subscribe(None, filter).await?.boxed())
        }
        ConfigChannelSource::Tcp { general, config } => {
            let connection = config.connect().await.map_err(ConnectError::Tcp)?;
            let filter = create_filter(general.disable_accounts);
            (general, connection.subscribe(None, filter).await?.boxed())
        }
        ConfigChannelSource::Grpc {
            general,
            source,
            config,
        } => {
            let mut connection = config.connect().await.map_err(ConnectError::Grpc)?;
            let stream = match source {
                ConfigGrpcClientSource::DragonsMouth => connection
                    .subscribe_dragons_mouth_once(SubscribeRequest {
                        accounts: if general.disable_accounts {
                            HashMap::new()
                        } else {
                            hashmap! { "".to_owned() => SubscribeRequestFilterAccounts::default() }
                        },
                        slots: hashmap! { "".to_owned() => SubscribeRequestFilterSlots {
                            filter_by_commitment: Some(false),
                            interslot_updates: Some(true),
                        } },
                        transactions: hashmap! { "".to_owned() => SubscribeRequestFilterTransactions::default() },
                        transactions_status: HashMap::new(),
                        blocks: HashMap::new(),
                        blocks_meta: hashmap! { "".to_owned() => SubscribeRequestFilterBlocksMeta::default() },
                        entry: hashmap! { "".to_owned() => SubscribeRequestFilterEntry::default() },
                        commitment: Some(CommitmentLevelProto::Processed as i32),
                        accounts_data_slice: vec![],
                        ping: None,
                        from_slot: None,
                    })
                    .await?
                    .boxed(),
                ConfigGrpcClientSource::Richat => connection
                    .subscribe_richat(GrpcSubscribeRequest {
                        replay_from_slot: None,
                        filter: create_filter(general.disable_accounts),
                    })
                    .await?
                    .boxed(),
            };
            (general, stream)
        }
    })
}

#[derive(Debug, Error)]
pub enum SubscribeError {
    #[error(transparent)]
    Receive(#[from] ReceiveError),
    #[error(transparent)]
    Parse(#[from] MessageParseError),
}

pub async fn subscribe(
    config: ConfigChannelSource,
) -> anyhow::Result<BoxStream<'static, Result<ParsedMessage, SubscribeError>>> {
    let (config, stream) = subscribe_plain(config).await?;
    Ok(stream
        .filter_map(move |value| async move {
            match value {
                Ok(data) => match Message::parse(data, config.parser) {
                    Ok(message) => Some(Ok(message.into())),
                    Err(MessageParseError::InvalidUpdateMessage("Ping")) => None,
                    Err(error) => Some(Err(error.into())),
                },
                Err(error) => Some(Err(error.into())),
            }
        })
        .boxed())
}

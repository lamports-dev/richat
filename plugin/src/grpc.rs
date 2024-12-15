use {
    crate::{
        channel::{Receiver, Sender},
        config::ConfigGrpc,
        version::GrpcVersionInfo,
    },
    anyhow::Context as _,
    futures::stream::Stream,
    log::{error, info},
    prost::{bytes::BufMut, Message},
    std::{
        marker::PhantomData,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    },
    tokio::sync::Notify,
    tonic::{
        codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
        transport::server::{Server, TcpIncoming},
        Code, Request, Response, Status, Streaming,
    },
    yellowstone_grpc_proto::geyser::{GetVersionRequest, GetVersionResponse, SubscribeRequest},
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
    type SubscribeStream = ReceiverStream;

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        tokio::spawn(async move {
            let mut stream = request.into_inner();
            loop {
                match stream.message().await {
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(_error) => {
                        //
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream {
            // TODO: recv sub req first
            rx: self.messages.subscribe(None).unwrap(),
        }))
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: serde_json::to_string(&GrpcVersionInfo::default()).unwrap(),
        }))
    }
}

#[derive(Debug)]
pub struct ReceiverStream {
    rx: Receiver,
}

impl Stream for ReceiverStream {
    type Item = Result<Arc<Vec<u8>>, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.recv_ref(cx.waker()) {
            Ok(Some(value)) => Poll::Ready(Some(Ok(value))),
            Ok(None) => Poll::Pending,
            Err(_error) => Poll::Ready(Some(Err(Status::out_of_range("lagged")))),
        }
    }
}

trait SubscribeMessage {
    fn encode(self, buf: &mut EncodeBuf<'_>);
}

impl SubscribeMessage for Arc<Vec<u8>> {
    fn encode(self, buf: &mut EncodeBuf<'_>) {
        let required = self.len();
        let remaining = buf.remaining_mut();
        if required > remaining {
            panic!("SubscribeMessage only errors if not enough space");
        }
        buf.put_slice(self.as_ref());
    }
}

struct SubscribeCodec<T, U> {
    _pd: PhantomData<(T, U)>,
}

impl<T, U> Default for SubscribeCodec<T, U> {
    fn default() -> Self {
        Self { _pd: PhantomData }
    }
}

impl<T, U> Codec for SubscribeCodec<T, U>
where
    T: SubscribeMessage + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;

    type Encoder = SubscribeEncoder<T>;
    type Decoder = ProstDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        SubscribeEncoder(PhantomData)
    }

    fn decoder(&mut self) -> Self::Decoder {
        ProstDecoder(PhantomData)
    }
}

/// A [`Encoder`] that knows how to encode `T`.
#[derive(Debug, Clone, Default)]
pub struct SubscribeEncoder<T>(PhantomData<T>);

impl<T: SubscribeMessage> Encoder for SubscribeEncoder<T> {
    type Item = T;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        item.encode(buf);
        Ok(())
    }
}

/// A [`Decoder`] that knows how to decode `U`.
#[derive(Debug, Clone, Default)]
pub struct ProstDecoder<U>(PhantomData<U>);

impl<U: Message + Default> Decoder for ProstDecoder<U> {
    type Item = U;
    type Error = Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        let item = Message::decode(buf)
            .map(Option::Some)
            .map_err(from_decode_error)?;

        Ok(item)
    }
}

fn from_decode_error(error: prost::DecodeError) -> Status {
    // Map Protobuf parse errors to an INTERNAL status code, as per
    // https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
    Status::new(Code::Internal, error.to_string())
}

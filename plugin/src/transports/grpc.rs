use {
    crate::{
        channel::{Receiver, RecvError, Sender, SubscribeError},
        metrics,
        version::VERSION,
    },
    futures::stream::Stream,
    log::{error, info},
    prost::{bytes::BufMut, Message},
    richat_shared::transports::grpc::{ConfigGrpcServer, GrpcSubscribeRequest},
    std::{
        borrow::Cow,
        future::Future,
        marker::PhantomData,
        pin::Pin,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        task::{Context, Poll},
    },
    tokio::task::JoinError,
    tonic::{
        codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder},
        service::interceptor::interceptor,
        Request, Response, Status, Streaming,
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
    subscribe_id: AtomicU64,
}

impl GrpcServer {
    pub async fn spawn(
        config: ConfigGrpcServer,
        messages: Sender,
        shutdown: impl Future<Output = ()> + Send + 'static,
    ) -> anyhow::Result<impl Future<Output = Result<(), JoinError>>> {
        let (incoming, server_builder) = config.create_server()?;
        info!("start server at {}", config.endpoint);

        let mut service = gen::geyser_server::GeyserServer::new(Self {
            messages,
            subscribe_id: AtomicU64::new(0),
        })
        .max_decoding_message_size(config.max_decoding_message_size);
        for encoding in config.compression.accept {
            service = service.accept_compressed(encoding);
        }
        for encoding in config.compression.send {
            service = service.send_compressed(encoding);
        }

        // Spawn server
        Ok(tokio::spawn(async move {
            if let Err(error) = server_builder
                .layer(interceptor(move |request: Request<()>| {
                    if config.x_tokens.is_empty() {
                        Ok(request)
                    } else {
                        match request.metadata().get("x-token") {
                            Some(token) if config.x_tokens.contains(token.as_bytes()) => {
                                Ok(request)
                            }
                            _ => Err(Status::unauthenticated("No valid auth token")),
                        }
                    }
                }))
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await
            {
                error!("server error: {error:?}")
            } else {
                info!("shutdown")
            }
        }))
    }
}

#[tonic::async_trait]
impl gen::geyser_server::Geyser for GrpcServer {
    type SubscribeStream = ReceiverStream;

    async fn subscribe(
        &self,
        mut request: Request<Streaming<GrpcSubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let id = self.subscribe_id.fetch_add(1, Ordering::Relaxed);
        info!("#{id}: new connection from {:?}", request.remote_addr());

        let replay_from_slot = match request.get_mut().message().await {
            Ok(Some(request)) => request.replay_from_slot,
            Ok(None) => {
                info!("#{id}: connection closed before receiving request");
                return Err(Status::aborted("stream closed before request received"));
            }
            Err(error) => {
                error!("#{id}: error receiving request {error}");
                return Err(Status::aborted("recv error"));
            }
        };

        match self.messages.subscribe(replay_from_slot) {
            Ok(rx) => {
                let pos = replay_from_slot
                    .map(|slot| format!("slot {slot}").into())
                    .unwrap_or(Cow::Borrowed("latest"));
                info!("#{id}: subscribed from {pos}");
                Ok(Response::new(ReceiverStream::new(rx, id)))
            }
            Err(SubscribeError::NotInitialized) => Err(Status::internal("not initialized")),
            Err(SubscribeError::SlotNotAvailable { first_available }) => Err(
                Status::invalid_argument(format!("first available slot: {first_available}")),
            ),
        }
    }

    async fn get_version(
        &self,
        _request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        Ok(Response::new(GetVersionResponse {
            version: VERSION.create_grpc_version_info().json(),
        }))
    }
}

#[derive(Debug)]
pub struct ReceiverStream {
    rx: Receiver,
    id: u64,
}

impl ReceiverStream {
    fn new(rx: Receiver, id: u64) -> Self {
        metrics::connections_total_add(metrics::ConnectionsTransport::Grpc);
        Self { rx, id }
    }
}

impl Drop for ReceiverStream {
    fn drop(&mut self) {
        info!("#{}: send stream closed", self.id);
        metrics::connections_total_dec(metrics::ConnectionsTransport::Grpc);
    }
}

impl Stream for ReceiverStream {
    type Item = Result<Arc<Vec<u8>>, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.recv_ref(cx.waker()) {
            Ok(Some(value)) => Poll::Ready(Some(Ok(value))),
            Ok(None) => Poll::Pending,
            Err(error) => {
                error!("#{}: failed to get message: {error}", self.id);
                match error {
                    RecvError::Lagged => Poll::Ready(Some(Err(Status::out_of_range("lagged")))),
                    RecvError::Closed => Poll::Ready(Some(Err(Status::out_of_range("closed")))),
                }
            }
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
    Status::new(tonic::Code::Internal, error.to_string())
}

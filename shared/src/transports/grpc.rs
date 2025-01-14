use {
    crate::{
        config::{deserialize_num_str, deserialize_x_token_set},
        shutdown::Shutdown,
        transports::{RecvError, RecvStream, Subscribe, SubscribeError},
        version::Version,
    },
    futures::{
        ready,
        stream::{Stream, StreamExt},
    },
    prost::{bytes::BufMut, Message},
    richat_proto::{
        geyser::{GetVersionRequest, GetVersionResponse},
        richat::GrpcSubscribeRequest,
    },
    serde::{
        de::{self, Deserializer},
        Deserialize,
    },
    std::{
        borrow::Cow,
        collections::HashSet,
        fmt, fs,
        future::Future,
        marker::PhantomData,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        pin::Pin,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        task::{Context, Poll},
        time::Duration,
    },
    thiserror::Error,
    tokio::task::JoinError,
    tonic::{
        codec::{Codec, CompressionEncoding, DecodeBuf, Decoder, EncodeBuf, Encoder},
        service::interceptor::interceptor,
        transport::{
            server::{Server, TcpIncoming},
            Identity, ServerTlsConfig,
        },
        Request, Response, Status, Streaming,
    },
    tracing::{error, info},
};

pub mod gen {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/geyser.Geyser.rs"));
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigGrpcCompression {
    #[serde(deserialize_with = "ConfigGrpcCompression::deserialize_compression")]
    pub accept: Vec<CompressionEncoding>,
    #[serde(deserialize_with = "ConfigGrpcCompression::deserialize_compression")]
    pub send: Vec<CompressionEncoding>,
}

impl Default for ConfigGrpcCompression {
    fn default() -> Self {
        Self {
            accept: Self::default_compression(),
            send: Self::default_compression(),
        }
    }
}

impl ConfigGrpcCompression {
    fn deserialize_compression<'de, D>(
        deserializer: D,
    ) -> Result<Vec<CompressionEncoding>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<&str>::deserialize(deserializer)?
            .into_iter()
            .map(|value| match value {
                "gzip" => Ok(CompressionEncoding::Gzip),
                "zstd" => Ok(CompressionEncoding::Zstd),
                value => Err(de::Error::custom(format!(
                    "Unknown compression format: {value}"
                ))),
            })
            .collect::<Result<_, _>>()
    }

    const fn default_compression() -> Vec<CompressionEncoding> {
        vec![]
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigGrpcServer {
    pub endpoint: SocketAddr,
    #[serde(deserialize_with = "ConfigGrpcServer::deserialize_tls_config")]
    pub tls_config: Option<ServerTlsConfig>,
    pub compression: ConfigGrpcCompression,
    /// Limits the maximum size of a decoded message, default is 4MiB
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max_decoding_message_size: usize,
    #[serde(with = "humantime_serde")]
    pub server_tcp_keepalive: Option<Duration>,
    pub server_tcp_nodelay: bool,
    pub server_http2_adaptive_window: Option<bool>,
    #[serde(with = "humantime_serde")]
    pub server_http2_keepalive_interval: Option<Duration>,
    #[serde(with = "humantime_serde")]
    pub server_http2_keepalive_timeout: Option<Duration>,
    pub server_initial_connection_window_size: Option<u32>,
    pub server_initial_stream_window_size: Option<u32>,
    #[serde(deserialize_with = "deserialize_x_token_set")]
    pub x_tokens: HashSet<Vec<u8>>,
}

impl Default for ConfigGrpcServer {
    fn default() -> Self {
        Self {
            endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10102),
            tls_config: None,
            compression: ConfigGrpcCompression::default(),
            max_decoding_message_size: 4 * 1024 * 1024, // 4MiB
            server_tcp_keepalive: Some(Duration::from_secs(15)),
            server_tcp_nodelay: true,
            server_http2_adaptive_window: None,
            server_http2_keepalive_interval: None,
            server_http2_keepalive_timeout: None,
            server_initial_connection_window_size: None,
            server_initial_stream_window_size: None,
            x_tokens: HashSet::new(),
        }
    }
}

impl ConfigGrpcServer {
    fn deserialize_tls_config<'de, D>(deserializer: D) -> Result<Option<ServerTlsConfig>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct ConfigTls<'a> {
            cert: &'a str,
            key: &'a str,
        }

        Option::<ConfigTls>::deserialize(deserializer)?
            .map(|config| {
                let cert = fs::read(config.cert).map_err(|error| {
                    de::Error::custom(format!("failed to read cert {}: {error:?}", config.cert))
                })?;
                let key = fs::read(config.key).map_err(|error| {
                    de::Error::custom(format!("failed to read key {}: {error:?}", config.key))
                })?;

                Ok(ServerTlsConfig::new().identity(Identity::from_pem(cert, key)))
            })
            .transpose()
    }

    pub fn create_server_builder(&self) -> Result<(TcpIncoming, Server), CreateServerError> {
        // Bind service address
        let incoming = TcpIncoming::new(
            self.endpoint,
            self.server_tcp_nodelay,
            self.server_tcp_keepalive,
        )
        .map_err(|error| CreateServerError::Bind {
            error,
            endpoint: self.endpoint,
        })?;

        // Create service
        let mut server_builder = Server::builder();
        if let Some(tls_config) = self.tls_config.clone() {
            server_builder = server_builder.tls_config(tls_config)?;
        }
        if let Some(enabled) = self.server_http2_adaptive_window {
            server_builder = server_builder.http2_adaptive_window(Some(enabled));
        }
        if let Some(http2_keepalive_interval) = self.server_http2_keepalive_interval {
            server_builder =
                server_builder.http2_keepalive_interval(Some(http2_keepalive_interval));
        }
        if let Some(http2_keepalive_timeout) = self.server_http2_keepalive_timeout {
            server_builder = server_builder.http2_keepalive_timeout(Some(http2_keepalive_timeout));
        }
        if let Some(sz) = self.server_initial_connection_window_size {
            server_builder = server_builder.initial_connection_window_size(sz);
        }
        if let Some(sz) = self.server_initial_stream_window_size {
            server_builder = server_builder.initial_stream_window_size(sz);
        }

        Ok((incoming, server_builder))
    }
}

#[derive(Debug, Error)]
pub enum CreateServerError {
    #[error("failed to bind {endpoint}: {error}")]
    Bind {
        error: Box<dyn std::error::Error + Send + Sync>,
        endpoint: SocketAddr,
    },
    #[error("failed to apply tls_config: {0}")]
    Tls(#[from] tonic::transport::Error),
}

pub struct GrpcServer<S, F1, F2> {
    messages: S,
    subscribe_id: AtomicU64,
    on_conn_new_cb: F1,
    on_conn_drop_cb: F2,
    version: Version,
}

impl<S, F1, F2> fmt::Debug for GrpcServer<S, F1, F2> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GrpcServer")
            .field("subscribe_id", &self.subscribe_id)
            .field("version", &self.version)
            .finish()
    }
}

impl<S, F1, F2> GrpcServer<S, F1, F2>
where
    S: Subscribe + Send + Sync + 'static,
    F1: Fn() + Copy + Unpin + Send + Sync + 'static,
    F2: Fn() + Copy + Unpin + Send + Sync + 'static,
{
    pub async fn spawn(
        config: ConfigGrpcServer,
        messages: S,
        on_conn_new_cb: F1,
        on_conn_drop_cb: F2,
        version: Version,
        shutdown: Shutdown,
    ) -> Result<impl Future<Output = Result<(), JoinError>>, CreateServerError> {
        let (incoming, server_builder) = config.create_server_builder()?;
        info!("start server at {}", config.endpoint);

        let mut service = gen::geyser_server::GeyserServer::new(Self {
            messages,
            subscribe_id: AtomicU64::new(0),
            on_conn_new_cb,
            on_conn_drop_cb,
            version,
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
impl<S, F1, F2> gen::geyser_server::Geyser for GrpcServer<S, F1, F2>
where
    S: Subscribe + Send + Sync + 'static,
    F1: Fn() + Copy + Unpin + Send + Sync + 'static,
    F2: Fn() + Copy + Unpin + Send + Sync + 'static,
{
    type SubscribeStream = ReceiverStream<F2>;

    async fn subscribe(
        &self,
        mut request: Request<Streaming<GrpcSubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let id = self.subscribe_id.fetch_add(1, Ordering::Relaxed);
        info!("#{id}: new connection from {:?}", request.remote_addr());

        let (replay_from_slot, filter) = match request.get_mut().message().await {
            Ok(Some(GrpcSubscribeRequest {
                replay_from_slot,
                filter,
            })) => (replay_from_slot, filter),
            Ok(None) => {
                info!("#{id}: connection closed before receiving request");
                return Err(Status::aborted("stream closed before request received"));
            }
            Err(error) => {
                error!("#{id}: error receiving request {error}");
                return Err(Status::aborted("recv error"));
            }
        };

        match self.messages.subscribe(replay_from_slot, filter) {
            Ok(rx) => {
                let pos = replay_from_slot
                    .map(|slot| format!("slot {slot}").into())
                    .unwrap_or(Cow::Borrowed("latest"));
                info!("#{id}: subscribed from {pos}");
                Ok(Response::new(ReceiverStream::new(
                    rx.boxed(),
                    id,
                    self.on_conn_new_cb,  // on new conn
                    self.on_conn_drop_cb, // on drop conn
                )))
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
            version: self.version.create_grpc_version_info().json(),
        }))
    }
}

pub struct ReceiverStream<F2: Fn()> {
    rx: RecvStream,
    id: u64,
    on_conn_drop_cb: F2,
}

impl<F2: Fn()> fmt::Debug for ReceiverStream<F2> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReceiverStream").finish()
    }
}

impl<F2: Fn()> ReceiverStream<F2> {
    fn new<F1: Fn()>(rx: RecvStream, id: u64, on_conn_new_cb: F1, on_conn_drop_cb: F2) -> Self {
        on_conn_new_cb();
        Self {
            rx,
            id,
            on_conn_drop_cb,
        }
    }
}

impl<F2: Fn()> Drop for ReceiverStream<F2> {
    fn drop(&mut self) {
        info!("#{}: send stream closed", self.id);
        (self.on_conn_drop_cb)();
    }
}

impl<F2: Fn() + Unpin> Stream for ReceiverStream<F2> {
    type Item = Result<Arc<Vec<u8>>, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.rx.poll_next_unpin(cx)) {
            Some(Ok(value)) => Poll::Ready(Some(Ok(value))),
            Some(Err(error)) => {
                error!("#{}: failed to get message: {error}", self.id);
                match error {
                    RecvError::Lagged => Poll::Ready(Some(Err(Status::out_of_range("lagged")))),
                    RecvError::Closed => Poll::Ready(Some(Err(Status::out_of_range("closed")))),
                }
            }
            None => Poll::Ready(None),
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

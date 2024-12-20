use {
    bytes::Bytes,
    futures::{
        channel::mpsc,
        sink::{Sink, SinkExt},
        stream::Stream,
    },
    std::{collections::HashMap, time::Duration},
    tonic::{
        codec::CompressionEncoding,
        metadata::{errors::InvalidMetadataValue, AsciiMetadataKey, AsciiMetadataValue},
        service::{interceptor::InterceptedService, Interceptor},
        transport::channel::{Channel, ClientTlsConfig, Endpoint},
        Request, Response, Status, Streaming,
    },
    yellowstone_grpc_proto::geyser::{
        geyser_client::GeyserClient, CommitmentLevel, GetBlockHeightRequest,
        GetBlockHeightResponse, GetLatestBlockhashRequest, GetLatestBlockhashResponse,
        GetSlotRequest, GetSlotResponse, GetVersionRequest, GetVersionResponse,
        IsBlockhashValidRequest, IsBlockhashValidResponse, PingRequest, PongResponse,
        SubscribeRequest, SubscribeUpdate,
    },
};

#[derive(Debug)]
pub struct GrpcClientBuilder {
    pub endpoint: Endpoint,
    pub send_compressed: Option<CompressionEncoding>,
    pub accept_compressed: Option<CompressionEncoding>,
    pub max_decoding_message_size: Option<usize>,
    pub max_encoding_message_size: Option<usize>,
    pub interceptor: GrpcInterceptor,
}

impl GrpcClientBuilder {
    // Create new builder
    fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            send_compressed: None,
            accept_compressed: None,
            max_decoding_message_size: None,
            max_encoding_message_size: None,
            interceptor: GrpcInterceptor::default(),
        }
    }

    pub fn from_shared(endpoint: impl Into<Bytes>) -> Result<Self, tonic::transport::Error> {
        Endpoint::from_shared(endpoint).map(Self::new)
    }

    pub fn from_static(endpoint: &'static str) -> Self {
        Self::new(Endpoint::from_static(endpoint))
    }

    // Endpoint options
    pub fn connect_timeout(self, dur: Duration) -> Self {
        Self {
            endpoint: self.endpoint.connect_timeout(dur),
            ..self
        }
    }

    pub fn buffer_size(self, sz: impl Into<Option<usize>>) -> Self {
        Self {
            endpoint: self.endpoint.buffer_size(sz),
            ..self
        }
    }

    pub fn http2_adaptive_window(self, enabled: bool) -> Self {
        Self {
            endpoint: self.endpoint.http2_adaptive_window(enabled),
            ..self
        }
    }

    pub fn http2_keep_alive_interval(self, interval: Duration) -> Self {
        Self {
            endpoint: self.endpoint.http2_keep_alive_interval(interval),
            ..self
        }
    }

    pub fn initial_connection_window_size(self, sz: impl Into<Option<u32>>) -> Self {
        Self {
            endpoint: self.endpoint.initial_connection_window_size(sz),
            ..self
        }
    }

    pub fn initial_stream_window_size(self, sz: impl Into<Option<u32>>) -> Self {
        Self {
            endpoint: self.endpoint.initial_stream_window_size(sz),
            ..self
        }
    }

    pub fn keep_alive_timeout(self, duration: Duration) -> Self {
        Self {
            endpoint: self.endpoint.keep_alive_timeout(duration),
            ..self
        }
    }

    pub fn keep_alive_while_idle(self, enabled: bool) -> Self {
        Self {
            endpoint: self.endpoint.keep_alive_while_idle(enabled),
            ..self
        }
    }

    pub fn tcp_keepalive(self, tcp_keepalive: Option<Duration>) -> Self {
        Self {
            endpoint: self.endpoint.tcp_keepalive(tcp_keepalive),
            ..self
        }
    }

    pub fn tcp_nodelay(self, enabled: bool) -> Self {
        Self {
            endpoint: self.endpoint.tcp_nodelay(enabled),
            ..self
        }
    }

    pub fn timeout(self, dur: Duration) -> Self {
        Self {
            endpoint: self.endpoint.timeout(dur),
            ..self
        }
    }

    pub fn tls_config(self, tls_config: ClientTlsConfig) -> Result<Self, tonic::transport::Error> {
        Ok(Self {
            endpoint: self.endpoint.tls_config(tls_config)?,
            ..self
        })
    }

    // gRPC options
    pub fn send_compressed(self, encoding: CompressionEncoding) -> Self {
        Self {
            send_compressed: Some(encoding),
            ..self
        }
    }

    pub fn accept_compressed(self, encoding: CompressionEncoding) -> Self {
        Self {
            accept_compressed: Some(encoding),
            ..self
        }
    }

    pub fn max_decoding_message_size(self, limit: usize) -> Self {
        Self {
            max_decoding_message_size: Some(limit),
            ..self
        }
    }

    pub fn max_encoding_message_size(self, limit: usize) -> Self {
        Self {
            max_encoding_message_size: Some(limit),
            ..self
        }
    }

    // Metadata
    pub fn x_token<T>(mut self, x_token: Option<T>) -> Result<Self, InvalidMetadataValue>
    where
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
    {
        if let Some(x_token) = x_token {
            self.interceptor.metadata.insert(
                AsciiMetadataKey::from_static("x-token"),
                x_token.try_into()?,
            );
        } else {
            self.interceptor.metadata.remove("x-token");
        }
        Ok(self)
    }

    // Create client
    fn build(self, channel: Channel) -> GrpcClient<impl Interceptor> {
        let mut geyser = GeyserClient::with_interceptor(channel, self.interceptor);
        if let Some(encoding) = self.send_compressed {
            geyser = geyser.send_compressed(encoding);
        }
        if let Some(encoding) = self.accept_compressed {
            geyser = geyser.accept_compressed(encoding);
        }
        if let Some(limit) = self.max_decoding_message_size {
            geyser = geyser.max_decoding_message_size(limit);
        }
        if let Some(limit) = self.max_encoding_message_size {
            geyser = geyser.max_encoding_message_size(limit);
        }
        GrpcClient::new(geyser)
    }

    pub async fn connect(self) -> Result<GrpcClient<impl Interceptor>, tonic::transport::Error> {
        let channel = self.endpoint.connect().await?;
        Ok(self.build(channel))
    }

    pub fn connect_lazy(self) -> Result<GrpcClient<impl Interceptor>, tonic::transport::Error> {
        let channel = self.endpoint.connect_lazy();
        Ok(self.build(channel))
    }
}

#[derive(Debug, Default)]
pub struct GrpcInterceptor {
    metadata: HashMap<AsciiMetadataKey, AsciiMetadataValue>,
}

impl Interceptor for GrpcInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        for (key, value) in self.metadata.iter() {
            request.metadata_mut().insert(key, value.clone());
        }
        Ok(request)
    }
}

#[derive(Debug)]
pub struct GrpcClient<F> {
    geyser: GeyserClient<InterceptedService<Channel, F>>,
}

impl GrpcClient<()> {
    pub fn build_from_shared(
        endpoint: impl Into<Bytes>,
    ) -> Result<GrpcClientBuilder, tonic::transport::Error> {
        Ok(GrpcClientBuilder::new(Endpoint::from_shared(endpoint)?))
    }

    pub fn build_from_static(endpoint: &'static str) -> GrpcClientBuilder {
        GrpcClientBuilder::new(Endpoint::from_static(endpoint))
    }
}

impl<F: Interceptor> GrpcClient<F> {
    pub const fn new(geyser: GeyserClient<InterceptedService<Channel, F>>) -> Self {
        Self { geyser }
    }

    // Subscribe
    async fn subscribe_with_request(
        &mut self,
        request: Option<SubscribeRequest>,
    ) -> Result<
        (
            impl Sink<SubscribeRequest, Error = mpsc::SendError>,
            impl Stream<Item = Result<SubscribeUpdate, Status>>,
        ),
        Status,
    > {
        let (mut subscribe_tx, subscribe_rx) = mpsc::unbounded();
        if let Some(request) = request {
            subscribe_tx
                .send(request)
                .await
                .expect("failed to send to unbounded channel");
        }
        let response: Response<Streaming<SubscribeUpdate>> =
            self.geyser.subscribe(subscribe_rx).await?;
        Ok((subscribe_tx, response.into_inner()))
    }

    pub async fn subscribe(
        &mut self,
    ) -> Result<
        (
            impl Sink<SubscribeRequest, Error = mpsc::SendError>,
            impl Stream<Item = Result<SubscribeUpdate, Status>>,
        ),
        Status,
    > {
        self.subscribe_with_request(None).await
    }

    pub async fn subscribe_once(
        &mut self,
        request: SubscribeRequest,
    ) -> Result<impl Stream<Item = Result<SubscribeUpdate, Status>>, Status> {
        self.subscribe_with_request(Some(request))
            .await
            .map(|(_sink, stream)| stream)
    }

    // RPC calls
    pub async fn ping(&mut self, count: i32) -> Result<PongResponse, Status> {
        let message = PingRequest { count };
        let request = Request::new(message);
        let response = self.geyser.ping(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_latest_blockhash(
        &mut self,
        commitment: Option<CommitmentLevel>,
    ) -> Result<GetLatestBlockhashResponse, Status> {
        let request = Request::new(GetLatestBlockhashRequest {
            commitment: commitment.map(|value| value as i32),
        });
        let response = self.geyser.get_latest_blockhash(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_block_height(
        &mut self,
        commitment: Option<CommitmentLevel>,
    ) -> Result<GetBlockHeightResponse, Status> {
        let request = Request::new(GetBlockHeightRequest {
            commitment: commitment.map(|value| value as i32),
        });
        let response = self.geyser.get_block_height(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_slot(
        &mut self,
        commitment: Option<CommitmentLevel>,
    ) -> Result<GetSlotResponse, Status> {
        let request = Request::new(GetSlotRequest {
            commitment: commitment.map(|value| value as i32),
        });
        let response = self.geyser.get_slot(request).await?;
        Ok(response.into_inner())
    }

    pub async fn is_blockhash_valid(
        &mut self,
        blockhash: String,
        commitment: Option<CommitmentLevel>,
    ) -> Result<IsBlockhashValidResponse, Status> {
        let request = Request::new(IsBlockhashValidRequest {
            blockhash,
            commitment: commitment.map(|value| value as i32),
        });
        let response = self.geyser.is_blockhash_valid(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_version(&mut self) -> Result<GetVersionResponse, Status> {
        let request = Request::new(GetVersionRequest {});
        let response = self.geyser.get_version(request).await?;
        Ok(response.into_inner())
    }
}

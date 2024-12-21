use {
    crate::error::{ReceiveError, SubscribeError},
    futures::{
        future::{BoxFuture, FutureExt},
        ready,
        stream::Stream,
    },
    pin_project_lite::pin_project,
    prost::Message,
    quinn::{
        crypto::rustls::{NoInitialCipherSuite, QuicClientConfig},
        ClientConfig, ConnectError, Connection, ConnectionError, Endpoint, RecvStream,
        TransportConfig, VarInt,
    },
    richat_shared::transports::{
        grpc::GrpcSubscribeRequest,
        quic::{QuicSubscribeClose, QuicSubscribeRequest},
    },
    rustls::{
        pki_types::{CertificateDer, ServerName, UnixTime},
        RootCertStore,
    },
    solana_sdk::clock::Slot,
    std::{
        collections::HashMap,
        fmt,
        future::Future,
        io,
        net::{IpAddr, Ipv6Addr, SocketAddr},
        path::PathBuf,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    },
    thiserror::Error,
    tokio::{
        fs,
        io::{AsyncReadExt, AsyncWriteExt},
        net::{lookup_host, ToSocketAddrs},
    },
    yellowstone_grpc_proto::geyser::SubscribeUpdate,
};

/// Dummy certificate verifier that treats any certificate as valid.
/// NOTE, such verification is vulnerable to MITM attacks, but convenient for testing.
#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[derive(Debug, Error)]
pub enum QuicConnectError {
    #[error("failed to create Quic ClientConfig from Rustls: {0}")]
    QuicClientConfig(#[from] NoInitialCipherSuite),
    #[error("failed to resolve endpoint: {0}")]
    LookupError(io::Error),
    #[error("failed to bind local port: {0}")]
    EndpointClient(io::Error),
    #[error("failed to connect: {0}")]
    Connect(#[from] ConnectError),
    #[error("connection failed: {0}")]
    Connection(#[from] ConnectionError),
    #[error("server name should be defined")]
    ServerName,
    #[error("errors occured when loading native certs: {0:?}")]
    LoadNativeCerts(Vec<rustls_native_certs::Error>),
    #[error("failed to read certificate chain: {0}")]
    LoadCert(io::Error),
    #[error("failed to add cert to roots: {0}")]
    AddCert(rustls::Error),
    #[error("invalid PEM-encoded certificate: {0}")]
    PemCert(io::Error),
}

#[derive(Debug)]
pub struct QuicClientBuilder {
    pub local_addr: SocketAddr,
    pub expected_rtt: u32,
    pub max_stream_bandwidth: u32,
    pub max_idle_timeout: Option<u32>,
    pub server_name: Option<String>,
    pub recv_streams: u32,
    pub max_backlog: Option<u32>,
}

impl Default for QuicClientBuilder {
    fn default() -> Self {
        Self {
            local_addr: SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0),
            expected_rtt: 100,
            max_stream_bandwidth: 12_500 * 1_000,
            max_idle_timeout: Some(30_000),
            server_name: None,
            recv_streams: 1,
            max_backlog: None,
        }
    }
}

impl QuicClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_local_addr(self, local_addr: Option<SocketAddr>) -> Self {
        Self {
            local_addr: local_addr.unwrap_or(Self::default().local_addr),
            ..self
        }
    }

    pub fn set_expected_rtt(self, expected_rtt: u32) -> Self {
        Self {
            expected_rtt,
            ..self
        }
    }

    pub fn set_max_stream_bandwidth(self, max_stream_bandwidth: u32) -> Self {
        Self {
            max_stream_bandwidth,
            ..self
        }
    }

    pub fn set_max_idle_timeout(self, max_idle_timeout: Option<u32>) -> Self {
        Self {
            max_idle_timeout,
            ..self
        }
    }

    pub fn set_server_name(self, server_name: Option<String>) -> Self {
        Self {
            server_name,
            ..self
        }
    }

    pub fn set_recv_streams(self, recv_streams: u32) -> Self {
        Self {
            recv_streams,
            ..self
        }
    }

    pub fn set_max_backlog(self, max_backlog: Option<u32>) -> Self {
        Self {
            max_backlog,
            ..self
        }
    }

    pub const fn insecure(self) -> QuicClientBuilderInsecure {
        QuicClientBuilderInsecure { builder: self }
    }

    pub const fn secure(self, cert: Option<PathBuf>) -> QuicClientBuilderSecure {
        QuicClientBuilderSecure {
            builder: self,
            cert,
        }
    }

    async fn connect<T: ToSocketAddrs>(
        self,
        endpoint: T,
        client_config: rustls::ClientConfig,
    ) -> Result<QuicClient, QuicConnectError> {
        let addr = lookup_host(endpoint)
            .await
            .map_err(QuicConnectError::LookupError)?
            .next()
            .ok_or(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "failed to resolve",
            ))
            .map_err(QuicConnectError::LookupError)?;
        let server_name = self.server_name.ok_or(QuicConnectError::ServerName)?;

        let mut transport_config = TransportConfig::default();
        transport_config.max_concurrent_bidi_streams(0u8.into());
        transport_config.max_concurrent_uni_streams(self.recv_streams.into());
        let stream_rwnd = self.max_stream_bandwidth / 1_000 * self.expected_rtt;
        transport_config.stream_receive_window(stream_rwnd.into());
        transport_config.send_window(8 * stream_rwnd as u64);
        transport_config.datagram_receive_buffer_size(Some(stream_rwnd as usize));
        transport_config
            .max_idle_timeout(self.max_idle_timeout.map(|ms| VarInt::from_u32(ms).into()));

        let crypto_config = Arc::new(QuicClientConfig::try_from(client_config)?);
        let mut client_config = ClientConfig::new(crypto_config);
        client_config.transport_config(Arc::new(transport_config));

        let mut endpoint =
            Endpoint::client(self.local_addr).map_err(QuicConnectError::EndpointClient)?;
        endpoint.set_default_client_config(client_config);

        let conn = endpoint.connect(addr, &server_name)?.await?;

        Ok(QuicClient {
            conn,
            recv_streams: self.recv_streams,
            max_backlog: self.max_backlog,
        })
    }
}

#[derive(Debug)]
pub struct QuicClientBuilderInsecure {
    pub builder: QuicClientBuilder,
}

impl QuicClientBuilderInsecure {
    pub async fn connect<T: ToSocketAddrs>(
        self,
        endpoint: T,
    ) -> Result<QuicClient, QuicConnectError> {
        self.builder
            .connect(
                endpoint,
                rustls::ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(SkipServerVerification::new())
                    .with_no_client_auth(),
            )
            .await
    }
}

#[derive(Debug)]
pub struct QuicClientBuilderSecure {
    pub builder: QuicClientBuilder,
    pub cert: Option<PathBuf>,
}

impl QuicClientBuilderSecure {
    pub async fn connect<T: ToSocketAddrs>(
        self,
        endpoint: T,
    ) -> Result<QuicClient, QuicConnectError> {
        let mut roots = RootCertStore::empty();
        // native
        let rustls_native_certs::CertificateResult { certs, errors, .. } =
            rustls_native_certs::load_native_certs();
        if !errors.is_empty() {
            return Err(QuicConnectError::LoadNativeCerts(errors));
        }
        roots.add_parsable_certificates(certs);
        // webpki
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        // custom
        if let Some(cert_path) = self.cert {
            let cert_chain = fs::read(&cert_path)
                .await
                .map_err(QuicConnectError::LoadCert)?;
            if cert_path.extension().is_some_and(|x| x == "der") {
                roots
                    .add(CertificateDer::from(cert_chain))
                    .map_err(QuicConnectError::AddCert)?;
            } else {
                for cert in rustls_pemfile::certs(&mut &*cert_chain) {
                    roots
                        .add(cert.map_err(QuicConnectError::PemCert)?)
                        .map_err(QuicConnectError::AddCert)?;
                }
            }
        }

        self.builder
            .connect(
                endpoint,
                rustls::ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth(),
            )
            .await
    }
}

#[derive(Debug)]
pub struct QuicClient {
    conn: Connection,
    recv_streams: u32,
    max_backlog: Option<u32>,
}

impl QuicClient {
    pub fn builder() -> QuicClientBuilder {
        QuicClientBuilder::new()
    }

    pub async fn subscribe(
        self,
        replay_from_slot: Option<Slot>,
    ) -> Result<QuicClientStream, SubscribeError> {
        let message = QuicSubscribeRequest {
            request: Some(GrpcSubscribeRequest { replay_from_slot }),
            recv_streams: self.recv_streams,
            max_backlog: self.max_backlog,
        }
        .encode_to_vec();

        let (mut send, mut recv) = self.conn.open_bi().await?;
        send.write_u64(message.len() as u64).await?;
        send.write_all(&message).await?;
        send.flush().await?;

        SubscribeError::parse_quic_response(&mut recv).await?;

        let mut readers = Vec::with_capacity(self.recv_streams as usize);
        for _ in 0..self.recv_streams {
            let stream = self.conn.accept_uni().await?;
            readers.push(QuicClientStreamReader::Init {
                recv: Some((stream, vec![])),
            });
        }

        Ok(QuicClientStream {
            conn: self.conn,
            msg_id: 0,
            messages: HashMap::new(),
            readers: QuicClientStreamReaders { readers, index: 0 },
        })
    }

    async fn recv(
        mut stream: RecvStream,
        mut buffer: Vec<u8>,
    ) -> Result<(RecvStream, Vec<u8>, u64, SubscribeUpdate), ReceiveError> {
        let msg_id = stream.read_u64().await?;
        let error = msg_id == u64::MAX;
        let size = stream.read_u64().await? as usize;
        if size > buffer.len() {
            buffer.resize(size, 0);
        }
        stream
            .read_exact(&mut buffer.as_mut_slice()[0..size])
            .await?;

        if error {
            let close = QuicSubscribeClose::decode(&buffer.as_slice()[0..size])?;
            Err(close.into())
        } else {
            let msg = SubscribeUpdate::decode(&buffer.as_slice()[0..size])?;
            Ok((stream, buffer, msg_id, msg))
        }
    }
}

pin_project! {
    pub struct QuicClientStream {
        conn: Connection,
        msg_id: u64,
        messages: HashMap<u64, SubscribeUpdate>,
        #[pin]
        readers: QuicClientStreamReaders,
    }
}

impl fmt::Debug for QuicClientStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicClientStream").finish()
    }
}

impl Stream for QuicClientStream {
    type Item = Result<SubscribeUpdate, ReceiveError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.project();

        if let Some(message) = me.messages.remove(me.msg_id) {
            *me.msg_id += 1;
            return Poll::Ready(Some(Ok(message)));
        }

        match ready!(me.readers.poll_next(cx)) {
            Some(Ok((msg_id, message))) => {
                if *me.msg_id == msg_id {
                    *me.msg_id += 1;
                    Poll::Ready(Some(Ok(message)))
                } else {
                    me.messages.insert(msg_id, message);
                    Poll::Pending
                }
            }
            Some(Err(error)) => Poll::Ready(Some(Err(error))),
            None => Poll::Ready(None),
        }
    }
}

pin_project! {
    pub struct QuicClientStreamReaders {
        #[pin]
        readers: Vec<QuicClientStreamReader>,
        index: usize,
    }
}

impl fmt::Debug for QuicClientStreamReaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicClientStreamReaders").finish()
    }
}

impl Stream for QuicClientStreamReaders {
    type Item = Result<(u64, SubscribeUpdate), ReceiveError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut me = self.project();
        let mut polled = 0;
        loop {
            // try to get value and increment index
            let value = Pin::new(&mut me.readers[*me.index]).poll_next(cx);
            *me.index += 1;
            if *me.index == me.readers.len() {
                *me.index = 0;
            }
            if value.is_ready() {
                return value;
            }

            // return pending if already polled all streams
            polled += 1;
            if polled == me.readers.len() {
                return Poll::Pending;
            }
        }
    }
}

pin_project! {
    #[project = QuicClientStreamReaderProj]
    pub enum QuicClientStreamReader {
        Init {
            recv: Option<(RecvStream, Vec<u8>)>,
        },
        Read {
            #[pin] future: BoxFuture<'static, Result<(RecvStream, Vec<u8>, u64, SubscribeUpdate), ReceiveError>>,
        },
    }
}

impl fmt::Debug for QuicClientStreamReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicClientStreamReader").finish()
    }
}

impl Stream for QuicClientStreamReader {
    type Item = Result<(u64, SubscribeUpdate), ReceiveError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.as_mut().project() {
                QuicClientStreamReaderProj::Init { recv } => {
                    let (stream, buffer) = recv.take().unwrap();
                    let future = QuicClient::recv(stream, buffer).boxed();
                    self.set(Self::Read { future })
                }
                QuicClientStreamReaderProj::Read { mut future } => {
                    return Poll::Ready(match ready!(future.as_mut().poll(cx)) {
                        Ok((stream, buffer, msg_id, message)) => {
                            self.set(Self::Init {
                                recv: Some((stream, buffer)),
                            });
                            Some(Ok((msg_id, message)))
                        }
                        Err(error) => {
                            if error.is_eof() {
                                None
                            } else {
                                Some(Err(error))
                            }
                        }
                    })
                }
            }
        }
    }
}

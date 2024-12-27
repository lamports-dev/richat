use {
    crate::{
        error::{ReceiveError, SubscribeError},
        stream::SubscribeStream,
    },
    futures::{
        future::{BoxFuture, FutureExt},
        ready,
        stream::{Stream, StreamExt},
    },
    pin_project_lite::pin_project,
    prost::Message,
    richat_shared::transports::{
        grpc::GrpcSubscribeRequest, quic::QuicSubscribeClose, tcp::ConfigTcpServer,
    },
    serde::Deserialize,
    solana_sdk::clock::Slot,
    std::{
        fmt,
        future::Future,
        io,
        net::SocketAddr,
        pin::Pin,
        task::{Context, Poll},
    },
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{lookup_host, TcpSocket, TcpStream, ToSocketAddrs},
    },
};

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigTcpClient {
    pub endpoint: String,
    pub keepalive: Option<bool>,
    pub nodelay: Option<bool>,
    pub recv_buffer_size: Option<u32>,
}

impl Default for ConfigTcpClient {
    fn default() -> Self {
        Self {
            endpoint: ConfigTcpServer::default().endpoint.to_string(),
            keepalive: None,
            nodelay: None,
            recv_buffer_size: None,
        }
    }
}

impl ConfigTcpClient {
    pub async fn connect(self) -> io::Result<TcpClient> {
        TcpClientBuilder::new()
            .set_keepalive(self.keepalive)
            .set_nodelay(self.nodelay)
            .set_recv_buffer_size(self.recv_buffer_size)
            .connect(self.endpoint)
            .await
    }
}

#[derive(Debug, Default)]
pub struct TcpClientBuilder {
    pub keepalive: Option<bool>,
    pub nodelay: Option<bool>,
    pub recv_buffer_size: Option<u32>,
}

impl TcpClientBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn connect<T: ToSocketAddrs>(self, endpoint: T) -> io::Result<TcpClient> {
        let addr = lookup_host(endpoint).await?.next().ok_or(io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            "failed to resolve",
        ))?;

        let socket = match addr {
            SocketAddr::V4(_) => TcpSocket::new_v4(),
            SocketAddr::V6(_) => TcpSocket::new_v6(),
        }?;

        if let Some(keepalive) = self.keepalive {
            socket.set_keepalive(keepalive)?;
        }
        if let Some(nodelay) = self.nodelay {
            socket.set_nodelay(nodelay)?;
        }
        if let Some(recv_buffer_size) = self.recv_buffer_size {
            socket.set_recv_buffer_size(recv_buffer_size)?;
        }

        let stream = socket.connect(addr).await?;
        Ok(TcpClient { stream })
    }

    pub const fn set_keepalive(self, keepalive: Option<bool>) -> Self {
        Self { keepalive, ..self }
    }

    pub const fn set_nodelay(self, nodelay: Option<bool>) -> Self {
        Self { nodelay, ..self }
    }

    pub const fn set_recv_buffer_size(self, recv_buffer_size: Option<u32>) -> Self {
        Self {
            recv_buffer_size,
            ..self
        }
    }
}

#[derive(Debug)]
pub struct TcpClient {
    stream: TcpStream,
}

impl TcpClient {
    pub fn build() -> TcpClientBuilder {
        TcpClientBuilder::new()
    }

    pub async fn subscribe(
        mut self,
        replay_from_slot: Option<Slot>,
    ) -> Result<TcpClientStream, SubscribeError> {
        let message = GrpcSubscribeRequest { replay_from_slot }.encode_to_vec();
        self.stream.write_u64(message.len() as u64).await?;
        self.stream.write_all(&message).await?;
        SubscribeError::parse_quic_response(&mut self.stream).await?;

        Ok(TcpClientStream::Init {
            stream: Some(self.stream),
        })
    }

    async fn recv(mut stream: TcpStream) -> Result<(TcpStream, Vec<u8>), ReceiveError> {
        // read size / error
        let mut size = stream.read_u64().await?;
        let is_error = if size == u64::MAX {
            size = stream.read_u64().await?;
            true
        } else {
            false
        };

        // create buffer
        let size = size as usize;
        let mut buffer = Vec::<u8>::with_capacity(size);

        // read
        stream
            .read_exact(unsafe { std::slice::from_raw_parts_mut(buffer.as_mut_ptr(), size) })
            .await?;
        unsafe {
            buffer.set_len(size);
        }

        // parse message if error
        if is_error {
            let close = QuicSubscribeClose::decode(buffer.as_slice())?;
            Err(close.into())
        } else {
            Ok((stream, buffer))
        }
    }
}

pin_project! {
    #[project = TcpClientStreamProj]
    pub enum TcpClientStream {
        Init {
            stream: Option<TcpStream>,
        },
        Read {
            #[pin] future: BoxFuture<'static, Result<(TcpStream, Vec<u8>), ReceiveError>>,
        },
    }
}

impl TcpClientStream {
    pub fn into_parsed(self) -> SubscribeStream {
        SubscribeStream::new(self.boxed())
    }
}

impl fmt::Debug for TcpClientStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpClientStream").finish()
    }
}

impl Stream for TcpClientStream {
    type Item = Result<Vec<u8>, ReceiveError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.as_mut().project() {
                TcpClientStreamProj::Init { stream } => {
                    let stream = stream.take().unwrap();
                    let future = TcpClient::recv(stream).boxed();
                    self.set(Self::Read { future })
                }
                TcpClientStreamProj::Read { mut future } => {
                    return Poll::Ready(match ready!(future.as_mut().poll(cx)) {
                        Ok((stream, message)) => {
                            self.set(Self::Init {
                                stream: Some(stream),
                            });
                            Some(Ok(message))
                        }
                        Err(error) => {
                            if error.is_eof() {
                                None
                            } else {
                                Some(Err(error))
                            }
                        }
                    });
                }
            }
        }
    }
}

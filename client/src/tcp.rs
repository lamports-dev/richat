use {
    crate::error::{ReceiveError, SubscribeError},
    futures::{
        future::{BoxFuture, FutureExt},
        ready,
        stream::Stream,
    },
    pin_project_lite::pin_project,
    prost::Message,
    richat_shared::transports::{grpc::GrpcSubscribeRequest, quic::QuicSubscribeClose},
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
        net::{TcpSocket, TcpStream},
    },
    yellowstone_grpc_proto::geyser::SubscribeUpdate,
};

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

    pub async fn connect(self, endpoint: SocketAddr) -> io::Result<TcpClient> {
        let socket = match endpoint {
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

        let stream = socket.connect(endpoint).await?;
        Ok(TcpClient {
            stream,
            buffer: Vec::new(),
        })
    }

    pub const fn set_keepalive(self, keepalive: bool) -> Self {
        Self {
            keepalive: Some(keepalive),
            ..self
        }
    }

    pub const fn set_nodelay(self, nodelay: bool) -> Self {
        Self {
            nodelay: Some(nodelay),
            ..self
        }
    }

    pub const fn set_recv_buffer_size(self, recv_buffer_size: u32) -> Self {
        Self {
            recv_buffer_size: Some(recv_buffer_size),
            ..self
        }
    }
}

#[derive(Debug)]
pub struct TcpClient {
    stream: TcpStream,
    buffer: Vec<u8>,
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
        Ok(TcpClientStream::Init { client: Some(self) })
    }

    async fn recv(mut self) -> Result<(Self, SubscribeUpdate), ReceiveError> {
        let mut size = self.stream.read_u64().await? as usize;
        let error = if size == 0 {
            size = self.stream.read_u64().await? as usize;
            true
        } else {
            false
        };
        if size > self.buffer.len() {
            self.buffer.resize(size, 0);
        }
        self.stream
            .read_exact(&mut self.buffer.as_mut_slice()[0..size])
            .await?;

        if error {
            let close = QuicSubscribeClose::decode(&self.buffer.as_slice()[0..size])?;
            Err(close.into())
        } else {
            let msg = SubscribeUpdate::decode(&self.buffer.as_slice()[0..size])?;
            Ok((self, msg))
        }
    }
}

pin_project! {
    #[project = TcpClientStreamProj]
    pub enum TcpClientStream {
        Init {
            client: Option<TcpClient>,
        },
        Read {
            #[pin] future: BoxFuture<'static, Result<(TcpClient, SubscribeUpdate), ReceiveError>>,
        },
    }
}

impl fmt::Debug for TcpClientStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpClientStream").finish()
    }
}

impl Stream for TcpClientStream {
    type Item = Result<SubscribeUpdate, ReceiveError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.as_mut().project() {
                TcpClientStreamProj::Init { client } => {
                    let future = client.take().unwrap().recv().boxed();
                    self.set(Self::Read { future })
                }
                TcpClientStreamProj::Read { mut future } => {
                    return Poll::Ready(match ready!(future.as_mut().poll(cx)) {
                        Ok((client, message)) => {
                            self.set(Self::Init {
                                client: Some(client),
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
                    })
                }
            }
        }
    }
}

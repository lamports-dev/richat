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
    richat_shared::transports::{grpc::GrpcSubscribeRequest, quic::QuicSubscribeClose},
    solana_sdk::clock::Slot,
    std::{
        borrow::Cow,
        fmt,
        future::Future,
        io,
        marker::PhantomData,
        net::SocketAddr,
        pin::Pin,
        task::{Context, Poll},
    },
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{lookup_host, TcpSocket, TcpStream, ToSocketAddrs},
    },
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
}

impl TcpClient {
    pub fn build() -> TcpClientBuilder {
        TcpClientBuilder::new()
    }

    pub async fn subscribe<'a>(
        mut self,
        replay_from_slot: Option<Slot>,
    ) -> Result<TcpClientBinaryRecv<'a>, SubscribeError> {
        let message = GrpcSubscribeRequest { replay_from_slot }.encode_to_vec();
        self.stream.write_u64(message.len() as u64).await?;
        self.stream.write_all(&message).await?;
        SubscribeError::parse_quic_response(&mut self.stream).await?;

        Ok(TcpClientBinaryRecv {
            stream: self.stream,
            buffer: Vec::new(),
            size: 0,
            msg_id: 0,
            _ph: PhantomData,
        })
    }
}

#[derive(Debug)]
pub struct TcpClientBinaryRecv<'a> {
    stream: TcpStream,
    buffer: Vec<u8>,
    size: usize,
    msg_id: u64,
    _ph: PhantomData<&'a ()>,
}

impl<'a> TcpClientBinaryRecv<'a> {
    pub async fn read(mut self) -> Result<Self, ReceiveError> {
        // read size / error
        let mut size = self.stream.read_u64().await?;
        let is_error = if size == u64::MAX {
            size = self.stream.read_u64().await?;
            true
        } else {
            false
        };

        // set size
        let size = size as usize;
        if self.buffer.len() < size {
            self.buffer.resize(size, 0);
        }
        self.size = size;

        // read
        self.stream
            .read_exact(&mut self.buffer.as_mut_slice()[0..size])
            .await?;

        // parse message if error
        if is_error {
            let close = QuicSubscribeClose::decode(&self.buffer.as_slice()[0..size])?;
            Err(close.into())
        } else {
            Ok(self)
        }
    }

    pub fn get_msg_id(&mut self) -> u64 {
        let msg_id = self.msg_id;
        self.msg_id += 1;
        msg_id
    }

    pub fn get_message(&'a self) -> &'a [u8] {
        &self.buffer.as_slice()[0..self.size]
    }

    pub const fn into_stream(self) -> TcpClientStream<'a> {
        TcpClientStream::Init { stream: Some(self) }
    }
}

pin_project! {
    #[project = TcpClientStreamProj]
    pub enum TcpClientStream<'a> {
        Init {
            stream: Option<TcpClientBinaryRecv<'a>>,
        },
        Read {
            #[pin] future: BoxFuture<'a, Result<TcpClientBinaryRecv<'a>, ReceiveError>>,
        },
    }
}

impl<'a> TcpClientStream<'a> {
    pub fn into_parsable_stream(self) -> SubscribeStream<'a> {
        SubscribeStream::new(self.boxed())
    }
}

impl<'a> fmt::Debug for TcpClientStream<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpClientStream").finish()
    }
}

impl<'a> Stream for TcpClientStream<'a> {
    type Item = Result<(u64, Cow<'a, [u8]>), ReceiveError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.as_mut().project() {
                TcpClientStreamProj::Init { stream } => {
                    let future = stream.take().unwrap().read().boxed();
                    self.set(Self::Read { future })
                }
                TcpClientStreamProj::Read { mut future } => {
                    return Poll::Ready(match ready!(future.as_mut().poll(cx)) {
                        Ok(mut stream) => {
                            let msg_id = stream.msg_id;
                            stream.msg_id += 1;
                            let data: *const u8 = stream.buffer.as_ptr();
                            let slice = unsafe { std::slice::from_raw_parts(data, stream.size) };
                            self.set(Self::Init {
                                stream: Some(stream),
                            });
                            Some(Ok((msg_id, Cow::Borrowed(slice))))
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

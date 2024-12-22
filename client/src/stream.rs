use {
    crate::error::ReceiveError,
    futures::{
        ready,
        stream::{BoxStream, Stream},
    },
    pin_project_lite::pin_project,
    prost::Message,
    std::{
        fmt,
        pin::Pin,
        task::{Context, Poll},
    },
    thiserror::Error,
    yellowstone_grpc_proto::geyser::SubscribeUpdate,
};

#[derive(Debug, Error)]
pub enum ReadBufferError {
    #[error("{len} overflow buffer capacity {capacity}")]
    Overflow { capacity: usize, len: usize },
}

#[derive(Debug)]
pub struct ReadBuffer {
    buf: Vec<u8>, // TODO: use `Box<[MaybeUninit<u8>]>`, `Box::new_uninit_slice` from 1.82.0
    len: usize,
}

impl ReadBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: vec![0; capacity],
            len: 0,
        }
    }

    #[allow(clippy::needless_lifetimes)]
    pub fn as_unsafe_slice<'a, 'b>(&'a self) -> &'b [u8] {
        let data: *const u8 = self.buf.as_ptr();
        unsafe { std::slice::from_raw_parts(data, self.len) }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf.as_slice()[0..self.len]
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buf.as_mut_slice()[0..self.len]
    }

    pub fn set_len(&mut self, len: usize) -> Result<(), ReadBufferError> {
        if len < self.buf.len() {
            self.len = len;
            Ok(())
        } else {
            Err(ReadBufferError::Overflow {
                capacity: self.buf.len(),
                len,
            })
        }
    }
}

type InputStream<'a> = BoxStream<'a, Result<&'a [u8], ReceiveError>>;

pin_project! {
    pub struct SubscribeStream<'a> {
        stream: InputStream<'a>,
    }
}

impl<'a> SubscribeStream<'a> {
    pub(crate) fn new(stream: InputStream<'a>) -> Self {
        Self { stream }
    }
}

impl<'a> fmt::Debug for SubscribeStream<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscribeStream").finish()
    }
}

impl<'a> Stream for SubscribeStream<'a> {
    type Item = Result<SubscribeUpdate, ReceiveError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut me = self.project();
        Poll::Ready(match ready!(Pin::new(&mut me.stream).poll_next(cx)) {
            Some(Ok(slice)) => Some(SubscribeUpdate::decode(slice).map_err(Into::into)),
            Some(Err(error)) => Some(Err(error)),
            None => None,
        })
    }
}

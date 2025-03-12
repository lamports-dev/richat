pub mod grpc;
pub mod quic;
pub mod tcp;

use {
    futures::stream::BoxStream,
    richat_proto::richat::RichatFilter,
    solana_sdk::clock::Slot,
    std::{
        future::Future,
        io::{self, IoSlice},
        pin::Pin,
        sync::Arc,
        task::{ready, Context, Poll},
    },
    thiserror::Error,
    tokio::io::AsyncWrite,
};

pub type RecvItem = Arc<Vec<u8>>;

pub type RecvStream = BoxStream<'static, Result<RecvItem, RecvError>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RecvError {
    #[error("channel lagged")]
    Lagged,
    #[error("channel closed")]
    Closed,
}

#[derive(Debug, Error)]
pub enum SubscribeError {
    #[error("channel is not initialized yet")]
    NotInitialized,
    #[error("only available from slot {first_available}")]
    SlotNotAvailable { first_available: Slot },
}

pub trait Subscribe {
    fn subscribe(
        &self,
        replay_from_slot: Option<Slot>,
        filter: Option<RichatFilter>,
    ) -> Result<RecvStream, SubscribeError>;
}

#[derive(Debug)]
pub struct WriteVectored<'a, W: ?Sized> {
    writer: &'a mut W,
    buffers: &'a mut [IoSlice<'a>],
    offset: usize,
}

impl<'a, W> WriteVectored<'a, W> {
    pub fn new(writer: &'a mut W, buffers: &'a mut [IoSlice<'a>]) -> Self {
        Self {
            writer,
            buffers,
            offset: 0,
        }
    }
}

impl<W> Future for WriteVectored<'_, W>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = unsafe { self.get_unchecked_mut() };
        while me.offset < me.buffers.len() {
            let bufs = &me.buffers[me.offset..];
            let mut n = ready!(Pin::new(&mut *me.writer).poll_write_vectored(cx, bufs))?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }

            while n > 0 {
                if n >= me.buffers[me.offset].len() {
                    n -= me.buffers[me.offset].len();
                    me.offset += 1;
                    continue;
                }

                me.buffers[me.offset].advance(n);
                n = 0;
            }
        }
        Poll::Ready(Ok(()))
    }
}

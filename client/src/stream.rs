use {
    crate::error::ReceiveError,
    futures::{
        ready,
        stream::{BoxStream, Stream},
    },
    pin_project_lite::pin_project,
    prost::Message,
    std::{
        borrow::Cow,
        fmt,
        pin::Pin,
        task::{Context, Poll},
    },
    yellowstone_grpc_proto::geyser::SubscribeUpdate,
};

type InputStream<'a> = BoxStream<'a, Result<(u64, Cow<'a, [u8]>), ReceiveError>>;

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
            Some(Ok((_msg_id, slice))) => {
                Some(SubscribeUpdate::decode(slice.as_ref()).map_err(Into::into))
            }
            Some(Err(error)) => Some(Err(error)),
            None => None,
        })
    }
}

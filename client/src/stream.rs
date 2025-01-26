use {
    crate::error::ReceiveError,
    futures::stream::{BoxStream, Stream},
    pin_project_lite::pin_project,
    prost::Message,
    richat_proto::geyser::SubscribeUpdate,
    std::{
        fmt,
        pin::Pin,
        task::{ready, Context, Poll},
    },
};

type InputStream = BoxStream<'static, Result<Vec<u8>, ReceiveError>>;

pin_project! {
    pub struct SubscribeStream {
        stream: InputStream,
    }
}

impl SubscribeStream {
    pub(crate) fn new(stream: InputStream) -> Self {
        Self { stream }
    }
}

impl fmt::Debug for SubscribeStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscribeStream").finish()
    }
}

impl Stream for SubscribeStream {
    type Item = Result<SubscribeUpdate, ReceiveError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut me = self.project();
        Poll::Ready(match ready!(Pin::new(&mut me.stream).poll_next(cx)) {
            Some(Ok(slice)) => Some(SubscribeUpdate::decode(slice.as_slice()).map_err(Into::into)),
            Some(Err(error)) => Some(Err(error)),
            None => None,
        })
    }
}

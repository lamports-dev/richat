use {
    crate::error::ReceiveError,
    futures::{
        future::{BoxFuture, FutureExt, TryFutureExt},
        ready,
        stream::{BoxStream, Stream},
    },
    pin_project_lite::pin_project,
    prost::Message,
    std::{
        borrow::Cow,
        collections::HashMap,
        fmt,
        pin::Pin,
        task::{Context, Poll},
    },
    tokio::task::JoinSet,
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

pin_project! {
    pub struct SubscribeStreamAsyncPar<'a, D> {
        stream: InputStream<'a>,
        decode: D,
        set: JoinSet<Result<(u64, SubscribeUpdate), ReceiveError>>,
        msg_id: u64,
        messages: HashMap<u64, SubscribeUpdate>,
        max_backlog: usize,
    }
}

impl<'a, D> SubscribeStreamAsyncPar<'a, D> {
    pub(crate) fn new(stream: InputStream<'a>, decode: D, max_backlog: usize) -> Self {
        Self {
            stream,
            decode,
            set: JoinSet::new(),
            msg_id: 0,
            messages: HashMap::new(),
            max_backlog,
        }
    }
}

impl<'a, F> fmt::Debug for SubscribeStreamAsyncPar<'a, F> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SubscribeStreamAsyncPar").finish()
    }
}

impl<'a, F> Stream for SubscribeStreamAsyncPar<'a, F>
where
    F: Fn(Vec<u8>) -> BoxFuture<'static, Result<SubscribeUpdate, prost::DecodeError>>,
{
    type Item = Result<SubscribeUpdate, ReceiveError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut me = self.project();

        if let Some(message) = me.messages.remove(me.msg_id) {
            *me.msg_id += 1;
            return Poll::Ready(Some(Ok(message)));
        }

        loop {
            if *me.max_backlog < me.set.len() + me.messages.len() {
                match Pin::new(&mut me.stream).poll_next(cx) {
                    Poll::Ready(Some(Ok((msg_id, slice)))) => {
                        me.set.spawn(
                            (me.decode)(slice.to_vec())
                                .map_ok(move |msg| (msg_id, msg))
                                .map_err(Into::into)
                                .boxed(),
                        );
                    }
                    Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                    Poll::Ready(None) => return Poll::Ready(None),
                    Poll::Pending => {}
                }
            }

            match ready!(me.set.poll_join_next(cx)) {
                Some(Ok(Ok((msg_id, msg)))) => {
                    if *me.msg_id == msg_id {
                        *me.msg_id += 1;
                        return Poll::Ready(Some(Ok(msg)));
                    } else {
                        me.messages.insert(msg_id, msg);
                    }
                }
                Some(Ok(Err(error))) => return Poll::Ready(Some(Err(error))),
                Some(Err(error)) => return Poll::Ready(Some(Err(error.into()))),
                None => return Poll::Ready(None),
            }
        }
    }
}

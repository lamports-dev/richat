use {
    prost::{DecodeError, Message},
    richat_shared::transports::quic::{
        QuicSubscribeClose, QuicSubscribeCloseError, QuicSubscribeResponse,
        QuicSubscribeResponseError,
    },
    std::io,
    thiserror::Error,
    tokio::io::{AsyncRead, AsyncReadExt},
};

#[derive(Debug, Error)]
pub enum SubscribeError {
    #[error("failed to send/recv data: {0}")]
    Io(#[from] io::Error),
    #[error("failed to decode response: {0}")]
    Decode(#[from] DecodeError),
    #[error("unknown subscribe response error: {0}")]
    Unknown(i32),
    #[error("recv stream should be greater than zero")]
    ZeroRecvStreams,
    #[error("exceed max number of recv streams: {0}")]
    ExceedRecvStreams(u32),
    #[error("stream not initialized yet")]
    NotInitialized,
    #[error("replay from slot is not available, lowest available: {0}")]
    ReplayFromSlotNotAvailable(u64),
}

impl SubscribeError {
    pub(crate) async fn parse_quic_response<R: AsyncRead + Unpin>(
        recv: &mut R,
    ) -> Result<(), Self> {
        let size = recv.read_u64().await?;
        let mut buf = vec![0; size as usize];
        recv.read_exact(buf.as_mut_slice()).await?;

        let response = QuicSubscribeResponse::decode(buf.as_slice())?;
        if let Some(error) = response.error {
            Err(match QuicSubscribeResponseError::try_from(error) {
                Ok(QuicSubscribeResponseError::ZeroRecvStreams) => SubscribeError::ZeroRecvStreams,
                Ok(QuicSubscribeResponseError::ExceedRecvStreams) => {
                    SubscribeError::ExceedRecvStreams(response.max_recv_streams())
                }
                Ok(QuicSubscribeResponseError::NotInitialized) => SubscribeError::NotInitialized,
                Ok(QuicSubscribeResponseError::SlotNotAvailable) => {
                    SubscribeError::ReplayFromSlotNotAvailable(response.first_available_slot())
                }
                Err(_error) => SubscribeError::Unknown(error),
            })
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Error)]
pub enum ReceiveError {
    #[error("failed to recv data: {0}")]
    Io(#[from] io::Error),
    #[error("failed to decode response: {0}")]
    Decode(#[from] DecodeError),
    #[error("unknown close error: {0}")]
    Unknown(i32),
    #[error("stream lagged")]
    Lagged,
    #[error("internal geyser stream is closed")]
    Closed,
}

impl From<QuicSubscribeClose> for ReceiveError {
    fn from(close: QuicSubscribeClose) -> Self {
        match QuicSubscribeCloseError::try_from(close.error) {
            Ok(QuicSubscribeCloseError::Lagged) => Self::Lagged,
            Ok(QuicSubscribeCloseError::Closed) => Self::Closed,
            Err(_error) => Self::Unknown(close.error),
        }
    }
}

impl ReceiveError {
    pub fn is_eof(&self) -> bool {
        if let Self::Io(error) = self {
            error.kind() == io::ErrorKind::UnexpectedEof
        } else {
            false
        }
    }
}

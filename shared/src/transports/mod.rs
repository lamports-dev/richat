pub mod grpc;
pub mod quic;
pub mod tcp;

use {futures::stream::BoxStream, solana_sdk::clock::Slot, std::sync::Arc, thiserror::Error};

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
    fn subscribe(&self, replay_from_slot: Option<Slot>) -> Result<RecvStream, SubscribeError>;
}

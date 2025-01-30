use {
    crate::{channel::Messages, pubsub::solana::SubscribeConfig},
    tokio::sync::{mpsc, oneshot},
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ClientRequest {
    Subscribe {
        client_id: u64,
        config: SubscribeConfig,
        tx: oneshot::Sender<u64>,
    },
    Unsubscribe {
        client_id: u64,
        subscription_id: u64,
        tx: oneshot::Sender<bool>,
    },
    Remove {
        client_id: u64,
    },
}

pub fn subscriptions_worker(
    messages: Messages,
    clients_rx: mpsc::Receiver<ClientRequest>,
) -> anyhow::Result<()> {
    Ok(())
}

use {
    crate::channel::ParsedMessage,
    futures::future::TryFutureExt,
    richat_proto::geyser::CommitmentLevel as CommitmentLevelProto,
    solana_sdk::clock::{Slot, MAX_PROCESSING_AGE},
    std::{collections::HashMap, future::Future, sync::Arc},
    tokio::sync::{mpsc, oneshot},
    tonic::Status,
};

#[derive(Debug, Default, Clone)]
pub struct BlockMeta {
    pub slot: Slot,
    pub blockhash: Arc<String>,
    pub block_height: Slot,

    processed: bool, // flag, means that we received block meta message
    confirmed: bool,
    finalized: bool,
}

#[derive(Debug, Default)]
struct BlockStatus {
    last_valid_block_height: Slot,

    processed: bool, // flag, means that we received block meta message
    confirmed: bool,
    finalized: bool,
}

#[derive(Debug, Clone)]
pub struct BlockMetaStorage {
    messages_tx: mpsc::UnboundedSender<ParsedMessage>,
    requests_tx: mpsc::Sender<Request>,
}

impl BlockMetaStorage {
    pub fn new(request_queue_size: usize) -> (Self, impl Future<Output = anyhow::Result<()>>) {
        let (messages_tx, messages_rx) = mpsc::unbounded_channel();
        let (requests_tx, requests_rx) = mpsc::channel(request_queue_size);

        let me = Self {
            messages_tx,
            requests_tx,
        };
        let fut = tokio::spawn(Self::work(messages_rx, requests_rx)).map_err(anyhow::Error::new);

        (me, fut)
    }

    async fn work(
        mut messages_rx: mpsc::UnboundedReceiver<ParsedMessage>,
        mut requests_rx: mpsc::Receiver<Request>,
    ) {
        let mut blocks = HashMap::<Slot, BlockMeta>::new();
        let mut blockhashes = HashMap::<Arc<String>, BlockStatus>::new();
        let mut processed = 0;
        let mut confirmed = 0;
        let mut finalized = 0;

        loop {
            tokio::select! {
                biased;
                message = messages_rx.recv() => match message {
                    Some(ParsedMessage::Slot(msg)) => {
                        let slot = msg.slot();
                        let commitment = msg.commitment();
                        if commitment == CommitmentLevelProto::Confirmed {
                            let entry = blocks.entry(slot).or_default();
                            entry.confirmed = true;
                            blockhashes.entry(Arc::clone(&entry.blockhash)).or_default().confirmed = true;
                            confirmed = slot;
                        } else if commitment == CommitmentLevelProto::Finalized {
                            let entry = blocks.entry(slot).or_default();
                            entry.finalized = true;
                            blockhashes.entry(Arc::clone(&entry.blockhash)).or_default().finalized = true;
                            finalized = slot;

                            // cleanup
                            blockhashes.retain(|_blockhash, bentry| bentry.last_valid_block_height < entry.block_height);
                            blocks.retain(|bslot, _block| *bslot >= slot);
                        }
                    }
                    Some(ParsedMessage::BlockMeta(msg)) => {
                        let slot = msg.slot();
                        let entry = blocks.entry(slot).or_default();
                        entry.slot = slot;
                        entry.blockhash = Arc::new(msg.blockhash().to_owned());
                        entry.block_height = msg.block_height().unwrap_or_default();
                        entry.processed = true;
                        let bentry = blockhashes.entry(Arc::clone(&entry.blockhash)).or_default();
                        bentry.last_valid_block_height = entry.block_height + MAX_PROCESSING_AGE as u64;
                        bentry.processed = true;
                        processed = processed.max(slot);
                    }
                    Some(_) => {}
                    None => break,
                },
                request = requests_rx.recv() => {
                    match request {
                        Some(Request::GetBlock(tx, commitment)) => {
                            let block = match commitment {
                                CommitmentLevelProto::Processed => Some(processed),
                                CommitmentLevelProto::Confirmed => Some(confirmed),
                                CommitmentLevelProto::Finalized => Some(finalized),
                                _ => None
                            }.and_then(|slot| blocks.get(&slot).cloned());
                            let _ = tx.send(block);
                        }
                        Some(Request::IsBlockhashValid(tx, blockhash, commitment)) => {
                            let block = match commitment {
                                CommitmentLevelProto::Processed => Some(processed),
                                CommitmentLevelProto::Confirmed => Some(confirmed),
                                CommitmentLevelProto::Finalized => Some(finalized),
                                _ => None
                            }.and_then(|slot| blocks.get(&slot).cloned());
                            let value = if let (Some(block), Some(entry)) = (block, blockhashes.get(&blockhash)) {
                                let valid = block.block_height < entry.last_valid_block_height;
                                Some((valid, block.slot))
                            } else {
                                None
                            };
                            let _ = tx.send(value);
                        }
                        None => break,
                    }
                }
            };
        }
    }

    pub fn push(&self, message: ParsedMessage) {
        let _ = self.messages_tx.send(message);
    }

    async fn send_request<T>(
        &self,
        request: Request,
        rx: oneshot::Receiver<Option<T>>,
    ) -> tonic::Result<T> {
        if self.requests_tx.try_send(request).is_err() {
            return Err(tonic::Status::resource_exhausted("queue channel is full"));
        }

        match rx.await {
            Ok(Some(block)) => Ok(block),
            Ok(None) => Err(Status::aborted("failed to get result")),
            Err(_) => Err(Status::aborted("failed to wait response")),
        }
    }

    pub async fn get_block(&self, commitment: CommitmentLevelProto) -> tonic::Result<BlockMeta> {
        let (tx, rx) = oneshot::channel();
        let request = Request::GetBlock(tx, commitment);
        self.send_request(request, rx).await
    }

    pub async fn is_blockhash_valid(
        &self,
        blockhash: String,
        commitment: CommitmentLevelProto,
    ) -> tonic::Result<(bool, Slot)> {
        let (tx, rx) = oneshot::channel();
        let request = Request::IsBlockhashValid(tx, blockhash, commitment);
        self.send_request(request, rx).await
    }
}

enum Request {
    GetBlock(oneshot::Sender<Option<BlockMeta>>, CommitmentLevelProto),
    IsBlockhashValid(
        oneshot::Sender<Option<(bool, Slot)>>,
        String,
        CommitmentLevelProto,
    ),
}

use {
    crate::{
        config::{ConfigChannelInner, ConfigChannelSource},
        metrics,
    },
    futures::stream::{BoxStream, StreamExt},
    richat_client::error::ReceiveError,
    richat_filter::message::{
        Message, MessageAccount, MessageBlock, MessageBlockMeta, MessageEntry, MessageParseError,
        MessageParserEncoding, MessageSlot, MessageTransaction,
    },
    richat_proto::{geyser::CommitmentLevel as CommitmentLevelProto, richat::GrpcSubscribeRequest},
    solana_sdk::{clock::Slot, commitment_config::CommitmentLevel, pubkey::Pubkey},
    std::{
        collections::{BTreeMap, HashMap},
        fmt, mem,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, RwLock, RwLockReadGuard, RwLockWriteGuard,
        },
    },
    thiserror::Error,
    tracing::error,
};

#[derive(Debug, Clone)]
pub enum ParsedMessage {
    Slot(Arc<MessageSlot>),
    Account(Arc<MessageAccount>),
    Transaction(Arc<MessageTransaction>),
    Entry(Arc<MessageEntry>),
    BlockMeta(Arc<MessageBlockMeta>),
    Block(Arc<MessageBlock>),
}

impl From<Message> for ParsedMessage {
    fn from(message: Message) -> Self {
        match message {
            Message::Slot(msg) => Self::Slot(Arc::new(msg)),
            Message::Account(msg) => Self::Account(Arc::new(msg)),
            Message::Transaction(msg) => Self::Transaction(Arc::new(msg)),
            Message::Entry(msg) => Self::Entry(Arc::new(msg)),
            Message::BlockMeta(msg) => Self::BlockMeta(Arc::new(msg)),
            Message::Block(msg) => Self::Block(Arc::new(msg)),
        }
    }
}

impl ParsedMessage {
    pub fn slot(&self) -> Slot {
        match self {
            Self::Slot(msg) => msg.slot(),
            Self::Account(msg) => msg.slot(),
            Self::Transaction(msg) => msg.slot(),
            Self::Entry(msg) => msg.slot(),
            Self::BlockMeta(msg) => msg.slot(),
            Self::Block(msg) => msg.slot(),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Slot(msg) => msg.size(),
            Self::Account(msg) => msg.size(),
            Self::Transaction(msg) => msg.size(),
            Self::Entry(msg) => msg.size(),
            Self::BlockMeta(msg) => msg.size(),
            Self::Block(msg) => msg.size(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Messages {
    shared: Arc<Shared>,
    max_messages: usize,
    max_slots: usize,
    max_bytes: usize,
    parser: MessageParserEncoding,
}

impl Messages {
    pub fn new(config: ConfigChannelInner) -> Self {
        let max_messages = config.max_messages.next_power_of_two();
        let mut buffer = Vec::with_capacity(max_messages);
        for i in 0..max_messages {
            buffer.push(RwLock::new(Item {
                pos: i as u64,
                slot: 0,
                data: None,
            }));
        }

        let shared = Arc::new(Shared {
            tail: AtomicU64::new(max_messages as u64),
            mask: (max_messages - 1) as u64,
            buffer: buffer.into_boxed_slice(),
        });

        Self {
            shared,
            max_messages,
            max_slots: config.max_slots,
            max_bytes: config.max_bytes,
            parser: config.parser,
        }
    }

    pub fn to_sender(&self) -> Sender {
        Sender {
            shared: Arc::clone(&self.shared),
            head: self.max_messages as u64,
            tail: self.max_messages as u64,
            slots: BTreeMap::new(),
            slots_max: self.max_slots,
            bytes_total: 0,
            bytes_max: self.max_bytes,
            parser: self.parser,
        }
    }

    pub fn to_receiver(&self) -> Receiver {
        Receiver {
            shared: Arc::clone(&self.shared),
        }
    }

    pub fn get_current_tail(&self, commitment: CommitmentLevel) -> u64 {
        self.shared.tail.load(Ordering::Relaxed)
    }

    pub async fn subscribe_source(
        config: ConfigChannelSource,
    ) -> anyhow::Result<BoxStream<'static, Result<Vec<u8>, ReceiveError>>> {
        Ok(match config {
            ConfigChannelSource::Quic(config) => {
                config.connect().await?.subscribe(None, None).await?.boxed()
            }
            ConfigChannelSource::Tcp(config) => {
                config.connect().await?.subscribe(None, None).await?.boxed()
            }
            ConfigChannelSource::Grpc(config) => config
                .connect()
                .await?
                .subscribe_richat_once(GrpcSubscribeRequest {
                    replay_from_slot: None,
                    filter: None,
                })
                .await?
                .boxed(),
        })
    }
}

#[derive(Debug)]
pub struct Sender {
    shared: Arc<Shared>,
    head: u64,
    tail: u64,
    slots: BTreeMap<Slot, SlotInfo>,
    slots_max: usize,
    bytes_total: usize,
    bytes_max: usize,
    parser: MessageParserEncoding,
}

impl Sender {
    pub fn push(&mut self, buffer: Vec<u8>) -> Result<(), MessageParseError> {
        let message: ParsedMessage = Message::parse(buffer, self.parser)?.into();
        let slot = message.slot();

        // get or create slot info
        let message_block = self
            .slots
            .entry(slot)
            .or_insert_with(|| SlotInfo::new(slot, self.tail))
            .get_block_message(&message);

        // drop extra messages by extra slots
        while self.slots.len() > self.slots_max {
            let (slot, slot_info) = self
                .slots
                .pop_first()
                .expect("nothing to remove to keep slots under limit #1");

            // remove everything up to beginning of removed slot (messages from geyser are not ordered)
            while self.head < slot_info.head {
                assert!(
                    self.head < self.tail,
                    "head overflow tail on remove process by slots limit #1"
                );

                let idx = self.shared.get_idx(self.head);
                let mut item = self.shared.buffer_idx_write(idx);
                let Some(message) = item.data.take() else {
                    panic!("nothing to remove to keep slots under limit #2")
                };

                self.head = self.head.wrapping_add(1);
                self.slots.remove(&item.slot);
                self.bytes_total -= message.size();
            }

            // remove messages while slot is same
            loop {
                assert!(
                    self.head < self.tail,
                    "head overflow tail on remove process by slots limit #2"
                );

                let idx = self.shared.get_idx(self.head);
                let mut item = self.shared.buffer_idx_write(idx);
                if slot != item.slot {
                    break;
                }
                let Some(message) = item.data.take() else {
                    panic!("nothing to remove to keep slots under limit #3")
                };

                self.head = self.head.wrapping_add(1);
                self.bytes_total -= message.size();
            }
        }

        // drop extra messages by max bytes
        self.bytes_total += message.size();
        while self.bytes_total > self.bytes_max {
            assert!(
                self.head < self.tail,
                "head overflow tail on remove process by bytes limit"
            );

            let idx = self.shared.get_idx(self.head);
            let mut item = self.shared.buffer_idx_write(idx);
            let Some(message) = item.data.take() else {
                panic!("nothing to remove to keep bytes under limit")
            };

            self.head = self.head.wrapping_add(1);
            self.slots.remove(&item.slot);
            self.bytes_total -= message.size();
        }

        for message in [Some(message), message_block].into_iter().flatten() {
            // push messages
            let pos = self.tail;
            self.tail = self.tail.wrapping_add(1);
            let idx = self.shared.get_idx(pos);
            let mut item = self.shared.buffer_idx_write(idx);
            if let Some(message) = item.data.take() {
                self.head = self.head.wrapping_add(1);
                self.slots.remove(&item.slot);
                self.bytes_total -= message.size();
            }
            item.pos = pos;
            item.slot = slot;
            item.data = Some(message);
            drop(item);

            // store new position for receivers
            self.shared.tail.store(pos, Ordering::Relaxed);
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum RecvError {
    #[error("channel lagged")]
    Lagged,
}

#[derive(Debug)]
pub struct Receiver {
    shared: Arc<Shared>,
}

impl Receiver {
    pub fn try_recv(
        &mut self,
        head: u64,
        commitment: CommitmentLevel,
    ) -> Result<Option<ParsedMessage>, RecvError> {
        let tail = self.shared.tail.load(Ordering::Relaxed);
        if head < tail {
            let idx = self.shared.get_idx(head);
            let item = self.shared.buffer_idx_read(idx);
            if item.pos != head {
                return Err(RecvError::Lagged);
            }

            return item.data.clone().ok_or(RecvError::Lagged).map(Some);
        }

        Ok(None)
    }
}

struct Shared {
    tail: AtomicU64,
    mask: u64,
    buffer: Box<[RwLock<Item>]>,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared").field("mask", &self.mask).finish()
    }
}

impl Shared {
    #[inline]
    const fn get_idx(&self, pos: u64) -> usize {
        (pos & self.mask) as usize
    }

    #[inline]
    fn buffer_idx_read(&self, idx: usize) -> RwLockReadGuard<'_, Item> {
        match self.buffer[idx].read() {
            Ok(guard) => guard,
            Err(p_err) => p_err.into_inner(),
        }
    }

    #[inline]
    fn buffer_idx_write(&self, idx: usize) -> RwLockWriteGuard<'_, Item> {
        match self.buffer[idx].write() {
            Ok(guard) => guard,
            Err(p_err) => p_err.into_inner(),
        }
    }
}

#[derive(Debug, Default)]
struct SlotInfo {
    slot: Slot,
    head: u64,
    block_created: bool,
    failed: bool,
    landed: bool,
    accounts: Vec<Option<Arc<MessageAccount>>>,
    accounts_dedup: HashMap<Pubkey, (u64, usize)>,
    transactions: Vec<Arc<MessageTransaction>>,
    entries: Vec<Arc<MessageEntry>>,
    block_meta: Option<Arc<MessageBlockMeta>>,
}

impl Drop for SlotInfo {
    fn drop(&mut self) {
        if !self.block_created && !self.failed && self.landed {
            let mut reasons = vec![];
            if let Some(block_meta) = &self.block_meta {
                if block_meta.executed_transaction_count() as usize != self.transactions.len() {
                    reasons.push(metrics::BlockMessageFailedReason::MismatchTransactions);
                }
                if block_meta.entries_count() as usize != self.entries.len() {
                    reasons.push(metrics::BlockMessageFailedReason::MismatchEntries);
                }
            } else {
                reasons.push(metrics::BlockMessageFailedReason::MissedBlockMeta);
            }

            metrics::block_message_failed_inc(self.slot, &reasons);
        }
    }
}

impl SlotInfo {
    fn new(slot: Slot, head: u64) -> Self {
        Self {
            slot,
            head,
            block_created: false,
            failed: false,
            landed: false,
            accounts: Vec::new(),
            accounts_dedup: HashMap::new(),
            transactions: Vec::new(),
            entries: Vec::new(),
            block_meta: None,
        }
    }

    fn get_block_message(&mut self, message: &ParsedMessage) -> Option<ParsedMessage> {
        // mark as landed
        if let ParsedMessage::Slot(message) = message {
            if matches!(
                message.commitment(),
                CommitmentLevelProto::Confirmed | CommitmentLevelProto::Finalized
            ) {
                self.landed = true;
            }
        }

        // report error if block already created
        if self.block_created {
            if !self.failed {
                self.failed = true;
                let mut reasons = vec![];
                match message {
                    ParsedMessage::Slot(_) => {}
                    ParsedMessage::Account(_) => {
                        reasons.push(metrics::BlockMessageFailedReason::ExtraAccount);
                    }
                    ParsedMessage::Transaction(_) => {
                        reasons.push(metrics::BlockMessageFailedReason::ExtraTransaction);
                    }
                    ParsedMessage::Entry(_) => {
                        reasons.push(metrics::BlockMessageFailedReason::ExtraEntry);
                    }
                    ParsedMessage::BlockMeta(_) => {
                        reasons.push(metrics::BlockMessageFailedReason::ExtraBlockMeta);
                    }
                    ParsedMessage::Block(_) => {}
                }
                metrics::block_message_failed_inc(self.slot, &reasons);
            }
            return None;
        }

        // store message
        match message {
            ParsedMessage::Account(message) => {
                let idx_new = self.accounts.len();
                self.accounts.push(Some(Arc::clone(message)));

                let pubkey = message.pubkey();
                let write_version = message.write_version();
                if let Some(entry) = self.accounts_dedup.get_mut(pubkey) {
                    if entry.0 < write_version {
                        self.accounts[entry.1] = None;
                        *entry = (write_version, idx_new);
                    }
                } else {
                    self.accounts_dedup
                        .insert(*pubkey, (write_version, idx_new));
                }
            }
            ParsedMessage::Slot(_message) => {}
            ParsedMessage::Transaction(message) => self.transactions.push(Arc::clone(message)),
            ParsedMessage::Entry(message) => self.entries.push(Arc::clone(message)),
            ParsedMessage::BlockMeta(message) => {
                self.block_meta = Some(Arc::clone(message));
            }
            ParsedMessage::Block(_message) => unreachable!(),
        }

        //  attempt to create Block
        if let Some(block_meta) = &self.block_meta {
            if block_meta.executed_transaction_count() as usize == self.transactions.len()
                && block_meta.entries_count() as usize == self.entries.len()
            {
                self.block_created = true;
                return Some(ParsedMessage::Block(Arc::new(
                    Message::unchecked_create_block(
                        self.accounts.drain(..).flatten().collect(),
                        mem::take(&mut self.transactions),
                        mem::take(&mut self.entries),
                        Arc::clone(block_meta),
                        block_meta.created_at(),
                    ),
                )));
            }
        }

        None
    }
}

#[derive(Debug)]
struct Item {
    pos: u64,
    slot: Slot,
    data: Option<ParsedMessage>,
}

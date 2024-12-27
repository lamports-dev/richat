pub mod message;
pub mod source;

use {
    crate::{
        channel::message::{
            Message, MessageAccount, MessageBlock, MessageBlockMeta, MessageEntry,
            MessageTransaction,
        },
        config::ConfigChannelInner,
        metrics,
    },
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    std::{
        collections::{BTreeMap, HashMap},
        fmt, mem,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, RwLock, RwLockReadGuard, RwLockWriteGuard,
        },
    },
    thiserror::Error,
    tracing::{debug, error},
    yellowstone_grpc_proto::geyser::CommitmentLevel,
};

#[derive(Debug, Clone)]
pub struct Messages {
    shared: Arc<Shared>,
    max_messages: usize,
    max_slots: usize,
    max_bytes: usize,
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
        }
    }

    pub fn to_sender(self) -> Sender {
        Sender {
            shared: self.shared,
            head: self.max_messages as u64,
            tail: self.max_messages as u64,
            slots: BTreeMap::new(),
            slots_max: self.max_slots,
            bytes_total: 0,
            bytes_max: self.max_bytes,
        }
    }

    pub fn subscribe(&self) -> Receiver {
        Receiver {
            shared: Arc::clone(&self.shared),
            head: self.shared.tail.load(Ordering::Relaxed),
        }
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
}

impl Sender {
    pub fn push(&mut self, buffer: Vec<u8>) -> Result<(), message::MessageError> {
        let message = Message::decode(buffer)?;
        let slot = message.get_slot();

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
            // update metrics
            if let Message::Slot(message) = &message {
                metrics::channel_slot_set(message);
                if message.commitment == CommitmentLevel::Processed {
                    debug!(
                        "new processed {slot} / {} messages / {} slots / {} bytes",
                        self.tail - self.head,
                        self.slots.len(),
                        self.bytes_total
                    );

                    metrics::channel_messages_set((self.tail - self.head) as usize);
                    metrics::channel_slots_set(self.slots.len());
                    metrics::channel_bytes_set(self.bytes_total);
                }
            }

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
    head: u64,
}

impl Receiver {
    pub fn try_recv(&mut self) -> Result<Option<Message>, RecvError> {
        let tail = self.shared.tail.load(Ordering::Relaxed);
        if self.head < tail {
            self.head = self.head.wrapping_add(1);

            let idx = self.shared.get_idx(self.head);
            let item = self.shared.buffer_idx_read(idx);
            if item.pos != self.head {
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

#[derive(Debug)]
struct SlotInfo {
    slot: Slot,
    head: u64,
    accounts: Vec<Option<MessageAccount>>,
    accounts_dedup: HashMap<Pubkey, usize>,
    transactions: Vec<MessageTransaction>,
    entries: Vec<MessageEntry>,
    block_meta: Option<MessageBlockMeta>,
    confirmed: bool,
    block_created: bool,
}

impl Drop for SlotInfo {
    fn drop(&mut self) {
        if self.confirmed && !self.block_created {
            let mut reasons = vec![];
            if let Some(block_meta) = &self.block_meta {
                if block_meta.executed_transaction_count as usize != self.transactions.len() {
                    reasons.push(metrics::BlockMessageFailedReason::TransactionsMismatch);
                }
                if block_meta.entry_count as usize != self.entries.len() {
                    reasons.push(metrics::BlockMessageFailedReason::EntriesMismatch);
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
            accounts: Vec::new(),
            accounts_dedup: HashMap::new(),
            transactions: Vec::new(),
            entries: Vec::new(),
            block_meta: None,
            confirmed: false,
            block_created: false,
        }
    }

    fn get_block_message(&mut self, message: &Message) -> Option<Message> {
        if self.block_created {
            if let Message::Slot(message) = message {
                metrics::block_message_failed_inc(
                    message.slot,
                    &[metrics::BlockMessageFailedReason::MissedAccountUpdate],
                );
                return None;
            }
        }

        match message {
            Message::Account(message) => {
                let idx_new = self.accounts.len();
                self.accounts.push(Some(message.clone()));

                if let Some(idx) = self.accounts_dedup.get_mut(&message.pubkey) {
                    self.accounts[*idx] = None;
                    *idx = idx_new;
                } else {
                    self.accounts_dedup.insert(message.pubkey, idx_new);
                }
            }
            Message::Slot(message) => {
                if message.commitment == CommitmentLevel::Confirmed {
                    self.confirmed = true;
                }
            }
            Message::Transaction(message) => self.transactions.push(message.clone()),
            Message::Entry(message) => self.entries.push(message.clone()),
            Message::BlockMeta(message) => {
                self.block_meta = Some(message.clone());
            }
            Message::Block(_message) => unreachable!(),
        }

        if let Some(block_meta) = &self.block_meta {
            if block_meta.executed_transaction_count as usize == self.transactions.len()
                && block_meta.entry_count as usize == self.entries.len()
            {
                self.block_created = true;
                return Some(Message::Block(MessageBlock::new(
                    block_meta,
                    self.accounts.drain(..).flatten().collect(),
                    mem::take(&mut self.transactions),
                    mem::take(&mut self.entries),
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
    data: Option<Message>,
}

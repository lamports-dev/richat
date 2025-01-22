use {
    crate::{
        config::{ConfigChannelInner, ConfigChannelSource},
        metrics,
    },
    futures::stream::{BoxStream, StreamExt},
    richat_client::error::ReceiveError,
    richat_filter::message::{
        Message, MessageAccount, MessageBlock, MessageBlockMeta, MessageEntry, MessageParseError,
        MessageParserEncoding, MessageRef, MessageSlot, MessageTransaction,
    },
    richat_proto::{geyser::CommitmentLevel as CommitmentLevelProto, richat::GrpcSubscribeRequest},
    richat_shared::transports::RecvError,
    solana_nohash_hasher::NoHashHasher,
    solana_sdk::{clock::Slot, commitment_config::CommitmentLevel, pubkey::Pubkey},
    std::{
        collections::{BTreeMap, HashMap},
        fmt,
        hash::BuildHasherDefault,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
        },
    },
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

impl<'a> From<&'a ParsedMessage> for MessageRef<'a> {
    fn from(message: &'a ParsedMessage) -> Self {
        match message {
            ParsedMessage::Slot(msg) => Self::Slot(msg.as_ref()),
            ParsedMessage::Account(msg) => Self::Account(msg.as_ref()),
            ParsedMessage::Transaction(msg) => Self::Transaction(msg.as_ref()),
            ParsedMessage::Entry(msg) => Self::Entry(msg.as_ref()),
            ParsedMessage::BlockMeta(msg) => Self::BlockMeta(msg.as_ref()),
            ParsedMessage::Block(msg) => Self::Block(msg.as_ref()),
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

    fn get_account(&self) -> Option<Arc<MessageAccount>> {
        if let Self::Account(msg) = self {
            Some(Arc::clone(msg))
        } else {
            None
        }
    }

    fn get_transaction(&self) -> Option<Arc<MessageTransaction>> {
        if let Self::Transaction(msg) = self {
            Some(Arc::clone(msg))
        } else {
            None
        }
    }

    fn get_entry(&self) -> Option<Arc<MessageEntry>> {
        if let Self::Entry(msg) = self {
            Some(Arc::clone(msg))
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct Messages {
    grpc: bool,
    shared_processed: Arc<Shared>,
    shared_confirmed: Arc<Shared>,
    shared_finalized: Arc<Shared>,
    max_messages: usize,
    max_slots: usize,
    max_bytes: usize,
    parser: MessageParserEncoding,
}

impl Messages {
    pub fn new(config: ConfigChannelInner, grpc: bool) -> Self {
        let max_messages = config.max_messages.next_power_of_two();
        Self {
            grpc,
            shared_processed: Arc::new(Shared::new(max_messages)),
            shared_confirmed: Arc::new(Shared::new(
                grpc.then_some(max_messages).unwrap_or_default(),
            )),
            shared_finalized: Arc::new(Shared::new(
                grpc.then_some(max_messages).unwrap_or_default(),
            )),
            max_messages,
            max_slots: config.max_slots,
            max_bytes: config.max_bytes,
            parser: config.parser,
        }
    }

    pub fn to_sender(&self) -> Sender {
        Sender {
            parser: self.parser,
            slots_max: self.max_slots,
            bytes_max: self.max_bytes,
            slots: BTreeMap::new(),
            grpc: self.grpc,
            processed: SenderShared::new(&self.shared_processed, self.max_messages),
            confirmed: SenderShared::new(&self.shared_confirmed, self.max_messages),
            finalized: SenderShared::new(&self.shared_finalized, self.max_messages),
        }
    }

    pub fn to_receiver(&self) -> ReceiverSync {
        ReceiverSync {
            shared_processed: Arc::clone(&self.shared_processed),
            shared_confirmed: Arc::clone(&self.shared_confirmed),
            shared_finalized: Arc::clone(&self.shared_finalized),
        }
    }

    pub fn get_current_tail(
        &self,
        commitment: CommitmentLevel,
        replay_from_slot: Option<Slot>,
    ) -> Option<u64> {
        let shared = match commitment {
            CommitmentLevel::Processed => &self.shared_processed,
            CommitmentLevel::Confirmed => &self.shared_confirmed,
            CommitmentLevel::Finalized => &self.shared_finalized,
        };

        if let Some(replay_from_slot) = replay_from_slot {
            shared
                .slots_lock()
                .get(&replay_from_slot)
                .map(|obj| obj.head)
        } else {
            Some(shared.tail.load(Ordering::Relaxed))
        }
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
                .subscribe_richat(GrpcSubscribeRequest {
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
    parser: MessageParserEncoding,
    slots_max: usize,
    bytes_max: usize,
    slots: BTreeMap<Slot, SlotInfo>,
    grpc: bool,
    processed: SenderShared,
    confirmed: SenderShared,
    finalized: SenderShared,
}

impl Sender {
    pub fn push(&mut self, buffer: Vec<u8>) -> Result<(), MessageParseError> {
        let message: ParsedMessage = Message::parse(buffer, self.parser)?.into();
        let slot = message.slot();

        // get or create slot info
        let message_block = self
            .slots
            .entry(slot)
            .or_insert_with(|| SlotInfo::new(slot))
            .get_block_message(&message);

        // push messages
        for message in [Some(message), message_block].into_iter().flatten() {
            // push messages to confirmed / finalized
            if self.grpc {
                if let ParsedMessage::Slot(msg) = &message {
                    self.confirmed.push(slot, message.clone());
                    self.finalized.push(slot, message.clone());

                    if let Some(sender_shared) = match msg.commitment() {
                        CommitmentLevelProto::Confirmed => Some(&mut self.confirmed),
                        CommitmentLevelProto::Finalized => Some(&mut self.finalized),
                        _ => None,
                    } {
                        if let Some(slot_info) = self.slots.get(&slot) {
                            for message in slot_info.get_messages() {
                                sender_shared.push(slot, message);
                            }
                            sender_shared.try_clear(self.bytes_max, self.slots_max);
                        }
                    }

                    // remove slot info
                    if msg.commitment() == CommitmentLevelProto::Finalized {
                        loop {
                            match self.slots.keys().next().copied() {
                                Some(slot_min) if slot_min <= slot => {
                                    self.slots.remove(&slot_min);
                                }
                                _ => break,
                            }
                        }
                    }
                }
            }

            // push to processed
            self.processed.push(slot, message);
        }
        self.processed.try_clear(self.bytes_max, self.slots_max);

        Ok(())
    }
}

#[derive(Debug)]
struct SenderShared {
    shared: Arc<Shared>,
    head: u64,
    tail: u64,
    bytes_total: usize,
    slots: SlotHeads,
}

impl SenderShared {
    fn new(shared: &Arc<Shared>, max_messages: usize) -> Self {
        Self {
            shared: Arc::clone(shared),
            head: max_messages as u64,
            tail: max_messages as u64,
            bytes_total: 0,
            slots: Default::default(),
        }
    }

    fn push(&mut self, slot: Slot, message: ParsedMessage) {
        // bump current tail
        let pos = self.tail;
        self.tail = self.tail.wrapping_add(1);

        // get item
        let idx = self.shared.get_idx(pos);
        let mut item = self.shared.buffer_idx_write(idx);

        // drop existed message
        if let Some(message) = item.data.take() {
            self.head = self.head.wrapping_add(1);
            self.bytes_total -= message.size();
            if self.slots.remove(&item.slot).is_some() {
                self.shared.slots_lock().remove(&item.slot);
            }
        }

        // store new message
        self.bytes_total += message.size();
        item.pos = pos;
        item.slot = slot;
        item.data = Some(message);
        drop(item);

        // store new position for receivers
        self.shared.tail.store(pos, Ordering::Relaxed);

        // update slot head info
        self.slots.entry(slot).or_insert_with(|| {
            let obj = SlotHead { head: pos };
            self.shared.slots_lock().insert(slot, obj);
            obj
        });
    }

    fn try_clear(&mut self, bytes_max: usize, slots_max: usize) {
        // drop messages by extra bytes
        while self.bytes_total > bytes_max {
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
            self.bytes_total -= message.size();
            if self.slots.remove(&item.slot).is_some() {
                self.shared.slots_lock().remove(&item.slot);
            }
        }

        // drop messages by extra slots
        while self.slots.len() > slots_max {
            let slot_min = self
                .slots
                .keys()
                .min()
                .copied()
                .expect("nothing to remove to keep slots under limit #1");
            let slot_info = self
                .slots
                .remove(&slot_min)
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
                self.bytes_total -= message.size();
                if self.slots.remove(&item.slot).is_some() {
                    self.shared.slots_lock().remove(&item.slot);
                }
            }

            // remove messages while slot is same
            loop {
                assert!(
                    self.head < self.tail,
                    "head overflow tail on remove process by slots limit #2"
                );

                let idx = self.shared.get_idx(self.head);
                let mut item = self.shared.buffer_idx_write(idx);
                if slot_min != item.slot {
                    break;
                }
                let Some(message) = item.data.take() else {
                    panic!("nothing to remove to keep slots under limit #3")
                };

                self.head = self.head.wrapping_add(1);
                self.bytes_total -= message.size();
            }
        }
    }
}

#[derive(Debug)]
pub struct ReceiverSync {
    shared_processed: Arc<Shared>,
    shared_confirmed: Arc<Shared>,
    shared_finalized: Arc<Shared>,
}

impl ReceiverSync {
    pub fn try_recv(
        &self,
        commitment: CommitmentLevel,
        head: u64,
    ) -> Result<Option<ParsedMessage>, RecvError> {
        let shared = match commitment {
            CommitmentLevel::Processed => &self.shared_processed,
            CommitmentLevel::Confirmed => &self.shared_confirmed,
            CommitmentLevel::Finalized => &self.shared_finalized,
        };

        let tail = shared.tail.load(Ordering::Relaxed);
        if head < tail {
            let idx = shared.get_idx(head);
            let item = shared.buffer_idx_read(idx);
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
    slots: Mutex<SlotHeads>,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared").field("mask", &self.mask).finish()
    }
}

impl Shared {
    fn new(max_messages: usize) -> Self {
        let mut buffer = Vec::with_capacity(max_messages);
        for i in 0..max_messages {
            buffer.push(RwLock::new(Item {
                pos: i as u64,
                slot: 0,
                data: None,
            }));
        }

        Self {
            tail: AtomicU64::new(max_messages as u64),
            mask: (max_messages - 1) as u64,
            buffer: buffer.into_boxed_slice(),
            slots: Mutex::default(),
        }
    }

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

    #[inline]
    fn slots_lock(&self) -> MutexGuard<'_, SlotHeads> {
        match self.slots.lock() {
            Ok(lock) => lock,
            Err(p_err) => p_err.into_inner(),
        }
    }
}

type SlotHeads = HashMap<Slot, SlotHead, BuildHasherDefault<NoHashHasher<Slot>>>;

#[derive(Debug, Clone, Copy)]
struct SlotHead {
    head: u64,
}

#[derive(Debug, Default)]
struct SlotInfo {
    slot: Slot,
    block_created: bool,
    failed: bool,
    landed: bool,
    messages: Vec<Option<ParsedMessage>>,
    accounts_dedup: HashMap<Pubkey, (u64, usize)>,
    transactions_count: usize,
    entries_count: usize,
    block_meta: Option<Arc<MessageBlockMeta>>,
}

impl Drop for SlotInfo {
    fn drop(&mut self) {
        if !self.block_created && !self.failed && self.landed {
            let mut reasons = vec![];
            if let Some(block_meta) = &self.block_meta {
                if block_meta.executed_transaction_count() as usize != self.transactions_count {
                    reasons.push(metrics::BlockMessageFailedReason::MismatchTransactions);
                }
                if block_meta.entries_count() as usize != self.entries_count {
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
    fn new(slot: Slot) -> Self {
        Self {
            slot,
            block_created: false,
            failed: false,
            landed: false,
            messages: Vec::with_capacity(16_384),
            accounts_dedup: HashMap::new(),
            transactions_count: 0,
            entries_count: 0,
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
                let idx_new = self.messages.len();
                let item = ParsedMessage::Account(Arc::clone(message));
                self.messages.push(Some(item));

                let pubkey = message.pubkey();
                let write_version = message.write_version();
                if let Some(entry) = self.accounts_dedup.get_mut(pubkey) {
                    if entry.0 < write_version {
                        self.messages[entry.1] = None;
                        *entry = (write_version, idx_new);
                    }
                } else {
                    self.accounts_dedup
                        .insert(*pubkey, (write_version, idx_new));
                }
            }
            ParsedMessage::Slot(_message) => {}
            ParsedMessage::Transaction(message) => {
                let item = ParsedMessage::Transaction(Arc::clone(message));
                self.messages.push(Some(item));
                self.transactions_count += 1;
            }
            ParsedMessage::Entry(message) => {
                let item = ParsedMessage::Entry(Arc::clone(message));
                self.messages.push(Some(item));
                self.entries_count += 1
            }
            ParsedMessage::BlockMeta(message) => {
                let item = ParsedMessage::BlockMeta(Arc::clone(message));
                self.messages.push(Some(item));
                self.block_meta = Some(Arc::clone(message));
            }
            ParsedMessage::Block(_message) => unreachable!(),
        }

        //  attempt to create Block
        if let Some(block_meta) = &self.block_meta {
            if block_meta.executed_transaction_count() as usize == self.transactions_count
                && block_meta.entries_count() as usize == self.entries_count
            {
                self.block_created = true;

                let accounts = self
                    .messages
                    .iter()
                    .filter_map(|item| item.as_ref().and_then(|item| item.get_account()))
                    .collect();
                let transactions = self
                    .messages
                    .iter()
                    .filter_map(|item| item.as_ref().and_then(|item| item.get_transaction()))
                    .collect();
                let entries = self
                    .messages
                    .iter()
                    .filter_map(|item| item.as_ref().and_then(|item| item.get_entry()))
                    .collect();
                let message = ParsedMessage::Block(Arc::new(Message::unchecked_create_block(
                    accounts,
                    transactions,
                    entries,
                    Arc::clone(block_meta),
                    block_meta.created_at(),
                )));
                self.messages.push(Some(message.clone()));

                return Some(message);
            }
        }

        None
    }

    fn get_messages(&self) -> impl Iterator<Item = ParsedMessage> + '_ {
        self.messages
            .iter()
            .filter_map(|item| item.as_ref().cloned())
    }
}

#[derive(Debug)]
struct Item {
    pos: u64,
    slot: Slot,
    data: Option<ParsedMessage>,
}

pub mod binary {
    use std::{
        fmt,
        sync::{Arc, Mutex, MutexGuard},
    };

    pub fn channel(max_messages: usize) -> (Sender, Receiver) {
        let shared = Arc::new(Shared::new(max_messages));
        (
            Sender {
                shared: Arc::clone(&shared),
                tail: 0,
            },
            Receiver { shared, head: 0 },
        )
    }

    #[derive(Debug)]
    pub struct Sender {
        shared: Arc<Shared>,
        tail: u64,
    }

    impl Sender {
        pub fn send(&mut self, item: Vec<u8>) -> Option<Vec<u8>> {
            let mut locked = self.shared.get_locked(self.tail);
            if locked.is_some() {
                Some(item)
            } else {
                *locked = Some(item);
                self.tail += 1;
                None
            }
        }
    }

    #[derive(Debug)]
    pub struct Receiver {
        shared: Arc<Shared>,
        head: u64,
    }

    impl Receiver {
        pub fn recv(&mut self) -> Option<Vec<u8>> {
            let item = self.shared.get_locked(self.head).take();
            if item.is_some() {
                self.head += 1;
            }
            item
        }
    }

    struct Shared {
        mask: u64,
        buffer: Box<[Mutex<Option<Vec<u8>>>]>,
    }

    impl fmt::Debug for Shared {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("Shared").field("mask", &self.mask).finish()
        }
    }

    impl Shared {
        fn new(max_messages: usize) -> Self {
            Self {
                mask: (max_messages - 1) as u64,
                buffer: (0..max_messages)
                    .map(|_| Mutex::new(None))
                    .collect::<Vec<_>>()
                    .into_boxed_slice(),
            }
        }

        #[inline]
        const fn get_idx(&self, pos: u64) -> usize {
            (pos & self.mask) as usize
        }

        #[inline]
        fn get_locked(&self, pos: u64) -> MutexGuard<'_, Option<Vec<u8>>> {
            match self.buffer[self.get_idx(pos)].lock() {
                Ok(lock) => lock,
                Err(p_err) => p_err.into_inner(),
            }
        }
    }
}

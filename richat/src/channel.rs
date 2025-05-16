use {
    crate::{config::ConfigChannelInner, metrics},
    ::metrics::gauge,
    futures::stream::{Stream, StreamExt},
    richat_filter::{
        filter::FilteredUpdate,
        message::{
            Message, MessageAccount, MessageBlock, MessageBlockMeta, MessageEntry, MessageRef,
            MessageSlot, MessageTransaction,
        },
    },
    richat_proto::{geyser::SlotStatus, richat::RichatFilter},
    richat_shared::transports::{RecvError, RecvItem, RecvStream, Subscribe, SubscribeError},
    smallvec::SmallVec,
    solana_sdk::{
        clock::Slot, commitment_config::CommitmentLevel, pubkey::Pubkey, signature::Signature,
    },
    std::{
        collections::{hash_map::Entry as HashMapEntry, BTreeMap, HashMap, HashSet},
        fmt,
        pin::Pin,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
        },
        task::{Context, Poll, Waker},
    },
    tracing::debug,
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
    shared_processed: Arc<Shared>,
    shared_confirmed: Option<Arc<Shared>>,
    shared_finalized: Option<Arc<Shared>>,
    max_messages: usize,
    max_bytes: usize,
}

impl Messages {
    pub fn new(config: ConfigChannelInner, richat: bool, grpc: bool, pubsub: bool) -> Self {
        let max_messages = config.max_messages.next_power_of_two();
        Self {
            shared_processed: Arc::new(Shared::new(max_messages, richat)),
            shared_confirmed: (grpc || pubsub).then(|| Arc::new(Shared::new(max_messages, richat))),
            shared_finalized: (grpc || pubsub).then(|| Arc::new(Shared::new(max_messages, richat))),
            max_messages,
            max_bytes: config.max_bytes,
        }
    }

    pub fn to_sender(&self) -> Sender {
        Sender {
            slots: BTreeMap::new(),
            dedup: BTreeMap::new(),
            finalized_slot: 0,
            processed: SenderShared::new(&self.shared_processed, self.max_messages, self.max_bytes),
            confirmed: self
                .shared_confirmed
                .as_ref()
                .map(|shared| SenderShared::new(shared, self.max_messages, self.max_bytes)),
            finalized: self
                .shared_finalized
                .as_ref()
                .map(|shared| SenderShared::new(shared, self.max_messages, self.max_bytes)),
            slot_confirmed: 0,
            slot_finalized: 0,
        }
    }

    pub fn to_receiver(&self) -> ReceiverSync {
        ReceiverSync {
            shared_processed: Arc::clone(&self.shared_processed),
            shared_confirmed: self.shared_confirmed.as_ref().map(Arc::clone),
            shared_finalized: self.shared_finalized.as_ref().map(Arc::clone),
        }
    }

    pub fn get_current_tail(
        &self,
        commitment: CommitmentLevel,
        replay_from_slot: Option<Slot>,
    ) -> Option<u64> {
        let shared = (match commitment {
            CommitmentLevel::Processed => Some(&self.shared_processed),
            CommitmentLevel::Confirmed => self.shared_confirmed.as_ref(),
            CommitmentLevel::Finalized => self.shared_finalized.as_ref(),
        })?;

        if let Some(replay_from_slot) = replay_from_slot {
            shared
                .slots_lock()
                .get(&replay_from_slot)
                .map(|obj| obj.head)
        } else {
            Some(shared.tail.load(Ordering::Relaxed))
        }
    }

    pub fn get_first_available_slot(&self) -> Option<Slot> {
        let slot = self
            .shared_processed
            .slots_lock()
            .first_key_value()
            .map(|(slot, _head)| *slot)?;
        if let Some(shared) = self.shared_confirmed.as_ref() {
            if !shared.slots_lock().contains_key(&slot) {
                return None;
            }
        }
        if let Some(shared) = self.shared_finalized.as_ref() {
            if !shared.slots_lock().contains_key(&slot) {
                return None;
            }
        }
        Some(slot)
    }
}

impl Subscribe for Messages {
    fn subscribe(
        &self,
        replay_from_slot: Option<Slot>,
        filter: Option<RichatFilter>,
    ) -> Result<RecvStream, SubscribeError> {
        let head = if let Some(replay_from_slot) = replay_from_slot {
            let state = self.shared_processed.slots_lock();
            match state.get(&replay_from_slot) {
                Some(obj) => obj.head,
                None => {
                    return Err(match state.keys().min().copied() {
                        Some(first_available) => {
                            SubscribeError::SlotNotAvailable { first_available }
                        }
                        None => SubscribeError::NotInitialized,
                    })
                }
            }
        } else {
            self.shared_processed.tail.load(Ordering::Relaxed)
        };

        let filter = filter.unwrap_or_default();

        Ok(ReceiverAsync {
            shared: Arc::clone(&self.shared_processed),
            head,
            finished: false,
            enable_notifications_accounts: !filter.disable_accounts,
            enable_notifications_transactions: !filter.disable_transactions,
            enable_notifications_entries: !filter.disable_entries,
        }
        .boxed())
    }
}

#[derive(Debug)]
pub struct Sender {
    slots: BTreeMap<Slot, SlotInfo>,
    dedup: BTreeMap<Slot, DedupInfo>,
    finalized_slot: Slot,
    processed: SenderShared,
    confirmed: Option<SenderShared>,
    finalized: Option<SenderShared>,
    slot_confirmed: Slot,
    slot_finalized: Slot,
}

impl Sender {
    pub fn push(&mut self, message: Message, index_info: Option<(usize, usize)>) {
        let slot = message.slot();

        // get or create slot info
        let mut messages = SmallVec::<[ParsedMessage; 16]>::new();
        let mut dedup_info = if let Some((index, streams_total)) = index_info {
            // return if we already processed and removed dedup for finalized slots
            if slot <= self.finalized_slot {
                return;
            }

            // remove outdated info
            if let Message::Slot(msg) = &message {
                if msg.status() == SlotStatus::SlotFinalized {
                    self.finalized_slot = slot;
                    loop {
                        match self.dedup.keys().next().copied() {
                            Some(slot_min) if slot_min < self.finalized_slot => {
                                self.dedup.remove(&slot_min);
                            }
                            _ => break,
                        }
                    }
                }
            }

            // dedup info
            let dedup = self
                .dedup
                .entry(slot)
                .or_insert_with(|| DedupInfo::new(streams_total));

            match message {
                Message::Slot(msg) => {
                    let index = msg.status() as i32 as usize;
                    if !dedup.slots[index] {
                        dedup.slots[index] = true;
                        messages.push(ParsedMessage::Slot(Arc::new(msg)));
                    }
                }
                Message::Account(mut msg) => {
                    if let Some(key) = DedupInfoAccountTransactionKey::try_create(&msg) {
                        if dedup.accounts_transactions.insert(key) {
                            match dedup.transactions.entry(key.signature) {
                                HashMapEntry::Occupied(mut entry) => match entry.get_mut() {
                                    DedupInfoTransactionIndex::Index(index) => {
                                        update_write_version(&mut msg, *index as u64);
                                        messages.push(ParsedMessage::Account(Arc::new(msg)));
                                    }
                                    DedupInfoTransactionIndex::Accounts(vec) => {
                                        vec.push(msg);
                                    }
                                },
                                HashMapEntry::Vacant(entry) => {
                                    entry.insert(DedupInfoTransactionIndex::Accounts(vec![msg]));
                                }
                            }
                        }
                    } else {
                        let msg = ParsedMessage::Account(Arc::new(msg));
                        if dedup.block_index == Some(index) {
                            messages.push(msg); // send to SlotInfo to generate error
                        } else {
                            dedup.accounts_phantom[index].push(msg);
                        }
                    }
                }
                Message::Transaction(msg) => {
                    let index = msg.index() as usize;
                    match dedup.transactions.entry(*msg.signature()) {
                        HashMapEntry::Occupied(mut entry) => {
                            let entry = entry.get_mut();
                            if let DedupInfoTransactionIndex::Accounts(vec) = entry {
                                for mut msg in vec.drain(..) {
                                    update_write_version(&mut msg, index as u64);
                                    messages.push(ParsedMessage::Account(Arc::new(msg)));
                                }

                                *entry = DedupInfoTransactionIndex::Index(index);
                                messages.push(ParsedMessage::Transaction(Arc::new(msg)));
                            }
                        }
                        HashMapEntry::Vacant(entry) => {
                            entry.insert(DedupInfoTransactionIndex::Index(index));
                            messages.push(ParsedMessage::Transaction(Arc::new(msg)));
                        }
                    }
                }
                Message::Entry(msg) => {
                    let index = msg.index() as usize;
                    if dedup.entries.len() <= index {
                        dedup.entries.resize(dedup.entries.len() * 2, false);
                    }
                    if !dedup.entries[index] {
                        dedup.entries[index] = true;
                        messages.push(ParsedMessage::Entry(Arc::new(msg)));
                    }
                }
                Message::BlockMeta(msg) => {
                    if !dedup.block_meta {
                        dedup.block_meta = true;
                        messages.push(ParsedMessage::BlockMeta(Arc::new(msg)));
                    }
                }
                Message::Block(_) => unreachable!(),
            };

            Some((index, dedup))
        } else {
            messages.push(message.into());
            None
        };

        // push messages
        for message in messages {
            let messages_with_block = self
                .slots
                .entry(slot)
                .or_insert_with(|| SlotInfo::new(slot))
                .get_messages_with_block(
                    &message,
                    dedup_info
                        .as_mut()
                        .map(|(index, dedup)| &mut dedup.accounts_phantom[*index]),
                );
            if let (Some((index, dedup)), Some(_)) = (dedup_info.as_mut(), &messages_with_block) {
                dedup.block_index = Some(*index);
            }

            for message in MessagesWithBlockIter::new(message, messages_with_block) {
                // push messages to confirmed / finalized
                if let ParsedMessage::Slot(msg) = &message {
                    // update metrics
                    if let Some(commitment) = match msg.status() {
                        SlotStatus::SlotProcessed => Some("processed"),
                        SlotStatus::SlotConfirmed => Some("confirmed"),
                        SlotStatus::SlotFinalized => Some("finalized"),
                        _ => None,
                    } {
                        gauge!(metrics::CHANNEL_SLOT, "commitment" => commitment)
                            .set(msg.slot() as f64)
                    }
                    if msg.status() == SlotStatus::SlotProcessed {
                        let processed_slots_len = self.processed.shared.slots_lock().len();
                        debug!(
                            "new processed {slot} / {} messages / {} slots / {} bytes",
                            self.processed.tail - self.processed.head,
                            processed_slots_len,
                            self.processed.bytes_total
                        );

                        gauge!(metrics::CHANNEL_MESSAGES_TOTAL)
                            .set((self.processed.tail - self.processed.head) as f64);
                        gauge!(metrics::CHANNEL_SLOTS_TOTAL).set(processed_slots_len as f64);
                        gauge!(metrics::CHANNEL_BYTES_TOTAL).set(self.processed.bytes_total as f64);
                    }

                    if msg.status() == SlotStatus::SlotConfirmed {
                        self.slot_confirmed = slot;
                        if let Some(shared) = self.confirmed.as_mut() {
                            if let Some(slot_info) = self.slots.get(&slot) {
                                for message in slot_info.get_messages_cloned() {
                                    shared.push(slot, message);
                                }
                            }
                        }
                    }

                    if msg.status() == SlotStatus::SlotFinalized {
                        self.slot_finalized = slot;
                        if let Some(shared) = self.finalized.as_mut() {
                            if let Some(mut slot_info) = self.slots.remove(&slot) {
                                for message in slot_info.get_messages_owned() {
                                    shared.push(slot, message);
                                }
                            }
                        }
                    }

                    // remove slot info
                    if msg.status() == SlotStatus::SlotFinalized {
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

                // push to confirmed and finalized (if we received SlotStatus or message after it)
                if slot <= self.slot_confirmed {
                    if let Some(shared) = self.confirmed.as_mut() {
                        shared.push(slot, message.clone());
                    }
                }
                if slot <= self.slot_finalized {
                    if let Some(shared) = self.finalized.as_mut() {
                        shared.push(slot, message.clone());
                    }
                }

                // push to processed
                self.processed.push(slot, message);
            }
        }

        if let Some(mut wakers) = self.processed.shared.wakers_lock() {
            for waker in wakers.drain(..) {
                waker.wake();
            }
        }
    }
}

#[derive(Debug)]
struct SenderShared {
    shared: Arc<Shared>,
    head: u64,
    tail: u64,
    bytes_total: usize,
    bytes_max: usize,
}

impl SenderShared {
    fn new(shared: &Arc<Shared>, max_messages: usize, max_bytes: usize) -> Self {
        Self {
            shared: Arc::clone(shared),
            head: max_messages as u64,
            tail: max_messages as u64,
            bytes_total: 0,
            bytes_max: max_bytes,
        }
    }

    fn push(&mut self, slot: Slot, message: ParsedMessage) {
        let mut slots_lock = self.shared.slots_lock();
        let mut removed_max_slot = None;

        // drop messages by extra bytes
        self.bytes_total += message.size();
        while self.bytes_total >= self.bytes_max {
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
            removed_max_slot = Some(match removed_max_slot {
                Some(slot) => item.slot.max(slot),
                None => item.slot,
            });
        }

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
            removed_max_slot = Some(match removed_max_slot {
                Some(slot) => item.slot.max(slot),
                None => item.slot,
            });
        }

        // store new message
        item.pos = pos;
        item.slot = slot;
        item.data = Some(message);
        drop(item);

        // store new position for receivers
        self.shared.tail.store(pos, Ordering::Relaxed);

        // update slot head info
        slots_lock
            .entry(slot)
            .or_insert_with(|| SlotHead { head: pos });

        // remove not-complete slots
        if let Some(remove_upto) = removed_max_slot {
            let mut slot = match slots_lock.first_key_value() {
                Some((slot, _)) => *slot,
                None => return,
            };
            while slot <= remove_upto {
                slots_lock.remove(&slot);
                slot = match slots_lock.first_key_value() {
                    Some((slot, _)) => *slot,
                    None => return,
                };
            }
        }
    }
}

#[derive(Debug)]
pub struct ReceiverAsync {
    shared: Arc<Shared>,
    head: u64,
    finished: bool,
    enable_notifications_accounts: bool,
    enable_notifications_transactions: bool,
    enable_notifications_entries: bool,
}

impl ReceiverAsync {
    fn recv_ref(&mut self, waker: &Waker) -> Result<Option<RecvItem>, RecvError> {
        let tail = self.shared.tail.load(Ordering::Relaxed);
        while self.head <= tail {
            let idx = self.shared.get_idx(self.head);
            let item = self.shared.buffer_idx_read(idx);
            if item.pos != self.head {
                return Err(RecvError::Lagged);
            }
            self.head = self.head.wrapping_add(1);

            let item = item.data.as_ref().ok_or(RecvError::Lagged)?;
            match item {
                ParsedMessage::Account(_) if !self.enable_notifications_accounts => continue,
                ParsedMessage::Transaction(_) if !self.enable_notifications_transactions => {
                    continue
                }
                ParsedMessage::Entry(_) if !self.enable_notifications_entries => continue,
                _ => {}
            }

            let data = FilteredUpdate {
                filters: SmallVec::new_const(),
                filtered_update: MessageRef::from(item).into(),
            }
            .encode();
            return Ok(Some(Arc::new(data)));
        }

        if let Some(mut wakers) = self.shared.wakers_lock() {
            wakers.push(waker.clone());
        }
        Ok(None)
    }
}

impl Stream for ReceiverAsync {
    type Item = Result<RecvItem, RecvError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        if me.finished {
            return Poll::Ready(None);
        }

        match me.recv_ref(cx.waker()) {
            Ok(Some(value)) => Poll::Ready(Some(Ok(value))),
            Ok(None) => Poll::Pending,
            Err(error) => {
                me.finished = true;
                Poll::Ready(Some(Err(error)))
            }
        }
    }
}

#[derive(Debug)]
pub struct ReceiverSync {
    shared_processed: Arc<Shared>,
    shared_confirmed: Option<Arc<Shared>>,
    shared_finalized: Option<Arc<Shared>>,
}

impl ReceiverSync {
    pub fn try_recv(
        &self,
        commitment: CommitmentLevel,
        head: u64,
    ) -> Result<Option<ParsedMessage>, RecvError> {
        let Some(shared) = (match commitment {
            CommitmentLevel::Processed => Some(&self.shared_processed),
            CommitmentLevel::Confirmed => self.shared_confirmed.as_ref(),
            CommitmentLevel::Finalized => self.shared_finalized.as_ref(),
        }) else {
            return Err(RecvError::Closed);
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
    slots: Mutex<BTreeMap<Slot, SlotHead>>,
    wakers: Option<Mutex<Vec<Waker>>>,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared").field("mask", &self.mask).finish()
    }
}

impl Shared {
    fn new(max_messages: usize, richat: bool) -> Self {
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
            wakers: richat.then_some(Mutex::default()),
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
    fn slots_lock(&self) -> MutexGuard<'_, BTreeMap<Slot, SlotHead>> {
        match self.slots.lock() {
            Ok(lock) => lock,
            Err(p_err) => p_err.into_inner(),
        }
    }

    #[inline]
    fn wakers_lock(&self) -> Option<MutexGuard<'_, Vec<Waker>>> {
        self.wakers.as_ref().map(|wakers| match wakers.lock() {
            Ok(lock) => lock,
            Err(p_err) => p_err.into_inner(),
        })
    }
}

#[derive(Debug)]
struct Item {
    pos: u64,
    slot: Slot,
    data: Option<ParsedMessage>,
}

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
                let executed_transaction_count = block_meta.executed_transaction_count() as usize;
                if executed_transaction_count != self.transactions_count {
                    reasons.push(metrics::BlockMessageFailedReason::MismatchTransactions {
                        actual: self.transactions_count,
                        expected: executed_transaction_count,
                    });
                }
                let entries_count = block_meta.entries_count() as usize;
                if entries_count != self.entries_count {
                    reasons.push(metrics::BlockMessageFailedReason::MismatchEntries {
                        actual: self.entries_count,
                        expected: entries_count,
                    });
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

    fn get_messages_with_block(
        &mut self,
        message: &ParsedMessage,
        deduped_accounts: Option<&mut SmallVec<[ParsedMessage; 8]>>,
    ) -> Option<MessagesWithBlock> {
        // mark as landed
        if let ParsedMessage::Slot(message) = message {
            if matches!(
                message.status(),
                SlotStatus::SlotConfirmed | SlotStatus::SlotFinalized
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

                if let Some(messages) = &deduped_accounts {
                    for message in messages.iter() {
                        self.messages.push(Some(message.clone()));
                    }
                }

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
                let block = ParsedMessage::Block(Arc::new(Message::unchecked_create_block(
                    accounts,
                    transactions,
                    entries,
                    Arc::clone(block_meta),
                    block_meta.created_at(),
                )));
                self.messages.push(Some(block.clone()));

                return Some(MessagesWithBlock {
                    accounts: deduped_accounts.map(std::mem::take).unwrap_or_default(),
                    block,
                });
            }
        }

        None
    }

    fn get_messages_cloned(&self) -> impl Iterator<Item = ParsedMessage> + '_ {
        self.messages
            .iter()
            .filter_map(|item| item.as_ref().cloned())
    }

    fn get_messages_owned(&mut self) -> impl Iterator<Item = ParsedMessage> + '_ {
        self.messages.drain(..).flatten()
    }
}

#[derive(Debug)]
struct MessagesWithBlock {
    accounts: SmallVec<[ParsedMessage; 8]>,
    block: ParsedMessage,
}

#[derive(Debug)]
struct MessagesWithBlockIter {
    accounts: Option<smallvec::IntoIter<[ParsedMessage; 8]>>,
    message: Option<ParsedMessage>,
    block: Option<ParsedMessage>,
}

impl Iterator for MessagesWithBlockIter {
    type Item = ParsedMessage;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(accounts) = self.accounts.as_mut() {
            if let Some(message) = accounts.next() {
                return Some(message);
            }
        }
        self.message.take().or_else(|| self.block.take())
    }
}

impl MessagesWithBlockIter {
    fn new(message: ParsedMessage, messages: Option<MessagesWithBlock>) -> Self {
        let (accounts, block) = match messages {
            Some(MessagesWithBlock { accounts, block }) => {
                (Some(accounts.into_iter()), Some(block))
            }
            None => (None, None),
        };

        Self {
            accounts,
            message: Some(message),
            block,
        }
    }
}

#[derive(Debug)]
struct DedupInfo {
    slots: [bool; 7],
    accounts_phantom: Vec<SmallVec<[ParsedMessage; 8]>>,
    accounts_transactions: HashSet<DedupInfoAccountTransactionKey>,
    transactions: HashMap<Signature, DedupInfoTransactionIndex>,
    entries: Vec<bool>,
    block_meta: bool,
    block_index: Option<usize>,
}

impl DedupInfo {
    fn new(streams_total: usize) -> Self {
        Self {
            slots: [false; 7],
            accounts_phantom: std::iter::repeat_with(SmallVec::new)
                .take(streams_total)
                .collect(),
            accounts_transactions: HashSet::with_capacity(8_192),
            transactions: HashMap::with_capacity(8_192),
            entries: std::iter::repeat(false).take(256).collect(),
            block_meta: false,
            block_index: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DedupInfoAccountTransactionKey {
    signature: Signature,
    pubkey: Pubkey,
}

impl DedupInfoAccountTransactionKey {
    fn try_create(msg: &MessageAccount) -> Option<Self> {
        let signature = match msg {
            MessageAccount::Limited {
                txn_signature_offset,
                buffer,
                ..
            } => txn_signature_offset.map(|offset| &buffer.as_slice()[offset..offset + 64]),
            MessageAccount::Prost { account, .. } => account.txn_signature.as_deref(),
        };

        signature.map(|signature| Self {
            signature: signature.try_into().expect("valid signature"),
            pubkey: *msg.pubkey(),
        })
    }
}

#[derive(Debug)]
enum DedupInfoTransactionIndex {
    Index(usize),
    Accounts(Vec<MessageAccount>),
}

fn update_write_version(msg: &mut MessageAccount, write_version: u64) {
    match msg {
        MessageAccount::Limited {
            data,
            write_version,
            range,
            buffer,
            ..
        } => {
            // todo: adjust buffer
        }
        MessageAccount::Prost { account, .. } => {
            account.write_version = write_version;
        }
    }
}

#[cfg(test)]
mod test {
    use {
        super::update_write_version,
        maplit::hashmap,
        richat_filter::{
            config::{ConfigFilter, ConfigFilterAccounts},
            filter::Filter,
            message::{Message, MessageAccount, MessageParserEncoding, MessageRef},
        },
        solana_sdk::commitment_config::CommitmentLevel,
    };

    static MESSAGE: &'static str = "0a0012af010aa6010a2088f1ffa3a2dfe617bdc4e3573251a322e3fcae81e5a457390e64751c00a465e210e0d54a1a2006aa09548b50476ad462f91f89a3015033264fc9abd5270020a9d142334742fb28ffffffffffffffffff013208c921f474e044612838e3e1acc2b53042405bd620fab28d3c0b78b3ead9f04d1c4d6dffeac4ffa7c679a6570b0226557c10b4c4016d937e06044b4e49d9d7916524d5dfa26297c5f638c3d11f846410bc0510e5ddaca2015a0c08e1c79ec10610ebef838601";

    fn encode_decode(msg: &MessageAccount, parser: MessageParserEncoding) -> MessageAccount {
        let filter = Filter::new(&ConfigFilter {
            accounts: hashmap! { "".to_owned() => ConfigFilterAccounts::default() },
            ..Default::default()
        });

        let message = Message::Account(msg.clone());
        let message_ref: MessageRef = (&message).into();

        let updates = filter.get_updates_ref(message_ref, CommitmentLevel::Processed);
        assert_eq!(updates.len(), 1, "unexpected number of updates");
        parse(updates[0].encode(), parser)
    }

    fn parse(data: Vec<u8>, parser: MessageParserEncoding) -> MessageAccount {
        if let Message::Account(msg) = Message::parse(data, parser).expect("valid message") {
            assert!(
                match parser {
                    MessageParserEncoding::Prost => matches!(msg, MessageAccount::Prost { .. }),
                    MessageParserEncoding::Limited => matches!(msg, MessageAccount::Limited { .. }),
                },
                "unexpected msg encoding"
            );

            msg
        } else {
            panic!("expected account message");
        }
    }

    #[test]
    fn test_limited() {
        let mut msg = parse(
            const_hex::decode(MESSAGE).expect("valid hex"),
            MessageParserEncoding::Limited,
        );
        assert_eq!(msg.write_version(), 1663633666275, "valid write version");

        update_write_version(&mut msg, 1);
        assert_eq!(msg.write_version(), 1, "dec valid write version");
        let msg2 = encode_decode(&msg, MessageParserEncoding::Limited);
        assert_eq!(msg, msg2, "write version update failed");

        update_write_version(&mut msg, u64::MAX);
        assert_eq!(msg.write_version(), u64::MAX, "inc valid write version");
        let msg2 = encode_decode(&msg, MessageParserEncoding::Limited);
        assert_eq!(msg, msg2, "write version update failed");
    }

    #[test]
    fn test_prost() {
        let mut msg = parse(
            const_hex::decode(MESSAGE).expect("valid hex"),
            MessageParserEncoding::Prost,
        );
        assert_eq!(msg.write_version(), 1663633666275, "valid write version");

        update_write_version(&mut msg, 1);
        assert_eq!(msg.write_version(), 1, "dec valid write version");
        let mut msg2 = encode_decode(&msg, MessageParserEncoding::Prost);
        if let (MessageAccount::Prost { size, .. }, MessageAccount::Prost { size: size2, .. }) =
            (&msg, &mut msg2)
        {
            *size2 = *size; // ignore size field
        }
        assert_eq!(msg, msg2, "write version update failed");

        update_write_version(&mut msg, u64::MAX);
        assert_eq!(msg.write_version(), u64::MAX, "inc valid write version");
        let mut msg2 = encode_decode(&msg, MessageParserEncoding::Prost);
        if let (MessageAccount::Prost { size, .. }, MessageAccount::Prost { size: size2, .. }) =
            (&msg, &mut msg2)
        {
            *size2 = *size; // ignore size field
        }
        assert_eq!(msg, msg2, "write version update failed");
    }
}

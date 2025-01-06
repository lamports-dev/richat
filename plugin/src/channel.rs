// Based on https://github.com/tokio-rs/tokio/blob/master/tokio/src/sync/broadcast.rs
use {
    crate::{
        config::ConfigChannel,
        metrics,
        protobuf::{ProtobufEncoder, ProtobufMessage},
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus,
    log::{debug, error},
    smallvec::SmallVec,
    solana_sdk::clock::Slot,
    std::{
        collections::BTreeMap,
        fmt,
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
        task::{Context, Poll, Waker},
    },
    thiserror::Error,
};

#[derive(Debug, Clone)]
pub struct Sender {
    shared: Arc<Shared>,
}

impl Sender {
    pub fn new(config: ConfigChannel) -> Self {
        let max_messages = config.max_messages.next_power_of_two();
        let mut buffer = Vec::with_capacity(max_messages);
        for i in 0..max_messages {
            buffer.push(RwLock::new(Item {
                pos: i as u64,
                slot: 0,
                data: None,
                closed: false,
            }));
        }

        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                head: max_messages as u64,
                tail: max_messages as u64,
                slots: BTreeMap::new(),
                slots_max: config.max_slots,
                bytes_total: 0,
                bytes_max: config.max_bytes,
                wakers: Vec::with_capacity(16),
            }),
            mask: (max_messages - 1) as u64,
            buffer: buffer.into_boxed_slice(),
        });

        Self { shared }
    }

    pub fn push(&self, message: ProtobufMessage, encoder: ProtobufEncoder) {
        // encode message
        let data = message.encode(encoder);

        // acquire state lock
        let mut state = self.shared.state_lock();

        // In March 2023 in Triton One we noticed that sometimes we do not receive
        // slots with Confirmed status, I'm not sure that this still a case but for
        // safety I added this hack
        let slot_status = if let ProtobufMessage::Slot { slot, status, .. } = &message {
            Some((*slot, *status))
        } else {
            None
        };

        let mut messages = SmallVec::<[(ProtobufMessage, Vec<u8>); 2]>::new();
        messages.push((message, data));

        if let Some((slot, status)) = slot_status {
            let mut slots = SmallVec::<[Slot; 4]>::new();
            slots.push(slot);

            while let Some((parent, Some(entry))) = slots
                .pop()
                .and_then(|slot| state.slots.get(&slot))
                .and_then(|entry| entry.parent_slot)
                .map(|parent| (parent, state.slots.get_mut(&parent)))
            {
                if (*status == SlotStatus::Confirmed && !entry.confirmed)
                    || (*status == SlotStatus::Rooted && !entry.finalized)
                {
                    slots.push(parent);

                    let message = ProtobufMessage::Slot {
                        slot: parent,
                        parent: entry.parent_slot,
                        status,
                    };
                    let data = message.encode(encoder);
                    messages.push((message, data));

                    error!("missed slot status update for {} ({:?})", parent, *status);
                    metrics::geyser_missed_slot_status_inc(status);
                }
            }
        }

        // push messages
        for (message, data) in messages.into_iter().rev() {
            self.push_msg(&mut state, message, data);
        }

        // notify receivers
        for waker in state.wakers.drain(..) {
            waker.wake();
        }
    }

    fn push_msg(&self, state: &mut MutexGuard<'_, State>, message: ProtobufMessage, data: Vec<u8>) {
        // position of the new message
        let pos = state.tail;

        // update slots info
        let slot = message.get_slot();
        let entry = state.slots.entry(slot).or_insert_with(|| SlotInfo {
            head: pos,
            parent_slot: None,
            confirmed: false,
            finalized: false,
        });
        if let ProtobufMessage::Slot { parent, status, .. } = &message {
            if let Some(parent) = parent {
                entry.parent_slot = Some(*parent);
            }
            if **status == SlotStatus::Confirmed {
                entry.confirmed = true;
            } else if **status == SlotStatus::Rooted {
                entry.finalized = true;
            }
        }

        // drop extra messages by extra slots
        while state.slots.len() > state.slots_max {
            let (slot, slot_info) = state
                .slots
                .pop_first()
                .expect("nothing to remove to keep slots under limit #1");

            // remove everything up to beginning of removed slot (messages from geyser are not ordered)
            while state.head < slot_info.head {
                assert!(
                    state.head < state.tail,
                    "head overflow tail on remove process by slots limit #1"
                );

                let idx = self.shared.get_idx(state.head);
                let mut item = self.shared.buffer_idx_write(idx);
                let Some(message) = item.data.take() else {
                    panic!("nothing to remove to keep slots under limit #2")
                };

                state.head = state.head.wrapping_add(1);
                state.remove_slots(Some(slot + 1), item.slot);
                state.bytes_total -= message.len();
            }

            // remove messages while slot is same
            loop {
                assert!(
                    state.head < state.tail,
                    "head overflow tail on remove process by slots limit #2"
                );

                let idx = self.shared.get_idx(state.head);
                let mut item = self.shared.buffer_idx_write(idx);
                if slot != item.slot {
                    break;
                }
                let Some(message) = item.data.take() else {
                    panic!("nothing to remove to keep slots under limit #3")
                };

                state.head = state.head.wrapping_add(1);
                state.bytes_total -= message.len();
            }
        }

        // drop extra messages by max bytes
        state.bytes_total += data.len();
        while state.bytes_total > state.bytes_max {
            assert!(
                state.head < state.tail,
                "head overflow tail on remove process by bytes limit"
            );

            let idx = self.shared.get_idx(state.head);
            let mut item = self.shared.buffer_idx_write(idx);
            let Some(message) = item.data.take() else {
                panic!("nothing to remove to keep bytes under limit")
            };

            state.head = state.head.wrapping_add(1);
            state.remove_slots(None, item.slot);
            state.bytes_total -= message.len();
        }

        // update tail
        state.tail = state.tail.wrapping_add(1);

        // lock and update item
        let idx = self.shared.get_idx(pos);
        let mut item = self.shared.buffer_idx_write(idx);
        if let Some(message) = item.data.take() {
            state.head = state.head.wrapping_add(1);
            state.remove_slots(None, item.slot);
            state.bytes_total -= message.len();
        }
        item.pos = pos;
        item.slot = slot;
        item.data = Some(Arc::new(data));
        drop(item);

        // update metrics
        if let ProtobufMessage::Slot { status, .. } = message {
            metrics::geyser_slot_status_set(slot, status);
            if *status == SlotStatus::Processed {
                debug!(
                    "new processed {slot} / {} messages / {} slots / {} bytes",
                    state.tail - state.head,
                    state.slots.len(),
                    state.bytes_total
                );

                metrics::channel_messages_set((state.tail - state.head) as usize);
                metrics::channel_slots_set(state.slots.len());
                metrics::channel_bytes_set(state.bytes_total);
            }
        }
    }

    pub fn subscribe(&self, replay_from_slot: Option<Slot>) -> Result<Receiver, SubscribeError> {
        let shared = Arc::clone(&self.shared);

        let state = shared.state_lock();
        let next = match replay_from_slot {
            Some(slot) => state.slots.get(&slot).map(|s| s.head).ok_or_else(|| {
                match state.slots.first_key_value() {
                    Some((key, _value)) => SubscribeError::SlotNotAvailable {
                        first_available: *key,
                    },
                    None => SubscribeError::NotInitialized,
                }
            })?,
            None => state.tail,
        };
        drop(state);

        Ok(Receiver { shared, next })
    }

    pub fn close(&self) {
        for idx in 0..self.shared.buffer.len() {
            self.shared.buffer_idx_write(idx).closed = true;
        }

        let mut state = self.shared.state_lock();
        for waker in state.wakers.drain(..) {
            waker.wake();
        }
    }
}

#[derive(Debug, Error)]
pub enum SubscribeError {
    #[error("channel is not initialized yet")]
    NotInitialized,
    #[error("only available from slot {first_available}")]
    SlotNotAvailable { first_available: Slot },
}

pub type ReceiverItem = Arc<Vec<u8>>;

#[derive(Debug)]
pub struct Receiver {
    shared: Arc<Shared>,
    next: u64,
}

impl Receiver {
    pub async fn recv(&mut self) -> Result<ReceiverItem, RecvError> {
        Recv::new(self).await
    }

    pub fn recv_ref(&mut self, waker: &Waker) -> Result<Option<ReceiverItem>, RecvError> {
        // read item with next value
        let idx = self.shared.get_idx(self.next);
        let mut item = self.shared.buffer_idx_read(idx);
        if item.closed {
            return Err(RecvError::Closed);
        }

        if item.pos != self.next {
            // release lock before attempting to acquire state
            drop(item);

            // acquire state to store waker
            let mut state = self.shared.state_lock();

            // make sure that position did not changed
            item = self.shared.buffer_idx_read(idx);
            if item.closed {
                return Err(RecvError::Closed);
            }
            if item.pos != self.next {
                return if item.pos < self.next {
                    state.wakers.push(waker.clone());
                    Ok(None)
                } else {
                    Err(RecvError::Lagged)
                };
            }
        }

        self.next = self.next.wrapping_add(1);
        item.data.clone().ok_or(RecvError::Lagged).map(Some)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RecvError {
    #[error("channel lagged")]
    Lagged,
    #[error("channel closed")]
    Closed,
}

struct Recv<'a> {
    receiver: &'a mut Receiver,
}

impl<'a> Recv<'a> {
    fn new(receiver: &'a mut Receiver) -> Self {
        Self { receiver }
    }
}

impl<'a> Future for Recv<'a> {
    type Output = Result<ReceiverItem, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        let receiver: &mut Receiver = me.receiver;

        match receiver.recv_ref(cx.waker()) {
            Ok(Some(value)) => Poll::Ready(Ok(value)),
            Ok(None) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

struct Shared {
    state: Mutex<State>,
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
    fn state_lock(&self) -> MutexGuard<'_, State> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(error) => error.into_inner(),
        }
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

struct State {
    head: u64,
    tail: u64,
    slots: BTreeMap<Slot, SlotInfo>,
    slots_max: usize,
    bytes_total: usize,
    bytes_max: usize,
    wakers: Vec<Waker>,
}

impl State {
    fn remove_slots(&mut self, first_slot: Option<Slot>, remove_upto: Slot) {
        let mut slot = match first_slot {
            Some(slot) => slot,
            None => match self.slots.first_key_value() {
                Some((slot, _)) => *slot,
                None => return,
            },
        };
        while slot <= remove_upto {
            self.slots.remove(&slot);
            slot = match self.slots.first_key_value() {
                Some((slot, _)) => *slot,
                None => return,
            };
        }
    }
}

struct SlotInfo {
    head: u64,
    parent_slot: Option<Slot>,
    confirmed: bool,
    finalized: bool,
}

struct Item {
    pos: u64,
    slot: Slot,
    data: Option<Arc<Vec<u8>>>,
    closed: bool,
}

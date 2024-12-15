// Based on https://github.com/tokio-rs/tokio/blob/master/tokio/src/sync/broadcast.rs

use {
    crate::{config::ConfigChannel, protobuf::ProtobufMessage},
    solana_sdk::clock::Slot,
    std::{
        cell::UnsafeCell,
        collections::BTreeMap,
        fmt,
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
        task::{Context, Poll, Waker},
        thread_local,
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
                pos: (i as u64).wrapping_sub(max_messages as u64),
                slot: 0,
                data: None,
            }));
        }

        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                head: 0,
                tail: 0,
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

    pub fn push(&self, slot: Slot, message: ProtobufMessage) {
        thread_local! {
            // 16MiB should be enough for any message
            // except blockinfo with rewards list (what doesn't make sense after partition reward, starts from epoch 706)
            static BUFFER: UnsafeCell<Vec<u8>> = UnsafeCell::new(Vec::with_capacity(16 * 1024 * 1024));
        }

        let message = BUFFER.with(|buffer| message.encode(unsafe { &mut *buffer.get() }));

        // acquire state lock
        let mut state = self.shared.state_lock();

        // position of the new message
        let pos = state.tail;

        // update slots info and drop extra messages by extra slots
        state.slots.entry(slot).or_insert(pos);
        while state.slots.len() > state.slots_max {
            let (slot, pos) = state
                .slots
                .pop_first()
                .expect("nothing to remove to keep slots under limit #1");

            // remove everything up to beginning of removed slot (messages from geyser are not ordered)
            while state.head < pos {
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
                state.slots.remove(&item.slot);
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
        state.bytes_total += message.len();
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
            state.slots.remove(&item.slot);
            state.bytes_total -= message.len();
        }

        // update tail
        state.tail = state.tail.wrapping_add(1);

        // lock and update item
        let idx = self.shared.get_idx(pos);
        let mut item = self.shared.buffer_idx_write(idx);
        if let Some(message) = item.data.take() {
            state.head = state.head.wrapping_add(1);
            state.slots.remove(&item.slot);
            state.bytes_total -= message.len();
        }
        item.pos = pos;
        item.slot = slot;
        item.data = Some(Arc::new(message));
        drop(item);

        // notify receivers
        for waker in state.wakers.drain(..) {
            waker.wake();
        }
    }

    pub fn subscribe(&self, replay_from_slot: Option<Slot>) -> Result<Receiver, SubscribeError> {
        let shared = Arc::clone(&self.shared);

        let state = shared.state_lock();
        let next = match replay_from_slot {
            Some(slot) => state.slots.get(&slot).copied().ok_or_else(|| {
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
}

#[derive(Debug, Error)]
pub enum SubscribeError {
    #[error("channel is not initialized yet")]
    NotInitialized,
    #[error("only available from slot {first_available}")]
    SlotNotAvailable { first_available: Slot },
}

#[derive(Debug)]
pub struct Receiver {
    shared: Arc<Shared>,
    next: u64,
}

impl Receiver {
    pub async fn recv(&mut self) -> Result<Arc<Vec<u8>>, RecvError> {
        Recv::new(self).await
    }

    fn recv_ref(&mut self, waker: &Waker) -> Result<Option<Arc<Vec<u8>>>, RecvError> {
        // read item with next value
        let idx = self.shared.get_idx(self.next);
        let mut item = self.shared.buffer_idx_read(idx);

        if item.pos != self.next {
            // release lock before attempting to acquire state
            drop(item);

            // acquire state to store waker
            let mut state = self.shared.state_lock();

            // make sure that position did not changed
            item = self.shared.buffer_idx_read(idx);
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

#[derive(Debug, Error)]
pub enum RecvError {
    #[error("channel lagged")]
    Lagged,
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
    type Output = Result<Arc<Vec<u8>>, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let receiver: &mut Receiver = unsafe {
            // Safety: Arc<Vec<u8>> is Unpin
            let me = self.get_unchecked_mut();
            me.receiver
        };

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
    slots: BTreeMap<Slot, u64>,
    slots_max: usize,
    bytes_total: usize,
    bytes_max: usize,
    wakers: Vec<Waker>,
}

struct Item {
    pos: u64,
    slot: Slot,
    data: Option<Arc<Vec<u8>>>,
}

use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Waker},
};

#[derive(Debug)]
pub struct Shutdown {
    state: Arc<Mutex<State>>,
    id: u64,
}

impl Shutdown {
    pub fn new() -> Self {
        let mut state = State {
            shutdown: false,
            map: BTreeMap::new(),
        };
        let id = state.get_next_id();
        state.map.insert(id, None);

        Self {
            state: Arc::new(Mutex::new(state)),
            id,
        }
    }

    fn state_lock(&self) -> MutexGuard<'_, State> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(error) => error.into_inner(),
        }
    }

    pub fn shutdown(&self) {
        let mut state = self.state_lock();
        state.shutdown = true;
        for value in state.map.values_mut() {
            if let Some(waker) = value.take() {
                waker.wake();
            }
        }
    }

    pub fn is_set(&self) -> bool {
        self.state_lock().shutdown
    }
}

impl Default for Shutdown {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for Shutdown {
    fn clone(&self) -> Self {
        let mut state = self.state_lock();
        let id = state.get_next_id();
        state.map.insert(id, None);

        Self {
            state: Arc::clone(&self.state),
            id,
        }
    }
}

impl Drop for Shutdown {
    fn drop(&mut self) {
        let mut state = self.state_lock();
        state.map.remove(&self.id);
    }
}

impl Future for Shutdown {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.as_ref().get_ref();
        let mut state = me.state_lock();

        if state.shutdown {
            return Poll::Ready(());
        }

        state.map.insert(self.id, Some(cx.waker().clone()));
        Poll::Pending
    }
}

#[derive(Debug)]
struct State {
    shutdown: bool,
    map: BTreeMap<u64, Option<Waker>>,
}

impl State {
    fn get_next_id(&self) -> u64 {
        for (index, key) in (0..u64::MAX).zip(self.map.keys()) {
            if index != *key {
                return index;
            }
        }
        self.map.len() as u64
    }
}

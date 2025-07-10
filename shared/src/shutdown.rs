use {
    crate::mutex_lock,
    slab::Slab,
    std::{
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex, MutexGuard},
        task::{Context, Poll, Waker},
    },
};

#[derive(Debug)]
pub struct Shutdown {
    state: Arc<Mutex<State>>,
    index: usize,
}

impl Shutdown {
    pub fn new() -> Self {
        let mut state = State {
            shutdown: false,
            wakers: Slab::with_capacity(64),
        };
        let index = state.wakers.insert(None);

        Self {
            state: Arc::new(Mutex::new(state)),
            index,
        }
    }

    fn state_lock(&self) -> MutexGuard<'_, State> {
        mutex_lock(&self.state)
    }

    pub fn shutdown(&self) {
        let mut state = self.state_lock();
        state.shutdown = true;
        for (_index, value) in state.wakers.iter_mut() {
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
        let index = state.wakers.insert(None);

        Self {
            state: Arc::clone(&self.state),
            index,
        }
    }
}

impl Drop for Shutdown {
    fn drop(&mut self) {
        let mut state = self.state_lock();
        state.wakers.remove(self.index);
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

        state.wakers[self.index] = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[derive(Debug)]
struct State {
    shutdown: bool,
    wakers: Slab<Option<Waker>>,
}

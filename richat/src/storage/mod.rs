pub mod metadata;
pub mod segments;

use {
    crate::{
        channel::{IndexLocation, ParsedMessage, SharedChannel},
        config::ConfigStorage,
        grpc::server::SubscribeClient,
        metrics::GrpcSubscribeMessage,
        storage::{
            metadata::Metadata,
            segments::{SegmentReader, WriterCommand},
        },
        util::SpawnedThreads,
    },
    ::metrics::Gauge,
    quanta::Instant,
    richat_filter::message::{MessageParserEncoding, MessageRef},
    richat_metrics::duration_to_seconds,
    richat_shared::mutex_lock,
    smallvec::SmallVec,
    solana_clock::Slot,
    solana_commitment_config::CommitmentLevel,
    std::{
        collections::{BTreeMap, VecDeque},
        sync::{Arc, Mutex, atomic::Ordering},
        thread,
        time::Duration,
    },
    tokio_util::sync::CancellationToken,
    tonic::Status,
};

/// Replay start metadata exposed to the rest of `richat` for each retained
/// slot.
#[derive(Debug, Clone, Copy)]
pub struct SlotIndexValue {
    pub finalized: bool,
    pub head: u64,
}

/// Public storage facade used by channel startup, replay bootstrap, and disk
/// replay workers.
#[derive(Debug, Clone)]
pub struct Storage {
    metadata: Metadata,
    write_tx: kanal::Sender<WriterCommand>,
    replay_queue: Arc<Mutex<ReplayQueue>>,
}

impl Storage {
    pub fn open(
        config: ConfigStorage,
        parser: MessageParserEncoding,
        shutdown: CancellationToken,
    ) -> anyhow::Result<(Self, SpawnedThreads)> {
        let replay_inflight_max = config.replay_inflight_max;
        let replay_threads = config.replay_threads;
        let replay_affinity = config.replay_affinity.clone();
        let replay_decode_per_tick = config.replay_decode_per_tick;

        let (metadata, write_tx, mut threads) = segments::open_storage(config)?;

        let storage = Self {
            metadata,
            write_tx,
            replay_queue: Arc::new(Mutex::new(ReplayQueue::new(replay_inflight_max))),
        };

        for index in 0..replay_threads {
            let th_name = format!("richatStrgRep{index:02}");
            let storage = storage.clone();
            let affinity = replay_affinity.clone();
            let shutdown = shutdown.clone();
            let jh = thread::Builder::new()
                .name(th_name.clone())
                .spawn(move || {
                    if let Some(cpus) = affinity {
                        affinity_linux::set_thread_affinity(cpus.into_iter())
                            .expect("failed to set affinity");
                    }
                    Self::spawn_replay(storage, parser, replay_decode_per_tick, shutdown)
                })?;
            threads.push((th_name, Some(jh)));
        }

        Ok((storage, threads))
    }

    fn spawn_replay(
        storage: Self,
        parser: MessageParserEncoding,
        messages_decode_per_tick: usize,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut shutdown_ts = Instant::now();
        let mut prev_request = None;
        loop {
            let Some(mut req) = ReplayQueue::pop_next(&storage.replay_queue, prev_request.take())
            else {
                if shutdown.is_cancelled() {
                    break;
                }
                thread::sleep(Duration::from_millis(1));
                continue;
            };

            let ts = Instant::now();
            if ts.duration_since(shutdown_ts) > Duration::from_millis(100) {
                shutdown_ts = ts;
                if shutdown.is_cancelled() {
                    break;
                }
            }

            let mut locked_state = req.client.state_lock();
            if locked_state.finished {
                ReplayQueue::drop_req(&storage.replay_queue);
                continue;
            }

            if let Some(error) = req.state.read_error.take() {
                drop(locked_state);
                req.client.push_error(error);
                ReplayQueue::drop_req(&storage.replay_queue);
                continue;
            }

            let IndexLocation::Storage(head) = locked_state.head else {
                ReplayQueue::drop_req(&storage.replay_queue);
                continue;
            };

            let mut current_head = *req.state.head.get_or_insert(head);
            if current_head != head {
                req.state.messages.clear();
            }

            let ts = Instant::now();
            let mut pushed = false;
            let mut messages_len = req.client.messages_len.load(Ordering::Relaxed);
            while messages_len <= req.client.messages_replay_len_max {
                let Some((index, message)) = req.state.messages.pop_front() else {
                    break;
                };

                let filter = locked_state.filter.as_ref().expect("defined filter");
                let message_ref: MessageRef = (&message).into();
                let items = filter
                    .get_updates_ref(message_ref, CommitmentLevel::Processed)
                    .iter()
                    .map(|msg| ((&msg.filtered_update).into(), msg.encode_to_vec()))
                    .collect::<SmallVec<[(GrpcSubscribeMessage, Vec<u8>); 2]>>();

                for (message, data) in items {
                    messages_len += data.len();
                    req.client.push_message(message, data);
                    pushed = true;
                }

                current_head = index;
            }

            locked_state.head = IndexLocation::Storage(current_head);
            req.state.head = Some(current_head);

            if req.state.read_finished && req.state.messages.is_empty() {
                if let Some(head) = req.messages.get_head_by_replay_index(current_head + 1) {
                    locked_state.head = IndexLocation::Memory(head);
                } else {
                    req.state.read_error = Some(Status::internal(
                        "failed to connect replay index to memory channel",
                    ));
                }

                req.metric_cpu_usage
                    .increment(duration_to_seconds(ts.elapsed()));
                drop(locked_state);
                if pushed {
                    req.client.wake();
                }
                ReplayQueue::drop_req(&storage.replay_queue);
                continue;
            }

            drop(locked_state);
            if pushed {
                req.client.wake();
            }

            if !req.state.read_finished && req.state.messages.len() < messages_decode_per_tick {
                let mut messages_decoded = 0;
                for chunk_result in storage.read_messages_from_index(current_head + 1) {
                    match chunk_result {
                        Ok(chunk) => match chunk.decode_records(parser) {
                            Ok(records) => {
                                messages_decoded += records.len();
                                req.state.messages.extend(records);
                            }
                            Err(error) => {
                                req.state.read_error = Some(Status::internal(error.to_string()));
                                break;
                            }
                        },
                        Err(error) => {
                            req.state.read_error = Some(Status::internal(error.to_string()));
                            break;
                        }
                    }
                    if messages_decoded >= messages_decode_per_tick {
                        break;
                    }
                }

                if messages_decoded < messages_decode_per_tick && req.state.read_error.is_none() {
                    req.state.read_finished = true;
                }
            }

            req.metric_cpu_usage
                .increment(duration_to_seconds(ts.elapsed()));
            prev_request = Some(req);
        }
        ReplayQueue::shutdown(&storage.replay_queue);
        Ok(())
    }

    pub fn push_message(
        &self,
        init: bool,
        slot: Slot,
        head: u64,
        index: u64,
        message: ParsedMessage,
    ) {
        let _ = self.write_tx.send(WriterCommand::PushMessage {
            init,
            slot,
            head,
            index,
            message,
        });
    }

    pub fn trim_messages(&self, slot: Slot, until: Option<u64>) {
        let _ = self
            .write_tx
            .send(WriterCommand::RemoveReplay { slot, until });
    }

    pub fn read_slots(&self) -> BTreeMap<Slot, SlotIndexValue> {
        let catalog = self.metadata.catalog();
        catalog
            .slots
            .iter()
            .map(|(slot, meta)| {
                (
                    *slot,
                    SlotIndexValue {
                        finalized: meta.finalized,
                        head: meta.first_index,
                    },
                )
            })
            .collect()
    }

    pub fn read_messages_from_index(&self, index: u64) -> SegmentReader {
        segments::read_messages_from_index(&self.metadata, index)
    }

    pub fn replay(
        &self,
        client: SubscribeClient,
        messages: Arc<SharedChannel>,
        metric_cpu_usage: Gauge,
    ) -> Result<(), &'static str> {
        ReplayQueue::push_new(
            &self.replay_queue,
            ReplayRequest {
                state: ReplayState::default(),
                client,
                messages,
                metric_cpu_usage,
            },
        )
        .map_err(|()| "replay queue is full; try again later")
    }
}

#[derive(Debug)]
struct ReplayRequest {
    state: ReplayState,
    client: SubscribeClient,
    messages: Arc<SharedChannel>,
    metric_cpu_usage: Gauge,
}

#[derive(Debug, Default)]
struct ReplayState {
    head: Option<u64>,
    messages: VecDeque<(u64, ParsedMessage)>,
    read_error: Option<Status>,
    read_finished: bool,
}

#[derive(Debug)]
struct ReplayQueue {
    capacity: usize,
    len: usize,
    requests: VecDeque<ReplayRequest>,
}

impl ReplayQueue {
    const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            len: 0,
            requests: VecDeque::new(),
        }
    }

    fn pop_next(queue: &Mutex<Self>, prev_request: Option<ReplayRequest>) -> Option<ReplayRequest> {
        let mut locked = mutex_lock(queue);
        if locked.len > 0 {
            if let Some(request) = prev_request {
                locked.requests.push_back(request);
            }
        }
        locked.requests.pop_front()
    }

    fn push_new(queue: &Mutex<Self>, request: ReplayRequest) -> Result<(), ()> {
        let mut locked = mutex_lock(queue);
        if locked.len < locked.capacity {
            locked.len += 1;
            locked.requests.push_back(request);
            Ok(())
        } else {
            Err(())
        }
    }

    fn drop_req(queue: &Mutex<Self>) {
        let mut locked = mutex_lock(queue);
        locked.len -= 1;
    }

    fn shutdown(queue: &Mutex<Self>) {
        let mut locked = mutex_lock(queue);
        locked.capacity = 0;
        locked.len = 0;
        locked.requests.clear();
    }
}

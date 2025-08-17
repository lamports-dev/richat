use {
    crate::{
        channel::{IndexLocation, ParsedMessage, SharedChannel},
        config::ConfigStorage,
        grpc::server::SubscribeClient,
        metrics::{
            GrpcSubscribeMessage, CHANNEL_STORAGE_WRITE_INDEX, CHANNEL_STORAGE_WRITE_SER_INDEX,
        },
        util::SpawnedThreads,
    },
    ::metrics::{counter, Gauge},
    anyhow::Context,
    hyper::body::Buf,
    prost::{
        bytes::BufMut,
        encoding::{decode_varint, encode_varint},
    },
    quanta::Instant,
    richat_filter::{
        filter::{FilteredUpdate, FilteredUpdateFilters},
        message::{Message, MessageParserEncoding, MessageRef},
    },
    richat_metrics::duration_to_seconds,
    richat_proto::geyser::SlotStatus,
    richat_shared::{mutex_lock, shutdown::Shutdown},
    rocksdb::{
        ColumnFamily, ColumnFamilyDescriptor, DBCompressionType, Direction, IteratorMode, Options,
        WriteBatch, DB,
    },
    smallvec::SmallVec,
    solana_sdk::{clock::Slot, commitment_config::CommitmentLevel},
    std::{
        borrow::Cow,
        collections::{BTreeMap, VecDeque},
        sync::{mpsc, Arc, Mutex},
        thread,
        time::Duration,
    },
    tonic::Status,
};

trait ColumnName {
    const NAME: &'static str;
}

#[derive(Debug)]
struct MessageIndex;

impl ColumnName for MessageIndex {
    const NAME: &'static str = "message_index";
}

impl MessageIndex {
    const fn encode(key: u64) -> [u8; 8] {
        key.to_be_bytes()
    }

    fn decode(slice: &[u8]) -> anyhow::Result<u64> {
        slice
            .try_into()
            .map(u64::from_be_bytes)
            .context("invalid slice size")
    }
}

#[derive(Debug)]
struct MessageIndexValue;

impl MessageIndexValue {
    fn encode(slot: Slot, message: FilteredUpdate, buf: &mut impl BufMut) {
        encode_varint(slot, buf);
        message.encode(buf);
    }

    fn decode(mut slice: &[u8], parser: MessageParserEncoding) -> anyhow::Result<(Slot, Message)> {
        let slot =
            decode_varint(&mut slice).context("invalid slice size, failed to decode slot")?;
        let message =
            Message::parse(Cow::Borrowed(slice), parser).context("failed to parse message")?;
        Ok((slot, message))
    }
}

#[derive(Debug)]
struct SlotIndex;

impl ColumnName for SlotIndex {
    const NAME: &'static str = "slot_index";
}

impl SlotIndex {
    const fn encode(key: Slot) -> [u8; 8] {
        key.to_be_bytes()
    }

    fn decode(slice: &[u8]) -> anyhow::Result<Slot> {
        slice
            .try_into()
            .map(Slot::from_be_bytes)
            .context("invalid slice size")
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SlotIndexValue {
    pub finalized: bool,
    pub head: u64,
}

impl SlotIndexValue {
    fn encode(finalized: bool, head: u64, buf: &mut impl BufMut) {
        buf.put_u8(if finalized { 1 } else { 0 });
        encode_varint(head, buf);
    }

    fn decode(mut slice: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            finalized: match slice.try_get_u8().context("failed to read finalized")? {
                0 => false,
                1 => true,
                value => anyhow::bail!("failed to read finalized, unknown value: {value}"),
            },
            head: decode_varint(&mut slice).context("failed to read head")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Storage {
    db: Arc<DB>,
    write_tx: mpsc::Sender<WriteRequest>,
    replay_queue: Arc<Mutex<ReplayQueue>>,
}

impl Storage {
    pub fn open(
        config: ConfigStorage,
        parser: MessageParserEncoding,
        shutdown: Shutdown,
    ) -> anyhow::Result<(Self, SpawnedThreads)> {
        let db_options = Self::get_db_options();
        let cf_descriptors = Self::cf_descriptors(config.messages_compression.into());

        let db = Arc::new(
            DB::open_cf_descriptors(&db_options, &config.path, cf_descriptors)
                .with_context(|| format!("failed to open rocksdb with path: {:?}", config.path))?,
        );

        let (ser_tx, ser_rx) = mpsc::channel();
        let (write_tx, write_rx) = mpsc::sync_channel(1);
        let replay_queue = Arc::new(Mutex::new(ReplayQueue::new(config.replay_inflight_max)));

        let storage = Self {
            db: Arc::clone(&db),
            write_tx: ser_tx,
            replay_queue: Arc::clone(&replay_queue),
        };

        let mut threads = vec![];
        let write_ser_jh = thread::Builder::new()
            .name("richatStrgSer".to_owned())
            .spawn({
                let db = Arc::clone(&db);
                || {
                    if let Some(cpus) = config.serialize_affinity {
                        affinity_linux::set_thread_affinity(cpus.into_iter())
                            .expect("failed to set affinity");
                    }
                    Self::spawn_ser(db, ser_rx, write_tx);
                    Ok(())
                }
            })?;
        threads.push(("richatStrgSer".to_owned(), Some(write_ser_jh)));
        let write_jh = thread::Builder::new()
            .name("richatStrgWrt".to_owned())
            .spawn({
                let db = Arc::clone(&db);
                || {
                    if let Some(cpus) = config.write_affinity {
                        affinity_linux::set_thread_affinity(cpus.into_iter())
                            .expect("failed to set affinity");
                    }
                    Self::spawn_write(db, write_rx)
                }
            })?;
        threads.push(("richatStrgWrt".to_owned(), Some(write_jh)));
        for index in 0..config.replay_threads {
            let th_name = format!("richatStrgRep{index:02}");
            let jh = thread::Builder::new().name(th_name.clone()).spawn({
                let affinity = config.replay_affinity.clone();
                let db = Arc::clone(&db);
                let replay_queue = Arc::clone(&replay_queue);
                let shutdown = shutdown.clone();
                move || {
                    if let Some(cpus) = affinity {
                        affinity_linux::set_thread_affinity(cpus.into_iter())
                            .expect("failed to set affinity");
                    }
                    Self::spawn_replay(
                        db,
                        replay_queue,
                        parser,
                        config.replay_decode_per_tick,
                        shutdown,
                    )
                }
            })?;
            threads.push((th_name, Some(jh)));
        }

        Ok((storage, threads))
    }

    fn get_db_options() -> Options {
        let mut options = Options::default();

        // Create if not exists
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Set_max_background_jobs(N), configures N/4 low priority threads and 3N/4 high priority threads
        options.set_max_background_jobs(num_cpus::get() as i32);

        // Set max total WAL size to 4GiB
        options.set_max_total_wal_size(4 * 1024 * 1024 * 1024);

        options
    }

    fn get_cf_options(compression: DBCompressionType) -> Options {
        let mut options = Options::default();

        const MAX_WRITE_BUFFER_SIZE: u64 = 256 * 1024 * 1024;
        options.set_max_write_buffer_number(8);
        options.set_write_buffer_size(MAX_WRITE_BUFFER_SIZE as usize);

        let file_num_compaction_trigger = 4;
        let total_size_base = MAX_WRITE_BUFFER_SIZE * file_num_compaction_trigger;
        let file_size_base = total_size_base / 10;
        options.set_level_zero_file_num_compaction_trigger(file_num_compaction_trigger as i32);
        options.set_max_bytes_for_level_base(total_size_base);
        options.set_target_file_size_base(file_size_base);

        options.set_compression_type(compression);

        options
    }

    fn cf_descriptors(message_compression: DBCompressionType) -> Vec<ColumnFamilyDescriptor> {
        vec![
            Self::cf_descriptor::<MessageIndex>(message_compression),
            Self::cf_descriptor::<SlotIndex>(DBCompressionType::None),
        ]
    }

    fn cf_descriptor<C: ColumnName>(compression: DBCompressionType) -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(C::NAME, Self::get_cf_options(compression))
    }

    fn cf_handle<C: ColumnName>(db: &DB) -> &ColumnFamily {
        db.cf_handle(C::NAME)
            .expect("should never get an unknown column")
    }

    fn spawn_ser(
        db: Arc<DB>,
        rx: mpsc::Receiver<WriteRequest>,
        tx: mpsc::SyncSender<(u64, WriteBatch)>,
    ) {
        let mut gindex = 0;
        let mut buf = Vec::with_capacity(16 * 1024 * 1024);
        let mut batch = WriteBatch::new();

        while let Ok(message) = rx.recv() {
            match message {
                WriteRequest::PushMessage {
                    init,
                    slot,
                    head,
                    index,
                    message,
                } => {
                    // initialize slot index
                    if init {
                        buf.clear();
                        SlotIndexValue::encode(false, head, &mut buf);
                        batch.put_cf(
                            Self::cf_handle::<SlotIndex>(&db),
                            SlotIndex::encode(slot),
                            &buf,
                        );
                    }

                    // make as finalized in slot index
                    if let ParsedMessage::Slot(message) = &message {
                        if message.status() == SlotStatus::SlotFinalized {
                            buf.clear();
                            SlotIndexValue::encode(true, head, &mut buf);
                            batch.put_cf(
                                Self::cf_handle::<SlotIndex>(&db),
                                SlotIndex::encode(slot),
                                &buf,
                            );
                        }
                    }

                    // push message
                    let message_ref: MessageRef = (&message).into();
                    let message = FilteredUpdate {
                        filters: FilteredUpdateFilters::new(),
                        filtered_update: message_ref.into(),
                    };
                    buf.clear();
                    MessageIndexValue::encode(slot, message, &mut buf);
                    batch.put_cf(
                        Self::cf_handle::<MessageIndex>(&db),
                        MessageIndex::encode(index),
                        &buf,
                    );

                    counter!(CHANNEL_STORAGE_WRITE_SER_INDEX).absolute(index);
                    gindex = index;
                }
                WriteRequest::RemoveReplay { slot, until } => {
                    batch.delete_cf(Self::cf_handle::<SlotIndex>(&db), SlotIndex::encode(slot));
                    if let Some(until) = until {
                        // remove range `[begin_key, end_key)`
                        batch.delete_range_cf(
                            Self::cf_handle::<MessageIndex>(&db),
                            MessageIndex::encode(0),     // begin_key
                            MessageIndex::encode(until), // end_key
                        );
                    }
                }
            }

            batch = match tx.try_send((gindex, batch)) {
                Ok(()) => WriteBatch::new(),
                Err(mpsc::TrySendError::Full((_index, value))) => value,
                Err(mpsc::TrySendError::Disconnected((_index, value))) => value,
            };
        }
        if !batch.is_empty() {
            let _ = tx.send((gindex, batch));
        }
    }

    fn spawn_write(db: Arc<DB>, rx: mpsc::Receiver<(u64, WriteBatch)>) -> anyhow::Result<()> {
        while let Ok((index, batch)) = rx.recv() {
            db.write(batch)?;
            counter!(CHANNEL_STORAGE_WRITE_INDEX).absolute(index);
        }
        Ok(())
    }

    fn spawn_replay(
        db: Arc<DB>,
        replay_queue: Arc<Mutex<ReplayQueue>>,
        parser: MessageParserEncoding,
        messages_decode_per_tick: usize,
        shutdown: Shutdown,
    ) -> anyhow::Result<()> {
        let mut shutdown_ts = Instant::now();
        let mut prev_request = None;
        loop {
            // get request and lock state
            let Some(mut req) = ReplayQueue::pop_next(&replay_queue, prev_request.take()) else {
                if shutdown.is_set() {
                    break;
                }
                thread::sleep(Duration::from_millis(1));
                continue;
            };
            let ts = Instant::now();
            if ts.duration_since(shutdown_ts) > Duration::from_millis(100) {
                shutdown_ts = ts;
                if shutdown.is_set() {
                    break;
                }
            }
            let mut locked_state = req.client.state_lock();

            // drop request if stream is finished
            if locked_state.finished {
                ReplayQueue::drop_req(&replay_queue);
                continue;
            }

            // send error
            if let Some(error) = req.state.read_error.take() {
                locked_state.push_error(error);
                ReplayQueue::drop_req(&replay_queue);
                continue;
            }

            // check replay head
            let IndexLocation::Storage(head) = locked_state.head else {
                ReplayQueue::drop_req(&replay_queue);
                continue;
            };

            // check that filter is same
            let mut current_head = *req.state.head.get_or_insert(head);
            if current_head != head {
                req.state.messages.clear();
            }

            // filter messages
            let ts = Instant::now();
            while !locked_state.is_full_replay() {
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
                    locked_state.push_message(message, data);
                }

                current_head = index;
            }

            // update head
            locked_state.head = IndexLocation::Storage(current_head);
            req.state.head = Some(current_head);

            // check read_finished and empty; update index and drop from replay queue
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
                ReplayQueue::drop_req(&replay_queue);
                continue;
            }

            // drop locked state before sync read
            drop(locked_state);

            // read messages
            if !req.state.read_finished && req.state.messages.len() < messages_decode_per_tick {
                let mut messages_decoded = 0;
                for item in db.iterator_cf(
                    Self::cf_handle::<MessageIndex>(&db),
                    IteratorMode::From(&MessageIndex::encode(current_head + 1), Direction::Forward),
                ) {
                    let item = match item {
                        Ok((key, value)) => {
                            match (
                                MessageIndex::decode(&key),
                                MessageIndexValue::decode(&value, parser),
                            ) {
                                (Ok(index), Ok((_slot, message))) => Ok((index, message.into())),
                                (Err(_error), _) => Err("failed to decode key"),
                                (_, Err(_error)) => Err("failed to parse message"),
                            }
                        }
                        Err(_error) => Err("failed to read message from the storage"),
                    };

                    match item {
                        Ok(item) => {
                            req.state.messages.push_back(item);
                            messages_decoded += 1;
                            if messages_decoded >= messages_decode_per_tick {
                                break;
                            }
                        }
                        Err(error) => {
                            req.state.read_error = Some(Status::internal(error));
                            break;
                        }
                    };
                }

                if messages_decoded < messages_decode_per_tick {
                    req.state.read_finished = true;
                }
            }

            req.metric_cpu_usage
                .increment(duration_to_seconds(ts.elapsed()));
            prev_request = Some(req);
        }
        ReplayQueue::shutdown(&replay_queue);
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
        let _ = self.write_tx.send(WriteRequest::PushMessage {
            init,
            slot,
            head,
            index,
            message,
        });
    }

    pub fn remove_replay(&self, slot: Slot, until: Option<u64>) {
        let _ = self
            .write_tx
            .send(WriteRequest::RemoveReplay { slot, until });
    }

    pub fn read_slots(&self) -> anyhow::Result<BTreeMap<Slot, SlotIndexValue>> {
        let mut slots = BTreeMap::new();
        for item in self
            .db
            .iterator_cf(Self::cf_handle::<SlotIndex>(&self.db), IteratorMode::Start)
        {
            let (key, value) = item.context("failed to read next row")?;
            slots.insert(
                SlotIndex::decode(&key).context("failed to decode key")?,
                SlotIndexValue::decode(&value).context("failed to decode value")?,
            );
        }
        Ok(slots)
    }

    pub fn read_messages_from_index(
        &self,
        index: u64,
        parser: MessageParserEncoding,
    ) -> impl Iterator<Item = anyhow::Result<(u64, ParsedMessage)>> + use<'_> {
        self.db
            .iterator_cf(
                Self::cf_handle::<MessageIndex>(&self.db),
                IteratorMode::From(&MessageIndex::encode(index), Direction::Forward),
            )
            .map(move |item| {
                let (key, value) = item.context("failed to read next row")?;
                let index = MessageIndex::decode(&key).context("failed to decode key")?;
                let (_slot, message) = MessageIndexValue::decode(&value, parser)?;
                Ok((index, message.into()))
            })
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
enum WriteRequest {
    PushMessage {
        init: bool,
        slot: Slot,
        head: u64,
        index: u64,
        message: ParsedMessage,
    },
    RemoveReplay {
        slot: Slot,
        until: Option<u64>,
    },
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
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            len: 0,
            requests: VecDeque::with_capacity(capacity),
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

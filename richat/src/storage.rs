use {
    crate::{
        channel::ParsedMessage,
        config::ConfigStorage,
        metrics::{CHANNEL_STORAGE_WRITE_INDEX, CHANNEL_STORAGE_WRITE_SER_INDEX},
        SpawnedThreads,
    },
    ::metrics::counter,
    anyhow::Context,
    prost::{bytes::BufMut, encoding::encode_varint},
    richat_filter::{
        filter::{FilteredUpdate, FilteredUpdateFilters},
        message::MessageRef,
    },
    richat_proto::geyser::SlotStatus,
    rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DBCompressionType, Options, WriteBatch, DB},
    solana_sdk::clock::Slot,
    std::{
        borrow::Cow,
        sync::{mpsc, Arc, Mutex},
        thread,
    },
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

    // fn decode(slice: &[u8]) -> anyhow::Result<u64> {
    //     slice
    //         .try_into()
    //         .map(u64::from_be_bytes)
    //         .context("invalid slice size")
    // }
}

#[derive(Debug)]
struct MessageIndexValue;

impl MessageIndexValue {
    fn encode(slot: Slot, message: FilteredUpdate, buf: &mut impl BufMut) {
        encode_varint(slot, buf);
        message.encode(buf);
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

    // fn decode(slice: &[u8]) -> anyhow::Result<Slot> {
    //     slice
    //         .try_into()
    //         .map(Slot::from_be_bytes)
    //         .context("invalid slice size")
    // }
}

#[derive(Debug)]
struct SlotIndexValue;

impl SlotIndexValue {
    fn encode(finalized: bool, index: u64, buf: &mut impl BufMut) {
        buf.put_u8(if finalized { 1 } else { 0 });
        encode_varint(index, buf);
    }

    // fn decode(mut slice: &[u8]) -> anyhow::Result<Self> {
    //     Ok(Self {
    //         index: decode_varint(&mut slice).context("failed to read index")?,
    //     })
    // }
}

#[derive(Debug, Clone)]
pub struct Storage {
    write_tx: mpsc::Sender<WriteRequest>,
    read_tx: mpsc::SyncSender<ReadRequest>,
}

impl Storage {
    pub fn open(config: ConfigStorage) -> anyhow::Result<(Self, SpawnedThreads)> {
        let db_options = Self::get_db_options();
        let cf_descriptors = Self::cf_descriptors(config.messages_compression.into());

        let db = Arc::new(
            DB::open_cf_descriptors(&db_options, &config.path, cf_descriptors)
                .with_context(|| format!("failed to open rocksdb with path: {:?}", config.path))?,
        );

        let (ser_tx, ser_rx) = mpsc::channel();
        let (write_tx, write_rx) = mpsc::sync_channel(1);
        let (read_tx, read_rx) = mpsc::sync_channel(config.read_channel_capacity);

        let storage = Self {
            write_tx: ser_tx,
            read_tx,
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
                    Self::spawn_ser_write(db, ser_rx, write_tx);
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
        let read_rx = Arc::new(Mutex::new(read_rx));
        for index in 0..config.read_threads {
            let th_name = format!("richatStrgRd{index:02}");
            let jh = thread::Builder::new().name(th_name.clone()).spawn({
                let affinity = config.read_affinity.clone();
                let db = Arc::clone(&db);
                let read_rx = Arc::clone(&read_rx);
                move || {
                    if let Some(cpus) = affinity {
                        affinity_linux::set_thread_affinity(cpus.into_iter())
                            .expect("failed to set affinity");
                    }
                    Self::spawn_read(db, read_rx)
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

    fn spawn_ser_write(
        db: Arc<DB>,
        rx: mpsc::Receiver<WriteRequest>,
        tx: mpsc::SyncSender<(u64, WriteBatch)>,
    ) {
        let mut buf = Vec::with_capacity(16 * 1024 * 1024);
        let mut batch = WriteBatch::new();

        while let Ok(WriteRequest {
            init,
            slot,
            head,
            index,
            message,
        }) = rx.recv()
        {
            // initialize slot index
            if init {
                buf.clear();
                SlotIndexValue::encode(false, index, &mut buf);
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
            let message: Cow<'_, ParsedMessage> = Cow::Owned(message);
            let message_ref: MessageRef = message.as_ref().into();
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
            match tx.try_send((index, batch)) {
                Ok(()) => {
                    batch = WriteBatch::new();
                }
                Err(mpsc::TrySendError::Full((_index, value))) => {
                    batch = value;
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    break;
                }
            }
        }
    }

    fn spawn_write(db: Arc<DB>, rx: mpsc::Receiver<(u64, WriteBatch)>) -> anyhow::Result<()> {
        while let Ok((index, batch)) = rx.recv() {
            db.write(batch)?;
            counter!(CHANNEL_STORAGE_WRITE_INDEX).absolute(index);
        }
        Ok(())
    }

    fn spawn_read(db: Arc<DB>, rx: Arc<Mutex<mpsc::Receiver<ReadRequest>>>) -> anyhow::Result<()> {
        loop {
            let rx = rx.lock().expect("unpoisoned mutex");
            let Ok(message) = rx.recv() else {
                break;
            };
            drop(rx);

            match message {
                ReadRequest::Slots => {}
            }
        }
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
        let _ = self.write_tx.send(WriteRequest {
            init,
            slot,
            head,
            index,
            message,
        });
    }
}

#[derive(Debug)]
struct WriteRequest {
    init: bool,
    slot: Slot,
    head: u64,
    index: u64,
    message: ParsedMessage,
}

#[derive(Debug)]
enum ReadRequest {
    Slots,
}

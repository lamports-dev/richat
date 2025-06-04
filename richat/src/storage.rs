use {
    crate::{
        channel::ParsedMessage,
        config::ConfigChannelStorage,
        metrics::{CHANNEL_STORAGE_WRITE_INDEX, CHANNEL_STORAGE_WRITE_PREPARE_INDEX},
        SpawnedThreads,
    },
    ::metrics::counter,
    anyhow::Context,
    richat_filter::{
        filter::{FilteredUpdate, FilteredUpdateFilters},
        message::MessageRef,
    },
    richat_proto::geyser::SlotStatus,
    rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DBCompressionType, Options, WriteBatch, DB},
    solana_sdk::clock::Slot,
    std::{
        borrow::Cow,
        sync::{mpsc, Arc},
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
    const fn key(key: u64) -> [u8; 8] {
        key.to_be_bytes()
    }

    fn decode(slice: &[u8]) -> anyhow::Result<u64> {
        slice
            .try_into()
            .map(Slot::from_be_bytes)
            .context("invalid slice size")
    }
}

#[derive(Debug)]
struct SlotIndex;

impl ColumnName for SlotIndex {
    const NAME: &'static str = "slot_index";
}

impl SlotIndex {
    // const fn key(key: u64) -> [u8; 8] {
    //     key.to_be_bytes()
    // }

    // fn decode(slice: &[u8]) -> anyhow::Result<u64> {
    //     slice
    //         .try_into()
    //         .map(Slot::from_be_bytes)
    //         .context("invalid slice size")
    // }
}

#[derive(Debug, Clone)]
pub struct Storage {
    tx: mpsc::Sender<StorageMessage>,
}

impl Storage {
    pub fn open(config: ConfigChannelStorage) -> anyhow::Result<(Self, SpawnedThreads)> {
        let db_options = Self::get_db_options();
        let cf_descriptors = Self::cf_descriptors();

        let db = Arc::new(
            DB::open_cf_descriptors(&db_options, &config.path, cf_descriptors)
                .with_context(|| format!("failed to open rocksdb with path: {:?}", config.path))?,
        );

        let (pre_tx, pre_rx) = mpsc::channel();
        let (tx, rx) = mpsc::sync_channel(1);

        let storage = Self { tx: pre_tx };

        let write_pre_jh = thread::Builder::new()
            .name("richatStrgPrWrt".to_owned())
            .spawn({
                let db = Arc::clone(&db);
                || {
                    if let Some(cpus) = config.thread_write_serialize_affinity {
                        affinity_linux::set_thread_affinity(cpus.into_iter())
                            .expect("failed to set affinity");
                    }
                    Self::spawn_pre_write(db, pre_rx, tx);
                    Ok(())
                }
            })?;
        let write_jh = thread::Builder::new()
            .name("richatStrgWrt".to_owned())
            .spawn(|| Self::spawn_write(db, rx))?;
        let threads = vec![
            ("write_serialize", Some(write_pre_jh)),
            ("write", Some(write_jh)),
        ];

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

    fn cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
        vec![Self::cf_descriptor::<MessageIndex>(DBCompressionType::None)]
    }

    fn cf_descriptor<C: ColumnName>(compression: DBCompressionType) -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(C::NAME, Self::get_cf_options(compression))
    }

    fn cf_handle<C: ColumnName>(db: &DB) -> &ColumnFamily {
        db.cf_handle(C::NAME)
            .expect("should never get an unknown column")
    }

    fn spawn_pre_write(
        db: Arc<DB>,
        rx: mpsc::Receiver<StorageMessage>,
        tx: mpsc::SyncSender<(u64, WriteBatch)>,
    ) {
        let mut buf = Vec::with_capacity(16 * 1024 * 1024);
        let mut global_index = 0;
        let mut batch = WriteBatch::new();

        while let Ok(message) = rx.recv() {
            match message {
                StorageMessage::Message { index, message } => {
                    let message: Cow<'_, ParsedMessage> = Cow::Owned(message);
                    let message_ref: MessageRef = message.as_ref().into();
                    let message = FilteredUpdate {
                        filters: FilteredUpdateFilters::new(),
                        filtered_update: message_ref.into(),
                    };

                    buf.clear();
                    message.encode(&mut buf);
                    batch.put_cf(
                        Self::cf_handle::<MessageIndex>(&db),
                        MessageIndex::key(index),
                        &buf,
                    );

                    global_index = index;
                    counter!(CHANNEL_STORAGE_WRITE_PREPARE_INDEX).absolute(index);
                }
                StorageMessage::Slot { index, status } => {
                    //

                    if let Some(index) = index {
                        global_index = index;
                        counter!(CHANNEL_STORAGE_WRITE_PREPARE_INDEX).absolute(index);
                    }
                }
            }

            match tx.try_send((global_index, batch)) {
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

    pub fn push_message(&self, index: u64, message: ParsedMessage) {
        let _ = self.tx.send(StorageMessage::Message { index, message });
    }

    pub fn push_slot(&self, index: Option<u64>, status: SlotStatus) {
        let _ = self.tx.send(StorageMessage::Slot { index, status });
    }
}

#[derive(Debug)]
enum StorageMessage {
    Message {
        index: u64,
        message: ParsedMessage,
    },
    Slot {
        index: Option<u64>,
        status: SlotStatus,
    },
}

// fn hash(message: ParsedMessage, mut hasher: FoldHasher) -> Vec<u8>, Vec<u8>) {
//     let mut key = vec![];
//     match &message {
//         ParsedMessage::Slot(msg) => {
//             key.push(0);
//             hasher.write_u64(msg.slot());
//             msg.status().hash(&mut hasher);
//         }
//         ParsedMessage::Account(msg) => {
//             key.push(1);
//             hasher.write_u64(msg.slot());
//             msg.pubkey().hash(&mut hasher);
//             if let Some(signature) = msg.txn_signature() {
//                 hasher.write(signature);
//             }
//         }
//         ParsedMessage::Transaction(msg) => {
//             key.push(2);
//             hasher.write_u64(msg.slot());
//             hasher.write(msg.signature().as_ref());
//         }
//         ParsedMessage::Entry(msg) => {
//             key.push(3);
//             hasher.write_u64(msg.slot());
//             hasher.write_u64(msg.index());
//         }
//         ParsedMessage::BlockMeta(msg) => {
//             key.push(4);
//             hasher.write_u64(msg.slot());
//         }
//         ParsedMessage::Block(_msg) => return (vec![5], vec![]),
//     }
//     key.extend_from_slice(&hasher.finish().to_be_bytes());

//     let message: Cow<'_, ParsedMessage> = Cow::Owned(message);
//     let message_ref: MessageRef = message.as_ref().into();
//     let bytes = FilteredUpdate {
//         filters: FilteredUpdateFilters::new(),
//         filtered_update: message_ref.into(),
//     }
//     .encode();

//     (key, bytes)
// }

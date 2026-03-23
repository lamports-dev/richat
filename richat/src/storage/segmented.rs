use {
    crate::{
        channel::ParsedMessage,
        config::ConfigStorage,
        storage::{
            SlotIndexValue,
            metadata::{GlobalState, MetadataCatalog, MetadataDb, SegmentMeta},
            segment_format::{
                ChunkCompression, SEGMENT_HEADER_LEN, SegmentHeader, segment_file_name,
                write_segment_header,
            },
            segment_reader::SegmentReader,
            segment_writer::{
                WriterCommand, spawn_collector, spawn_compressor_pool, spawn_writer,
            },
        },
        util::SpawnedThreads,
    },
    anyhow::Context,
    richat_filter::message::MessageParserEncoding,
    solana_clock::Slot,
    std::{
        collections::BTreeMap,
        fs::OpenOptions,
        path::PathBuf,
        sync::{Arc, RwLock},
        thread,
        time::{SystemTime, UNIX_EPOCH},
    },
};

/// Runtime storage settings derived from [`ConfigStorage`] and normalized into
/// concrete filesystem paths and write thresholds.
#[derive(Debug, Clone)]
pub(crate) struct SegmentedConfig {
    pub(crate) metadata_path: PathBuf,
    pub(crate) segments_path: PathBuf,
    pub(crate) serialize_affinity: Option<Vec<usize>>,
    pub(crate) write_affinity: Option<Vec<usize>>,
    pub(crate) segment_target_size: usize,
    pub(crate) chunk_target_size: usize,
    pub(crate) chunk_compression: Option<ChunkCompression>,
    pub(crate) compressor_threads: usize,
    pub(crate) compressor_affinity: Option<Vec<usize>>,
    pub(crate) compressor_channel_size: usize,
    pub(crate) collector_channel_size: usize,
    pub(crate) writer_channel_size: usize,
}

impl SegmentedConfig {
    pub(crate) fn from_config(config: &ConfigStorage) -> Self {
        Self {
            metadata_path: config.path.join("metadata"),
            segments_path: config.path.join("segments"),
            serialize_affinity: config.serialize_affinity.clone(),
            write_affinity: config.write_affinity.clone(),
            segment_target_size: config.segment_target_size,
            chunk_target_size: config.chunk_target_size,
            chunk_compression: config.chunk_compression,
            compressor_threads: config.compressor_threads,
            compressor_affinity: config.compressor_affinity.clone(),
            compressor_channel_size: config.compressor_channel_size,
            collector_channel_size: config.collector_channel_size,
            writer_channel_size: config.writer_channel_size,
        }
    }
}

/// Segmented replay store used by the rest of the channel pipeline.
///
/// This owns the metadata catalog used for lookups and the writer channel used
/// to append new replay records onto the active segment.
#[derive(Debug, Clone)]
pub(crate) struct SegmentedStorage {
    config: SegmentedConfig,
    catalog: Arc<RwLock<MetadataCatalog>>,
    pub(crate) write_tx: kanal::Sender<WriterCommand>,
}

impl SegmentedStorage {
    pub(crate) fn open(config: ConfigStorage) -> anyhow::Result<(Self, SpawnedThreads)> {
        let config = SegmentedConfig::from_config(&config);
        std::fs::create_dir_all(&config.segments_path).with_context(|| {
            format!("failed to create segments path: {:?}", config.segments_path)
        })?;

        let metadata = MetadataDb::open(&config.metadata_path)?;
        let mut catalog = metadata.load_catalog()?;

        if catalog.state.active_segment_id == 0 {
            let mut state = GlobalState::new();
            state.active_segment_id = state.next_segment_id;
            state.next_segment_id += 1;

            let segment = SegmentMeta::empty(
                state.active_segment_id,
                current_unix_ms()?,
                SEGMENT_HEADER_LEN as u64,
            );
            create_segment_file(&config, segment)?;
            metadata.initialize_empty(state, segment)?;
            catalog = metadata.load_catalog()?;
        }

        let catalog = Arc::new(RwLock::new(catalog));
        let (write_tx, write_rx) = kanal::bounded(config.collector_channel_size);
        let (collector_tx, collector_rx) = kanal::bounded(config.compressor_channel_size);
        let (compressor_tx, compressor_rx) = kanal::bounded(config.writer_channel_size);

        let collector_thread = spawn_collector(
            config.chunk_target_size,
            config.serialize_affinity.clone(),
            write_rx,
            collector_tx,
        )?;
        let compressor_threads = spawn_compressor_pool(
            config.compressor_threads,
            config.chunk_compression,
            config.compressor_affinity.clone(),
            collector_rx,
            compressor_tx,
        )?;

        let writer_config = config.clone();
        let writer_catalog = Arc::clone(&catalog);
        let writer_metadata = metadata.clone();
        let writer_affinity = config.write_affinity.clone();
        let writer_jh = thread::Builder::new()
            .name("richatStrgWrt".to_owned())
            .spawn(move || {
                if let Some(cpus) = writer_affinity {
                    affinity_linux::set_thread_affinity(cpus.into_iter())
                        .expect("failed to set affinity");
                }
                spawn_writer(
                    writer_config,
                    writer_metadata,
                    writer_catalog,
                    compressor_rx,
                )
            })?;
        let mut threads = vec![collector_thread];
        threads.extend(compressor_threads);
        threads.push(("richatStrgWrt".to_owned(), Some(writer_jh)));

        let storage = Self {
            config,
            catalog,
            write_tx,
        };
        Ok((storage, threads))
    }

    pub(crate) fn remove_replay(&self, slot: Slot, until: Option<u64>) {
        let _ = self.write_tx.send(WriterCommand::RemoveReplay { slot, until });
    }

    pub(crate) fn read_slots(&self) -> anyhow::Result<BTreeMap<Slot, SlotIndexValue>> {
        let catalog = self.catalog.read().expect("segment catalog poisoned");
        Ok(catalog
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
            .collect())
    }

    pub(crate) fn read_messages_from_index(
        &self,
        index: u64,
        parser: MessageParserEncoding,
    ) -> Box<dyn Iterator<Item = anyhow::Result<(u64, ParsedMessage)>> + '_> {
        let chunks = {
            let catalog = self.catalog.read().expect("segment catalog poisoned");
            let start = catalog
                .chunks
                .partition_point(|chunk| chunk.last_index < index);
            catalog.chunks[start..].to_vec()
        };
        if chunks.is_empty() {
            return Box::new(std::iter::empty());
        }

        match SegmentReader::new(self.config.segments_path.clone(), chunks, index, parser) {
            Ok(reader) => Box::new(reader),
            Err(error) => Box::new(std::iter::once(Err(error))),
        }
    }
}

fn create_segment_file(config: &SegmentedConfig, segment: SegmentMeta) -> anyhow::Result<()> {
    let path = config
        .segments_path
        .join(segment_file_name(segment.segment_id));
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("failed to create segment file: {path:?}"))?;
    write_segment_header(
        &mut file,
        SegmentHeader {
            segment_id: segment.segment_id,
            created_unix_ms: segment.created_unix_ms,
        },
    )?;
    file.sync_data()?;
    Ok(())
}

fn current_unix_ms() -> anyhow::Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use {
        super::{ChunkCompression, SegmentedStorage, segment_file_name},
        crate::{
            channel::ParsedMessage, config::ConfigStorage, storage::segment_writer::WriterCommand,
            util::SpawnedThreads,
        },
        prost_types::Timestamp,
        richat_filter::message::{MessageParserEncoding, MessageSlot},
        richat_proto::geyser::SlotStatus,
        std::{
            path::{Path, PathBuf},
            sync::Arc,
            thread::sleep,
            time::{Duration, SystemTime, UNIX_EPOCH},
        },
    };

    #[test]
    fn segmented_storage_roundtrip_and_segment_trim() {
        let root = temp_dir("segmented-roundtrip");
        let config = test_config(&root, 1, 1);

        let (storage, threads) = SegmentedStorage::open(config.clone()).unwrap();
        let _ = storage.write_tx.send(WriterCommand::PushMessage {
            init: true,
            slot: 10,
            head: 0,
            index: 0,
            message: slot_message(10, SlotStatus::SlotProcessed),
        });
        let _ = storage.write_tx.send(WriterCommand::PushMessage {
            init: false,
            slot: 10,
            head: 0,
            index: 1,
            message: slot_message(10, SlotStatus::SlotFinalized),
        });
        let _ = storage.write_tx.send(WriterCommand::PushMessage {
            init: true,
            slot: 11,
            head: 2,
            index: 2,
            message: slot_message(11, SlotStatus::SlotProcessed),
        });

        wait_for(
            || {
                let slots = storage.read_slots().unwrap();
                slots.get(&10).is_some_and(|slot| slot.finalized)
                    && slots.get(&11).is_some_and(|slot| slot.head == 2)
            },
            "slot metadata flush",
        );

        let items = storage
            .read_messages_from_index(0, MessageParserEncoding::Prost)
            .collect::<Vec<_>>();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].as_ref().unwrap().0, 0);
        assert_eq!(items[1].as_ref().unwrap().0, 1);
        assert_eq!(items[2].as_ref().unwrap().0, 2);

        let segments_path = config.path.join("segments");
        let first_segment = segments_path.join(segment_file_name(1));
        assert!(first_segment.exists());

        storage.remove_replay(10, Some(2));
        wait_for(
            || {
                let slots = storage.read_slots().unwrap();
                !slots.contains_key(&10) && slots.contains_key(&11)
            },
            "slot trim",
        );
        wait_for(|| !first_segment.exists(), "segment delete");

        drop(storage);
        join_threads(threads);
        let _ = std::fs::remove_dir_all(root);
    }

    fn test_config(
        root: &Path,
        segment_target_size: usize,
        chunk_target_size: usize,
    ) -> ConfigStorage {
        ConfigStorage {
            path: root.join("storage"),
            max_slots: 1024,
            serialize_affinity: None,
            write_affinity: None,
            segment_target_size,
            chunk_target_size,
            chunk_compression: Some(ChunkCompression::Zstd(1)),
            replay_inflight_max: 1,
            replay_threads: 1,
            replay_affinity: None,
            replay_decode_per_tick: 32,
            compressor_threads: 1,
            compressor_affinity: None,
            compressor_channel_size: 2,
            collector_channel_size: 64,
            writer_channel_size: 2,
        }
    }

    fn slot_message(slot: u64, status: SlotStatus) -> ParsedMessage {
        ParsedMessage::Slot(Arc::new(MessageSlot::Prost {
            slot,
            parent: None,
            status,
            dead_error: None,
            created_at: Timestamp::default(),
            size: 64,
        }))
    }

    fn wait_for(mut predicate: impl FnMut() -> bool, label: &str) {
        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(5) {
            if predicate() {
                return;
            }
            sleep(Duration::from_millis(10));
        }
        panic!("timed out waiting for {label}");
    }

    fn temp_dir(prefix: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path =
            std::env::temp_dir().join(format!("richat-{prefix}-{}-{nonce}", std::process::id()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn join_threads(threads: SpawnedThreads) {
        for (_name, handle) in threads {
            if let Some(handle) = handle {
                handle.join().unwrap().unwrap();
            }
        }
    }
}

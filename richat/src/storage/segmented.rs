use {
    super::{
        SlotIndexValue,
        metadata::{GlobalState, MetadataCatalog, MetadataDb, SegmentMeta},
        recovery::recover_active_segment,
        segment_format::{
            ChunkCompression, SEGMENT_HEADER_LEN, SegmentHeader, segment_file_name,
            write_segment_header,
        },
        segment_reader::SegmentReader,
        segment_writer::{
            StorageWriteQueueMetrics, WriterCommand, spawn_serialize_worker, spawn_writer,
        },
    },
    crate::{
        channel::ParsedMessage, config::ConfigStorage, metrics::STORAGE_DISK_SIZE_BYTES,
        util::SpawnedThreads,
    },
    ::metrics::gauge,
    anyhow::Context,
    richat_filter::message::MessageParserEncoding,
    solana_clock::Slot,
    std::{
        collections::BTreeMap,
        fs::OpenOptions,
        path::{Path, PathBuf},
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
    pub(crate) recovery_rebuild_active: bool,
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
            recovery_rebuild_active: config.recovery_rebuild_active,
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
    queue_metrics: Arc<StorageWriteQueueMetrics>,
    write_tx: kanal::Sender<WriterCommand>,
}

impl SegmentedStorage {
    pub(crate) fn open(
        config: ConfigStorage,
        parser: MessageParserEncoding,
    ) -> anyhow::Result<(Self, SpawnedThreads)> {
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

        if config.recovery_rebuild_active {
            let recovery = recover_active_segment(&config, &catalog, parser)?;
            metadata.apply_recovery_commit(&recovery)?;
            catalog.apply_recovery_commit(&recovery);
        }

        let catalog = Arc::new(RwLock::new(catalog));
        let queue_metrics = Arc::new(StorageWriteQueueMetrics::default());
        queue_metrics.publish_current();
        let (write_tx, write_rx) = kanal::unbounded();
        let (compression_tx, compression_rx) = kanal::unbounded();
        let (serialize_thread, compression_tx) = spawn_serialize_worker(&config, compression_tx)?;
        let mut threads = vec![serialize_thread];
        let writer_config = config.clone();
        let writer_catalog = Arc::clone(&catalog);
        let writer_metadata = metadata.clone();
        let writer_queue_metrics = Arc::clone(&queue_metrics);
        let writer_affinity = config.write_affinity.clone();
        let writer_jh = thread::Builder::new()
            .name("richatStrgSeg".to_owned())
            .spawn(move || {
                if let Some(cpus) = writer_affinity {
                    affinity_linux::set_thread_affinity(cpus.into_iter())
                        .expect("failed to set affinity");
                }
                spawn_writer(
                    writer_config,
                    writer_metadata,
                    writer_catalog,
                    writer_queue_metrics,
                    compression_tx,
                    compression_rx,
                    write_rx,
                )
            })?;
        threads.push(("richatStrgSeg".to_owned(), Some(writer_jh)));

        let storage = Self {
            config,
            catalog,
            queue_metrics,
            write_tx,
        };
        Ok((storage, threads))
    }

    pub(crate) fn push_message(
        &self,
        init: bool,
        slot: Slot,
        head: u64,
        index: u64,
        message: ParsedMessage,
    ) {
        let command = WriterCommand::push_message(init, slot, head, index, message);
        let approx_bytes = command.approx_bytes();
        self.queue_metrics.begin_enqueue(approx_bytes);
        if self.write_tx.send(command).is_ok() {
            self.queue_metrics.commit_enqueue(approx_bytes);
        } else {
            self.queue_metrics.cancel_enqueue(approx_bytes);
        }
    }

    pub(crate) fn remove_replay(&self, slot: Slot, until: Option<u64>) {
        let command = WriterCommand::remove_replay(slot, until);
        let approx_bytes = command.approx_bytes();
        self.queue_metrics.begin_enqueue(approx_bytes);
        if self.write_tx.send(command).is_ok() {
            self.queue_metrics.commit_enqueue(approx_bytes);
        } else {
            self.queue_metrics.cancel_enqueue(approx_bytes);
        }
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

    pub(crate) fn publish_disk_size_metric(&self) -> anyhow::Result<()> {
        gauge!(STORAGE_DISK_SIZE_BYTES).set(self.disk_size_bytes()? as f64);
        Ok(())
    }

    fn disk_size_bytes(&self) -> anyhow::Result<u64> {
        Ok(dir_size_bytes(&self.config.metadata_path)?.saturating_add(self.segment_bytes()))
    }

    fn segment_bytes(&self) -> u64 {
        let catalog = self.catalog.read().expect("segment catalog poisoned");
        catalog.segments.values().fold(0u64, |bytes, segment| {
            bytes.saturating_add(segment.file_len)
        })
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

fn dir_size_bytes(path: &Path) -> anyhow::Result<u64> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read metadata for {path:?}"));
        }
    };

    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        return Ok(0);
    }

    let mut bytes = 0u64;
    for entry in
        std::fs::read_dir(path).with_context(|| format!("failed to read directory {path:?}"))?
    {
        let entry = entry.with_context(|| format!("failed to read directory entry in {path:?}"))?;
        bytes = bytes.saturating_add(dir_size_bytes(&entry.path())?);
    }
    Ok(bytes)
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
        super::{ChunkCompression, SegmentedStorage, dir_size_bytes, segment_file_name},
        crate::{channel::ParsedMessage, config::ConfigStorage, util::SpawnedThreads},
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

        let (storage, threads) =
            SegmentedStorage::open(config.clone(), MessageParserEncoding::Prost).unwrap();
        storage.push_message(true, 10, 0, 0, slot_message(10, SlotStatus::SlotProcessed));
        storage.push_message(false, 10, 0, 1, slot_message(10, SlotStatus::SlotFinalized));
        storage.push_message(true, 11, 2, 2, slot_message(11, SlotStatus::SlotProcessed));

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

    #[test]
    fn recovery_truncates_partial_tail_and_preserves_trim_floor() {
        let root = temp_dir("segmented-recovery");
        let config = test_config(&root, usize::MAX / 2, 1);

        let (storage, threads) =
            SegmentedStorage::open(config.clone(), MessageParserEncoding::Prost).unwrap();
        storage.push_message(true, 20, 0, 0, slot_message(20, SlotStatus::SlotProcessed));
        storage.push_message(true, 21, 1, 1, slot_message(21, SlotStatus::SlotProcessed));
        wait_for(|| storage.read_slots().unwrap().len() == 2, "initial flush");

        storage.remove_replay(20, Some(1));
        wait_for(
            || {
                let slots = storage.read_slots().unwrap();
                !slots.contains_key(&20) && slots.contains_key(&21)
            },
            "trim floor update",
        );

        let active_segment_id = {
            let catalog = storage.catalog.read().expect("segment catalog poisoned");
            catalog.state.active_segment_id
        };
        drop(storage);
        join_threads(threads);

        let segments_path = config.path.join("segments");
        let segment_path = segments_path.join(segment_file_name(active_segment_id));
        {
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(&segment_path)
                .unwrap();
            use std::io::Write as _;
            file.write_all(&[0xde, 0xad, 0xbe, 0xef]).unwrap();
            file.sync_data().unwrap();
        }

        let (recovered, threads) =
            SegmentedStorage::open(config.clone(), MessageParserEncoding::Prost).unwrap();
        let slots = recovered.read_slots().unwrap();
        assert!(!slots.contains_key(&20));
        assert_eq!(slots.get(&21).unwrap().head, 1);

        let items = recovered
            .read_messages_from_index(1, MessageParserEncoding::Prost)
            .collect::<Vec<_>>();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].as_ref().unwrap().0, 1);

        drop(recovered);
        join_threads(threads);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn dir_size_bytes_sums_nested_files() {
        let root = temp_dir("dir-size");
        let nested = root.join("nested");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(root.join("a.bin"), [1u8, 2, 3]).unwrap();
        std::fs::write(nested.join("b.bin"), [4u8, 5, 6, 7]).unwrap();

        assert_eq!(dir_size_bytes(&root).unwrap(), 7);
        assert_eq!(dir_size_bytes(&root.join("missing")).unwrap(), 0);

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
            recovery_rebuild_active: true,
            replay_inflight_max: 1,
            replay_threads: 1,
            replay_affinity: None,
            replay_decode_per_tick: 32,
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

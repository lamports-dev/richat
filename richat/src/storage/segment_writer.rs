use {
    crate::{
        channel::ParsedMessage,
        metrics::{
            CHANNEL_STORAGE_WRITE_INDEX, CHANNEL_STORAGE_WRITE_SER_INDEX,
            STORAGE_SEGMENT_CHUNKS_WRITTEN_TOTAL, STORAGE_WRITE_APPEND_MICROS_TOTAL,
            STORAGE_WRITE_CHUNK_COMPRESSED_BYTES_TOTAL,
            STORAGE_WRITE_CHUNK_UNCOMPRESSED_BYTES_TOTAL, STORAGE_WRITE_COMPRESS_MICROS_TOTAL,
            STORAGE_WRITE_FSYNC_MICROS_TOTAL, STORAGE_WRITE_METADATA_MICROS_TOTAL,
            STORAGE_WRITE_ROTATE_MICROS_TOTAL, STORAGE_WRITE_TRIM_MICROS_TOTAL,
        },
        storage::{
            MessageRecordCodec,
            metadata::{
                ChunkMeta, MetadataCatalog, MetadataChunkCommit, MetadataDb, MetadataTrimCommit,
                RotationCommit, SegmentMeta, SlotMeta,
            },
            segment_format::{
                ChunkCompression, SEGMENT_HEADER_LEN, SegmentHeader, segment_file_name,
                write_segment_header,
            },
            segmented::SegmentedConfig,
        },
    },
    ::metrics::counter,
    anyhow::{Context, anyhow},
    richat_proto::geyser::SlotStatus,
    solana_clock::Slot,
    std::{
        collections::{BTreeMap, HashSet},
        fs::{File, OpenOptions},
        io::{Seek, SeekFrom, Write},
        sync::{Arc, RwLock},
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
    zstd::bulk::Compressor,
};

#[derive(Debug)]
pub(crate) enum WriterCommand {
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
struct PendingSlot {
    slot: Slot,
    first_index: u64,
}

#[derive(Debug, Default)]
struct PendingChunkMeta {
    estimated_uncompressed_size: usize,
    record_count: u32,
    first_index: u64,
    last_index: u64,
    first_slot: Slot,
    last_slot: Slot,
    pending_slots: Vec<PendingSlot>,
    pending_finalized: HashSet<Slot>,
}

impl PendingChunkMeta {
    fn push(&mut self, init: bool, slot: Slot, head: u64, index: u64, message: &ParsedMessage) {
        if self.record_count == 0 {
            self.first_index = index;
            self.first_slot = slot;
        }
        self.last_index = index;
        self.last_slot = slot;
        self.estimated_uncompressed_size = self
            .estimated_uncompressed_size
            .saturating_add(estimate_record_size(slot, message));

        if init {
            self.pending_slots.push(PendingSlot {
                slot,
                first_index: head,
            });
        }
        if let ParsedMessage::Slot(message) = message {
            if message.status() == SlotStatus::SlotFinalized {
                self.pending_finalized.insert(slot);
            }
        }

        self.record_count += 1;
    }

    const fn should_flush(&self, target_size: usize) -> bool {
        self.estimated_uncompressed_size >= target_size
    }

    const fn is_empty(&self) -> bool {
        self.record_count == 0
    }
}

pub(crate) enum CollectorOutput {
    SerializedChunk {
        seq: u64,
        chunk: SerializedChunk,
    },
    Trim {
        seq: u64,
        slot: Slot,
        until: Option<u64>,
    },
}

pub(crate) struct SerializedChunk {
    first_index: u64,
    last_index: u64,
    first_slot: Slot,
    last_slot: Slot,
    uncompressed_payload: Vec<u8>,
    pending_slots: Vec<PendingSlot>,
    pending_finalized: HashSet<Slot>,
}

pub(crate) enum CompressorOutput {
    CompressedChunk {
        seq: u64,
        chunk: CompressedChunk,
    },
    Trim {
        seq: u64,
        slot: Slot,
        until: Option<u64>,
    },
}

impl CompressorOutput {
    const fn seq(&self) -> u64 {
        match self {
            Self::CompressedChunk { seq, .. } => *seq,
            Self::Trim { seq, .. } => *seq,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CompressedChunk {
    compression: u8,
    first_index: u64,
    last_index: u64,
    first_slot: Slot,
    last_slot: Slot,
    payload: Vec<u8>,
    pending_slots: Vec<PendingSlot>,
    pending_finalized: HashSet<Slot>,
}

// ── Collector thread ───────────────────────────────────────────────

pub(crate) fn spawn_collector(
    chunk_target_size: usize,
    serialize_affinity: Option<Vec<usize>>,
    rx: kanal::Receiver<WriterCommand>,
    tx: kanal::Sender<CollectorOutput>,
) -> anyhow::Result<crate::util::SpawnedThread> {
    let th_name = "richatStrgCol".to_owned();
    let jh = thread::Builder::new()
        .name(th_name.clone())
        .spawn(move || {
            if let Some(cpus) = serialize_affinity {
                affinity_linux::set_thread_affinity(cpus.into_iter())
                    .expect("failed to set affinity");
            }
            run_collector(chunk_target_size, rx, tx)
        })?;
    Ok((th_name, Some(jh)))
}

fn run_collector(
    chunk_target_size: usize,
    rx: kanal::Receiver<WriterCommand>,
    tx: kanal::Sender<CollectorOutput>,
) -> anyhow::Result<()> {
    // 11MiB should be enough for all types of messages
    let mut record_buf = Vec::with_capacity(11 * 1024 * 1024);
    let mut chunk_buf = Vec::with_capacity(chunk_target_size);
    let mut pending = PendingChunkMeta::default();
    let mut next_seq: u64 = 0;

    let mut closed = false;
    while !closed {
        let mut should_flush = false;
        let mut trim = None;

        closed = match rx.recv() {
            Ok(command) => {
                match command {
                    WriterCommand::PushMessage {
                        init,
                        slot,
                        head,
                        index,
                        message,
                    } => {
                        record_buf.clear();
                        MessageRecordCodec::encode(slot, &message, &mut record_buf);
                        prost::encoding::encode_varint(record_buf.len() as u64, &mut chunk_buf);
                        chunk_buf.extend_from_slice(&record_buf);

                        pending.push(init, slot, head, index, &message);
                        counter!(CHANNEL_STORAGE_WRITE_SER_INDEX).absolute(index);

                        should_flush = pending.should_flush(chunk_target_size);
                    }
                    WriterCommand::RemoveReplay { slot, until } => {
                        should_flush = true;
                        trim = Some((slot, until));
                    }
                }
                false
            }
            Err(_error) => true,
        };

        if (should_flush || closed) && !pending.is_empty() {
            let flushed = std::mem::take(&mut pending);
            counter!(STORAGE_WRITE_CHUNK_UNCOMPRESSED_BYTES_TOTAL)
                .increment(chunk_buf.len() as u64);

            tx.send(CollectorOutput::SerializedChunk {
                seq: next_seq,
                chunk: SerializedChunk {
                    first_index: flushed.first_index,
                    last_index: flushed.last_index,
                    first_slot: flushed.first_slot,
                    last_slot: flushed.last_slot,
                    uncompressed_payload: std::mem::replace(
                        &mut chunk_buf,
                        Vec::with_capacity(chunk_target_size),
                    ),
                    pending_slots: flushed.pending_slots,
                    pending_finalized: flushed.pending_finalized,
                },
            })
            .map_err(|_| anyhow!("collector output channel closed"))?;
            next_seq += 1;
        }

        if let Some((slot, until)) = trim {
            tx.send(CollectorOutput::Trim {
                seq: next_seq,
                slot,
                until,
            })
            .map_err(|_| anyhow!("collector output channel closed"))?;
            next_seq += 1;
        }
    }

    Ok(())
}

// ── Compressor thread ──────────────────────────────────────────────

pub(crate) fn spawn_compressor_pool(
    threads: usize,
    chunk_compression: Option<ChunkCompression>,
    affinity: Option<Vec<usize>>,
    rx: kanal::Receiver<CollectorOutput>,
    tx: kanal::Sender<CompressorOutput>,
) -> anyhow::Result<Vec<crate::util::SpawnedThread>> {
    let mut handles = Vec::with_capacity(threads);
    for i in 0..threads {
        let th_name = format!("richatStrgCmp{i:02}");
        let rx = rx.clone();
        let tx = tx.clone();
        let cpus = affinity.as_ref().map(|aff| {
            if threads == aff.len() {
                vec![aff[i]]
            } else {
                aff.clone()
            }
        });
        let jh = thread::Builder::new()
            .name(th_name.clone())
            .spawn(move || {
                if let Some(cpus) = cpus {
                    affinity_linux::set_thread_affinity(cpus.into_iter())
                        .expect("failed to set affinity");
                }
                run_compressor(chunk_compression, rx, tx)
            })?;
        handles.push((th_name, Some(jh)));
    }
    Ok(handles)
}

fn run_compressor(
    chunk_compression: Option<ChunkCompression>,
    rx: kanal::Receiver<CollectorOutput>,
    tx: kanal::Sender<CompressorOutput>,
) -> anyhow::Result<()> {
    let mut compressor = match chunk_compression {
        Some(ChunkCompression::Zstd(level)) => {
            Some(Compressor::new(level).context("failed to create zstd compressor")?)
        }
        _ => None,
    };
    let compression_tag = chunk_compression.unwrap_or(ChunkCompression::None).tag();

    loop {
        match rx.recv() {
            Ok(CollectorOutput::SerializedChunk { seq, chunk }) => {
                let payload = if let Some(compressor) = compressor.as_mut() {
                    let compress_started_at = Instant::now();
                    let compressed = compressor
                        .compress(&chunk.uncompressed_payload)
                        .context("failed to compress chunk")?;
                    counter!(STORAGE_WRITE_COMPRESS_MICROS_TOTAL)
                        .increment(duration_as_micros(compress_started_at.elapsed()));
                    compressed
                } else {
                    chunk.uncompressed_payload
                };
                counter!(STORAGE_WRITE_CHUNK_COMPRESSED_BYTES_TOTAL)
                    .increment(payload.len() as u64);

                let compressed = CompressedChunk {
                    compression: compression_tag,
                    first_index: chunk.first_index,
                    last_index: chunk.last_index,
                    first_slot: chunk.first_slot,
                    last_slot: chunk.last_slot,
                    payload,
                    pending_slots: chunk.pending_slots,
                    pending_finalized: chunk.pending_finalized,
                };

                if tx
                    .send(CompressorOutput::CompressedChunk {
                        seq,
                        chunk: compressed,
                    })
                    .is_err()
                {
                    break;
                }
            }
            Ok(CollectorOutput::Trim { seq, slot, until }) => {
                if tx
                    .send(CompressorOutput::Trim { seq, slot, until })
                    .is_err()
                {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    Ok(())
}

// ── Writer thread ──────────────────────────────────────────────────

/// Long-lived state for the ordered storage append path.
pub(crate) struct SegmentWriter {
    config: SegmentedConfig,
    metadata: MetadataDb,
    catalog: Arc<RwLock<MetadataCatalog>>,
    active_segment: SegmentMeta,
    active_file: File,
}

pub(crate) fn spawn_writer(
    config: SegmentedConfig,
    metadata: MetadataDb,
    catalog: Arc<RwLock<MetadataCatalog>>,
    rx: kanal::Receiver<CompressorOutput>,
) -> anyhow::Result<()> {
    let mut writer = SegmentWriter::open(config, metadata, catalog)?;
    let mut next_seq: u64 = 0;
    let mut pending: BTreeMap<u64, CompressorOutput> = BTreeMap::new();

    while let Ok(output) = rx.recv() {
        pending.insert(output.seq(), output);
        while let Some(item) = pending.remove(&next_seq) {
            match item {
                CompressorOutput::CompressedChunk { chunk, .. } => {
                    writer.append_chunk(chunk)?;
                }
                CompressorOutput::Trim { slot, until, .. } => {
                    writer.handle_trim(slot, until)?;
                }
            }
            next_seq += 1;
        }
    }

    Ok(())
}

impl SegmentWriter {
    fn open(
        config: SegmentedConfig,
        metadata: MetadataDb,
        catalog: Arc<RwLock<MetadataCatalog>>,
    ) -> anyhow::Result<Self> {
        let active_segment = {
            let catalog = catalog.read().expect("segment catalog poisoned");
            catalog
                .segments
                .get(&catalog.state.active_segment_id)
                .copied()
                .context("missing active segment metadata")?
        };
        let active_path = config
            .segments_path
            .join(segment_file_name(active_segment.segment_id));
        let mut active_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&active_path)
            .with_context(|| format!("failed to open active segment: {active_path:?}"))?;
        active_file.seek(SeekFrom::Start(active_segment.file_len))?;

        Ok(Self {
            config,
            metadata,
            catalog,
            active_segment,
            active_file,
        })
    }

    fn handle_trim(&mut self, slot: Slot, until: Option<u64>) -> anyhow::Result<()> {
        let trim_started_at = Instant::now();
        let commit = {
            let catalog = self.catalog.read().expect("segment catalog poisoned");
            build_trim_commit(&catalog, slot, until)
        };
        self.metadata.apply_trim_commit(&commit)?;
        {
            let mut catalog = self.catalog.write().expect("segment catalog poisoned");
            catalog.apply_trim_commit(&commit);
        }

        for segment_id in &commit.deleted_segments {
            let path = self
                .config
                .segments_path
                .join(segment_file_name(*segment_id));
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to delete segment {path:?}"));
                }
            }
        }
        counter!(STORAGE_WRITE_TRIM_MICROS_TOTAL)
            .increment(duration_as_micros(trim_started_at.elapsed()));
        Ok(())
    }

    fn append_chunk(&mut self, chunk: CompressedChunk) -> anyhow::Result<()> {
        let chunk_meta = ChunkMeta {
            segment_id: self.active_segment.segment_id,
            offset: self.active_segment.file_len,
            size: chunk.payload.len() as u32,
            compression: chunk.compression,
            first_index: chunk.first_index,
            last_index: chunk.last_index,
        };

        let append_started_at = Instant::now();
        self.active_file
            .seek(SeekFrom::Start(self.active_segment.file_len))?;
        self.active_file.write_all(&chunk.payload)?;
        counter!(STORAGE_WRITE_APPEND_MICROS_TOTAL)
            .increment(duration_as_micros(append_started_at.elapsed()));

        let fsync_started_at = Instant::now();
        self.active_file.sync_data()?;
        counter!(STORAGE_WRITE_FSYNC_MICROS_TOTAL)
            .increment(duration_as_micros(fsync_started_at.elapsed()));

        let metadata_started_at = Instant::now();
        let commit = {
            let catalog = self.catalog.read().expect("segment catalog poisoned");
            build_chunk_commit(
                &catalog,
                self.active_segment,
                chunk_meta,
                chunk.first_slot,
                chunk.last_slot,
                &chunk.pending_slots,
                &chunk.pending_finalized,
            )?
        };
        self.metadata.apply_chunk_commit(&commit)?;
        {
            let mut catalog = self.catalog.write().expect("segment catalog poisoned");
            catalog.apply_chunk_commit(&commit);
        }
        counter!(STORAGE_WRITE_METADATA_MICROS_TOTAL)
            .increment(duration_as_micros(metadata_started_at.elapsed()));

        counter!(CHANNEL_STORAGE_WRITE_INDEX).absolute(chunk_meta.last_index);
        counter!(STORAGE_SEGMENT_CHUNKS_WRITTEN_TOTAL).increment(1);

        self.active_segment = commit.segment;
        if self.active_segment.file_len >= self.config.segment_target_size as u64 {
            self.rotate_segment()?;
        }
        Ok(())
    }

    fn rotate_segment(&mut self) -> anyhow::Result<()> {
        let rotate_started_at = Instant::now();
        let (new_segment, state) = {
            let catalog = self.catalog.read().expect("segment catalog poisoned");
            let mut state = catalog.state;
            let new_segment_id = state.next_segment_id;
            state.next_segment_id += 1;
            state.active_segment_id = new_segment_id;

            (
                SegmentMeta::empty(
                    new_segment_id,
                    current_unix_ms()?,
                    SEGMENT_HEADER_LEN as u64,
                ),
                state,
            )
        };

        let new_path = self
            .config
            .segments_path
            .join(segment_file_name(new_segment.segment_id));
        let mut new_file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&new_path)
            .with_context(|| format!("failed to create next segment: {new_path:?}"))?;
        write_segment_header(
            &mut new_file,
            SegmentHeader {
                segment_id: new_segment.segment_id,
                created_unix_ms: new_segment.created_unix_ms,
            },
        )?;
        new_file.sync_data()?;

        let sealed_segment = SegmentMeta {
            sealed: true,
            ..self.active_segment
        };
        let commit = RotationCommit {
            sealed_segment,
            new_segment,
            state,
        };
        self.metadata.apply_rotation_commit(commit)?;
        {
            let mut catalog = self.catalog.write().expect("segment catalog poisoned");
            catalog.apply_rotation_commit(commit);
        }

        counter!(STORAGE_WRITE_ROTATE_MICROS_TOTAL)
            .increment(duration_as_micros(rotate_started_at.elapsed()));
        self.active_segment = new_segment;
        self.active_file = new_file;
        Ok(())
    }
}

fn build_chunk_commit(
    catalog: &MetadataCatalog,
    active_segment: SegmentMeta,
    chunk: ChunkMeta,
    first_slot: Slot,
    last_slot: Slot,
    pending_slots: &[PendingSlot],
    pending_finalized: &HashSet<Slot>,
) -> anyhow::Result<MetadataChunkCommit> {
    let mut segment = active_segment;
    if segment.chunk_count == 0 {
        segment.first_slot = first_slot;
        segment.first_index = chunk.first_index;
    }
    segment.last_slot = last_slot;
    segment.last_index = chunk.last_index;
    segment.file_len += u64::from(chunk.size);
    segment.chunk_count += 1;

    let mut new_slots = Vec::with_capacity(pending_slots.len());
    for pending in pending_slots {
        new_slots.push(SlotMeta {
            slot: pending.slot,
            first_index: pending.first_index,
            segment_id: chunk.segment_id,
            finalized: pending_finalized.contains(&pending.slot),
        });
    }

    let mut updated_slots = Vec::with_capacity(pending_finalized.len());
    for slot in pending_finalized {
        if let Some(meta) = catalog.slots.get(slot) {
            if !new_slots.iter().any(|new_meta| new_meta.slot == *slot) {
                let mut meta = *meta;
                meta.finalized = true;
                updated_slots.push(meta);
            }
        }
    }

    Ok(MetadataChunkCommit {
        new_slots,
        updated_slots,
        chunk,
        segment,
        state: catalog.state,
    })
}

fn build_trim_commit(
    catalog: &MetadataCatalog,
    slot: Slot,
    until: Option<u64>,
) -> MetadataTrimCommit {
    let mut state = catalog.state;
    state.trim_floor_slot = state.trim_floor_slot.max(slot);
    if let Some(until) = until {
        state.trim_floor_index = state.trim_floor_index.max(until);
    }

    let deleted_segments = until
        .map(|floor| {
            catalog
                .segments
                .values()
                .filter(|segment| {
                    segment.sealed && segment.chunk_count > 0 && segment.last_index < floor
                })
                .map(|segment| segment.segment_id)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    let deleted_slots = catalog
        .slots
        .values()
        .filter(|meta| deleted_segments.contains(&meta.segment_id))
        .map(|meta| meta.slot)
        .collect::<Vec<_>>();
    let deleted_chunks = catalog
        .chunks
        .iter()
        .filter(|chunk| deleted_segments.contains(&chunk.segment_id))
        .map(|chunk| chunk.first_index)
        .collect::<Vec<_>>();

    MetadataTrimCommit {
        removed_slot: slot,
        deleted_slots,
        deleted_segments,
        deleted_chunks,
        state,
    }
}

fn estimate_record_size(_slot: Slot, message: &ParsedMessage) -> usize {
    message.size().saturating_add(16)
}

fn duration_as_micros(duration: Duration) -> u64 {
    duration.as_micros().min(u64::MAX as u128) as u64
}

fn current_unix_ms() -> anyhow::Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_millis() as u64)
}

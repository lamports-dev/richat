use {
    super::{
        metadata::{
            ChunkMeta, MetadataCatalog, MetadataChunkCommit, MetadataDb, MetadataTrimCommit,
            RotationCommit, SegmentMeta, SlotMeta,
        },
        segment_format::{
            CHUNK_HEADER_LEN, ChunkHeader, SEGMENT_HEADER_LEN, SegmentHeader, append_record,
            chunk_crc32, segment_file_name, write_segment_header,
        },
        segmented::SegmentedConfig,
    },
    crate::{
        channel::ParsedMessage,
        metrics::{
            CHANNEL_STORAGE_WRITE_INDEX, CHANNEL_STORAGE_WRITE_SER_INDEX,
            STORAGE_SEGMENT_ACTIVE_ID, STORAGE_SEGMENT_ACTIVE_SIZE_BYTES,
            STORAGE_SEGMENT_CHUNKS_WRITTEN_TOTAL, STORAGE_SEGMENT_ROTATIONS_TOTAL,
            STORAGE_SEGMENTS_DELETED_TOTAL,
        },
    },
    ::metrics::{counter, gauge},
    anyhow::Context,
    kanal::ReceiveErrorTimeout,
    richat_proto::geyser::SlotStatus,
    solana_clock::Slot,
    std::{
        collections::HashSet,
        fs::{File, OpenOptions},
        io::{Seek, SeekFrom, Write},
        sync::{Arc, RwLock},
        time::{Instant, SystemTime, UNIX_EPOCH},
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
    record_ordinal: u32,
}

/// Long-lived state for the single threaded append path.
///
/// It batches records into a chunk, writes compressed payloads into the active
/// segment file, and keeps metadata updates aligned with those writes.
pub(crate) struct SegmentWriter {
    config: SegmentedConfig,
    metadata: MetadataDb,
    catalog: Arc<RwLock<MetadataCatalog>>,
    active_segment: SegmentMeta,
    active_file: File,
    compressor: Compressor<'static>,
    chunk_buf: Vec<u8>,
    record_buf: Vec<u8>,
    pending_slots: Vec<PendingSlot>,
    pending_finalized: HashSet<Slot>,
    chunk_started_at: Option<Instant>,
    chunk_first_index: u64,
    chunk_last_index: u64,
    chunk_first_slot: Slot,
    chunk_last_slot: Slot,
    chunk_record_count: u32,
}

pub(crate) fn spawn_writer(
    config: SegmentedConfig,
    metadata: MetadataDb,
    catalog: Arc<RwLock<MetadataCatalog>>,
    rx: kanal::Receiver<WriterCommand>,
) -> anyhow::Result<()> {
    let mut writer = SegmentWriter::open(config, metadata, catalog)?;
    loop {
        if writer.has_pending_chunk() {
            match rx.recv_timeout(writer.config.chunk_flush_delay) {
                Ok(command) => writer.handle(command)?,
                Err(ReceiveErrorTimeout::Timeout) => writer.flush_chunk()?,
                Err(ReceiveErrorTimeout::SendClosed | ReceiveErrorTimeout::Closed) => {
                    writer.flush_chunk()?;
                    break;
                }
            }
        } else {
            match rx.recv() {
                Ok(command) => writer.handle(command)?,
                Err(_) => break,
            }
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
        let segment_zstd_level = config.segment_zstd_level;

        Ok(Self {
            config,
            metadata,
            catalog,
            active_segment,
            active_file,
            compressor: Compressor::new(segment_zstd_level)
                .context("failed to create zstd compressor")?,
            chunk_buf: Vec::with_capacity(4 * 1024 * 1024),
            record_buf: Vec::with_capacity(256 * 1024),
            pending_slots: Vec::new(),
            pending_finalized: HashSet::new(),
            chunk_started_at: None,
            chunk_first_index: 0,
            chunk_last_index: 0,
            chunk_first_slot: 0,
            chunk_last_slot: 0,
            chunk_record_count: 0,
        })
    }

    fn handle(&mut self, command: WriterCommand) -> anyhow::Result<()> {
        match command {
            WriterCommand::PushMessage {
                init,
                slot,
                head,
                index,
                message,
            } => self.handle_push(init, slot, head, index, message),
            WriterCommand::RemoveReplay { slot, until } => self.handle_trim(slot, until),
        }
    }

    fn handle_push(
        &mut self,
        init: bool,
        slot: Slot,
        head: u64,
        index: u64,
        message: ParsedMessage,
    ) -> anyhow::Result<()> {
        append_record(slot, &message, &mut self.record_buf, &mut self.chunk_buf);
        if self.chunk_record_count == 0 {
            self.chunk_started_at = Some(Instant::now());
            self.chunk_first_index = index;
            self.chunk_first_slot = slot;
        }
        self.chunk_last_index = index;
        self.chunk_last_slot = slot;

        if init {
            self.pending_slots.push(PendingSlot {
                slot,
                first_index: head,
                record_ordinal: self.chunk_record_count,
            });
        }
        if let ParsedMessage::Slot(message) = &message {
            if message.status() == SlotStatus::SlotFinalized {
                self.pending_finalized.insert(slot);
            }
        }

        self.chunk_record_count += 1;
        counter!(CHANNEL_STORAGE_WRITE_SER_INDEX).absolute(index);
        if self.chunk_buf.len() >= self.config.chunk_target_size {
            self.flush_chunk()?;
        }
        Ok(())
    }

    fn handle_trim(&mut self, slot: Slot, until: Option<u64>) -> anyhow::Result<()> {
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
        if !commit.deleted_segments.is_empty() {
            counter!(STORAGE_SEGMENTS_DELETED_TOTAL)
                .increment(commit.deleted_segments.len() as u64);
        }
        Ok(())
    }

    fn flush_chunk(&mut self) -> anyhow::Result<()> {
        if self.chunk_record_count == 0 {
            return Ok(());
        }

        let compressed = self
            .compressor
            .compress(&self.chunk_buf)
            .context("failed to compress chunk")?;
        let crc32 = chunk_crc32(&self.chunk_buf);
        let chunk_ordinal = self.active_segment.chunk_count;
        let chunk_header = ChunkHeader {
            chunk_ordinal,
            first_index: self.chunk_first_index,
            last_index: self.chunk_last_index,
            first_slot: self.chunk_first_slot,
            last_slot: self.chunk_last_slot,
            record_count: self.chunk_record_count,
            compressed_size: compressed.len() as u32,
            uncompressed_size: self.chunk_buf.len() as u32,
            crc32,
        };
        let chunk_meta = ChunkMeta {
            segment_id: self.active_segment.segment_id,
            chunk_ordinal,
            file_offset: self.active_segment.file_len,
            first_slot: chunk_header.first_slot,
            last_slot: chunk_header.last_slot,
            first_index: chunk_header.first_index,
            last_index: chunk_header.last_index,
            record_count: chunk_header.record_count,
            compressed_size: chunk_header.compressed_size,
            uncompressed_size: chunk_header.uncompressed_size,
            crc32: chunk_header.crc32,
        };

        self.active_file
            .seek(SeekFrom::Start(self.active_segment.file_len))?;
        self.active_file.write_all(&chunk_header.encode())?;
        self.active_file.write_all(&compressed)?;
        if self.config.segment_fsync {
            self.active_file.sync_data()?;
        }

        let commit = {
            let catalog = self.catalog.read().expect("segment catalog poisoned");
            build_chunk_commit(
                &catalog,
                self.active_segment,
                chunk_meta,
                &self.pending_slots,
                &self.pending_finalized,
            )?
        };
        self.metadata.apply_chunk_commit(&commit)?;
        {
            let mut catalog = self.catalog.write().expect("segment catalog poisoned");
            catalog.apply_chunk_commit(&commit);
        }

        counter!(CHANNEL_STORAGE_WRITE_INDEX).absolute(chunk_meta.last_index);
        gauge!(STORAGE_SEGMENT_ACTIVE_ID).set(self.active_segment.segment_id as f64);
        gauge!(STORAGE_SEGMENT_ACTIVE_SIZE_BYTES).set(commit.segment.file_len as f64);
        counter!(STORAGE_SEGMENT_CHUNKS_WRITTEN_TOTAL).increment(1);

        self.active_segment = commit.segment;
        self.chunk_buf.clear();
        self.pending_slots.clear();
        self.pending_finalized.clear();
        self.chunk_started_at = None;
        self.chunk_record_count = 0;

        if self.active_segment.file_len >= self.config.segment_target_size as u64 {
            self.rotate_segment()?;
        }
        Ok(())
    }

    fn has_pending_chunk(&self) -> bool {
        self.chunk_record_count > 0
    }

    fn rotate_segment(&mut self) -> anyhow::Result<()> {
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
        if self.config.segment_fsync {
            new_file.sync_data()?;
        }

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

        counter!(STORAGE_SEGMENT_ROTATIONS_TOTAL).increment(1);
        self.active_segment = new_segment;
        self.active_file = new_file;
        self.compressor = Compressor::new(self.config.segment_zstd_level)
            .context("failed to recreate zstd compressor")?;
        Ok(())
    }
}

fn build_chunk_commit(
    catalog: &MetadataCatalog,
    active_segment: SegmentMeta,
    chunk: ChunkMeta,
    pending_slots: &[PendingSlot],
    pending_finalized: &HashSet<Slot>,
) -> anyhow::Result<MetadataChunkCommit> {
    let mut segment = active_segment;
    if segment.chunk_count == 0 {
        segment.first_slot = chunk.first_slot;
        segment.first_index = chunk.first_index;
    }
    segment.last_slot = chunk.last_slot;
    segment.last_index = chunk.last_index;
    segment.file_len += CHUNK_HEADER_LEN as u64 + u64::from(chunk.compressed_size);
    segment.chunk_count += 1;

    let mut new_slots = Vec::with_capacity(pending_slots.len());
    for pending in pending_slots {
        new_slots.push(SlotMeta {
            slot: pending.slot,
            first_index: pending.first_index,
            segment_id: chunk.segment_id,
            chunk_ordinal: chunk.chunk_ordinal,
            record_ordinal: pending.record_ordinal,
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
        .map(|chunk| (chunk.segment_id, chunk.chunk_ordinal))
        .collect::<Vec<_>>();

    MetadataTrimCommit {
        removed_slot: slot,
        deleted_slots,
        deleted_segments,
        deleted_chunks,
        state,
    }
}

fn current_unix_ms() -> anyhow::Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_millis() as u64)
}

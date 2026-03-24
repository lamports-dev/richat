use {
    crate::{
        channel::ParsedMessage,
        config::ConfigStorage,
        metrics::{
            CHANNEL_STORAGE_WRITE_INDEX, CHANNEL_STORAGE_WRITE_SER_INDEX,
            STORAGE_REPLAY_COMPRESSED_BYTES_TOTAL, STORAGE_REPLAY_DECOMPRESSED_BYTES_TOTAL,
            STORAGE_SEGMENT_CHUNKS_WRITTEN_TOTAL, STORAGE_WRITE_APPEND_MICROS_TOTAL,
            STORAGE_WRITE_CHUNK_COMPRESSED_BYTES_TOTAL,
            STORAGE_WRITE_CHUNK_UNCOMPRESSED_BYTES_TOTAL, STORAGE_WRITE_COMPRESS_MICROS_TOTAL,
            STORAGE_WRITE_FSYNC_MICROS_TOTAL, STORAGE_WRITE_METADATA_MICROS_TOTAL,
            STORAGE_WRITE_ROTATE_MICROS_TOTAL, STORAGE_WRITE_TRIM_MICROS_TOTAL,
        },
        storage::metadata::{
            ChunkMeta, Metadata, MetadataChunkCommit, MetadataMirror, MetadataTrimCommit,
            RotationCommit, SegmentMeta, SlotMeta,
        },
        util::{SpawnedThread, SpawnedThreads},
    },
    ::metrics::counter,
    anyhow::{Context, anyhow},
    prost::{
        bytes::BufMut,
        encoding::{decode_varint, encode_varint},
    },
    richat_filter::{
        filter::{FilteredUpdate, FilteredUpdateFilters},
        message::{Message, MessageParserEncoding, MessageRef},
    },
    richat_proto::geyser::SlotStatus,
    solana_clock::Slot,
    std::{
        borrow::Cow,
        collections::{BTreeMap, HashSet},
        fs::{File, OpenOptions},
        io::{Read, Seek, SeekFrom, Write},
        path::PathBuf,
        thread,
        time::{Duration, Instant},
    },
    zstd::{bulk::Compressor, stream::decode_all as zstd_decode_all},
};

/// Compression algorithm used for a chunk payload.
///
/// Stored as a single byte in chunk metadata.  The writer picks the
/// algorithm from config; the reader uses the tag stored in metadata
/// so it can always decode regardless of the current config.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkCompression {
    None,
    Zstd(i32),
}

impl ChunkCompression {
    const TAG_NONE: u8 = 0;
    const TAG_ZSTD: u8 = 1;

    const fn tag(self) -> u8 {
        match self {
            Self::None => Self::TAG_NONE,
            Self::Zstd(_) => Self::TAG_ZSTD,
        }
    }

    fn from_tag(tag: u8) -> anyhow::Result<Self> {
        match tag {
            Self::TAG_NONE => Ok(Self::None),
            Self::TAG_ZSTD => Ok(Self::Zstd(0)),
            other => anyhow::bail!("unsupported chunk compression tag: {other}"),
        }
    }
}

struct MessageRecordCodec;

impl MessageRecordCodec {
    fn encode(slot: Slot, message: &ParsedMessage, buf: &mut impl BufMut) {
        let message_ref: MessageRef = message.into();
        let filtered = FilteredUpdate {
            filters: FilteredUpdateFilters::new(),
            filtered_update: message_ref.into(),
        };
        encode_varint(slot, buf);
        filtered.encode(buf);
    }

    fn decode(mut slice: &[u8], parser: MessageParserEncoding) -> anyhow::Result<(Slot, Message)> {
        let slot =
            decode_varint(&mut slice).context("invalid slice size, failed to decode slot")?;
        let message =
            Message::parse(Cow::Borrowed(slice), parser).context("failed to parse message")?;
        Ok((slot, message))
    }
}

fn next_record<'a>(slice: &mut &'a [u8]) -> anyhow::Result<&'a [u8]> {
    let record_len = decode_varint(slice).context("failed to decode record len")? as usize;
    let (record, rest) = slice
        .split_at_checked(record_len)
        .context("record extends past chunk boundary")?;
    *slice = rest;
    Ok(record)
}

fn segment_file_name(segment_id: u64) -> String {
    format!("{segment_id:012}.seg")
}

// ── Segment reader ─────────────────────────────────────────────────

/// A single decompressed chunk ready for record decoding.
pub struct DecompressedChunk {
    first_index: u64,
    skip: usize,
    data: Vec<u8>,
}

impl DecompressedChunk {
    pub fn decode_records(
        self,
        parser: MessageParserEncoding,
    ) -> anyhow::Result<Vec<(u64, ParsedMessage)>> {
        let mut records = Vec::new();
        let mut slice = self.data.as_slice();
        let mut ordinal = 0usize;
        while !slice.is_empty() {
            let record = next_record(&mut slice)?;
            if ordinal >= self.skip {
                let (_slot, message) = MessageRecordCodec::decode(record, parser)?;
                records.push((self.first_index + ordinal as u64, message.into()));
            }
            ordinal += 1;
        }
        Ok(records)
    }
}

/// Sequential replay reader over chunk metadata and segment files.
///
/// Each iteration reads, decompresses, and validates one chunk, yielding a
/// [`DecompressedChunk`] that the caller can decode on demand.
pub struct SegmentReader {
    segments_path: PathBuf,
    chunks: Vec<ChunkMeta>,
    next_chunk: usize,
    next_index: u64,
    current_segment_id: Option<u64>,
    current_file: Option<File>,
    failed: bool,
}

impl SegmentReader {
    pub const fn new(segments_path: PathBuf, chunks: Vec<ChunkMeta>, start_index: u64) -> Self {
        Self {
            segments_path,
            chunks,
            next_chunk: 0,
            next_index: start_index,
            current_segment_id: None,
            current_file: None,
            failed: false,
        }
    }

    fn load_next_chunk(&mut self) -> anyhow::Result<DecompressedChunk> {
        let chunk = self.chunks[self.next_chunk];
        self.next_chunk += 1;

        let file = self.open_segment(chunk.segment_id)?;
        file.seek(SeekFrom::Start(chunk.offset))?;

        let mut payload = vec![0; chunk.size as usize];
        file.read_exact(&mut payload)?;
        counter!(STORAGE_REPLAY_COMPRESSED_BYTES_TOTAL).increment(payload.len() as u64);

        let compression = ChunkCompression::from_tag(chunk.compression)
            .context("unsupported chunk compression")?;
        let uncompressed = match compression {
            ChunkCompression::None => payload,
            ChunkCompression::Zstd(_) => {
                zstd_decode_all(payload.as_slice()).context("failed to decompress chunk")?
            }
        };
        counter!(STORAGE_REPLAY_DECOMPRESSED_BYTES_TOTAL).increment(uncompressed.len() as u64);

        let skip = self.next_index.saturating_sub(chunk.first_index) as usize;
        self.next_index = chunk.last_index + 1;

        Ok(DecompressedChunk {
            first_index: chunk.first_index,
            skip,
            data: uncompressed,
        })
    }

    fn open_segment(&mut self, segment_id: u64) -> anyhow::Result<&mut File> {
        if self.current_segment_id != Some(segment_id) {
            let path = self.segments_path.join(segment_file_name(segment_id));
            let file = File::open(&path)
                .with_context(|| format!("failed to open segment file: {path:?}"))?;
            self.current_segment_id = Some(segment_id);
            self.current_file = Some(file);
        }

        self.current_file
            .as_mut()
            .context("segment file should be opened")
    }
}

impl Iterator for SegmentReader {
    type Item = anyhow::Result<DecompressedChunk>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed || self.next_chunk >= self.chunks.len() {
            return None;
        }
        match self.load_next_chunk() {
            Ok(chunk) => Some(Ok(chunk)),
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

// ── Segment writer ─────────────────────────────────────────────────

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
    pending_slots: Vec<PendingSlot>,
    pending_finalized: HashSet<Slot>,
}

impl PendingChunkMeta {
    fn push(&mut self, init: bool, slot: Slot, head: u64, index: u64, message: &ParsedMessage) {
        if self.record_count == 0 {
            self.first_index = index;
        }
        self.last_index = index;
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

enum CollectorOutput {
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

struct SerializedChunk {
    first_index: u64,
    last_index: u64,
    uncompressed_payload: Vec<u8>,
    pending_slots: Vec<PendingSlot>,
    pending_finalized: HashSet<Slot>,
}

enum CompressorOutput {
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
struct CompressedChunk {
    compression: u8,
    first_index: u64,
    last_index: u64,
    payload: Vec<u8>,
    pending_slots: Vec<PendingSlot>,
    pending_finalized: HashSet<Slot>,
}

// ── Collector thread ───────────────────────────────────────────────

fn spawn_collector(
    chunk_target_size: usize,
    serialize_affinity: Option<Vec<usize>>,
    rx: kanal::Receiver<WriterCommand>,
    tx: kanal::Sender<CollectorOutput>,
) -> anyhow::Result<SpawnedThread> {
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
                        encode_varint(record_buf.len() as u64, &mut chunk_buf);
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

fn spawn_compressor_pool(
    threads: usize,
    chunk_compression: Option<ChunkCompression>,
    affinity: Option<Vec<usize>>,
    rx: kanal::Receiver<CollectorOutput>,
    tx: kanal::Sender<CompressorOutput>,
) -> anyhow::Result<Vec<SpawnedThread>> {
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
struct SegmentWriter {
    config: SegmentedConfig,
    metadata: Metadata,
    active_segment: SegmentMeta,
    active_file: File,
}

fn spawn_writer(
    config: SegmentedConfig,
    metadata: Metadata,
    rx: kanal::Receiver<CompressorOutput>,
) -> anyhow::Result<()> {
    let mut writer = SegmentWriter::open(config, metadata)?;
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
    fn open(config: SegmentedConfig, metadata: Metadata) -> anyhow::Result<Self> {
        let active_segment = {
            let catalog = metadata.catalog();
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
            active_segment,
            active_file,
        })
    }

    fn handle_trim(&mut self, slot: Slot, until: Option<u64>) -> anyhow::Result<()> {
        let trim_started_at = Instant::now();
        let commit = {
            let catalog = self.metadata.catalog();
            build_trim_commit(&catalog, slot, until)
        };
        self.metadata.apply_trim_commit(&commit)?;

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
            let catalog = self.metadata.catalog();
            build_chunk_commit(
                &catalog,
                self.active_segment,
                chunk_meta,
                &chunk.pending_slots,
                &chunk.pending_finalized,
            )?
        };
        self.metadata.apply_chunk_commit(&commit)?;
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
            let catalog = self.metadata.catalog();
            let mut state = catalog.state;
            let new_segment_id = state.next_segment_id;
            state.next_segment_id += 1;
            state.active_segment_id = new_segment_id;
            (SegmentMeta::empty(new_segment_id, 0), state)
        };

        let new_path = self
            .config
            .segments_path
            .join(segment_file_name(new_segment.segment_id));
        let new_file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&new_path)
            .with_context(|| format!("failed to create next segment: {new_path:?}"))?;

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

        counter!(STORAGE_WRITE_ROTATE_MICROS_TOTAL)
            .increment(duration_as_micros(rotate_started_at.elapsed()));
        self.active_segment = new_segment;
        self.active_file = new_file;
        Ok(())
    }
}

fn build_chunk_commit(
    catalog: &MetadataMirror,
    active_segment: SegmentMeta,
    chunk: ChunkMeta,
    pending_slots: &[PendingSlot],
    pending_finalized: &HashSet<Slot>,
) -> anyhow::Result<MetadataChunkCommit> {
    let mut segment = active_segment;
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
    catalog: &MetadataMirror,
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

// ── Segmented storage ──────────────────────────────────────────────

/// Runtime storage settings derived from [`ConfigStorage`] and normalized into
/// concrete filesystem paths and write thresholds.
#[derive(Debug, Clone)]
struct SegmentedConfig {
    metadata_path: PathBuf,
    segments_path: PathBuf,
    serialize_affinity: Option<Vec<usize>>,
    write_affinity: Option<Vec<usize>>,
    segment_target_size: usize,
    chunk_target_size: usize,
    chunk_compression: Option<ChunkCompression>,
    compressor_threads: usize,
    compressor_affinity: Option<Vec<usize>>,
    compressor_channel_size: usize,
    collector_channel_size: usize,
    writer_channel_size: usize,
}

impl SegmentedConfig {
    fn from_config(config: &ConfigStorage) -> Self {
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

/// Initializes segment pipeline: opens metadata, creates channels, spawns
/// collector/compressor/writer threads.
pub(crate) fn open_storage(
    config: ConfigStorage,
) -> anyhow::Result<(Metadata, kanal::Sender<WriterCommand>, SpawnedThreads)> {
    let config = SegmentedConfig::from_config(&config);
    std::fs::create_dir_all(&config.segments_path)
        .with_context(|| format!("failed to create segments path: {:?}", config.segments_path))?;

    let metadata = Metadata::open(&config.metadata_path, config.segments_path.clone())?;

    {
        let catalog = metadata.catalog();
        if catalog.state.active_segment_id == 0 {
            let mut state = catalog.state;
            state.active_segment_id = state.next_segment_id;
            state.next_segment_id += 1;
            let segment = SegmentMeta::empty(state.active_segment_id, 0);
            drop(catalog);
            create_segment_file(&config, &segment)?;
            metadata.initialize_empty(&state, segment)?;
        }
    }

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
    let writer_metadata = metadata.clone();
    let writer_affinity = config.write_affinity.clone();
    let writer_jh = thread::Builder::new()
        .name("richatStrgWrt".to_owned())
        .spawn(move || {
            if let Some(cpus) = writer_affinity {
                affinity_linux::set_thread_affinity(cpus.into_iter())
                    .expect("failed to set affinity");
            }
            spawn_writer(writer_config, writer_metadata, compressor_rx)
        })?;
    let mut threads = vec![collector_thread];
    threads.extend(compressor_threads);
    threads.push(("richatStrgWrt".to_owned(), Some(writer_jh)));

    Ok((metadata, write_tx, threads))
}

/// Reads decompressed chunks starting from `index`.
pub fn read_messages_from_index(metadata: &Metadata, index: u64) -> SegmentReader {
    let chunks = {
        let catalog = metadata.catalog();
        let start = catalog
            .chunks
            .partition_point(|chunk| chunk.last_index < index);
        catalog.chunks[start..].to_vec()
    };

    SegmentReader::new(metadata.segments_path().to_path_buf(), chunks, index)
}

fn create_segment_file(config: &SegmentedConfig, segment: &SegmentMeta) -> anyhow::Result<()> {
    let path = config
        .segments_path
        .join(segment_file_name(segment.segment_id));
    OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("failed to create segment file: {path:?}"))?;
    Ok(())
}

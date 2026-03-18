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
            STORAGE_SEGMENT_CHUNKS_WRITTEN_TOTAL, STORAGE_WRITE_APPEND_MICROS_TOTAL,
            STORAGE_WRITE_CHUNK_COMPRESSED_BYTES_TOTAL,
            STORAGE_WRITE_CHUNK_UNCOMPRESSED_BYTES_TOTAL, STORAGE_WRITE_COMPRESS_MICROS_TOTAL,
            STORAGE_WRITE_FSYNC_MICROS_TOTAL, STORAGE_WRITE_METADATA_MICROS_TOTAL,
            STORAGE_WRITE_QUEUE_BYTES, STORAGE_WRITE_QUEUE_DEQUEUED_BYTES_TOTAL,
            STORAGE_WRITE_QUEUE_DEQUEUED_TOTAL, STORAGE_WRITE_QUEUE_ENQUEUED_BYTES_TOTAL,
            STORAGE_WRITE_QUEUE_ENQUEUED_TOTAL, STORAGE_WRITE_QUEUE_WAIT_MICROS_TOTAL,
            STORAGE_WRITE_ROTATE_MICROS_TOTAL, STORAGE_WRITE_SERIALIZE_MICROS_TOTAL,
            STORAGE_WRITE_TRIM_MICROS_TOTAL,
        },
        util::SpawnedThreads,
    },
    ::metrics::{counter, gauge},
    anyhow::{Context, anyhow},
    kanal::{ReceiveError, ReceiveErrorTimeout},
    richat_proto::geyser::SlotStatus,
    solana_clock::Slot,
    std::{
        collections::{BTreeMap, HashSet},
        fs::{File, OpenOptions},
        io::{Seek, SeekFrom, Write},
        sync::{
            Arc, RwLock,
            atomic::{AtomicU64, Ordering},
        },
        thread,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
    zstd::bulk::Compressor,
};

#[derive(Debug)]
pub(crate) struct WriterCommand {
    enqueued_at: Instant,
    approx_bytes: usize,
    kind: WriterCommandKind,
}

impl WriterCommand {
    pub(crate) fn push_message(
        init: bool,
        slot: Slot,
        head: u64,
        index: u64,
        message: ParsedMessage,
    ) -> Self {
        let approx_bytes = message.size();
        Self {
            enqueued_at: Instant::now(),
            approx_bytes,
            kind: WriterCommandKind::PushMessage {
                init,
                slot,
                head,
                index,
                message,
            },
        }
    }

    pub(crate) fn remove_replay(slot: Slot, until: Option<u64>) -> Self {
        Self {
            enqueued_at: Instant::now(),
            approx_bytes: 0,
            kind: WriterCommandKind::RemoveReplay { slot, until },
        }
    }

    pub(crate) const fn approx_bytes(&self) -> usize {
        self.approx_bytes
    }

    fn wait_micros(&self) -> u64 {
        duration_as_micros(self.enqueued_at.elapsed())
    }

    fn into_kind(self) -> WriterCommandKind {
        self.kind
    }
}

#[derive(Debug)]
pub(crate) enum WriterCommandKind {
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

#[derive(Debug, Default)]
pub(crate) struct StorageWriteQueueMetrics {
    queued_bytes: AtomicU64,
}

impl StorageWriteQueueMetrics {
    pub(crate) fn publish_current(&self) {
        gauge!(STORAGE_WRITE_QUEUE_BYTES).set(self.queued_bytes.load(Ordering::Relaxed) as f64);
    }

    pub(crate) fn begin_enqueue(&self, approx_bytes: usize) {
        let approx_bytes = approx_bytes as u64;
        let queued_bytes =
            self.queued_bytes.fetch_add(approx_bytes, Ordering::Relaxed) + approx_bytes;
        gauge!(STORAGE_WRITE_QUEUE_BYTES).set(queued_bytes as f64);
    }

    pub(crate) fn commit_enqueue(&self, approx_bytes: usize) {
        let approx_bytes = approx_bytes as u64;
        counter!(STORAGE_WRITE_QUEUE_ENQUEUED_TOTAL).increment(1);
        counter!(STORAGE_WRITE_QUEUE_ENQUEUED_BYTES_TOTAL).increment(approx_bytes);
    }

    pub(crate) fn cancel_enqueue(&self, approx_bytes: usize) {
        let approx_bytes = approx_bytes as u64;
        let queued_bytes =
            self.queued_bytes.fetch_sub(approx_bytes, Ordering::Relaxed) - approx_bytes;
        gauge!(STORAGE_WRITE_QUEUE_BYTES).set(queued_bytes as f64);
    }

    pub(crate) fn record_dequeue(&self, command: &WriterCommand) {
        let approx_bytes = command.approx_bytes() as u64;
        let queued_bytes =
            self.queued_bytes.fetch_sub(approx_bytes, Ordering::Relaxed) - approx_bytes;

        counter!(STORAGE_WRITE_QUEUE_DEQUEUED_TOTAL).increment(1);
        counter!(STORAGE_WRITE_QUEUE_DEQUEUED_BYTES_TOTAL).increment(approx_bytes);
        counter!(STORAGE_WRITE_QUEUE_WAIT_MICROS_TOTAL).increment(command.wait_micros());
        gauge!(STORAGE_WRITE_QUEUE_BYTES).set(queued_bytes as f64);
    }
}

#[derive(Debug)]
struct PendingSlot {
    slot: Slot,
    first_index: u64,
    record_ordinal: u32,
}

#[derive(Debug)]
struct PendingRecord {
    slot: Slot,
    message: ParsedMessage,
}

#[derive(Debug)]
struct PendingChunk {
    started_at: Instant,
    estimated_uncompressed_size: usize,
    first_index: u64,
    last_index: u64,
    first_slot: Slot,
    last_slot: Slot,
    records: Vec<PendingRecord>,
    pending_slots: Vec<PendingSlot>,
    pending_finalized: HashSet<Slot>,
}

impl PendingChunk {
    fn new(slot: Slot, index: u64) -> Self {
        Self {
            started_at: Instant::now(),
            estimated_uncompressed_size: 0,
            first_index: index,
            last_index: index,
            first_slot: slot,
            last_slot: slot,
            records: Vec::new(),
            pending_slots: Vec::new(),
            pending_finalized: HashSet::new(),
        }
    }

    fn push(&mut self, init: bool, slot: Slot, head: u64, index: u64, message: ParsedMessage) {
        let record_ordinal = self.records.len() as u32;
        self.last_index = index;
        self.last_slot = slot;
        self.estimated_uncompressed_size = self
            .estimated_uncompressed_size
            .saturating_add(estimate_record_size(slot, &message));

        if init {
            self.pending_slots.push(PendingSlot {
                slot,
                first_index: head,
                record_ordinal,
            });
        }
        if let ParsedMessage::Slot(message) = &message {
            if message.status() == SlotStatus::SlotFinalized {
                self.pending_finalized.insert(slot);
            }
        }

        self.records.push(PendingRecord { slot, message });
    }

    const fn should_flush(&self, target_size: usize) -> bool {
        self.estimated_uncompressed_size >= target_size
    }

    fn is_flush_due(&self, delay: Duration) -> bool {
        self.started_at.elapsed() >= delay
    }

    fn into_job(self, chunk_id: u64) -> CompressionJob {
        CompressionJob {
            chunk_id,
            first_index: self.first_index,
            last_index: self.last_index,
            first_slot: self.first_slot,
            last_slot: self.last_slot,
            records: self.records,
            pending_slots: self.pending_slots,
            pending_finalized: self.pending_finalized,
            estimated_uncompressed_size: self.estimated_uncompressed_size,
        }
    }
}

#[derive(Debug)]
pub(crate) struct CompressionJob {
    chunk_id: u64,
    first_index: u64,
    last_index: u64,
    first_slot: Slot,
    last_slot: Slot,
    records: Vec<PendingRecord>,
    pending_slots: Vec<PendingSlot>,
    pending_finalized: HashSet<Slot>,
    estimated_uncompressed_size: usize,
}

#[derive(Debug)]
pub(crate) struct CompressedChunk {
    chunk_id: u64,
    first_index: u64,
    last_index: u64,
    first_slot: Slot,
    last_slot: Slot,
    record_count: u32,
    uncompressed_size: u32,
    compressed: Vec<u8>,
    crc32: u32,
    pending_slots: Vec<PendingSlot>,
    pending_finalized: HashSet<Slot>,
}

/// Long-lived state for the ordered storage append path.
///
/// It batches records into chunks, sends chunk serialization / compression to a
/// worker pool, and appends completed chunks to the active segment strictly in
/// chunk-id order.
pub(crate) struct SegmentWriter {
    config: SegmentedConfig,
    metadata: MetadataDb,
    catalog: Arc<RwLock<MetadataCatalog>>,
    active_segment: SegmentMeta,
    active_file: File,
    pending_chunk: Option<PendingChunk>,
    compression_txs: Vec<kanal::Sender<CompressionJob>>,
    compression_rx: kanal::Receiver<anyhow::Result<CompressedChunk>>,
    ready_chunks: BTreeMap<u64, CompressedChunk>,
    next_worker: usize,
    next_chunk_id: u64,
    next_append_chunk_id: u64,
    inflight_chunks: usize,
}

pub(crate) fn spawn_serialize_workers(
    config: &SegmentedConfig,
    results_tx: kanal::Sender<anyhow::Result<CompressedChunk>>,
) -> anyhow::Result<(Vec<kanal::Sender<CompressionJob>>, SpawnedThreads)> {
    let workers = config.serialize_workers.max(1);
    let mut job_txs = Vec::with_capacity(workers);
    let mut threads = Vec::with_capacity(workers);

    for index in 0..workers {
        let (job_tx, job_rx) = kanal::unbounded();
        let results_tx = results_tx.clone();
        let cpus = thread_affinity_for_worker(&config.serialize_affinity, workers, index);
        let th_name = format!("richatStrgSer{index:02}");
        let segment_zstd_level = config.segment_zstd_level;
        let jh = thread::Builder::new()
            .name(th_name.clone())
            .spawn(move || {
                if let Some(cpus) = cpus {
                    affinity_linux::set_thread_affinity(cpus.into_iter())
                        .expect("failed to set affinity");
                }
                run_serialize_worker(segment_zstd_level, job_rx, results_tx)
            })?;
        job_txs.push(job_tx);
        threads.push((th_name, Some(jh)));
    }

    Ok((job_txs, threads))
}

fn run_serialize_worker(
    segment_zstd_level: i32,
    rx: kanal::Receiver<CompressionJob>,
    tx: kanal::Sender<anyhow::Result<CompressedChunk>>,
) -> anyhow::Result<()> {
    let mut compressor =
        Compressor::new(segment_zstd_level).context("failed to create zstd compressor")?;
    let mut record_buf = Vec::with_capacity(256 * 1024);
    let mut chunk_buf = Vec::with_capacity(4 * 1024 * 1024);

    loop {
        let job = match rx.recv() {
            Ok(job) => job,
            Err(_) => break,
        };

        chunk_buf.clear();
        chunk_buf.reserve(job.estimated_uncompressed_size);

        let serialize_started_at = Instant::now();
        for record in &job.records {
            append_record(
                record.slot,
                &record.message,
                &mut record_buf,
                &mut chunk_buf,
            );
        }
        counter!(STORAGE_WRITE_SERIALIZE_MICROS_TOTAL)
            .increment(duration_as_micros(serialize_started_at.elapsed()));

        let compress_started_at = Instant::now();
        let compressed = match compressor.compress(&chunk_buf) {
            Ok(compressed) => compressed,
            Err(error) => {
                let _ = tx.send(Err(error).context("failed to compress chunk"));
                break;
            }
        };
        counter!(STORAGE_WRITE_COMPRESS_MICROS_TOTAL)
            .increment(duration_as_micros(compress_started_at.elapsed()));
        counter!(STORAGE_WRITE_CHUNK_UNCOMPRESSED_BYTES_TOTAL).increment(chunk_buf.len() as u64);
        counter!(STORAGE_WRITE_CHUNK_COMPRESSED_BYTES_TOTAL).increment(compressed.len() as u64);

        let result = CompressedChunk {
            chunk_id: job.chunk_id,
            first_index: job.first_index,
            last_index: job.last_index,
            first_slot: job.first_slot,
            last_slot: job.last_slot,
            record_count: job.records.len() as u32,
            uncompressed_size: chunk_buf.len() as u32,
            crc32: chunk_crc32(&chunk_buf),
            compressed,
            pending_slots: job.pending_slots,
            pending_finalized: job.pending_finalized,
        };

        if tx.send(Ok(result)).is_err() {
            break;
        }
    }

    Ok(())
}

pub(crate) fn spawn_writer(
    config: SegmentedConfig,
    metadata: MetadataDb,
    catalog: Arc<RwLock<MetadataCatalog>>,
    queue_metrics: Arc<StorageWriteQueueMetrics>,
    compression_txs: Vec<kanal::Sender<CompressionJob>>,
    compression_rx: kanal::Receiver<anyhow::Result<CompressedChunk>>,
    rx: kanal::Receiver<WriterCommand>,
) -> anyhow::Result<()> {
    let mut writer =
        SegmentWriter::open(config, metadata, catalog, compression_txs, compression_rx)?;

    loop {
        writer.drain_completed_chunks()?;
        if writer.is_flush_due() {
            writer.dispatch_chunk()?;
            continue;
        }

        let wait = writer.next_wait_duration();
        let received = match wait {
            Some(duration) => match rx.recv_timeout(duration) {
                Ok(command) => Some(command),
                Err(ReceiveErrorTimeout::Timeout) => None,
                Err(ReceiveErrorTimeout::SendClosed | ReceiveErrorTimeout::Closed) => {
                    writer.dispatch_chunk()?;
                    writer.finish_all_chunks()?;
                    break;
                }
            },
            None => match rx.recv() {
                Ok(command) => Some(command),
                Err(_) => break,
            },
        };

        if let Some(command) = received {
            queue_metrics.record_dequeue(&command);
            writer.handle(command.into_kind())?;
        }
    }

    Ok(())
}

impl SegmentWriter {
    fn open(
        config: SegmentedConfig,
        metadata: MetadataDb,
        catalog: Arc<RwLock<MetadataCatalog>>,
        compression_txs: Vec<kanal::Sender<CompressionJob>>,
        compression_rx: kanal::Receiver<anyhow::Result<CompressedChunk>>,
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
            pending_chunk: None,
            compression_txs,
            compression_rx,
            ready_chunks: BTreeMap::new(),
            next_worker: 0,
            next_chunk_id: 0,
            next_append_chunk_id: 0,
            inflight_chunks: 0,
        })
    }

    fn handle(&mut self, command: WriterCommandKind) -> anyhow::Result<()> {
        match command {
            WriterCommandKind::PushMessage {
                init,
                slot,
                head,
                index,
                message,
            } => self.handle_push(init, slot, head, index, message),
            WriterCommandKind::RemoveReplay { slot, until } => self.handle_trim(slot, until),
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
        let pending_chunk = self
            .pending_chunk
            .get_or_insert_with(|| PendingChunk::new(slot, index));
        pending_chunk.push(init, slot, head, index, message);

        counter!(CHANNEL_STORAGE_WRITE_SER_INDEX).absolute(index);
        if pending_chunk.should_flush(self.config.chunk_target_size) {
            self.dispatch_chunk()?;
        }
        Ok(())
    }

    fn handle_trim(&mut self, slot: Slot, until: Option<u64>) -> anyhow::Result<()> {
        self.dispatch_chunk()?;
        self.finish_all_chunks()?;

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

    fn dispatch_chunk(&mut self) -> anyhow::Result<()> {
        let Some(pending_chunk) = self.pending_chunk.take() else {
            return Ok(());
        };

        let chunk_id = self.next_chunk_id;
        self.next_chunk_id += 1;

        let worker_index = self.next_worker % self.compression_txs.len();
        self.next_worker += 1;
        self.compression_txs[worker_index]
            .send(pending_chunk.into_job(chunk_id))
            .map_err(|error| anyhow!("failed to send chunk to serialize worker: {error}"))?;

        self.inflight_chunks += 1;
        Ok(())
    }

    fn drain_completed_chunks(&mut self) -> anyhow::Result<()> {
        loop {
            match self.compression_rx.try_recv() {
                Ok(Some(result)) => self.handle_compression_result(result)?,
                Ok(None) => break,
                Err(ReceiveError::SendClosed) => {
                    anyhow::ensure!(
                        self.inflight_chunks == 0,
                        "compression workers closed with {} chunks still in flight",
                        self.inflight_chunks
                    );
                    break;
                }
                Err(ReceiveError::Closed) => break,
            }
        }
        self.append_ready_chunks()
    }

    fn finish_all_chunks(&mut self) -> anyhow::Result<()> {
        while self.inflight_chunks > 0 {
            let result = self
                .compression_rx
                .recv()
                .map_err(|error| anyhow!("failed to receive compressed chunk: {error}"))?;
            self.handle_compression_result(result)?;
            self.append_ready_chunks()?;
        }
        Ok(())
    }

    fn handle_compression_result(
        &mut self,
        result: anyhow::Result<CompressedChunk>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.inflight_chunks > 0,
            "compression result without inflight chunk"
        );
        self.inflight_chunks -= 1;

        let chunk = result?;
        let prev = self.ready_chunks.insert(chunk.chunk_id, chunk);
        anyhow::ensure!(prev.is_none(), "duplicate compressed chunk id");
        Ok(())
    }

    fn append_ready_chunks(&mut self) -> anyhow::Result<()> {
        while let Some(chunk) = self.ready_chunks.remove(&self.next_append_chunk_id) {
            self.append_chunk(chunk)?;
            self.next_append_chunk_id += 1;
        }
        Ok(())
    }

    fn append_chunk(&mut self, chunk: CompressedChunk) -> anyhow::Result<()> {
        let chunk_ordinal = self.active_segment.chunk_count;
        let chunk_header = ChunkHeader {
            chunk_ordinal,
            first_index: chunk.first_index,
            last_index: chunk.last_index,
            first_slot: chunk.first_slot,
            last_slot: chunk.last_slot,
            record_count: chunk.record_count,
            compressed_size: chunk.compressed.len() as u32,
            uncompressed_size: chunk.uncompressed_size,
            crc32: chunk.crc32,
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

        let append_started_at = Instant::now();
        self.active_file
            .seek(SeekFrom::Start(self.active_segment.file_len))?;
        self.active_file.write_all(&chunk_header.encode())?;
        self.active_file.write_all(&chunk.compressed)?;
        counter!(STORAGE_WRITE_APPEND_MICROS_TOTAL)
            .increment(duration_as_micros(append_started_at.elapsed()));

        if self.config.segment_fsync {
            let fsync_started_at = Instant::now();
            self.active_file.sync_data()?;
            counter!(STORAGE_WRITE_FSYNC_MICROS_TOTAL)
                .increment(duration_as_micros(fsync_started_at.elapsed()));
        }

        let metadata_started_at = Instant::now();
        let commit = {
            let catalog = self.catalog.read().expect("segment catalog poisoned");
            build_chunk_commit(
                &catalog,
                self.active_segment,
                chunk_meta,
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

    fn is_flush_due(&self) -> bool {
        self.pending_chunk
            .as_ref()
            .is_some_and(|chunk| chunk.is_flush_due(self.config.chunk_flush_delay))
    }

    fn next_wait_duration(&self) -> Option<Duration> {
        const POLL_INTERVAL: Duration = Duration::from_millis(1);

        if let Some(chunk) = &self.pending_chunk {
            let remaining = self
                .config
                .chunk_flush_delay
                .saturating_sub(chunk.started_at.elapsed());
            Some(remaining.min(POLL_INTERVAL))
        } else if self.inflight_chunks > 0 {
            Some(POLL_INTERVAL)
        } else {
            None
        }
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

fn estimate_record_size(_slot: Slot, message: &ParsedMessage) -> usize {
    message.size().saturating_add(16)
}

fn thread_affinity_for_worker(
    affinity: &Option<Vec<usize>>,
    workers: usize,
    index: usize,
) -> Option<Vec<usize>> {
    affinity.as_ref().map(|cpus| {
        if cpus.len() >= workers {
            vec![cpus[index]]
        } else {
            cpus.clone()
        }
    })
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

use {
    anyhow::Context,
    rocksdb::{
        ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, IteratorMode, Options,
        WriteBatch, WriteOptions,
    },
    solana_clock::Slot,
    std::{
        collections::BTreeMap,
        path::{Path, PathBuf},
        sync::{Arc, RwLock, RwLockReadGuard},
    },
};

trait ColumnName {
    const NAME: &'static str;
}

#[derive(Debug)]
struct StateCf;

impl ColumnName for StateCf {
    const NAME: &'static str = "state";
}

#[derive(Debug)]
struct SlotsCf;

impl ColumnName for SlotsCf {
    const NAME: &'static str = "slots";
}

#[derive(Debug)]
struct SegmentsCf;

impl ColumnName for SegmentsCf {
    const NAME: &'static str = "segments";
}

#[derive(Debug)]
struct ChunksCf;

impl ColumnName for ChunksCf {
    const NAME: &'static str = "chunks";
}

/// Singleton state persisted alongside metadata tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetadataState {
    pub next_segment_id: u64,
    pub active_segment_id: u64,
}

impl Default for MetadataState {
    fn default() -> Self {
        Self {
            next_segment_id: 1,
            active_segment_id: 0,
        }
    }
}

impl MetadataState {
    const STATE_FORMAT_VERSION: u16 = 1;

    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(18);
        buf.extend_from_slice(&Self::STATE_FORMAT_VERSION.to_be_bytes());
        buf.extend_from_slice(&self.next_segment_id.to_be_bytes());
        buf.extend_from_slice(&self.active_segment_id.to_be_bytes());
        buf
    }

    fn decode(value: &[u8]) -> anyhow::Result<Self> {
        let mut value = value;
        let format_version = take_u16(&mut value)?;
        anyhow::ensure!(
            format_version == Self::STATE_FORMAT_VERSION,
            "unsupported format version: {format_version}"
        );
        Ok(Self {
            next_segment_id: take_u64(&mut value)?,
            active_segment_id: take_u64(&mut value)?,
        })
    }
}

/// Replay start lookup for a retained slot.
///
/// This is the minimal metadata needed to answer "where does replay for this
/// slot begin inside the segment files?"
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotMeta {
    pub slot: Slot,
    pub first_index: u64,
    pub segment_id: u64,
    pub finalized: bool,
}

impl SlotMeta {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(17);
        buf.extend_from_slice(&self.first_index.to_be_bytes());
        buf.extend_from_slice(&self.segment_id.to_be_bytes());
        buf.push(u8::from(self.finalized));
        buf
    }

    fn decode(slot: Slot, value: &[u8]) -> anyhow::Result<Self> {
        let mut value = value;
        Ok(Self {
            slot,
            first_index: take_u64(&mut value)?,
            segment_id: take_u64(&mut value)?,
            finalized: take_bool(&mut value)?,
        })
    }
}

/// Metadata for one segment file in the append-only payload store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentMeta {
    pub segment_id: u64,
    pub last_index: u64,
    pub sealed: bool,
    pub file_len: u64,
    pub chunk_count: u32,
}

impl SegmentMeta {
    pub const fn empty(segment_id: u64, file_len: u64) -> Self {
        Self {
            segment_id,
            last_index: 0,
            sealed: false,
            file_len,
            chunk_count: 0,
        }
    }

    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(21);
        buf.extend_from_slice(&self.last_index.to_be_bytes());
        buf.push(u8::from(self.sealed));
        buf.extend_from_slice(&self.file_len.to_be_bytes());
        buf.extend_from_slice(&self.chunk_count.to_be_bytes());
        buf
    }

    fn decode(segment_id: u64, value: &[u8]) -> anyhow::Result<Self> {
        let mut value = value;
        Ok(Self {
            segment_id,
            last_index: take_u64(&mut value)?,
            sealed: take_bool(&mut value)?,
            file_len: take_u64(&mut value)?,
            chunk_count: take_u32(&mut value)?,
        })
    }
}

/// Metadata for one independently compressed chunk inside a segment file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkMeta {
    pub segment_id: u64,
    pub offset: u64,
    pub size: u32,
    pub compression: u8,
    pub first_index: u64,
    pub last_index: u64,
}

impl ChunkMeta {
    fn encode(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(29);
        buf.extend_from_slice(&self.segment_id.to_be_bytes());
        buf.extend_from_slice(&self.offset.to_be_bytes());
        buf.extend_from_slice(&self.size.to_be_bytes());
        buf.push(self.compression);
        buf.extend_from_slice(&self.last_index.to_be_bytes());
        buf
    }

    fn decode(first_index: u64, value: &[u8]) -> anyhow::Result<Self> {
        let mut value = value;
        Ok(Self {
            segment_id: take_u64(&mut value)?,
            offset: take_u64(&mut value)?,
            size: take_u32(&mut value)?,
            compression: take_u8(&mut value)?,
            first_index,
            last_index: take_u64(&mut value)?,
        })
    }
}

/// Atomic metadata update produced by flushing one chunk.
#[derive(Debug, Clone)]
pub struct MetadataChunkCommit {
    pub new_slots: Vec<SlotMeta>,
    pub updated_slots: Vec<SlotMeta>,
    pub chunk: ChunkMeta,
    pub segment: SegmentMeta,
    pub state: MetadataState,
}

/// Atomic metadata update produced by retention trimming.
#[derive(Debug, Clone)]
pub struct MetadataTrimCommit {
    pub removed_slot: Slot,
    pub deleted_slots: Vec<Slot>,
    pub deleted_segments: Vec<u64>,
    pub deleted_chunks: Vec<u64>,
    pub state: MetadataState,
}

/// Metadata update produced when the writer seals one segment and opens the
/// next one.
#[derive(Debug, Clone, Copy)]
pub struct RotationCommit {
    pub sealed_segment: SegmentMeta,
    pub new_segment: SegmentMeta,
    pub state: MetadataState,
}

/// In-memory view of the metadata DB used for fast replay lookups and trim
/// decisions.
#[derive(Debug, Clone, Default)]
pub struct MetadataMirror {
    pub slots: BTreeMap<Slot, SlotMeta>,
    pub segments: BTreeMap<u64, SegmentMeta>,
    pub chunks: Vec<ChunkMeta>,
    pub state: MetadataState,
}

impl MetadataMirror {
    fn apply_chunk_commit(&mut self, commit: &MetadataChunkCommit) {
        for meta in &commit.new_slots {
            self.slots.insert(meta.slot, *meta);
        }
        for meta in &commit.updated_slots {
            self.slots.insert(meta.slot, *meta);
        }
        self.segments
            .insert(commit.segment.segment_id, commit.segment);
        self.chunks.push(commit.chunk);
        self.state = commit.state;
    }

    fn apply_trim_commit(&mut self, commit: &MetadataTrimCommit) {
        self.slots.remove(&commit.removed_slot);
        for slot in &commit.deleted_slots {
            self.slots.remove(slot);
        }
        for segment_id in &commit.deleted_segments {
            self.segments.remove(segment_id);
        }
        self.chunks
            .retain(|chunk| !commit.deleted_segments.contains(&chunk.segment_id));
        self.state = commit.state;
    }

    fn apply_rotation_commit(&mut self, commit: RotationCommit) {
        self.segments
            .insert(commit.sealed_segment.segment_id, commit.sealed_segment);
        self.segments
            .insert(commit.new_segment.segment_id, commit.new_segment);
        self.state = commit.state;
    }
}

/// Typed wrapper around the metadata RocksDB instance.
///
/// Payload bytes are stored in segment files; this DB keeps the exact lookup
/// state needed to find them again.
#[derive(Debug, Clone)]
pub struct Metadata {
    db: Arc<DB>,
    catalog: Arc<RwLock<MetadataMirror>>,
    segments_path: PathBuf,
}

impl Metadata {
    fn db_options() -> Options {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_compression_type(DBCompressionType::None);
        options
    }

    fn cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
        vec![
            Self::cf_descriptor::<SlotsCf>(),
            Self::cf_descriptor::<SegmentsCf>(),
            Self::cf_descriptor::<ChunksCf>(),
            Self::cf_descriptor::<StateCf>(),
        ]
    }

    fn cf_descriptor<C: ColumnName>() -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(C::NAME, Options::default())
    }

    fn cf_handle<C: ColumnName>(db: &DB) -> &ColumnFamily {
        db.cf_handle(C::NAME)
            .expect("should never get an unknown column")
    }

    pub fn catalog(&self) -> RwLockReadGuard<'_, MetadataMirror> {
        self.catalog.read().expect("segment catalog poisoned")
    }

    #[allow(clippy::missing_const_for_fn)]
    pub fn segments_path(&self) -> &Path {
        &self.segments_path
    }

    pub fn open(path: &Path, segments_path: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(path)
            .with_context(|| format!("failed to create metadata path: {path:?}"))?;

        let db_options = Self::db_options();
        let cf_descriptors = Self::cf_descriptors();
        let db = Arc::new(
            DB::open_cf_descriptors(&db_options, path, cf_descriptors)
                .with_context(|| format!("failed to open metadata rocksdb at {path:?}"))?,
        );
        let db = Self {
            db,
            catalog: Arc::new(RwLock::new(MetadataMirror::default())),
            segments_path,
        };

        if db.read_state()?.is_none() {
            let mut batch = WriteBatch::new();
            batch.put_cf(
                Self::cf_handle::<StateCf>(&db.db),
                b"state",
                MetadataState::default().encode(),
            );
            db.write_batch(batch)?;
        }
        *db.catalog.write().expect("poisoned") = db.load_catalog_from_db()?;

        Ok(db)
    }

    fn load_catalog_from_db(&self) -> anyhow::Result<MetadataMirror> {
        let state = self.read_state()?.unwrap_or_default();

        let mut slots = BTreeMap::new();
        for item in self
            .db
            .iterator_cf(Self::cf_handle::<SlotsCf>(&self.db), IteratorMode::Start)
        {
            let (key, value) = item.context("failed to read slots row")?;
            let slot = decode_u64_key(&key).context("failed to decode slot key")?;
            let meta = SlotMeta::decode(slot, &value).context("failed to decode slot meta")?;
            slots.insert(slot, meta);
        }

        let mut segments = BTreeMap::new();
        for item in self
            .db
            .iterator_cf(Self::cf_handle::<SegmentsCf>(&self.db), IteratorMode::Start)
        {
            let (key, value) = item.context("failed to read segments row")?;
            let segment_id = decode_u64_key(&key).context("failed to decode segment key")?;
            let meta =
                SegmentMeta::decode(segment_id, &value).context("failed to decode segment meta")?;
            segments.insert(segment_id, meta);
        }

        let mut chunks = Vec::new();
        for item in self
            .db
            .iterator_cf(Self::cf_handle::<ChunksCf>(&self.db), IteratorMode::Start)
        {
            let (key, value) = item.context("failed to read chunks row")?;
            let first_index = decode_u64_key(&key).context("failed to decode chunk key")?;
            let meta =
                ChunkMeta::decode(first_index, &value).context("failed to decode chunk meta")?;
            chunks.push(meta);
        }
        chunks.sort_by_key(|chunk| chunk.first_index);

        Ok(MetadataMirror {
            slots,
            segments,
            chunks,
            state,
        })
    }

    fn read_state(&self) -> anyhow::Result<Option<MetadataState>> {
        self.db
            .get_cf(Self::cf_handle::<StateCf>(&self.db), b"state")?
            .map(|value| MetadataState::decode(&value))
            .transpose()
    }

    pub fn initialize_empty(
        &self,
        state: &MetadataState,
        segment: SegmentMeta,
    ) -> anyhow::Result<()> {
        let mut batch = WriteBatch::new();
        batch.put_cf(
            Self::cf_handle::<SegmentsCf>(&self.db),
            encode_u64_key(segment.segment_id),
            segment.encode(),
        );
        batch.put_cf(
            Self::cf_handle::<StateCf>(&self.db),
            b"state",
            state.encode(),
        );
        self.write_batch(batch)?;
        *self.catalog.write().expect("poisoned") = self.load_catalog_from_db()?;
        Ok(())
    }

    pub fn apply_chunk_commit(&self, commit: &MetadataChunkCommit) -> anyhow::Result<()> {
        let mut batch = WriteBatch::new();
        for meta in &commit.new_slots {
            batch.put_cf(
                Self::cf_handle::<SlotsCf>(&self.db),
                encode_u64_key(meta.slot),
                meta.encode(),
            );
        }
        for meta in &commit.updated_slots {
            batch.put_cf(
                Self::cf_handle::<SlotsCf>(&self.db),
                encode_u64_key(meta.slot),
                meta.encode(),
            );
        }
        batch.put_cf(
            Self::cf_handle::<ChunksCf>(&self.db),
            encode_u64_key(commit.chunk.first_index),
            commit.chunk.encode(),
        );
        batch.put_cf(
            Self::cf_handle::<SegmentsCf>(&self.db),
            encode_u64_key(commit.segment.segment_id),
            commit.segment.encode(),
        );
        batch.put_cf(
            Self::cf_handle::<StateCf>(&self.db),
            b"state",
            commit.state.encode(),
        );
        self.write_batch(batch)?;
        self.catalog
            .write()
            .expect("poisoned")
            .apply_chunk_commit(commit);
        Ok(())
    }

    pub fn apply_trim_commit(&self, commit: &MetadataTrimCommit) -> anyhow::Result<()> {
        let mut batch = WriteBatch::new();
        batch.delete_cf(
            Self::cf_handle::<SlotsCf>(&self.db),
            encode_u64_key(commit.removed_slot),
        );
        for slot in &commit.deleted_slots {
            batch.delete_cf(Self::cf_handle::<SlotsCf>(&self.db), encode_u64_key(*slot));
        }
        for first_index in &commit.deleted_chunks {
            batch.delete_cf(
                Self::cf_handle::<ChunksCf>(&self.db),
                encode_u64_key(*first_index),
            );
        }
        for segment_id in &commit.deleted_segments {
            batch.delete_cf(
                Self::cf_handle::<SegmentsCf>(&self.db),
                encode_u64_key(*segment_id),
            );
        }
        batch.put_cf(
            Self::cf_handle::<StateCf>(&self.db),
            b"state",
            commit.state.encode(),
        );
        self.write_batch(batch)?;
        self.catalog
            .write()
            .expect("poisoned")
            .apply_trim_commit(commit);
        Ok(())
    }

    pub fn apply_rotation_commit(&self, commit: RotationCommit) -> anyhow::Result<()> {
        let mut batch = WriteBatch::new();
        batch.put_cf(
            Self::cf_handle::<SegmentsCf>(&self.db),
            encode_u64_key(commit.sealed_segment.segment_id),
            commit.sealed_segment.encode(),
        );
        batch.put_cf(
            Self::cf_handle::<SegmentsCf>(&self.db),
            encode_u64_key(commit.new_segment.segment_id),
            commit.new_segment.encode(),
        );
        batch.put_cf(
            Self::cf_handle::<StateCf>(&self.db),
            b"state",
            commit.state.encode(),
        );
        self.write_batch(batch)?;
        self.catalog
            .write()
            .expect("poisoned")
            .apply_rotation_commit(commit);
        Ok(())
    }

    fn write_batch(&self, batch: WriteBatch) -> anyhow::Result<()> {
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        self.db.write_opt(batch, &write_options).map_err(Into::into)
    }
}

const fn encode_u64_key(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

fn decode_u64_key(value: &[u8]) -> anyhow::Result<u64> {
    value
        .try_into()
        .map(u64::from_be_bytes)
        .context("invalid u64 key length")
}

fn take_u16(slice: &mut &[u8]) -> anyhow::Result<u16> {
    let value = slice
        .get(..2)
        .context("unexpected eof while decoding u16")?
        .try_into()
        .map(u16::from_be_bytes)
        .context("invalid u16 bytes")?;
    *slice = &slice[2..];
    Ok(value)
}

fn take_u32(slice: &mut &[u8]) -> anyhow::Result<u32> {
    let value = slice
        .get(..4)
        .context("unexpected eof while decoding u32")?
        .try_into()
        .map(u32::from_be_bytes)
        .context("invalid u32 bytes")?;
    *slice = &slice[4..];
    Ok(value)
}

fn take_u64(slice: &mut &[u8]) -> anyhow::Result<u64> {
    let value = slice
        .get(..8)
        .context("unexpected eof while decoding u64")?
        .try_into()
        .map(u64::from_be_bytes)
        .context("invalid u64 bytes")?;
    *slice = &slice[8..];
    Ok(value)
}

fn take_u8(slice: &mut &[u8]) -> anyhow::Result<u8> {
    let value = *slice.first().context("unexpected eof while decoding u8")?;
    *slice = &slice[1..];
    Ok(value)
}

fn take_bool(slice: &mut &[u8]) -> anyhow::Result<bool> {
    let value = *slice
        .first()
        .context("unexpected eof while decoding bool")?;
    *slice = &slice[1..];
    match value {
        0 => Ok(false),
        1 => Ok(true),
        value => anyhow::bail!("invalid bool value: {value}"),
    }
}

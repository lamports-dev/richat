use {
    crate::{
        channel::ParsedMessage,
        metrics::{STORAGE_REPLAY_COMPRESSED_BYTES_TOTAL, STORAGE_REPLAY_DECOMPRESSED_BYTES_TOTAL},
        storage::{
            MessageRecordCodec,
            metadata::ChunkMeta,
            segment_format::{
                CHUNK_HEADER_LEN, ChunkCompression, ChunkHeader, SEGMENT_HEADER_LEN, chunk_crc32,
                next_record, read_segment_header, segment_file_name,
            },
        },
    },
    ::metrics::counter,
    anyhow::Context,
    richat_filter::message::MessageParserEncoding,
    std::{
        collections::VecDeque,
        fs::File,
        io::{Read, Seek, SeekFrom},
        path::PathBuf,
    },
    zstd::bulk::Decompressor,
};

/// Sequential replay reader over chunk metadata and segment files.
///
/// It seeks to the first relevant chunk, validates headers and CRCs, then
/// yields decoded replay records in index order.
pub(crate) struct SegmentReader {
    segments_path: PathBuf,
    parser: MessageParserEncoding,
    chunks: Vec<ChunkMeta>,
    next_chunk: usize,
    next_index: u64,
    current_records: VecDeque<anyhow::Result<(u64, ParsedMessage)>>,
    current_segment_id: Option<u64>,
    current_file: Option<File>,
    decompressor: Decompressor<'static>,
    failed: bool,
}

impl SegmentReader {
    pub(crate) fn new(
        segments_path: PathBuf,
        chunks: Vec<ChunkMeta>,
        start_index: u64,
        parser: MessageParserEncoding,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            segments_path,
            parser,
            chunks,
            next_chunk: 0,
            next_index: start_index,
            current_records: VecDeque::new(),
            current_segment_id: None,
            current_file: None,
            decompressor: Decompressor::new().context("failed to create zstd decompressor")?,
            failed: false,
        })
    }

    fn load_next_chunk(&mut self) -> anyhow::Result<()> {
        let chunk = self.chunks[self.next_chunk];
        self.next_chunk += 1;

        let file = self.open_segment(chunk.segment_id)?;
        file.seek(SeekFrom::Start(chunk.file_offset))?;

        let mut header_buf = [0u8; CHUNK_HEADER_LEN];
        file.read_exact(&mut header_buf)?;
        let header = ChunkHeader::decode(&header_buf).context("failed to decode chunk header")?;
        anyhow::ensure!(
            header.chunk_ordinal == chunk.chunk_ordinal
                && header.first_index == chunk.first_index
                && header.last_index == chunk.last_index
                && header.first_slot == chunk.first_slot
                && header.last_slot == chunk.last_slot
                && header.record_count == chunk.record_count
                && header.compressed_size == chunk.compressed_size
                && header.uncompressed_size == chunk.uncompressed_size
                && header.crc32 == chunk.crc32,
            "chunk header does not match metadata"
        );

        let mut payload = vec![0; header.compressed_size as usize];
        file.read_exact(&mut payload)?;
        counter!(STORAGE_REPLAY_COMPRESSED_BYTES_TOTAL).increment(payload.len() as u64);

        let compression = ChunkCompression::from_tag(header.compression)
            .context("unsupported chunk compression")?;
        let uncompressed = match compression {
            ChunkCompression::None => payload,
            ChunkCompression::Zstd(_) => self
                .decompressor
                .decompress(&payload, header.uncompressed_size as usize)
                .context("failed to decompress chunk")?,
        };
        anyhow::ensure!(
            uncompressed.len() == header.uncompressed_size as usize,
            "unexpected uncompressed size: {}",
            uncompressed.len()
        );
        anyhow::ensure!(
            chunk_crc32(&uncompressed) == header.crc32,
            "chunk crc32 mismatch"
        );
        counter!(STORAGE_REPLAY_DECOMPRESSED_BYTES_TOTAL).increment(uncompressed.len() as u64);

        let mut slice = uncompressed.as_slice();
        let skip = self.next_index.saturating_sub(chunk.first_index) as usize;
        for record_ordinal in 0..header.record_count as usize {
            let record = next_record(&mut slice)?;
            if record_ordinal < skip {
                continue;
            }

            let (_slot, message) = MessageRecordCodec::decode(record, self.parser)?;
            self.current_records.push_back(Ok((
                chunk.first_index + record_ordinal as u64,
                message.into(),
            )));
        }
        anyhow::ensure!(slice.is_empty(), "extra trailing bytes in chunk");
        self.next_index = chunk.last_index + 1;
        Ok(())
    }

    fn open_segment(&mut self, segment_id: u64) -> anyhow::Result<&mut File> {
        if self.current_segment_id != Some(segment_id) {
            let path = self.segments_path.join(segment_file_name(segment_id));
            let mut file = File::open(&path)
                .with_context(|| format!("failed to open segment file: {path:?}"))?;
            let header = read_segment_header(&mut file)
                .with_context(|| format!("failed to read segment header for {path:?}"))?;
            anyhow::ensure!(
                header.segment_id == segment_id,
                "segment header id mismatch: expected {segment_id}, got {}",
                header.segment_id
            );
            let metadata_len = file.metadata()?.len();
            anyhow::ensure!(
                metadata_len >= SEGMENT_HEADER_LEN as u64,
                "segment file shorter than header: {path:?}"
            );
            self.current_segment_id = Some(segment_id);
            self.current_file = Some(file);
        }

        self.current_file
            .as_mut()
            .context("segment file should be opened")
    }
}

impl Iterator for SegmentReader {
    type Item = anyhow::Result<(u64, ParsedMessage)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(item) = self.current_records.pop_front() {
                return Some(item);
            }
            if self.failed || self.next_chunk >= self.chunks.len() {
                return None;
            }
            if let Err(error) = self.load_next_chunk() {
                self.failed = true;
                return Some(Err(error));
            }
        }
    }
}

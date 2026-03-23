use {
    crate::{
        channel::ParsedMessage,
        metrics::{STORAGE_REPLAY_COMPRESSED_BYTES_TOTAL, STORAGE_REPLAY_DECOMPRESSED_BYTES_TOTAL},
        storage::{
            MessageRecordCodec,
            metadata::ChunkMeta,
            segment_format::{
                ChunkCompression, SEGMENT_HEADER_LEN, next_record, read_segment_header,
                segment_file_name,
            },
        },
    },
    ::metrics::counter,
    anyhow::Context,
    richat_filter::message::MessageParserEncoding,
    std::{
        fs::File,
        io::{Read, Seek, SeekFrom},
        path::PathBuf,
    },
    zstd::stream::decode_all as zstd_decode_all,
};

/// A single decompressed chunk ready for record decoding.
pub(crate) struct DecompressedChunk {
    pub(crate) first_index: u64,
    skip: usize,
    data: Vec<u8>,
}

impl DecompressedChunk {
    pub(crate) fn decode_records(
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
pub(crate) struct SegmentReader {
    segments_path: PathBuf,
    chunks: Vec<ChunkMeta>,
    next_chunk: usize,
    next_index: u64,
    current_segment_id: Option<u64>,
    current_file: Option<File>,
    failed: bool,
}

impl SegmentReader {
    pub(crate) const fn new(
        segments_path: PathBuf,
        chunks: Vec<ChunkMeta>,
        start_index: u64,
    ) -> Self {
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

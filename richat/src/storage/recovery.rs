use {
    super::{
        api::MessageRecordCodec,
        metadata::{ChunkMeta, MetadataCatalog, RecoveryCommit, SegmentMeta, SlotMeta},
        segment_format::{
            CHUNK_HEADER_LEN, ChunkHeader, SEGMENT_HEADER_LEN, SegmentHeader, chunk_crc32,
            next_record, read_segment_header, segment_file_name, write_segment_header,
        },
        segmented::SegmentedConfig,
    },
    anyhow::Context,
    richat_filter::message::MessageParserEncoding,
    richat_proto::geyser::SlotStatus,
    std::{
        collections::BTreeMap,
        fs::OpenOptions,
        io::{ErrorKind, Read, Seek, SeekFrom},
    },
    zstd::bulk::Decompressor,
};

pub(crate) fn recover_active_segment(
    config: &SegmentedConfig,
    catalog: &MetadataCatalog,
    parser: MessageParserEncoding,
) -> anyhow::Result<RecoveryCommit> {
    let active_segment = catalog
        .segments
        .get(&catalog.state.active_segment_id)
        .copied()
        .context("missing active segment metadata")?;
    let active_path = config
        .segments_path
        .join(segment_file_name(active_segment.segment_id));
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&active_path)
        .with_context(|| format!("failed to open active segment for recovery: {active_path:?}"))?;

    let header = match read_segment_header(&mut file) {
        Ok(header) if header.segment_id == active_segment.segment_id => header,
        Ok(header) => {
            anyhow::bail!(
                "active segment id mismatch: expected {}, got {}",
                active_segment.segment_id,
                header.segment_id
            );
        }
        Err(_error) => {
            file.set_len(0)?;
            let header = SegmentHeader {
                segment_id: active_segment.segment_id,
                created_unix_ms: active_segment.created_unix_ms,
            };
            write_segment_header(&mut file, header)?;
            if config.segment_fsync {
                file.sync_data()?;
            }
            header
        }
    };

    let mut valid_end = SEGMENT_HEADER_LEN as u64;
    let mut chunks = Vec::new();
    let mut slots = BTreeMap::new();
    let mut decompressor = Decompressor::new().context("failed to create zstd decompressor")?;

    loop {
        file.seek(SeekFrom::Start(valid_end))?;
        let Some(header_buf) = read_exact_or_eof(&mut file, CHUNK_HEADER_LEN)? else {
            break;
        };
        let chunk_header = match ChunkHeader::decode(&header_buf) {
            Ok(header) => header,
            Err(_error) => break,
        };

        let Some(compressed) = read_exact_or_eof(&mut file, chunk_header.compressed_size as usize)?
        else {
            break;
        };
        let uncompressed = match decompressor
            .decompress(&compressed, chunk_header.uncompressed_size as usize)
        {
            Ok(uncompressed) if uncompressed.len() == chunk_header.uncompressed_size as usize => {
                uncompressed
            }
            _ => break,
        };
        if chunk_crc32(&uncompressed) != chunk_header.crc32 {
            break;
        }

        let chunk_meta = ChunkMeta {
            segment_id: active_segment.segment_id,
            chunk_ordinal: chunk_header.chunk_ordinal,
            file_offset: valid_end,
            first_slot: chunk_header.first_slot,
            last_slot: chunk_header.last_slot,
            first_index: chunk_header.first_index,
            last_index: chunk_header.last_index,
            record_count: chunk_header.record_count,
            compressed_size: chunk_header.compressed_size,
            uncompressed_size: chunk_header.uncompressed_size,
            crc32: chunk_header.crc32,
        };

        let mut slice = uncompressed.as_slice();
        let mut chunk_valid = true;
        for record_ordinal in 0..chunk_header.record_count {
            let record = match next_record(&mut slice) {
                Ok(record) => record,
                Err(_error) => {
                    chunk_valid = false;
                    break;
                }
            };
            let index = chunk_header.first_index + u64::from(record_ordinal);
            let (slot, message) = match MessageRecordCodec::decode(record, parser) {
                Ok(message) => message,
                Err(_error) => {
                    chunk_valid = false;
                    break;
                }
            };

            if index >= catalog.state.trim_floor_index {
                let entry = slots.entry(slot).or_insert(SlotMeta {
                    slot,
                    first_index: index,
                    segment_id: active_segment.segment_id,
                    chunk_ordinal: chunk_header.chunk_ordinal,
                    record_ordinal,
                    finalized: false,
                });
                if let richat_filter::message::Message::Slot(message) = message {
                    if message.status() == SlotStatus::SlotFinalized {
                        entry.finalized = true;
                    }
                }
            }
        }
        if !chunk_valid || !slice.is_empty() {
            break;
        }

        valid_end += CHUNK_HEADER_LEN as u64 + u64::from(chunk_header.compressed_size);
        chunks.push(chunk_meta);
    }

    let file_len = file.metadata()?.len();
    if file_len != valid_end {
        file.set_len(valid_end)?;
        if config.segment_fsync {
            file.sync_data()?;
        }
    }

    let segment = if let Some(last_chunk) = chunks.last().copied() {
        SegmentMeta {
            segment_id: active_segment.segment_id,
            first_slot: chunks[0].first_slot,
            last_slot: last_chunk.last_slot,
            first_index: chunks[0].first_index,
            last_index: last_chunk.last_index,
            created_unix_ms: header.created_unix_ms,
            sealed: false,
            file_len: valid_end,
            chunk_count: chunks.len() as u32,
        }
    } else {
        SegmentMeta::empty(
            active_segment.segment_id,
            header.created_unix_ms,
            SEGMENT_HEADER_LEN as u64,
        )
    };

    Ok(RecoveryCommit {
        deleted_slots: catalog
            .slots
            .values()
            .filter(|slot| slot.segment_id == active_segment.segment_id)
            .map(|slot| slot.slot)
            .collect(),
        deleted_chunks: catalog
            .chunks
            .iter()
            .filter(|chunk| chunk.segment_id == active_segment.segment_id)
            .map(|chunk| (chunk.segment_id, chunk.chunk_ordinal))
            .collect(),
        segment,
        slots: slots.into_values().collect(),
        chunks,
        state: catalog.state,
    })
}

fn read_exact_or_eof(file: &mut std::fs::File, len: usize) -> anyhow::Result<Option<Vec<u8>>> {
    let mut buf = vec![0; len];
    match file.read_exact(&mut buf) {
        Ok(()) => Ok(Some(buf)),
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => Ok(None),
        Err(error) => Err(error).context("failed to read segment bytes"),
    }
}

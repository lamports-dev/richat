use {
    super::api::MessageRecordCodec,
    crate::channel::ParsedMessage,
    anyhow::Context,
    prost::encoding::decode_varint,
    solana_clock::Slot,
    std::{
        fs::File,
        io::{Read, Seek, SeekFrom, Write},
    },
};

pub(crate) const SEGMENT_MAGIC: [u8; 4] = *b"RSEG";
pub(crate) const CHUNK_MAGIC: [u8; 4] = *b"RCHK";
pub(crate) const SEGMENT_FORMAT_VERSION: u16 = 1;
pub(crate) const SEGMENT_HEADER_LEN: usize = 32;
pub(crate) const CHUNK_HEADER_LEN: usize = 60;

/// Fixed header stored at the start of every segment file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentHeader {
    pub(crate) segment_id: u64,
    pub(crate) created_unix_ms: u64,
}

/// Fixed header stored in front of every compressed chunk payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ChunkHeader {
    pub(crate) chunk_ordinal: u32,
    pub(crate) first_index: u64,
    pub(crate) last_index: u64,
    pub(crate) first_slot: Slot,
    pub(crate) last_slot: Slot,
    pub(crate) record_count: u32,
    pub(crate) compressed_size: u32,
    pub(crate) uncompressed_size: u32,
    pub(crate) crc32: u32,
}

impl SegmentHeader {
    pub(crate) fn encode(self) -> [u8; SEGMENT_HEADER_LEN] {
        let mut buf = [0u8; SEGMENT_HEADER_LEN];
        buf[..4].copy_from_slice(&SEGMENT_MAGIC);
        buf[4..6].copy_from_slice(&SEGMENT_FORMAT_VERSION.to_be_bytes());
        buf[8..16].copy_from_slice(&self.segment_id.to_be_bytes());
        buf[16..24].copy_from_slice(&self.created_unix_ms.to_be_bytes());
        buf
    }

    pub(crate) fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes.len() == SEGMENT_HEADER_LEN,
            "invalid segment header length: {}",
            bytes.len()
        );
        anyhow::ensure!(bytes[..4] == SEGMENT_MAGIC, "invalid segment magic");
        let version = u16::from_be_bytes(bytes[4..6].try_into().unwrap());
        anyhow::ensure!(
            version == SEGMENT_FORMAT_VERSION,
            "unsupported segment format version: {version}"
        );
        Ok(Self {
            segment_id: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
            created_unix_ms: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
        })
    }
}

impl ChunkHeader {
    pub(crate) fn encode(self) -> [u8; CHUNK_HEADER_LEN] {
        let mut buf = [0u8; CHUNK_HEADER_LEN];
        buf[..4].copy_from_slice(&CHUNK_MAGIC);
        buf[4..6].copy_from_slice(&(CHUNK_HEADER_LEN as u16).to_be_bytes());
        buf[8..12].copy_from_slice(&self.chunk_ordinal.to_be_bytes());
        buf[12..20].copy_from_slice(&self.first_index.to_be_bytes());
        buf[20..28].copy_from_slice(&self.last_index.to_be_bytes());
        buf[28..36].copy_from_slice(&self.first_slot.to_be_bytes());
        buf[36..44].copy_from_slice(&self.last_slot.to_be_bytes());
        buf[44..48].copy_from_slice(&self.record_count.to_be_bytes());
        buf[48..52].copy_from_slice(&self.compressed_size.to_be_bytes());
        buf[52..56].copy_from_slice(&self.uncompressed_size.to_be_bytes());
        buf[56..60].copy_from_slice(&self.crc32.to_be_bytes());
        buf
    }

    pub(crate) fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes.len() == CHUNK_HEADER_LEN,
            "invalid chunk header length: {}",
            bytes.len()
        );
        anyhow::ensure!(bytes[..4] == CHUNK_MAGIC, "invalid chunk magic");
        let header_len = u16::from_be_bytes(bytes[4..6].try_into().unwrap()) as usize;
        anyhow::ensure!(
            header_len == CHUNK_HEADER_LEN,
            "unsupported chunk header length: {header_len}"
        );
        Ok(Self {
            chunk_ordinal: u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
            first_index: u64::from_be_bytes(bytes[12..20].try_into().unwrap()),
            last_index: u64::from_be_bytes(bytes[20..28].try_into().unwrap()),
            first_slot: u64::from_be_bytes(bytes[28..36].try_into().unwrap()),
            last_slot: u64::from_be_bytes(bytes[36..44].try_into().unwrap()),
            record_count: u32::from_be_bytes(bytes[44..48].try_into().unwrap()),
            compressed_size: u32::from_be_bytes(bytes[48..52].try_into().unwrap()),
            uncompressed_size: u32::from_be_bytes(bytes[52..56].try_into().unwrap()),
            crc32: u32::from_be_bytes(bytes[56..60].try_into().unwrap()),
        })
    }
}

pub(crate) fn append_record(
    slot: Slot,
    message: &ParsedMessage,
    record_buf: &mut Vec<u8>,
    chunk_buf: &mut Vec<u8>,
) {
    record_buf.clear();
    MessageRecordCodec::encode(slot, message, record_buf);
    prost::encoding::encode_varint(record_buf.len() as u64, chunk_buf);
    chunk_buf.extend_from_slice(record_buf);
}

pub(crate) fn next_record<'a>(slice: &mut &'a [u8]) -> anyhow::Result<&'a [u8]> {
    let record_len = decode_varint(slice).context("failed to decode record len")? as usize;
    let (record, rest) = slice
        .split_at_checked(record_len)
        .context("record extends past chunk boundary")?;
    *slice = rest;
    Ok(record)
}

pub(crate) fn write_segment_header(file: &mut File, header: SegmentHeader) -> anyhow::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    file.write_all(&header.encode())?;
    Ok(())
}

pub(crate) fn read_segment_header(file: &mut File) -> anyhow::Result<SegmentHeader> {
    let mut buf = [0u8; SEGMENT_HEADER_LEN];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut buf)?;
    SegmentHeader::decode(&buf)
}

pub(crate) fn chunk_crc32(bytes: &[u8]) -> u32 {
    crc32fast::hash(bytes)
}

pub(crate) fn segment_file_name(segment_id: u64) -> String {
    format!("{segment_id:012}.seg")
}

#[cfg(test)]
mod tests {
    use super::{
        CHUNK_HEADER_LEN, ChunkHeader, SEGMENT_FORMAT_VERSION, SEGMENT_HEADER_LEN, SegmentHeader,
    };

    #[test]
    fn segment_header_roundtrip() {
        let header = SegmentHeader {
            segment_id: 7,
            created_unix_ms: 9,
        };
        assert_eq!(SegmentHeader::decode(&header.encode()).unwrap(), header);
    }

    #[test]
    fn chunk_header_roundtrip() {
        let header = ChunkHeader {
            chunk_ordinal: 1,
            first_index: 10,
            last_index: 20,
            first_slot: 100,
            last_slot: 101,
            record_count: 11,
            compressed_size: 512,
            uncompressed_size: 1024,
            crc32: 42,
        };
        assert_eq!(ChunkHeader::decode(&header.encode()).unwrap(), header);
    }

    #[test]
    fn invalid_header_lengths_rejected() {
        assert!(SegmentHeader::decode(&[0u8; SEGMENT_HEADER_LEN - 1]).is_err());
        assert!(ChunkHeader::decode(&[0u8; CHUNK_HEADER_LEN - 1]).is_err());
    }

    #[test]
    fn invalid_segment_version_rejected() {
        let mut header = SegmentHeader {
            segment_id: 1,
            created_unix_ms: 2,
        }
        .encode();
        header[4..6].copy_from_slice(&(SEGMENT_FORMAT_VERSION + 1).to_be_bytes());
        assert!(SegmentHeader::decode(&header).is_err());
    }
}

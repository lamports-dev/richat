use {
    anyhow::Context,
    prost::encoding::decode_varint,
    std::{
        fs::File,
        io::{Read, Seek, SeekFrom, Write},
    },
};

pub(crate) const SEGMENT_MAGIC: [u8; 4] = *b"RSEG";
pub(crate) const SEGMENT_FORMAT_VERSION: u16 = 1;
pub(crate) const SEGMENT_HEADER_LEN: usize = 32;

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

    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::None => Self::TAG_NONE,
            Self::Zstd(_) => Self::TAG_ZSTD,
        }
    }

    pub(crate) fn from_tag(tag: u8) -> anyhow::Result<Self> {
        match tag {
            Self::TAG_NONE => Ok(Self::None),
            Self::TAG_ZSTD => Ok(Self::Zstd(0)),
            other => anyhow::bail!("unsupported chunk compression tag: {other}"),
        }
    }
}

/// Fixed header stored at the start of every segment file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SegmentHeader {
    pub(crate) segment_id: u64,
    pub(crate) created_unix_ms: u64,
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

pub(crate) fn segment_file_name(segment_id: u64) -> String {
    format!("{segment_id:012}.seg")
}

#[cfg(test)]
mod tests {
    use super::{SEGMENT_FORMAT_VERSION, SEGMENT_HEADER_LEN, SegmentHeader};

    #[test]
    fn segment_header_roundtrip() {
        let header = SegmentHeader {
            segment_id: 7,
            created_unix_ms: 9,
        };
        assert_eq!(SegmentHeader::decode(&header.encode()).unwrap(), header);
    }

    #[test]
    fn invalid_segment_header_length_rejected() {
        assert!(SegmentHeader::decode(&[0u8; SEGMENT_HEADER_LEN - 1]).is_err());
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

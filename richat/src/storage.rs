use {
    crate::{channel::ParsedMessage, config::ConfigChannelStorage},
    anyhow::Context,
    rocksdb::{
        ColumnFamily, ColumnFamilyDescriptor, DBCompressionType, Direction, IteratorMode, Options,
        WriteBatch, DB,
    },
    solana_sdk::clock::Slot,
    std::sync::Arc,
};

trait ColumnName {
    const NAME: &'static str;
}

#[derive(Debug)]
struct MessageInfoIndex;

impl ColumnName for MessageInfoIndex {
    const NAME: &'static str = "message_info_index";
}

impl MessageInfoIndex {
    fn key(slot: Slot, key: u64) -> [u8; 16] {
        let mut index = [0; 16];
        index[0..8].copy_from_slice(&slot.to_be_bytes());
        index[8..16].copy_from_slice(&key.to_be_bytes());
        index
    }

    fn decode(slice: &[u8]) -> anyhow::Result<(Slot, u64)> {
        let (slot, key) = slice.split_at(8);
        let slot = Slot::from_be_bytes(slot.try_into().context("invalid slice size")?);
        let key = Slot::from_be_bytes(key.try_into().context("invalid slice size")?);
        Ok((slot, key))
    }
}

#[derive(Debug)]
struct MessageIndex;

impl ColumnName for MessageIndex {
    const NAME: &'static str = "message_index";
}

impl MessageIndex {
    const fn key(key: u64) -> [u8; 8] {
        key.to_be_bytes()
    }

    fn decode(slice: &[u8]) -> anyhow::Result<u64> {
        slice
            .try_into()
            .map(Slot::from_be_bytes)
            .context("invalid slice size")
    }
}

#[derive(Debug, Clone)]
pub struct Storage {
    // db: Arc<>
}

impl Storage {
    pub fn open(config: ConfigChannelStorage) -> anyhow::Result<Self> {
        let db_options = Self::get_db_options();
        let cf_descriptors = Self::cf_descriptors();

        let db = Arc::new(
            DB::open_cf_descriptors(&db_options, &config.path, cf_descriptors)
                .with_context(|| format!("failed to open rocksdb with path: {:?}", config.path))?,
        );

        Ok(Self {
            // db,
        })
    }

    fn get_db_options() -> Options {
        let mut options = Options::default();

        // Create if not exists
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Set_max_background_jobs(N), configures N/4 low priority threads and 3N/4 high priority threads
        options.set_max_background_jobs(num_cpus::get() as i32);

        // Set max total WAL size to 4GiB
        options.set_max_total_wal_size(4 * 1024 * 1024 * 1024);

        options
    }

    fn get_cf_options(compression: DBCompressionType) -> Options {
        let mut options = Options::default();

        const MAX_WRITE_BUFFER_SIZE: u64 = 256 * 1024 * 1024;
        options.set_max_write_buffer_number(8);
        options.set_write_buffer_size(MAX_WRITE_BUFFER_SIZE as usize);

        let file_num_compaction_trigger = 4;
        let total_size_base = MAX_WRITE_BUFFER_SIZE * file_num_compaction_trigger;
        let file_size_base = total_size_base / 10;
        options.set_level_zero_file_num_compaction_trigger(file_num_compaction_trigger as i32);
        options.set_max_bytes_for_level_base(total_size_base);
        options.set_target_file_size_base(file_size_base);

        options.set_compression_type(compression);

        options
    }

    fn cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
        vec![
            Self::cf_descriptor::<MessageInfoIndex>(DBCompressionType::None),
            Self::cf_descriptor::<MessageIndex>(DBCompressionType::None),
        ]
    }

    fn cf_descriptor<C: ColumnName>(compression: DBCompressionType) -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(C::NAME, Self::get_cf_options(compression))
    }

    fn cf_handle<C: ColumnName>(db: &DB) -> &ColumnFamily {
        db.cf_handle(C::NAME)
            .expect("should never get an unknown column")
    }

    pub fn push_msg(&self, index: u64, message: ParsedMessage) {
        //
    }
}

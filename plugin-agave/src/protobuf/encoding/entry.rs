use {
    super::{bytes_encode, bytes_encoded_len},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaEntryInfoV2,
    prost::encoding,
};

#[derive(Debug)]
pub struct Entry<'a> {
    entry: &'a ReplicaEntryInfoV2<'a>,
}

impl<'a> Entry<'a> {
    pub const fn new(entry: &'a ReplicaEntryInfoV2<'a>) -> Self {
        Self { entry }
    }
}

impl prost::Message for Entry<'_> {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        let index = self.entry.index as u64;
        let starting_transaction_index = self.entry.starting_transaction_index as u64;

        if self.entry.slot != 0 {
            encoding::uint64::encode(1, &self.entry.slot, buf);
        }
        if index != 0 {
            encoding::uint64::encode(2, &index, buf);
        }
        if self.entry.num_hashes != 0 {
            encoding::uint64::encode(3, &self.entry.num_hashes, buf);
        }
        bytes_encode(4, self.entry.hash, buf);
        if self.entry.executed_transaction_count != 0 {
            encoding::uint64::encode(5, &self.entry.executed_transaction_count, buf);
        }
        if starting_transaction_index != 0 {
            encoding::uint64::encode(6, &starting_transaction_index, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let index = self.entry.index as u64;
        let starting_transaction_index = self.entry.starting_transaction_index as u64;

        (if self.entry.slot != 0 {
            encoding::uint64::encoded_len(1, &self.entry.slot)
        } else {
            0
        }) + if index != 0 {
            encoding::uint64::encoded_len(2, &index)
        } else {
            0
        } + if self.entry.num_hashes != 0 {
            encoding::uint64::encoded_len(3, &self.entry.num_hashes)
        } else {
            0
        } + bytes_encoded_len(4, self.entry.hash)
            + if self.entry.executed_transaction_count != 0 {
                encoding::uint64::encoded_len(5, &self.entry.executed_transaction_count)
            } else {
                0
            }
            + if starting_transaction_index != 0 {
                encoding::uint64::encoded_len(6, &starting_transaction_index)
            } else {
                0
            }
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: encoding::WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }

    fn clear(&mut self) {
        unimplemented!()
    }
}

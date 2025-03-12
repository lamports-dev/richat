use {
    super::{bytes_encode, bytes_encoded_len, RewardWrapper},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoV4,
    prost::{
        bytes::BufMut,
        encoding::{self, WireType},
    },
    richat_proto::convert_to,
    solana_transaction_status::RewardsAndNumPartitions,
    std::ops::Deref,
};

#[derive(Debug)]
pub struct BlockMeta<'a> {
    blockinfo: &'a ReplicaBlockInfoV4<'a>,
}

impl<'a> BlockMeta<'a> {
    pub const fn new(blockinfo: &'a ReplicaBlockInfoV4<'a>) -> Self {
        Self { blockinfo }
    }
}

impl prost::Message for BlockMeta<'_> {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        let rewards = RewardsAndNumPartitionsWrapper(self.blockinfo.rewards);

        if self.blockinfo.slot != 0 {
            encoding::uint64::encode(1, &self.blockinfo.slot, buf);
        }
        if !self.blockinfo.blockhash.is_empty() {
            bytes_encode(2, self.blockinfo.blockhash.as_ref(), buf);
        }
        encoding::message::encode(3, &rewards, buf);
        if let Some(block_time) = self.blockinfo.block_time {
            encoding::message::encode(4, &convert_to::create_timestamp(block_time), buf);
        }
        if let Some(block_height) = self.blockinfo.block_height {
            encoding::message::encode(5, &convert_to::create_block_height(block_height), buf);
        }
        if self.blockinfo.parent_slot != 0 {
            encoding::uint64::encode(6, &self.blockinfo.parent_slot, buf);
        }
        if !self.blockinfo.parent_blockhash.is_empty() {
            bytes_encode(7, self.blockinfo.parent_blockhash.as_ref(), buf);
        }
        if self.blockinfo.executed_transaction_count != 0 {
            encoding::uint64::encode(8, &self.blockinfo.executed_transaction_count, buf);
        }
        if self.blockinfo.entry_count != 0 {
            encoding::uint64::encode(9, &self.blockinfo.entry_count, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let rewards = RewardsAndNumPartitionsWrapper(self.blockinfo.rewards);

        (if self.blockinfo.slot != 0 {
            encoding::uint64::encoded_len(1, &self.blockinfo.slot)
        } else {
            0
        }) + if !self.blockinfo.blockhash.is_empty() {
            bytes_encoded_len(2, self.blockinfo.blockhash.as_ref())
        } else {
            0
        } + encoding::message::encoded_len(3, &rewards)
            + if let Some(block_time) = self.blockinfo.block_time {
                encoding::message::encoded_len(4, &convert_to::create_timestamp(block_time))
            } else {
                0
            }
            + if let Some(block_height) = self.blockinfo.block_height {
                encoding::message::encoded_len(5, &convert_to::create_block_height(block_height))
            } else {
                0
            }
            + if self.blockinfo.parent_slot != 0 {
                encoding::uint64::encoded_len(6, &self.blockinfo.parent_slot)
            } else {
                0
            }
            + if !self.blockinfo.parent_blockhash.is_empty() {
                bytes_encoded_len(7, self.blockinfo.parent_blockhash.as_ref())
            } else {
                0
            }
            + if self.blockinfo.executed_transaction_count != 0 {
                encoding::uint64::encoded_len(8, &self.blockinfo.executed_transaction_count)
            } else {
                0
            }
            + if self.blockinfo.entry_count != 0 {
                encoding::uint64::encoded_len(9, &self.blockinfo.entry_count)
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

#[derive(Debug)]
struct RewardsAndNumPartitionsWrapper<'a>(&'a RewardsAndNumPartitions);

impl<'a> Deref for RewardsAndNumPartitionsWrapper<'a> {
    type Target = &'a RewardsAndNumPartitions;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl prost::Message for RewardsAndNumPartitionsWrapper<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        encoding::message::encode_repeated(1, RewardWrapper::new(&self.rewards), buf);
        if let Some(num_partitions) = self.num_partitions {
            encoding::message::encode(2, &convert_to::create_num_partitions(num_partitions), buf);
        }
    }

    fn encoded_len(&self) -> usize {
        encoding::message::encoded_len_repeated(1, RewardWrapper::new(&self.rewards))
            + if let Some(num_partitions) = self.num_partitions {
                encoding::message::encoded_len(
                    2,
                    &convert_to::create_num_partitions(num_partitions),
                )
            } else {
                0
            }
    }

    fn clear(&mut self) {
        unimplemented!()
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: WireType,
        _buf: &mut impl bytes::Buf,
        _ctx: encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!()
    }
}

use {
    super::{bytes_encode, bytes_encoded_len, encode_rewards, rewards_encoded_len},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoV4,
    prost::{
        bytes::BufMut,
        encoding::{self, WireType},
    },
    solana_transaction_status::RewardsAndNumPartitions,
};

#[derive(Debug)]
pub struct BlockMeta<'a> {
    blockinfo: &'a ReplicaBlockInfoV4<'a>,
}

impl<'a> prost::Message for BlockMeta<'a> {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        let rewards = RewardsAndNumPartitionsWrapper(self.blockinfo.rewards);
        let block_time = BlockTime(self.blockinfo.block_time);
        let block_height = BlockHeight(self.blockinfo.block_height);

        if self.blockinfo.slot != 0 {
            encoding::uint64::encode(1, &self.blockinfo.slot, buf)
        }
        if !self.blockinfo.blockhash.is_empty() {
            bytes_encode(2, self.blockinfo.blockhash.as_ref(), buf)
        }
        encoding::message::encode(3, &rewards, buf);
        encoding::message::encode(4, &block_time, buf);
        encoding::message::encode(5, &block_height, buf);
        if self.blockinfo.parent_slot != 0 {
            encoding::uint64::encode(6, &self.blockinfo.parent_slot, buf)
        }
        if !self.blockinfo.parent_blockhash.is_empty() {
            bytes_encode(7, self.blockinfo.parent_blockhash.as_ref(), buf);
        }
        if self.blockinfo.executed_transaction_count != 0 {
            encoding::uint64::encode(8, &self.blockinfo.executed_transaction_count, buf)
        }
        if self.blockinfo.entry_count != 0 {
            encoding::uint64::encode(9, &self.blockinfo.entry_count, buf)
        }
    }

    fn encoded_len(&self) -> usize {
        let rewards = RewardsAndNumPartitionsWrapper(self.blockinfo.rewards);
        let block_time = BlockTime(self.blockinfo.block_time);
        let block_height = BlockHeight(self.blockinfo.block_height);

        (if self.blockinfo.slot != 0 {
            encoding::uint64::encoded_len(1, &self.blockinfo.slot)
        } else {
            0
        }) + if !self.blockinfo.blockhash.is_empty() {
            bytes_encoded_len(2, self.blockinfo.blockhash.as_ref())
        } else {
            0
        } + encoding::message::encoded_len(3, &rewards)
            + encoding::message::encoded_len(4, &block_time)
            + encoding::message::encoded_len(5, &block_height)
            + if self.blockinfo.parent_slot != 0 {
                encoding::uint64::encoded_len(7, &self.blockinfo.parent_slot)
            } else {
                0
            }
            + if !self.blockinfo.parent_blockhash.is_empty() {
                bytes_encoded_len(8, self.blockinfo.parent_blockhash.as_ref())
            } else {
                0
            }
            + if self.blockinfo.executed_transaction_count != 0 {
                encoding::uint64::encoded_len(9, &self.blockinfo.executed_transaction_count)
            } else {
                0
            }
            + if self.blockinfo.entry_count != 0 {
                encoding::uint64::encoded_len(12, &self.blockinfo.entry_count)
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

impl<'a> BlockMeta<'a> {
    pub const fn new(blockinfo: &'a ReplicaBlockInfoV4<'a>) -> Self {
        Self { blockinfo }
    }
}

#[derive(Debug)]
struct RewardsAndNumPartitionsWrapper<'a>(&'a RewardsAndNumPartitions);

impl<'a> prost::Message for RewardsAndNumPartitionsWrapper<'a> {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        let num_partitions = NumPartitions(self.0.num_partitions);

        encode_rewards(1, &self.0.rewards, buf);
        encoding::message::encode(2, &num_partitions, buf)
    }
    fn encoded_len(&self) -> usize {
        let num_partitions = NumPartitions(self.0.num_partitions);

        rewards_encoded_len(1, &self.0.rewards) + encoding::message::encoded_len(2, &num_partitions)
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

#[derive(Debug)]
struct BlockTime(Option<i64>);

impl prost::Message for BlockTime {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        if let Some(block_time) = self.0 {
            if block_time != 0 {
                encoding::int64::encode(1, &block_time, buf)
            }
        }
    }
    fn encoded_len(&self) -> usize {
        self.0.map_or(0, |block_time| {
            if block_time != 0 {
                encoding::int64::encoded_len(1, &block_time)
            } else {
                0
            }
        })
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

#[derive(Debug)]
struct BlockHeight(Option<u64>);

impl prost::Message for BlockHeight {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        if let Some(block_height) = self.0 {
            if block_height != 0 {
                encoding::uint64::encode(1, &block_height, buf)
            }
        }
    }
    fn encoded_len(&self) -> usize {
        self.0.map_or(0, |block_height| {
            if block_height != 0 {
                encoding::uint64::encoded_len(1, &block_height)
            } else {
                0
            }
        })
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

#[derive(Debug)]
struct NumPartitions(Option<u64>);

impl prost::Message for NumPartitions {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        if let Some(num_partitions) = self.0 {
            if num_partitions != 0 {
                encoding::uint64::encode(1, &num_partitions, buf)
            }
        }
    }
    fn encoded_len(&self) -> usize {
        self.0.map_or(0, |num_partitions| {
            if num_partitions != 0 {
                encoding::uint64::encoded_len(1, &num_partitions)
            } else {
                0
            }
        })
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

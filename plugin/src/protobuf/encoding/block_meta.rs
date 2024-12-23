use {
    super::{
        bytes_encode, bytes_encoded_len, encode_rewards, field_encoded_len, rewards_encoded_len,
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoV4,
    prost::{
        bytes::BufMut,
        encoding::{self, encode_key, encode_varint, WireType},
    },
    solana_transaction_status::RewardsAndNumPartitions,
};

#[derive(Debug)]
pub struct BlockMeta<'a> {
    blockinfo: &'a ReplicaBlockInfoV4<'a>,
}

impl<'a> prost::Message for BlockMeta<'a> {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        if self.blockinfo.slot != 0 {
            encoding::uint64::encode(1, &self.blockinfo.slot, buf)
        }
        if !self.blockinfo.blockhash.is_empty() {
            bytes_encode(2, self.blockinfo.blockhash.as_ref(), buf)
        }
        encode_rewards_and_num_partitions(3, self.blockinfo.rewards, buf);
        encode_block_time(4, &self.blockinfo.block_time, buf);
        encode_uint64_optional_message(5, &self.blockinfo.block_height, buf);
        if self.blockinfo.parent_slot != 0 {
            encoding::uint64::encode(7, &self.blockinfo.parent_slot, buf)
        }
        if !self.blockinfo.parent_blockhash.is_empty() {
            bytes_encode(8, self.blockinfo.parent_blockhash.as_ref(), buf);
        }
        if self.blockinfo.executed_transaction_count != 0 {
            encoding::uint64::encode(9, &self.blockinfo.executed_transaction_count, buf)
        }
        if self.blockinfo.entry_count != 0 {
            encoding::uint64::encode(12, &self.blockinfo.entry_count, buf)
        }
    }

    fn encoded_len(&self) -> usize {
        (if self.blockinfo.slot != 0 {
            encoding::uint64::encoded_len(1, &self.blockinfo.slot)
        } else {
            0
        }) + if !self.blockinfo.blockhash.is_empty() {
            bytes_encoded_len(2, self.blockinfo.blockhash.as_ref())
        } else {
            0
        } + rewards_and_num_partitions_encoded_len(3, self.blockinfo.rewards)
            + block_time_encoded_len(4, &self.blockinfo.block_time)
            + uint64_optional_message_encoded_len(5, &self.blockinfo.block_height)
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
        _buf: &mut impl hyper::body::Buf,
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

fn encode_rewards_and_num_partitions(
    tag: u32,
    rewards: &RewardsAndNumPartitions,
    buf: &mut impl BufMut,
) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(
        rewards_and_num_partitions_encoded_len(tag, rewards) as u64,
        buf,
    );

    encode_rewards(1, &rewards.rewards, buf);
    encode_uint64_optional_message(2, &rewards.num_partitions, buf)
}

fn rewards_and_num_partitions_encoded_len(tag: u32, rewards: &RewardsAndNumPartitions) -> usize {
    let len = rewards_encoded_len(1, &rewards.rewards)
        + uint64_optional_message_encoded_len(2, &rewards.num_partitions);
    field_encoded_len(tag, len)
}

fn encode_block_time(tag: u32, block_time: &Option<i64>, buf: &mut impl BufMut) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(block_time_encoded_len(tag, block_time) as u64, buf);

    if let Some(block_time) = block_time {
        if *block_time != 0 {
            encoding::int64::encode(1, block_time, buf)
        }
    }
}

fn block_time_encoded_len(tag: u32, block_time: &Option<i64>) -> usize {
    let len = block_time.map_or(0, |block_time| encoding::int64::encoded_len(1, &block_time));
    field_encoded_len(tag, len)
}

fn encode_uint64_optional_message(tag: u32, value: &Option<u64>, buf: &mut impl BufMut) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(uint64_optional_message_encoded_len(tag, value) as u64, buf);

    if let Some(value) = value {
        if *value != 0 {
            encoding::uint64::encode(1, value, buf)
        }
    }
}

fn uint64_optional_message_encoded_len(tag: u32, value: &Option<u64>) -> usize {
    let len = value.map_or(0, |value| encoding::uint64::encoded_len(1, &value));
    field_encoded_len(tag, len)
}

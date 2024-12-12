use {
    super::bytes_encoded_len,
    crate::protobuf::encoding::{
        bytes_encode,
        proto::{convert_to, BlockHeight, Rewards, UnixTimestamp},
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoV4,
    prost::encoding::{self, message},
    solana_sdk::clock::Slot,
};

#[derive(Debug)]
pub struct BlockMeta<'a> {
    slot: Slot,
    blockhash: &'a str,
    rewards: Rewards,
    block_time: UnixTimestamp,
    block_height: BlockHeight,
    parent_slot: u64,
    parent_blockhash: &'a str,
    executed_transaction_count: u64,
    entries_count: u64,
}

impl<'a> super::super::Message for BlockMeta<'a> {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        encoding::uint64::encode(1, &self.slot, buf);
        bytes_encode(2, self.blockhash.as_ref(), buf);
        message::encode(3, &self.rewards, buf);
        message::encode(4, &self.block_time, buf);
        message::encode(5, &self.block_height, buf);
        encoding::uint64::encode(6, &self.parent_slot, buf);
        bytes_encode(7, self.parent_blockhash.as_ref(), buf);
        encoding::uint64::encode(8, &self.executed_transaction_count, buf);
        encoding::uint64::encode(9, &self.entries_count, buf);
    }
    fn encoded_len(&self) -> usize {
        encoding::uint64::encoded_len(1, &self.slot)
            + bytes_encoded_len(2, self.blockhash.as_ref())
            + message::encoded_len(3, &self.rewards)
            + message::encoded_len(4, &self.block_time)
            + message::encoded_len(5, &self.block_height)
            + encoding::uint64::encoded_len(6, &self.parent_slot)
            + bytes_encoded_len(7, self.parent_blockhash.as_ref())
            + encoding::uint64::encoded_len(8, &self.executed_transaction_count)
            + encoding::uint64::encoded_len(9, &self.entries_count)
    }
}

impl<'a> From<&'a ReplicaBlockInfoV4<'a>> for BlockMeta<'a> {
    fn from(value: &'a ReplicaBlockInfoV4<'a>) -> Self {
        Self {
            slot: value.slot,
            blockhash: value.blockhash,
            rewards: convert_to::create_rewards_obj(
                &value.rewards.rewards,
                value.rewards.num_partitions,
            ),
            block_time: convert_to::create_timestamp(value.block_time.unwrap_or(0)),
            block_height: convert_to::create_block_height(value.block_height.unwrap_or(0)),
            parent_slot: value.parent_slot,
            parent_blockhash: value.parent_blockhash,
            executed_transaction_count: value.executed_transaction_count,
            entries_count: value.entry_count,
        }
    }
}

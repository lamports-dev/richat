use {
    super::bytes_encoded_len,
    crate::protobuf::encoding::bytes_encode,
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoV4,
    prost::encoding::{self, message},
};

#[derive(Debug)]
pub struct BlockMeta<'a> {
    blockinfo: &'a ReplicaBlockInfoV4<'a>,
}

impl<'a> prost::Message for BlockMeta<'a> {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        let rewards = convert_to::create_rewards_obj(
            &self.blockinfo.rewards.rewards,
            self.blockinfo.rewards.num_partitions,
        );
        let block_time = convert_to::create_timestamp(self.blockinfo.block_time.unwrap_or(0));
        let block_height =
            convert_to::create_block_height(self.blockinfo.block_height.unwrap_or(0));

        encoding::uint64::encode(1, &self.blockinfo.slot, buf);
        bytes_encode(2, self.blockinfo.blockhash.as_ref(), buf);
        message::encode(3, &rewards, buf); // TODO
        message::encode(4, &block_time, buf); // TODO
        message::encode(5, &block_height, buf); // TODO
        encoding::uint64::encode(6, &self.blockinfo.parent_slot, buf);
        bytes_encode(7, self.blockinfo.parent_blockhash.as_ref(), buf);
        encoding::uint64::encode(8, &self.blockinfo.executed_transaction_count, buf);
        encoding::uint64::encode(9, &self.blockinfo.entry_count, buf);
    }
    fn encoded_len(&self) -> usize {
        let rewards = convert_to::create_rewards_obj(
            &self.blockinfo.rewards.rewards,
            self.blockinfo.rewards.num_partitions,
        );
        let block_time = convert_to::create_timestamp(self.blockinfo.block_time.unwrap_or(0));
        let block_height =
            convert_to::create_block_height(self.blockinfo.block_height.unwrap_or(0));

        encoding::uint64::encoded_len(1, &self.blockinfo.slot)
            + bytes_encoded_len(2, self.blockinfo.blockhash.as_ref())
            + message::encoded_len(3, &rewards)
            + message::encoded_len(4, &block_time)
            + message::encoded_len(5, &block_height)
            + encoding::uint64::encoded_len(6, &self.blockinfo.parent_slot)
            + bytes_encoded_len(7, self.blockinfo.parent_blockhash.as_ref())
            + encoding::uint64::encoded_len(8, &self.blockinfo.executed_transaction_count)
            + encoding::uint64::encoded_len(9, &self.blockinfo.entry_count)
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

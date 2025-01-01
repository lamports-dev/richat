use {
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus, prost::encoding,
    yellowstone_grpc_proto::geyser::CommitmentLevel,
};

const fn slot_status_as_i32(status: SlotStatus) -> i32 {
    match status {
        SlotStatus::Processed => 0,
        SlotStatus::Rooted => 2,
        SlotStatus::Confirmed => 1,
    }
}

#[derive(Debug)]
pub struct Slot<'a> {
    slot: solana_sdk::clock::Slot,
    parent: Option<u64>,
    status: &'a SlotStatus,
}

impl<'a> Slot<'a> {
    pub const fn new(
        slot: solana_sdk::clock::Slot,
        parent: Option<u64>,
        status: &'a SlotStatus,
    ) -> Self {
        Self {
            slot,
            parent,
            status,
        }
    }
}

impl<'a> prost::Message for Slot<'a> {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        let status = slot_status_as_i32(*self.status);

        if self.slot != 0u64 {
            encoding::uint64::encode(1u32, &self.slot, buf);
        }
        if let Some(value) = &self.parent {
            encoding::uint64::encode(2u32, value, buf);
        }
        if status != CommitmentLevel::default() as i32 {
            encoding::int32::encode(3u32, &status, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let status = slot_status_as_i32(*self.status);

        (if self.slot != 0u64 {
            encoding::uint64::encoded_len(1u32, &self.slot)
        } else {
            0
        }) + self
            .parent
            .as_ref()
            .map_or(0, |value| encoding::uint64::encoded_len(2u32, value))
            + if status != CommitmentLevel::default() as i32 {
                encoding::int32::encoded_len(3u32, &status)
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

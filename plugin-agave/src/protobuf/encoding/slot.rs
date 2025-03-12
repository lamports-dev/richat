use {
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus, prost::encoding,
    richat_proto::geyser::CommitmentLevel,
};

const fn slot_status_as_i32(status: &SlotStatus) -> i32 {
    match status {
        SlotStatus::Processed => 0,
        SlotStatus::Rooted => 2,
        SlotStatus::Confirmed => 1,
        SlotStatus::FirstShredReceived => 3,
        SlotStatus::Completed => 4,
        SlotStatus::CreatedBank => 5,
        SlotStatus::Dead(_) => 6,
    }
}

const fn slot_status_as_dead_error(status: &SlotStatus) -> Option<&String> {
    if let SlotStatus::Dead(dead) = status {
        Some(dead)
    } else {
        None
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

impl prost::Message for Slot<'_> {
    fn encode_raw(&self, buf: &mut impl bytes::BufMut) {
        let status = slot_status_as_i32(self.status);
        let dead_error = slot_status_as_dead_error(self.status);

        if self.slot != 0u64 {
            encoding::uint64::encode(1u32, &self.slot, buf);
        }
        if let Some(value) = &self.parent {
            encoding::uint64::encode(2u32, value, buf);
        }
        if status != CommitmentLevel::default() as i32 {
            encoding::int32::encode(3u32, &status, buf);
        }
        if let Some(value) = dead_error {
            encoding::string::encode(4u32, value, buf);
        }
    }

    fn encoded_len(&self) -> usize {
        let status = slot_status_as_i32(self.status);
        let dead_error = slot_status_as_dead_error(self.status);

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
            + dead_error
                .as_ref()
                .map_or(0, |value| encoding::string::encoded_len(4u32, value))
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

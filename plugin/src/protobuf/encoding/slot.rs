use {agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus, prost::encoding};

const fn slot_status_as_i32(status: &SlotStatus) -> i32 {
    match status {
        SlotStatus::Processed => 0,
        SlotStatus::Rooted => 1,
        SlotStatus::Confirmed => 2,
        SlotStatus::FirstShredReceived => 3,
        SlotStatus::Completed => 4,
        SlotStatus::CreatedBank => 5,
        SlotStatus::Dead(_) => 6,
    }
}

const fn is_slot_status_dead(status: &SlotStatus) -> Option<&String> {
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

impl<'a> prost::Message for Slot<'a> {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        let status = slot_status_as_i32(self.status);
        let dead = is_slot_status_dead(self.status);
        if self.slot != 0 {
            encoding::uint64::encode(1, &self.slot, buf)
        }
        if let Some(ref value) = self.parent {
            encoding::uint64::encode(2, value, buf)
        }
        if status != slot_status_as_i32(&SlotStatus::Processed) {
            encoding::int32::encode(3, &status, buf)
        }
        if let Some(value) = dead {
            encoding::string::encode(4, value, buf)
        }
    }

    fn encoded_len(&self) -> usize {
        let status = slot_status_as_i32(self.status);
        let dead = is_slot_status_dead(self.status);
        (if self.slot != 0 {
            encoding::uint64::encoded_len(1, &self.slot)
        } else {
            0
        }) + self
            .parent
            .as_ref()
            .map_or(0, |value| encoding::uint64::encoded_len(2, value))
            + if status != slot_status_as_i32(&SlotStatus::Processed) {
                encoding::int32::encoded_len(3, &status)
            } else {
                0
            }
            + dead.map_or(0, |dead| encoding::string::encoded_len(4, dead))
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

#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus,
    arbitrary::Arbitrary,
    libfuzzer_sys::fuzz_target,
    richat_plugin::protobuf::{ProtobufEncoder, ProtobufMessage},
};

#[derive(Clone, Copy, Arbitrary, Debug)]
#[repr(i32)]
pub enum FuzzSlotStatus {
    Processed = 0,
    Rooted = 1,
    Confirmed = 2,
}

impl From<FuzzSlotStatus> for SlotStatus {
    fn from(fuzz: FuzzSlotStatus) -> Self {
        match fuzz {
            FuzzSlotStatus::Processed => SlotStatus::Processed,
            FuzzSlotStatus::Rooted => SlotStatus::Rooted,
            FuzzSlotStatus::Confirmed => SlotStatus::Confirmed,
        }
    }
}

#[derive(Arbitrary, Debug)]
pub struct FuzzSlot {
    slot: u64,
    parent: Option<u64>,
    status: FuzzSlotStatus,
}

fuzz_target!(|fuzz_slot: FuzzSlot| {
    let mut buf = Vec::new();
    let message = ProtobufMessage::Slot {
        slot: fuzz_slot.slot,
        parent: fuzz_slot.parent,
        status: &fuzz_slot.status.into(),
    };
    message.encode(ProtobufEncoder::Raw, &mut buf);
    assert!(!buf.is_empty())
});

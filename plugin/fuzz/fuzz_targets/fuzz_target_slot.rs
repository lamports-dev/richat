#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus, arbitrary::Arbitrary,
    libfuzzer_sys::fuzz_target, richat_plugin::protobuf::ProtobufMessage,
};

#[derive(Clone, Copy, Arbitrary, Debug)]
#[repr(i32)]
pub enum FuzzSlotStatus {
    Processed = 0,
    Rooted = 1,
    Confirmed = 2,
    FirstShredReceived = 3,
    Completed = 4,
    CreatedBank = 5,
    Dead = 6,
}

impl From<FuzzSlotStatus> for SlotStatus {
    fn from(fuzz: FuzzSlotStatus) -> Self {
        match fuzz {
            FuzzSlotStatus::Processed => SlotStatus::Processed,
            FuzzSlotStatus::Rooted => SlotStatus::Rooted,
            FuzzSlotStatus::Confirmed => SlotStatus::Confirmed,
            FuzzSlotStatus::FirstShredReceived => SlotStatus::FirstShredReceived,
            FuzzSlotStatus::Completed => SlotStatus::Completed,
            FuzzSlotStatus::CreatedBank => SlotStatus::CreatedBank,
            FuzzSlotStatus::Dead => SlotStatus::Dead(String::new()),
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
    message.encode(&mut buf);
    assert!(!buf.is_empty())
});

#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus, arbitrary::Arbitrary,
    libfuzzer_sys::fuzz_target, richat_plugin::protobuf::ProtobufMessage,
};

#[derive(Arbitrary, Debug)]
pub enum FuzzSlotStatus {
    Processed,
    Rooted,
    Confirmed,
    FirstShredReceived,
    Completed,
    CreatedBank,
    Dead(String),
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
            FuzzSlotStatus::Dead(dead) => SlotStatus::Dead(dead),
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

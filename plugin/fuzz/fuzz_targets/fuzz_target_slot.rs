#![no_main]

use agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus;
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use richat_plugin::protobuf::ProtobufMessage;

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

impl FuzzSlotStatus {
    pub fn into_solana(self) -> SlotStatus {
        match self {
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
        status: &fuzz_slot.status.into_solana(),
    };
    message.encode(&mut buf);
    assert!(!buf.is_empty())
});

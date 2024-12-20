#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus,
    arbitrary::Arbitrary,
    libfuzzer_sys::fuzz_target,
    prost_011::{Enumeration, Message},
    richat_plugin::protobuf::ProtobufMessage,
};

#[derive(Message)] // FIXME: compile error!!!
pub struct Slot {
    #[prost(uint64, tag = "1")]
    slot: u64,
    #[prost(uint64, optional, tag = "2")]
    parent: Option<u64>,
    #[prost(enumeration = "FuzzSlotStatus", tag = "3")]
    status: i32,
}

#[derive(Arbitrary, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Enumeration)]
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

impl Into<SlotStatus> for FuzzSlotStatus {
    fn into(self) -> SlotStatus {
        match self {
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

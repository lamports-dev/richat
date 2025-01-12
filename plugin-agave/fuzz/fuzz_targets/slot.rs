#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus, arbitrary::Arbitrary,
    richat_plugin_agave::protobuf::ProtobufMessage, std::time::SystemTime,
};

#[derive(Debug, Clone, Copy, Arbitrary)]
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

libfuzzer_sys::fuzz_target!(|fuzz_slot: FuzzSlot| {
    let message = ProtobufMessage::Slot {
        slot: fuzz_slot.slot,
        parent: fuzz_slot.parent,
        status: &fuzz_slot.status.into(),
    };
    let created_at = SystemTime::now();

    let vec_prost = message.encode_prost(created_at);
    let vec_raw = message.encode_raw(created_at);

    assert_eq!(
        vec_prost,
        vec_raw,
        "prost hex: {}",
        const_hex::encode(&vec_prost)
    );
});

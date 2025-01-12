#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaEntryInfoV2,
    arbitrary::Arbitrary, richat_plugin_agave::protobuf::ProtobufMessage, std::time::SystemTime,
};

#[derive(Debug, Arbitrary)]
pub struct FuzzEntry {
    pub slot: u64,
    pub index: usize,
    pub num_hashes: u64,
    pub hash: Vec<u8>,
    pub executed_transaction_count: u64,
    pub starting_transaction_index: usize,
}

libfuzzer_sys::fuzz_target!(|fuzz_entry: FuzzEntry| {
    let message = ProtobufMessage::Entry {
        entry: &ReplicaEntryInfoV2 {
            slot: fuzz_entry.slot,
            index: fuzz_entry.index,
            num_hashes: fuzz_entry.num_hashes,
            hash: &fuzz_entry.hash,
            executed_transaction_count: fuzz_entry.executed_transaction_count,
            starting_transaction_index: fuzz_entry.starting_transaction_index,
        },
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

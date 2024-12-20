#![no_main]

use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaEntryInfoV2;
use libfuzzer_sys::fuzz_target;
use prost_011::Message;
use richat_plugin::protobuf::ProtobufMessage;
use solana_storage_proto::convert::entries::Entry;

#[derive(arbitrary::Arbitrary, Debug)]
pub struct FuzzEntry {
    pub slot: u64,
    pub index: usize,
    pub num_hashes: u64,
    pub hash: Vec<u8>,
    pub executed_transaction_count: u64,
    pub starting_transaction_index: usize,
}

fuzz_target!(|fuzz_entry: FuzzEntry| {
    let mut buf = Vec::new();
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
    message.encode(&mut buf);
    assert!(!buf.is_empty())
});

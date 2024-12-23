#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaEntryInfoV2,
    libfuzzer_sys::fuzz_target, prost::Message, richat_plugin::protobuf::ProtobufMessage,
};

#[derive(Message)]
pub struct Entry {
    #[prost(uint32, tag = "1")]
    pub index: u32,
    #[prost(uint64, tag = "2")]
    pub num_hashes: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub hash: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub num_transactions: u64,
    #[prost(uint32, tag = "5")]
    pub starting_transaction_index: u32,
}

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
    assert!(!buf.is_empty());

    let decoded = Entry::decode(buf.as_slice()).expect("failed to decode `Entry` from buf");
    assert_eq!(decoded.index as usize, fuzz_entry.index);
    assert_eq!(decoded.num_hashes, fuzz_entry.num_hashes);
    assert_eq!(decoded.hash, fuzz_entry.hash);
    assert_eq!(
        decoded.num_transactions,
        fuzz_entry.executed_transaction_count
    );
    assert_eq!(
        decoded.starting_transaction_index as usize,
        fuzz_entry.starting_transaction_index
    )
});

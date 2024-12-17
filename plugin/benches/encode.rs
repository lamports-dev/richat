use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaEntryInfoV2;
use solana_sdk::hash::Hash;

pub struct Account;

pub fn accounts() -> Vec<Account> {
    todo!()
}

pub fn entries<'a>() -> [ReplicaEntryInfoV2<'static>; 2] {
    const FIRST_ENTRY_HASH: Hash = Hash::new_from_array([98; 32]);
    const SECOND_ENTRY_HASH: Hash = Hash::new_from_array([42; 32]);
    [
        ReplicaEntryInfoV2 {
            slot: 299888121,
            index: 42,
            num_hashes: 128,
            hash: FIRST_ENTRY_HASH.as_ref(),
            executed_transaction_count: 32,
            starting_transaction_index: 1000,
        },
        ReplicaEntryInfoV2 {
            slot: 299888121,
            index: 0,
            num_hashes: 16,
            hash: SECOND_ENTRY_HASH.as_ref(),
            executed_transaction_count: 32,
            starting_transaction_index: 1000,
        },
    ]
}

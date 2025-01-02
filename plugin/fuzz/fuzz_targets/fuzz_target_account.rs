#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoV3,
    libfuzzer_sys::fuzz_target,
    richat_plugin::protobuf::{ProtobufEncoder, ProtobufMessage},
    solana_sdk::pubkey::PUBKEY_BYTES,
};

#[derive(arbitrary::Arbitrary, Debug)]
pub struct FuzzAccount<'a> {
    pubkey: [u8; PUBKEY_BYTES],
    lamports: u64,
    owner: [u8; PUBKEY_BYTES],
    executable: bool,
    rent_epoch: u64,
    data: &'a [u8],
    write_version: u64,
}

#[derive(arbitrary::Arbitrary, Debug)]
pub struct FuzzAccountMessage<'a> {
    slot: u64,
    account: FuzzAccount<'a>,
}

fuzz_target!(|fuzz_message: FuzzAccountMessage| {
    let mut buf = Vec::new();
    let message = ProtobufMessage::Account {
        account: &ReplicaAccountInfoV3 {
            pubkey: &fuzz_message.account.pubkey,
            lamports: fuzz_message.account.lamports,
            owner: &fuzz_message.account.owner,
            executable: fuzz_message.account.executable,
            rent_epoch: fuzz_message.account.rent_epoch,
            data: fuzz_message.account.data,
            write_version: fuzz_message.account.write_version,
            txn: None,
        },
        slot: fuzz_message.slot,
    };
    message.encode(ProtobufEncoder::Raw, &mut buf);
    assert!(!buf.is_empty())
});

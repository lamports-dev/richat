#![no_main]

use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoV3;
use libfuzzer_sys::fuzz_target;
use richat_plugin::protobuf::ProtobufMessage;

const PUBKEY_LEN: usize = 32;

#[derive(arbitrary::Arbitrary, Debug)]
pub struct Account<'a> {
    pubkey: &'a [u8],
    lamports: u64,
    owner: &'a [u8],
    executable: bool,
    rent_epoch: u64,
    data: &'a [u8],
    write_version: u64,
}

#[derive(arbitrary::Arbitrary, Debug)]
pub struct FuzzMessage<'a> {
    slot: u64,
    account: Account<'a>,
}

fuzz_target!(|fuzz_message: FuzzMessage| {
    if fuzz_message.account.pubkey.len() != PUBKEY_LEN
        || fuzz_message.account.owner.len() != PUBKEY_LEN
    {
        return;
    }

    let mut buf = Vec::new();
    let message = ProtobufMessage::Account {
        slot: fuzz_message.slot,
        account: &ReplicaAccountInfoV3 {
            pubkey: fuzz_message.account.pubkey,
            lamports: fuzz_message.account.lamports,
            owner: fuzz_message.account.owner,
            executable: fuzz_message.account.executable,
            rent_epoch: fuzz_message.account.rent_epoch,
            data: fuzz_message.account.data,
            write_version: fuzz_message.account.write_version,
            txn: None,
        },
    };
    message.encode(&mut buf);
    assert!(!buf.is_empty())
});

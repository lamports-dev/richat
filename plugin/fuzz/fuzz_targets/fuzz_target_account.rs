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
pub struct Message<'a> {
    slot: u64,
    account: Account<'a>,
}

fuzz_target!(|message: Message| {
    if message.account.pubkey.len() != PUBKEY_LEN || message.account.owner.len() != PUBKEY_LEN {
        return;
    }

    let mut buf = Vec::new();
    let message = ProtobufMessage::Account {
        slot: message.slot,
        account: &ReplicaAccountInfoV3 {
            pubkey: message.account.pubkey,
            lamports: message.account.lamports,
            owner: message.account.owner,
            executable: message.account.executable,
            rent_epoch: message.account.rent_epoch,
            data: message.account.data,
            write_version: message.account.write_version,
            txn: None,
        },
    };
    let encoded = message.encode(&mut buf);
    assert!(!encoded.is_empty())
});

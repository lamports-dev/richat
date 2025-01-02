#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoV3,
    arbitrary::Arbitrary,
    richat_plugin::protobuf::ProtobufMessage,
    solana_sdk::{
        message::{LegacyMessage, Message, SanitizedMessage},
        pubkey::PUBKEY_BYTES,
        signature::SIGNATURE_BYTES,
        transaction::SanitizedTransaction,
    },
    std::{collections::HashSet, time::SystemTime},
};

#[derive(Debug, Arbitrary)]
pub struct FuzzAccount<'a> {
    pubkey: [u8; PUBKEY_BYTES],
    lamports: u64,
    owner: [u8; PUBKEY_BYTES],
    executable: bool,
    rent_epoch: u64,
    data: &'a [u8],
    write_version: u64,
    txn: Option<[u8; SIGNATURE_BYTES]>,
}

#[derive(Debug, Arbitrary)]
pub struct FuzzAccountMessage<'a> {
    slot: u64,
    account: FuzzAccount<'a>,
}

libfuzzer_sys::fuzz_target!(|fuzz_message: FuzzAccountMessage| {
    let txn = fuzz_message.account.txn.map(|signature| {
        SanitizedTransaction::new_for_tests(
            SanitizedMessage::Legacy(LegacyMessage::new(Message::default(), &HashSet::new())),
            vec![signature.as_slice().try_into().unwrap()],
            false,
        )
    });

    let message = ProtobufMessage::Account {
        account: &ReplicaAccountInfoV3 {
            pubkey: &fuzz_message.account.pubkey,
            lamports: fuzz_message.account.lamports,
            owner: &fuzz_message.account.owner,
            executable: fuzz_message.account.executable,
            rent_epoch: fuzz_message.account.rent_epoch,
            data: fuzz_message.account.data,
            write_version: fuzz_message.account.write_version,
            txn: txn.as_ref(),
        },
        slot: fuzz_message.slot,
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

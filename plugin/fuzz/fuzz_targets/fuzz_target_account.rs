#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoV3,
    libfuzzer_sys::fuzz_target, prost::Message, richat_plugin::protobuf::ProtobufMessage,
    solana_sdk::pubkey::PUBKEY_BYTES,
};

#[derive(Message)]
pub struct Account {
    #[prost(bytes = "vec", tag = "1")]
    pubkey: Vec<u8>,
    #[prost(uint64, tag = "2")]
    lamports: u64,
    #[prost(bytes = "vec", tag = "3")]
    owner: Vec<u8>,
    #[prost(bool, tag = "4")]
    executable: bool,
    #[prost(uint64, tag = "5")]
    rent_epoch: u64,
    #[prost(bytes = "vec", tag = "6")]
    data: Vec<u8>,
    #[prost(uint64, tag = "7")]
    write_version: u64,
}

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
        slot: fuzz_message.slot,
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
    };
    message.encode(&mut buf);
    assert!(!buf.is_empty());

    let decoded = Account::decode(buf.as_slice()).expect("failed to decode `Account` from buf");
    assert_eq!(&decoded.pubkey, &fuzz_message.account.pubkey);
    assert_eq!(decoded.lamports, fuzz_message.account.lamports);
    assert_eq!(&decoded.owner, &fuzz_message.account.owner);
    assert_eq!(decoded.executable, fuzz_message.account.executable);
    assert_eq!(decoded.rent_epoch, fuzz_message.account.rent_epoch);
    assert_eq!(decoded.write_version, fuzz_message.account.write_version)
});

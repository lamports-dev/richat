#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoV3,
    arbitrary::Arbitrary,
    richat_plugin_agave::protobuf::ProtobufMessage,
    solana_hash::{HASH_BYTES, Hash},
    solana_message::{LegacyMessage, Message, MessageHeader, SanitizedMessage},
    solana_pubkey::{PUBKEY_BYTES, Pubkey},
    solana_signature::{SIGNATURE_BYTES, Signature},
    solana_transaction::sanitized::SanitizedTransaction,
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
    let txn = fuzz_message.account.txn.and_then(|signature| {
        SanitizedTransaction::try_new_from_fields(
            SanitizedMessage::Legacy(LegacyMessage::new(
                Message {
                    header: MessageHeader {
                        num_required_signatures: 1,
                        num_readonly_signed_accounts: 0,
                        num_readonly_unsigned_accounts: 0,
                    },
                    account_keys: vec![Pubkey::new_from_array(fuzz_message.account.pubkey)],
                    recent_blockhash: Hash::new_from_array([0; HASH_BYTES]),
                    instructions: Vec::new(),
                },
                &HashSet::new(),
            )),
            Hash::new_from_array([0; HASH_BYTES]),
            false,
            vec![Signature::from(signature)],
        )
        .ok()
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

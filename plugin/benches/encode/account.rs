use {
    super::encode_protobuf_message,
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoV3,
    criterion::{black_box, BenchmarkId, Criterion},
    richat_plugin::protobuf::ProtobufMessage,
    solana_sdk::pubkey::Pubkey,
};

pub struct Account<'a> {
    pubkey: &'a [u8],
    lamports: u64,
    owner: &'a [u8],
    executable: bool,
    rent_epoch: u64,
    data: Vec<u8>,
    write_version: u64,
}

pub fn accounts<'a>() -> Vec<Account<'a>> {
    const PUBKEY: Pubkey = Pubkey::from_str_const("28Dncoh8nmzXYEGLUcBA5SUw5WDwDBn15uUCwrWBbyuu");
    const OWNER: Pubkey = Pubkey::from_str_const("5jrPJWVGrFvQ2V9wRZC3kHEZhxo9pmMir15x73oHT6mn");

    let mut accounts = Vec::new();
    for lamports in [0, 8123] {
        for executable in [true, false] {
            for rent_epoch in [0, 4242] {
                for data in [
                    vec![],
                    vec![42; 165],
                    vec![42; 1024],
                    vec![42; 2 * 1024 * 1024],
                ] {
                    for write_version in [0, 1] {
                        accounts.push(Account {
                            pubkey: PUBKEY.as_ref(),
                            lamports,
                            owner: OWNER.as_ref(),
                            executable,
                            rent_epoch,
                            data: data.to_owned(),
                            write_version,
                        })
                    }
                }
            }
        }
    }
    accounts
}

pub fn bench_encode_accounts(criterion: &mut Criterion) {
    let accounts = accounts();
    criterion.bench_with_input(
        BenchmarkId::new("bench_encode_accounts", "many accounts"),
        &accounts,
        |criterion, accounts| {
            criterion.iter(|| {
                black_box(for account in accounts {
                    encode_protobuf_message(ProtobufMessage::Account {
                        slot: 0,
                        account: &ReplicaAccountInfoV3 {
                            pubkey: account.pubkey,
                            owner: account.owner,
                            lamports: account.lamports,
                            executable: account.executable,
                            rent_epoch: account.rent_epoch,
                            data: &account.data,
                            write_version: account.write_version,
                            txn: None,
                        },
                    });
                })
            })
        },
    );
}

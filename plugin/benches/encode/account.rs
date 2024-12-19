use {
    super::encode_protobuf_message,
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoV3,
    criterion::{black_box, BatchSize, Criterion},
    prost::Message,
    prost_types::Timestamp,
    richat_plugin::protobuf::ProtobufMessage,
    solana_sdk::pubkey::Pubkey,
    std::{ops::Range, sync::Arc, time::SystemTime},
    yellowstone_grpc_proto::plugin::{
        filter::{
            message::{FilteredUpdate, FilteredUpdateFilters, FilteredUpdateOneof},
            FilterAccountsDataSlice,
        },
        message::MessageAccount,
    },
};

pub struct Account {
    pubkey: Pubkey,
    lamports: u64,
    owner: Pubkey,
    executable: bool,
    rent_epoch: u64,
    data: Vec<u8>,
    write_version: u64,
}

pub fn generate_accounts() -> Vec<Account> {
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
                            pubkey: PUBKEY,
                            lamports,
                            owner: OWNER,
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

pub fn generate_data_slice() -> Vec<FilterAccountsDataSlice> {
    [
        vec![],
        vec![Range { start: 0, end: 0 }],
        vec![Range { start: 2, end: 3 }],
        vec![Range { start: 1, end: 3 }, Range { start: 5, end: 10 }],
    ]
    .into_iter()
    .map(Arc::new)
    .map(FilterAccountsDataSlice::new_unchecked)
    .collect()
}

pub fn generate_data_slices<'a>(
    replicas: &'a [ReplicaAccountInfoV3<'a>],
) -> Vec<(&'a ReplicaAccountInfoV3<'a>, FilterAccountsDataSlice)> {
    let mut accounts_and_data_slice = Vec::new();
    for replica in replicas {
        let data_slices = generate_data_slice();
        for data_slice in data_slices {
            accounts_and_data_slice.push((replica, data_slice))
        }
    }
    accounts_and_data_slice
}

pub fn bench_encode_accounts(criterion: &mut Criterion) {
    let accounts_data = generate_accounts();
    let accounts = accounts_data
        .iter()
        .map(|account| ReplicaAccountInfoV3 {
            pubkey: account.pubkey.as_ref(),
            owner: account.owner.as_ref(),
            lamports: account.lamports,
            executable: account.executable,
            rent_epoch: account.rent_epoch,
            data: &account.data,
            write_version: account.write_version,
            txn: None,
        })
        .collect::<Vec<_>>();
    let protobuf_account_messages = accounts
        .iter()
        .map(|account| ProtobufMessage::Account { slot: 0, account })
        .collect::<Vec<_>>();
    let replica_accounts_and_data_slices = generate_data_slices(&accounts);
    let message_accounts_and_data_slices = replica_accounts_and_data_slices
        .iter()
        .map(|(replica, data_slice)| {
            (
                MessageAccount::from_geyser(replica, 0, false),
                data_slice.to_owned(),
            )
        })
        .collect::<Vec<_>>();

    criterion
        .benchmark_group("encode_accounts")
        .bench_with_input(
            "richat/encoding-only",
            &protobuf_account_messages,
            |criterion, protobuf_account_messages| {
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for message in protobuf_account_messages {
                            encode_protobuf_message(message)
                        }
                    })
                })
            },
        )
        .bench_with_input("richat/full-pipeline", &accounts, |criterion, accounts| {
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for account in accounts {
                        let message = ProtobufMessage::Account { slot: 0, account };
                        encode_protobuf_message(&message)
                    }
                })
            })
        })
        .bench_with_input(
            "dragons-mouth/encoding-only",
            &message_accounts_and_data_slices,
            |criterion, message_accounts_and_data_slices| {
                let created_at = Timestamp::from(SystemTime::now());
                criterion.iter_batched(
                    || message_accounts_and_data_slices.to_owned(),
                    |message_accounts_and_data_slices| {
                        #[allow(clippy::unit_arg)]
                        black_box({
                            for (message, data_slice) in message_accounts_and_data_slices {
                                let update = FilteredUpdate {
                                    filters: FilteredUpdateFilters::new(),
                                    message: FilteredUpdateOneof::account(&message, data_slice),
                                    created_at,
                                };
                                update.encode_to_vec();
                            }
                        })
                    },
                    BatchSize::LargeInput,
                );
            },
        )
        .bench_with_input(
            "dragons-mouth/full-pipeline",
            &replica_accounts_and_data_slices,
            |criterion, replica_accounts_and_data_slices| {
                let created_at = Timestamp::from(SystemTime::now());
                criterion.iter_batched(
                    || replica_accounts_and_data_slices.to_owned(),
                    |replica_accounts_and_data_slices| {
                        black_box(
                            for (replica, data_slice) in replica_accounts_and_data_slices {
                                let message = MessageAccount::from_geyser(replica, 0, false);
                                let update = FilteredUpdate {
                                    filters: FilteredUpdateFilters::new(),
                                    message: FilteredUpdateOneof::account(&message, data_slice),
                                    created_at,
                                };
                                update.encode_to_vec();
                            },
                        )
                    },
                    BatchSize::LargeInput,
                );
            },
        );
}

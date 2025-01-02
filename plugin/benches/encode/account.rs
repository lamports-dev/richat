use {
    criterion::{black_box, Criterion},
    prost::Message,
    prost_types::Timestamp,
    richat_plugin::protobuf::{fixtures::generate_accounts, ProtobufEncoder, ProtobufMessage},
    std::time::SystemTime,
    yellowstone_grpc_proto::plugin::{
        filter::{
            message::{FilteredUpdate, FilteredUpdateFilters, FilteredUpdateOneof},
            FilterAccountsDataSlice,
        },
        message::MessageAccount,
    },
};

pub fn bench_encode_accounts(criterion: &mut Criterion) {
    let accounts = generate_accounts();

    let accounts_replica = accounts
        .iter()
        .map(|acc| acc.to_replica())
        .collect::<Vec<_>>();

    let grpc_replicas = accounts_replica
        .iter()
        .cloned()
        .map(|account| {
            (
                account,
                FilterAccountsDataSlice::new(&[], usize::MAX).unwrap(),
            )
        })
        .collect::<Vec<_>>();
    let grpc_messages = grpc_replicas
        .iter()
        .map(|((slot, account), data_slice)| {
            (
                MessageAccount::from_geyser(account, *slot, false),
                data_slice.clone(),
            )
        })
        .collect::<Vec<_>>();

    criterion
        .benchmark_group("encode_accounts")
        .bench_with_input("richat/prost", &accounts_replica, |criterion, accounts| {
            let created_at = SystemTime::now();
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for (slot, account) in accounts {
                        let message = ProtobufMessage::Account {
                            slot: *slot,
                            account,
                        };
                        message.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
                    }
                })
            })
        })
        .bench_with_input("richat/raw", &accounts_replica, |criterion, accounts| {
            let created_at = SystemTime::now();
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for (slot, account) in accounts {
                        let message = ProtobufMessage::Account {
                            slot: *slot,
                            account,
                        };
                        message.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
                    }
                })
            })
        })
        .bench_with_input(
            "dragons-mouth/encoding-only",
            &grpc_messages,
            |criterion, grpc_messages| {
                let created_at = Timestamp::from(SystemTime::now());
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for (message, data_slice) in grpc_messages {
                            let update = FilteredUpdate {
                                filters: FilteredUpdateFilters::new(),
                                message: FilteredUpdateOneof::account(message, data_slice.clone()),
                                created_at,
                            };
                            update.encode_to_vec();
                        }
                    })
                });
            },
        )
        .bench_with_input(
            "dragons-mouth/full-pipeline",
            &grpc_replicas,
            |criterion, grpc_replicas| {
                let created_at = Timestamp::from(SystemTime::now());
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box(for ((slot, account), data_slice) in grpc_replicas {
                        let message = MessageAccount::from_geyser(account, *slot, false);
                        let update = FilteredUpdate {
                            filters: FilteredUpdateFilters::new(),
                            message: FilteredUpdateOneof::account(&message, data_slice.clone()),
                            created_at,
                        };
                        update.encode_to_vec();
                    })
                });
            },
        );
}

use {
    super::encode_protobuf_message,
    criterion::{black_box, BatchSize, Criterion},
    prost::Message,
    prost_types::Timestamp,
    richat_plugin::protobuf::{
        fixtures::{generate_accounts, generate_accounts_replica},
        ProtobufMessage,
    },
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
    let accounts_replica = generate_accounts_replica(&accounts);

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
        .map(|(replica, data_slice)| {
            (
                MessageAccount::from_geyser(replica, 0, false),
                data_slice.clone(),
            )
        })
        .collect::<Vec<_>>();

    criterion
        .benchmark_group("encode_accounts")
        .bench_with_input("richat", &accounts_replica, |criterion, accounts| {
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
            &grpc_messages,
            |criterion, grpc_messages| {
                let created_at = Timestamp::from(SystemTime::now());
                criterion.iter_batched(
                    || grpc_messages.to_owned(),
                    |grpc_messages| {
                        #[allow(clippy::unit_arg)]
                        black_box({
                            for (message, data_slice) in grpc_messages {
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
            &grpc_replicas,
            |criterion, grpc_replicas| {
                let created_at = Timestamp::from(SystemTime::now());
                criterion.iter_batched(
                    || grpc_replicas.to_owned(),
                    |grpc_replicas| {
                        #[allow(clippy::unit_arg)]
                        black_box(for (replica, data_slice) in grpc_replicas {
                            let message = MessageAccount::from_geyser(&replica, 0, false);
                            let update = FilteredUpdate {
                                filters: FilteredUpdateFilters::new(),
                                message: FilteredUpdateOneof::account(&message, data_slice),
                                created_at,
                            };
                            update.encode_to_vec();
                        })
                    },
                    BatchSize::LargeInput,
                );
            },
        );
}

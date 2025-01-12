use {
    criterion::{black_box, Criterion},
    prost::Message,
    prost_types::Timestamp,
    richat_plugin_agave::protobuf::{
        fixtures::generate_transactions, ProtobufEncoder, ProtobufMessage,
    },
    std::time::SystemTime,
    yellowstone_grpc_proto::plugin::{
        filter::message::{FilteredUpdate, FilteredUpdateFilters, FilteredUpdateOneof},
        message::MessageTransaction,
    },
};

pub fn bench_encode_transactions(criterion: &mut Criterion) {
    let transactions = generate_transactions();

    let transactions_replica = transactions
        .iter()
        .map(|tx| tx.to_replica())
        .collect::<Vec<_>>();

    let transactions_grpc = transactions_replica
        .iter()
        .map(|(slot, transaction)| MessageTransaction::from_geyser(transaction, *slot))
        .collect::<Vec<_>>();

    criterion
        .benchmark_group("encode_transaction")
        .bench_with_input(
            "richat/prost",
            &transactions_replica,
            |criterion, transactions| {
                let created_at = SystemTime::now();
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for (slot, transaction) in transactions {
                            let message = ProtobufMessage::Transaction {
                                slot: *slot,
                                transaction,
                            };
                            message.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
                        }
                    })
                });
            },
        )
        .bench_with_input(
            "richat/raw",
            &transactions_replica,
            |criterion, transactions| {
                let created_at = SystemTime::now();
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for (slot, transaction) in transactions {
                            let message = ProtobufMessage::Transaction {
                                slot: *slot,
                                transaction,
                            };
                            message.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
                        }
                    })
                });
            },
        )
        .bench_with_input(
            "dragons-mouth/encoding-only",
            &transactions_grpc,
            |criterion, transaction_messages| {
                let created_at = Timestamp::from(SystemTime::now());
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for message in transaction_messages {
                            let update = FilteredUpdate {
                                filters: FilteredUpdateFilters::new(),
                                message: FilteredUpdateOneof::transaction(message),
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
            &transactions_replica,
            |criterion, transactions| {
                let created_at = Timestamp::from(SystemTime::now());
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for (slot, transaction) in transactions {
                            let message = MessageTransaction::from_geyser(transaction, *slot);
                            let update = FilteredUpdate {
                                filters: FilteredUpdateFilters::new(),
                                message: FilteredUpdateOneof::transaction(&message),
                                created_at,
                            };
                            update.encode_to_vec();
                        }
                    })
                });
            },
        );
}

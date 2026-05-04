use {
    criterion::Criterion,
    richat_benches::fixtures::generate_transactions,
    richat_plugin_agave::protobuf::{ProtobufEncoder, ProtobufMessage},
    std::{hint::black_box, time::SystemTime},
};

pub fn bench_encode_transactions(criterion: &mut Criterion) {
    let transactions = generate_transactions();

    let transactions_replica = transactions
        .iter()
        .map(|tx| tx.to_replica())
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
        );
}

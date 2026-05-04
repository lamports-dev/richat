use {
    criterion::Criterion,
    richat_benches::fixtures::generate_accounts,
    richat_plugin_agave::protobuf::{ProtobufEncoder, ProtobufMessage},
    std::{hint::black_box, time::SystemTime},
};

pub fn bench_encode_accounts(criterion: &mut Criterion) {
    let accounts = generate_accounts();

    let accounts_replica = accounts
        .iter()
        .map(|acc| acc.to_replica())
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
        });
}

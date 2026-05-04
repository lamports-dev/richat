use {
    criterion::Criterion,
    richat_benches::fixtures::generate_entries,
    richat_plugin_agave::protobuf::{ProtobufEncoder, ProtobufMessage},
    std::{hint::black_box, time::SystemTime},
};

pub fn bench_encode_entries(criterion: &mut Criterion) {
    let entries = generate_entries();

    let entries_replica = entries.iter().map(|e| e.to_replica()).collect::<Vec<_>>();

    criterion
        .benchmark_group("encode_entry")
        .bench_with_input("richat/prost", &entries_replica, |criterion, entries| {
            let created_at = SystemTime::now();
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for entry in entries {
                        let message = ProtobufMessage::Entry { entry };
                        message.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
                    }
                })
            });
        })
        .bench_with_input("richat/raw", &entries_replica, |criterion, entries| {
            let created_at = SystemTime::now();
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for entry in entries {
                        let message = ProtobufMessage::Entry { entry };
                        message.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
                    }
                })
            });
        });
}

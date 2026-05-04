use {
    criterion::Criterion,
    richat_benches::fixtures::generate_slots,
    richat_plugin_agave::protobuf::{ProtobufEncoder, ProtobufMessage},
    std::{hint::black_box, time::SystemTime},
};

pub fn bench_encode_slot(criterion: &mut Criterion) {
    let slots = generate_slots();

    let slots_replica = slots.iter().map(|s| s.to_replica()).collect::<Vec<_>>();

    criterion
        .benchmark_group("encode_slot")
        .bench_with_input("richat/prost", &slots_replica, |criterion, slots| {
            let created_at = SystemTime::now();
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for (slot, parent, status) in slots {
                        let message = ProtobufMessage::Slot {
                            slot: *slot,
                            parent: *parent,
                            status,
                        };
                        message.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
                    }
                })
            });
        })
        .bench_with_input("richat/raw", &slots_replica, |criterion, slots| {
            let created_at = SystemTime::now();
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for (slot, parent, status) in slots {
                        let message = ProtobufMessage::Slot {
                            slot: *slot,
                            parent: *parent,
                            status,
                        };
                        message.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
                    }
                })
            });
        });
}

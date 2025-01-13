use {
    criterion::{black_box, Criterion},
    prost::Message,
    prost_types::Timestamp,
    richat_plugin_agave::protobuf::{fixtures::generate_slots, ProtobufEncoder, ProtobufMessage},
    richat_proto::plugin::{
        filter::message::{FilteredUpdate, FilteredUpdateFilters, FilteredUpdateOneof},
        message::MessageSlot,
    },
    std::time::SystemTime,
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
        })
        .bench_with_input(
            "dragons-mouth/full-pipeline",
            &slots_replica,
            |criterion, slots| {
                let created_at = Timestamp::from(SystemTime::now());
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for (slot, parent, status) in slots {
                            let message = MessageSlot::from_geyser(*slot, *parent, status);
                            let update = FilteredUpdate {
                                filters: FilteredUpdateFilters::new(),
                                message: FilteredUpdateOneof::slot(message),
                                created_at,
                            };
                            update.encode_to_vec();
                        }
                    })
                });
            },
        );
}

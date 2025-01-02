use {
    crate::encode_protobuf_message,
    criterion::{black_box, Criterion},
    prost::Message,
    prost_types::Timestamp,
    richat_plugin::protobuf::{fixtures::generate_slots, ProtobufEncoder, ProtobufMessage},
    std::time::SystemTime,
    yellowstone_grpc_proto::plugin::{
        filter::message::{FilteredUpdate, FilteredUpdateFilters, FilteredUpdateOneof},
        message::MessageSlot,
    },
};

pub fn bench_encode_slot(criterion: &mut Criterion) {
    let slots = generate_slots();

    let slots_replica = slots.iter().map(|s| s.to_replica()).collect::<Vec<_>>();

    criterion
        .benchmark_group("encode_slot")
        .bench_with_input("richat/prost", &slots_replica, |criterion, slots| {
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for (slot, parent, status) in slots {
                        encode_protobuf_message(
                            &ProtobufMessage::Slot {
                                slot: *slot,
                                parent: *parent,
                                status,
                            },
                            ProtobufEncoder::Prost,
                        );
                    }
                })
            });
        })
        .bench_with_input("richat/raw", &slots_replica, |criterion, slots| {
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for (slot, parent, status) in slots {
                        encode_protobuf_message(
                            &ProtobufMessage::Slot {
                                slot: *slot,
                                parent: *parent,
                                status,
                            },
                            ProtobufEncoder::Raw,
                        );
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
                            let message = MessageSlot::from_geyser(*slot, *parent, **status);
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

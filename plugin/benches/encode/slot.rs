use {
    crate::encode_protobuf_message,
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus,
    criterion::{black_box, Criterion},
    prost::Message,
    prost_types::Timestamp,
    richat_plugin::protobuf::ProtobufMessage,
    std::time::SystemTime,
    yellowstone_grpc_proto::plugin::{
        filter::message::{FilteredUpdate, FilteredUpdateFilters, FilteredUpdateOneof},
        message::MessageSlot,
    },
};

pub fn bench_encode_slot(criterion: &mut Criterion) {
    criterion
        .benchmark_group("encode_slot")
        .bench_function("richat/encoding-only", |criterion| {
            let message = ProtobufMessage::Slot {
                slot: 1000,
                parent: Some(1001),
                status: &SlotStatus::Completed,
            };
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for _ in 0..1000 {
                        encode_protobuf_message(&message)
                    }
                })
            });
        })
        .bench_function("richat/full-pipeline", |criterion| {
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for slot in 0..1000 {
                        encode_protobuf_message(&ProtobufMessage::Slot {
                            slot,
                            parent: Some(slot.wrapping_add(1)),
                            status: &SlotStatus::Completed,
                        });
                    }
                })
            });
        })
        .bench_function("dragons-mouth/full-pipeline", |criterion| {
            let created_at = Timestamp::from(SystemTime::now());
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for slot in 0..1000 {
                        let message = MessageSlot::from_geyser(
                            slot,
                            Some(slot.wrapping_add(1)),
                            &SlotStatus::Completed,
                        );
                        let update = FilteredUpdate {
                            filters: FilteredUpdateFilters::new(),
                            message: FilteredUpdateOneof::slot(message),
                            created_at,
                        };
                        update.encode_to_vec();
                    }
                })
            });
        });
}

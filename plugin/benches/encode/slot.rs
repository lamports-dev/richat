use {
    crate::encode_protobuf_message,
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus,
    criterion::{black_box, Criterion},
    richat_plugin::protobuf::ProtobufMessage,
};

pub fn bench_encode_slot(criterion: &mut Criterion) {
    criterion.bench_function("bench_encode_slot", |criterion| {
        criterion.iter(|| {
            black_box(|| {
                for _ in 0..1000 {
                    encode_protobuf_message(ProtobufMessage::Slot {
                        slot: 1000,
                        parent: Some(1001),
                        status: &SlotStatus::Completed,
                    });
                }
            })
        });
    });
}

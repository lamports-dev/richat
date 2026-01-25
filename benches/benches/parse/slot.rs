use {
    criterion::Criterion,
    richat_benches::fixtures::generate_slots,
    richat_filter::message::{Message, MessageParserEncoding},
    richat_plugin_agave::protobuf::{ProtobufEncoder, ProtobufMessage},
    std::{borrow::Cow, hint::black_box, time::SystemTime},
};

pub fn bench_parse_slots(criterion: &mut Criterion) {
    let slots = generate_slots();
    let created_at = SystemTime::now();

    let encoded_slots = slots
        .iter()
        .map(|s| {
            let (slot, parent, status) = s.to_replica();
            let msg = ProtobufMessage::Slot {
                slot,
                parent,
                status,
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        })
        .collect::<Vec<_>>();

    criterion
        .benchmark_group("parse_slots")
        .bench_with_input("limited", &encoded_slots, |criterion, encoded| {
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for data in encoded {
                        let _ = Message::parse(
                            Cow::Borrowed(data.as_slice()),
                            MessageParserEncoding::Limited,
                        );
                    }
                })
            })
        })
        .bench_with_input("prost", &encoded_slots, |criterion, encoded| {
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for data in encoded {
                        let _ = Message::parse(
                            Cow::Borrowed(data.as_slice()),
                            MessageParserEncoding::Prost,
                        );
                    }
                })
            })
        });
}

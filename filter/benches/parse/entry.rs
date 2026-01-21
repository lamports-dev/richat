use {
    criterion::Criterion,
    richat_filter::message::{Message, MessageParserEncoding},
    richat_plugin_agave::protobuf::{ProtobufEncoder, ProtobufMessage, fixtures::generate_entries},
    std::{borrow::Cow, hint::black_box, time::SystemTime},
};

pub fn bench_parse_entries(criterion: &mut Criterion) {
    let entries = generate_entries();
    let created_at = SystemTime::now();

    let encoded_entries = entries
        .iter()
        .map(|e| {
            let replica = e.to_replica();
            let msg = ProtobufMessage::Entry { entry: &replica };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        })
        .collect::<Vec<_>>();

    criterion
        .benchmark_group("parse_entries")
        .bench_with_input("limited", &encoded_entries, |criterion, encoded| {
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
        .bench_with_input("prost", &encoded_entries, |criterion, encoded| {
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

use {
    criterion::Criterion,
    richat_benches::fixtures::generate_block_metas,
    richat_filter::message::{Message, MessageParserEncoding},
    richat_plugin_agave::protobuf::{ProtobufEncoder, ProtobufMessage},
    std::{borrow::Cow, hint::black_box, time::SystemTime},
};

pub fn bench_parse_block_metas(criterion: &mut Criterion) {
    let block_metas = generate_block_metas();
    let created_at = SystemTime::now();

    let encoded_block_metas = block_metas
        .iter()
        .map(|b| {
            let replica = b.to_replica();
            let msg = ProtobufMessage::BlockMeta {
                blockinfo: &replica,
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        })
        .collect::<Vec<_>>();

    criterion
        .benchmark_group("parse_block_metas")
        .bench_with_input("limited", &encoded_block_metas, |criterion, encoded| {
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
        .bench_with_input("prost", &encoded_block_metas, |criterion, encoded| {
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

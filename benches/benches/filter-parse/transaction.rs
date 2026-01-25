use {
    criterion::Criterion,
    richat_benches::fixtures::generate_transactions,
    richat_filter::message::{Message, MessageParserEncoding},
    richat_plugin_agave::protobuf::{ProtobufEncoder, ProtobufMessage},
    std::{borrow::Cow, hint::black_box, time::SystemTime},
};

pub fn bench_parse_transactions(criterion: &mut Criterion) {
    let transactions = generate_transactions();
    let created_at = SystemTime::now();

    let encoded_transactions = transactions
        .iter()
        .map(|tx| {
            let (slot, replica) = tx.to_replica();
            let msg = ProtobufMessage::Transaction {
                slot,
                transaction: &replica,
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        })
        .collect::<Vec<_>>();

    criterion
        .benchmark_group("parse_transactions")
        .bench_with_input("limited", &encoded_transactions, |criterion, encoded| {
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
        .bench_with_input("prost", &encoded_transactions, |criterion, encoded| {
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

use {
    criterion::Criterion,
    richat_filter::message::{Message, MessageParserEncoding},
    richat_plugin_agave::protobuf::{
        ProtobufEncoder, ProtobufMessage, fixtures::generate_accounts,
    },
    std::{borrow::Cow, hint::black_box, time::SystemTime},
};

pub fn bench_parse_accounts(criterion: &mut Criterion) {
    let accounts = generate_accounts();
    let created_at = SystemTime::now();

    let encoded_accounts = accounts
        .iter()
        .map(|acc| {
            let (slot, replica) = acc.to_replica();
            let msg = ProtobufMessage::Account {
                slot,
                account: &replica,
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        })
        .collect::<Vec<_>>();

    criterion
        .benchmark_group("parse_accounts")
        .bench_with_input("limited", &encoded_accounts, |criterion, encoded| {
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
        .bench_with_input("prost", &encoded_accounts, |criterion, encoded| {
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

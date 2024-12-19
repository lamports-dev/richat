use {
    super::encode_protobuf_message,
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaEntryInfoV2,
    criterion::{black_box, BatchSize, Criterion},
    prost::Message,
    prost_types::Timestamp,
    richat_plugin::protobuf::ProtobufMessage,
    solana_sdk::hash::Hash,
    std::{sync::Arc, time::SystemTime},
    yellowstone_grpc_proto::plugin::{
        filter::message::{FilteredUpdate, FilteredUpdateFilters, FilteredUpdateOneof},
        message::MessageEntry,
    },
};

pub fn generate_entries() -> [ReplicaEntryInfoV2<'static>; 2] {
    const FIRST_ENTRY_HASH: Hash = Hash::new_from_array([98; 32]);
    const SECOND_ENTRY_HASH: Hash = Hash::new_from_array([42; 32]);
    [
        ReplicaEntryInfoV2 {
            slot: 299888121,
            index: 42,
            num_hashes: 128,
            hash: FIRST_ENTRY_HASH.as_ref(),
            executed_transaction_count: 32,
            starting_transaction_index: 1000,
        },
        ReplicaEntryInfoV2 {
            slot: 299888121,
            index: 0,
            num_hashes: 16,
            hash: SECOND_ENTRY_HASH.as_ref(),
            executed_transaction_count: 32,
            starting_transaction_index: 1000,
        },
    ]
}

pub fn bench_encode_entries(criterion: &mut Criterion) {
    let entries = generate_entries();

    let protobuf_entry_messages = entries
        .iter()
        .map(|entry| ProtobufMessage::Entry { entry })
        .collect::<Vec<_>>();
    let entry_messages = entries
        .iter()
        .map(MessageEntry::from_geyser)
        .map(Arc::new)
        .collect::<Vec<_>>();

    criterion
        .benchmark_group("encode_entry")
        .bench_with_input(
            "richat/encoding-only",
            &protobuf_entry_messages,
            |criterion, protobuf_entry_messages| {
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for message in protobuf_entry_messages {
                            encode_protobuf_message(message)
                        }
                    })
                });
            },
        )
        .bench_with_input("richat/full-pipeline", &entries, |criterion, entries| {
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for entry in entries {
                        let message = ProtobufMessage::Entry { entry };
                        encode_protobuf_message(&message)
                    }
                })
            });
        })
        .bench_with_input(
            "dragons-mouth/encoding-only",
            &entry_messages,
            |criterion, entry_messages| {
                let created_at = Timestamp::from(SystemTime::now());
                criterion.iter_batched(
                    || entry_messages.to_owned(),
                    |entry_messages| {
                        #[allow(clippy::unit_arg)]
                        black_box({
                            for message in entry_messages {
                                let update = FilteredUpdate {
                                    filters: FilteredUpdateFilters::new(),
                                    message: FilteredUpdateOneof::entry(message),
                                    created_at,
                                };
                                update.encode_to_vec();
                            }
                        })
                    },
                    BatchSize::LargeInput,
                );
            },
        )
        .bench_with_input(
            "dragons-mouth/full-pipeline",
            &entries,
            |criterion, entries| {
                let created_at = Timestamp::from(SystemTime::now());
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for entry in entries {
                            let message = MessageEntry::from_geyser(entry);
                            let update = FilteredUpdate {
                                filters: FilteredUpdateFilters::new(),
                                message: FilteredUpdateOneof::entry(Arc::new(message)),
                                created_at,
                            };
                            update.encode_to_vec();
                        }
                    })
                });
            },
        );
}

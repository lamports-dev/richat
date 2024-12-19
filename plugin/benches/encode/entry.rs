use {
    super::encode_protobuf_message,
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaEntryInfoV2,
    criterion::{black_box, BatchSize, BenchmarkId, Criterion},
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

    criterion.bench_with_input(
        BenchmarkId::new("encode_entry", "richat"),
        &entries,
        |criterion, entries| {
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for entry in entries {
                        encode_protobuf_message(&ProtobufMessage::Entry { entry })
                    }
                })
            });
        },
    );

    let created_at = Timestamp::from(SystemTime::now());
    let messages = entries
        .iter()
        .map(MessageEntry::from_geyser)
        .map(Arc::new)
        .collect::<Vec<_>>();

    criterion.bench_with_input(
        BenchmarkId::new("encode_entry", "dragons-mouth"),
        &messages,
        |criterion, messages| {
            criterion.iter_batched(
                || messages.to_owned(),
                |messages| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for message in messages {
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
    );
}

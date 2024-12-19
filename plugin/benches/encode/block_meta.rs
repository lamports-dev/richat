use {
    super::{encode_protobuf_message, predefined::load_predefined_blocks},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoV4,
    criterion::{black_box, BatchSize, BenchmarkId, Criterion},
    prost::Message,
    prost_types::Timestamp,
    richat_plugin::protobuf::ProtobufMessage,
    solana_transaction_status::RewardsAndNumPartitions,
    std::{sync::Arc, time::SystemTime},
    yellowstone_grpc_proto::plugin::{
        filter::message::{FilteredUpdate, FilteredUpdateFilters, FilteredUpdateOneof},
        message::MessageBlockMeta,
    },
};

pub fn bench_encode_block_meta(criterion: &mut Criterion) {
    let blocks = load_predefined_blocks();

    let rewards_and_num_partitions = blocks
        .iter()
        .map(|(_, block)| RewardsAndNumPartitions {
            rewards: block.rewards.to_owned(),
            num_partitions: block.num_partitions,
        })
        .collect::<Vec<_>>();

    let block_metas = blocks
        .iter()
        .zip(rewards_and_num_partitions.iter())
        .map(
            |((slot, block), rewards_and_num_partitions)| ReplicaBlockInfoV4 {
                parent_slot: block.parent_slot,
                slot: *slot,
                parent_blockhash: &block.previous_blockhash,
                blockhash: &block.blockhash,
                rewards: rewards_and_num_partitions,
                block_time: block.block_time,
                block_height: block.block_height,
                executed_transaction_count: 0,
                entry_count: 0,
            },
        )
        .collect::<Vec<_>>();

    criterion.bench_with_input(
        BenchmarkId::new("encode_block_meta", "richat"),
        &block_metas,
        |criterion, block_metas| {
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for blockinfo in block_metas {
                        encode_protobuf_message(&ProtobufMessage::BlockMeta { blockinfo });
                    }
                })
            })
        },
    );

    let created_at = Timestamp::from(SystemTime::now());
    let messages = block_metas
        .iter()
        .map(MessageBlockMeta::from_geyser)
        .map(Arc::new)
        .collect::<Vec<_>>();

    criterion.bench_with_input(
        BenchmarkId::new("encode_block_meta", "dragons-mouth"),
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
                                message: FilteredUpdateOneof::block_meta(message),
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

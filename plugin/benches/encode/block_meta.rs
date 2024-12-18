use {
    super::{encode_protobuf_message, predefined::load_predefined_blocks},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoV4,
    criterion::{black_box, BatchSize, BenchmarkId, Criterion},
    richat_plugin::protobuf::ProtobufMessage,
    solana_transaction_status::RewardsAndNumPartitions,
};

pub fn bench_encode_block_meta(criterion: &mut Criterion) {
    let blocks = load_predefined_blocks().expect("failed to load predefined blocks");
    criterion.bench_with_input(
        BenchmarkId::new("bench_encode_block_meta", "three block metas"),
        &blocks,
        |criterion, blocks| {
            criterion.iter_batched(
                || blocks.to_owned(),
                |blocks| {
                    black_box(|| {
                        for block in blocks {
                            encode_protobuf_message(ProtobufMessage::BlockMeta {
                                blockinfo: &ReplicaBlockInfoV4 {
                                    parent_slot: block.parent_slot,
                                    slot: 0,
                                    parent_blockhash: &block.previous_blockhash,
                                    blockhash: &block.blockhash,
                                    rewards: &RewardsAndNumPartitions {
                                        rewards: block.rewards,
                                        num_partitions: block.num_partitions,
                                    },
                                    block_time: block.block_time,
                                    block_height: block.block_height,
                                    executed_transaction_count: 0,
                                    entry_count: 0,
                                },
                            })
                        }
                    })
                },
                BatchSize::SmallInput,
            )
        },
    );
}

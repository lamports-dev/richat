use {
    super::{encode_protobuf_message, predefined::load_predefined_blocks},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoV4,
    criterion::{black_box, BenchmarkId, Criterion},
    richat_plugin::protobuf::ProtobufMessage,
    solana_transaction_status::RewardsAndNumPartitions,
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
                        encode_protobuf_message(ProtobufMessage::BlockMeta { blockinfo });
                    }
                })
            })
        },
    );
}

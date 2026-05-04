use {
    criterion::Criterion,
    richat_benches::fixtures::generate_block_metas,
    richat_plugin_agave::protobuf::{ProtobufEncoder, ProtobufMessage},
    std::{hint::black_box, time::SystemTime},
};

pub fn bench_encode_block_metas(criterion: &mut Criterion) {
    let blocks_meta = generate_block_metas();

    let blocks_meta_replica = blocks_meta
        .iter()
        .map(|b| b.to_replica())
        .collect::<Vec<_>>();

    criterion
        .benchmark_group("encode_block_meta")
        .bench_with_input(
            "richat/prost",
            &blocks_meta_replica,
            |criterion, block_metas| {
                let created_at = SystemTime::now();
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for blockinfo in block_metas {
                            let message = ProtobufMessage::BlockMeta { blockinfo };
                            message.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
                        }
                    })
                })
            },
        )
        .bench_with_input(
            "richat/raw",
            &blocks_meta_replica,
            |criterion, block_metas| {
                let created_at = SystemTime::now();
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for blockinfo in block_metas {
                            let message = ProtobufMessage::BlockMeta { blockinfo };
                            message.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
                        }
                    })
                })
            },
        );
}

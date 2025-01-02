use {
    criterion::{black_box, BatchSize, Criterion},
    prost::Message,
    prost_types::Timestamp,
    richat_plugin::protobuf::{fixtures::generate_block_metas, ProtobufEncoder, ProtobufMessage},
    std::{sync::Arc, time::SystemTime},
    yellowstone_grpc_proto::plugin::{
        filter::message::{FilteredUpdate, FilteredUpdateFilters, FilteredUpdateOneof},
        message::MessageBlockMeta,
    },
};

pub fn bench_encode_block_metas(criterion: &mut Criterion) {
    let blocks_meta = generate_block_metas();

    let blocks_meta_replica = blocks_meta
        .iter()
        .map(|b| b.to_replica())
        .collect::<Vec<_>>();

    let blocks_meta_grpc = blocks_meta_replica
        .iter()
        .map(MessageBlockMeta::from_geyser)
        .map(Arc::new)
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
        )
        .bench_with_input(
            "dragons-mouth/encoding-only",
            &blocks_meta_grpc,
            |criterion, messages| {
                let created_at = Timestamp::from(SystemTime::now());
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
        )
        .bench_with_input(
            "dragons-mouth/full-pipeline",
            &blocks_meta_replica,
            |criterion, block_metas| {
                let created_at = Timestamp::from(SystemTime::now());
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box(for blockinfo in block_metas {
                        let message = MessageBlockMeta::from_geyser(blockinfo);
                        let update = FilteredUpdate {
                            filters: FilteredUpdateFilters::new(),
                            message: FilteredUpdateOneof::block_meta(Arc::new(message)),
                            created_at,
                        };
                        update.encode_to_vec();
                    })
                });
            },
        );
}

use {
    super::{encode_protobuf_message, predefined::load_predefined_blocks},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
    criterion::{black_box, Criterion},
    prost::Message,
    prost_types::Timestamp,
    richat_plugin::protobuf::ProtobufMessage,
    solana_sdk::{hash::Hash, message::SimpleAddressLoader, transaction::SanitizedTransaction},
    std::{collections::HashSet, time::SystemTime},
    yellowstone_grpc_proto::plugin::{
        filter::message::{FilteredUpdate, FilteredUpdateFilters, FilteredUpdateOneof},
        message::MessageTransaction,
    },
};

pub fn bench_encode_transactions(criterion: &mut Criterion) {
    let blocks = load_predefined_blocks();

    let transactions_data = blocks
        .into_iter()
        .flat_map(|(slot, block)| {
            block
                .transactions
                .into_iter()
                .enumerate()
                .map(move |(index, transaction)| {
                    let sanitized_transaction = SanitizedTransaction::try_create(
                        transaction.get_transaction(),
                        Hash::new_unique(),
                        None,
                        SimpleAddressLoader::Disabled,
                        &HashSet::new(),
                    )
                    .expect("failed to create `SanitazedTransaction`");
                    let transaction_status_meta = transaction
                        .get_status_meta()
                        .expect("failed to get `TransactionStatusMeta`");

                    (
                        slot,
                        index,
                        *transaction.transaction_signature(),
                        sanitized_transaction,
                        transaction_status_meta,
                    )
                })
        })
        .collect::<Vec<_>>();
    let transactions = transactions_data
        .iter()
        .map(
            |(slot, index, signature, transaction, transaction_status_meta)| {
                (
                    *slot,
                    ReplicaTransactionInfoV2 {
                        signature,
                        is_vote: false,
                        transaction,
                        transaction_status_meta,
                        index: *index,
                    },
                )
            },
        )
        .collect::<Vec<_>>();
    let protobuf_transaction_messages = transactions
        .iter()
        .map(|(slot, transaction)| ProtobufMessage::Transaction {
            slot: *slot,
            transaction,
        })
        .collect::<Vec<_>>();
    let transaction_messages = transactions
        .iter()
        .map(|(slot, transaction)| MessageTransaction::from_geyser(transaction, *slot))
        .collect::<Vec<_>>();

    criterion
        .benchmark_group("encode_transaction")
        .bench_with_input(
            "richat/encoding-only",
            &protobuf_transaction_messages,
            |criterion, protobuf_transaction_messages| {
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for message in protobuf_transaction_messages {
                            encode_protobuf_message(message)
                        }
                    })
                });
            },
        )
        .bench_with_input(
            "richat/full-pipeline",
            &transactions,
            |criterion, transactions| {
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for (slot, transaction) in transactions {
                            let message = ProtobufMessage::Transaction {
                                slot: *slot,
                                transaction,
                            };
                            encode_protobuf_message(&message)
                        }
                    })
                });
            },
        )
        .bench_with_input(
            "dragons-mouth/encoding-only",
            &transaction_messages,
            |criterion, transaction_messages| {
                let created_at = Timestamp::from(SystemTime::now());
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for message in transaction_messages {
                            let update = FilteredUpdate {
                                filters: FilteredUpdateFilters::new(),
                                message: FilteredUpdateOneof::transaction(message),
                                created_at,
                            };
                            update.encode_to_vec();
                        }
                    })
                });
            },
        )
        .bench_with_input(
            "dragons-mouth/full-pipeline",
            &transactions,
            |criterion, transactions| {
                let created_at = Timestamp::from(SystemTime::now());
                criterion.iter(|| {
                    #[allow(clippy::unit_arg)]
                    black_box({
                        for (slot, transaction) in transactions {
                            let message = MessageTransaction::from_geyser(transaction, *slot);
                            let update = FilteredUpdate {
                                filters: FilteredUpdateFilters::new(),
                                message: FilteredUpdateOneof::transaction(&message),
                                created_at,
                            };
                            update.encode_to_vec();
                        }
                    })
                });
            },
        );
}

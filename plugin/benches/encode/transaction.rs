use {
    super::{encode_protobuf_message, predefined::load_predefined_blocks},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
    criterion::{black_box, BenchmarkId, Criterion},
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

pub fn bench_encode_transaction(criterion: &mut Criterion) {
    let blocks = load_predefined_blocks();

    let transactions_data = blocks
        .into_iter()
        .flat_map(|(slot, block)| {
            block
                .transactions
                .into_iter()
                .enumerate()
                .map(move |(index, transaction)| {
                    let sanitazed_transaction = SanitizedTransaction::try_create(
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
                        sanitazed_transaction,
                        transaction_status_meta,
                    )
                })
        })
        .collect::<Vec<_>>();
    let transactions = transactions_data
        .iter()
        .map(|transaction_data| {
            (
                transaction_data.0,
                ReplicaTransactionInfoV2 {
                    signature: &transaction_data.2,
                    is_vote: false,
                    transaction: &transaction_data.3,
                    transaction_status_meta: &transaction_data.4,
                    index: transaction_data.1,
                },
            )
        })
        .collect::<Vec<_>>();

    criterion.bench_with_input(
        BenchmarkId::new("encode_transaction", "richat"),
        &transactions,
        |criterion, transactions| {
            criterion.iter(|| {
                #[allow(clippy::unit_arg)]
                black_box({
                    for (slot, transaction) in transactions {
                        encode_protobuf_message(ProtobufMessage::Transaction {
                            slot: *slot,
                            transaction,
                        })
                    }
                })
            });
        },
    );

    let created_at = Timestamp::from(SystemTime::now());

    criterion.bench_with_input(
        BenchmarkId::new("encode_transaction", "dragons-mouth"),
        &transactions,
        |criterion, transactions| {
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
                        let _ = update.encode_to_vec();
                    }
                })
            });
        },
    );
}

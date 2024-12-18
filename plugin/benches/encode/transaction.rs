use {
    super::{encode_protobuf_message, predefined::load_predefined_blocks},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
    criterion::{black_box, BenchmarkId, Criterion},
    richat_plugin::protobuf::ProtobufMessage,
    solana_sdk::{hash::Hash, message::SimpleAddressLoader, transaction::SanitizedTransaction},
    std::collections::HashSet,
};

pub fn bench_encode_transaction(criterion: &mut Criterion) {
    let blocks = load_predefined_blocks().expect("failed to load predefined blocks");
    let capacity = blocks.iter().map(|block| block.transactions.len()).sum();
    let mut transactions_data = Vec::with_capacity(capacity);
    blocks.into_iter().for_each(|block| {
        block.transactions.into_iter().for_each(|transaction| {
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
            transactions_data.push((
                *transaction.transaction_signature(),
                sanitazed_transaction,
                transaction_status_meta,
            ))
        })
    });
    criterion.bench_with_input(
        BenchmarkId::new("bench_encode_transaction", "many transactions"),
        &transactions_data,
        |criterion, transactions_data| {
            criterion.iter(|| {
                black_box(|| {
                    for transaction_data in transactions_data {
                        encode_protobuf_message(ProtobufMessage::Transaction {
                            slot: 0,
                            transaction: &ReplicaTransactionInfoV2 {
                                signature: &transaction_data.0,
                                is_vote: false,
                                transaction: &transaction_data.1,
                                transaction_status_meta: &transaction_data.2,
                                index: 0,
                            },
                        })
                    }
                })
            });
        },
    );
}

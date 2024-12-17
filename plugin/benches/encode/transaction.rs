use {
    super::{encode_protobuf_message, predefined::load_predefined_blocks},
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2,
    criterion::{black_box, BenchmarkId, Criterion},
    richat_plugin::protobuf::ProtobufMessage,
    solana_sdk::{hash::Hash, message::SimpleAddressLoader, transaction::SanitizedTransaction},
    std::collections::HashSet,
};

pub fn bench_encode_transaction(criterion: &mut Criterion) {
    let blocks = load_predefined_blocks().unwrap_or_default();
    let capacity = blocks
        .iter()
        .map(|block| block.transactions.iter().count())
        .count();
    let mut transactions = Vec::with_capacity(capacity);
    blocks.into_iter().for_each(|block| {
        block
            .transactions
            .into_iter()
            .for_each(|transaction| transactions.push(transaction))
    });
    criterion.bench_with_input(
        BenchmarkId::new("bench_encode_transaction", "many transactions"),
        &transactions,
        |criterion, transactions| {
            criterion.iter(|| {
                black_box(for transaction in transactions {
                    encode_protobuf_message(ProtobufMessage::Transaction {
                        slot: 0, // ???
                        transaction: &ReplicaTransactionInfoV2 {
                            signature: transaction.transaction_signature(),
                            is_vote: false, // ???
                            transaction: &SanitizedTransaction::try_create(
                                transaction.get_transaction(),
                                Hash::new_unique(),
                                None,
                                SimpleAddressLoader::Disabled,
                                &HashSet::new(),
                            )
                            .unwrap(),
                            transaction_status_meta: &transaction
                                .get_status_meta()
                                .unwrap_or_default(),
                            index: 0, // ???
                        },
                    })
                })
            });
        },
    );
}

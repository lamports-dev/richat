use criterion::{criterion_group, criterion_main};

mod account;
mod block_meta;
mod entry;
mod slot;
mod transaction;

criterion_group!(
    benches,
    account::bench_encode_accounts,
    slot::bench_encode_slot,
    entry::bench_encode_entries,
    block_meta::bench_encode_block_metas,
    transaction::bench_encode_transactions
);

criterion_main!(benches);

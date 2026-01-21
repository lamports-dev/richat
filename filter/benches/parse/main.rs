use criterion::{criterion_group, criterion_main};

mod account;
mod block_meta;
mod entry;
mod slot;
mod transaction;

criterion_group!(
    benches,
    account::bench_parse_accounts,
    slot::bench_parse_slots,
    entry::bench_parse_entries,
    block_meta::bench_parse_block_metas,
    transaction::bench_parse_transactions
);

criterion_main!(benches);

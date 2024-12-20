use {
    criterion::{criterion_group, criterion_main},
    richat_plugin::protobuf::ProtobufMessage,
    std::cell::RefCell,
};

mod account;
mod block_meta;
mod entry;
mod predefined;
mod slot;
mod transaction;

const BUFFER_CAPACITY: usize = 16 * 1024 * 1024;
thread_local! {
    static BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(BUFFER_CAPACITY));
}

pub fn encode_protobuf_message(message: &ProtobufMessage) {
    BUFFER.with(|cell| {
        let mut borrow_mut = cell.borrow_mut();
        borrow_mut.clear();
        message.encode(&mut borrow_mut);
    })
}

criterion_group!(
    benches,
    account::bench_encode_accounts,
    slot::bench_encode_slot,
    entry::bench_encode_entries,
    block_meta::bench_encode_block_metas,
    transaction::bench_encode_transactions
);

criterion_main!(benches);

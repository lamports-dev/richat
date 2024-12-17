use criterion::{criterion_group, criterion_main};
use prost_011::Message;
use richat_plugin::protobuf::ProtobufMessage;
use solana_storage_proto::convert::generated;
use solana_transaction_status::ConfirmedBlock;
use std::{cell::RefCell, fs, io};

mod account;
mod entry;

const BUFFER_CAPACITY: usize = 16 * 1024 * 1024;

thread_local! {
    static BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(BUFFER_CAPACITY));
}

pub fn encode_protobuf_message(message: ProtobufMessage) {
    BUFFER.with(|cell| {
        let mut borrow_mut = cell.borrow_mut();
        borrow_mut.clear();
        message.encode(&mut *borrow_mut);
    })
}

pub fn load_predefined() -> io::Result<Vec<ConfirmedBlock>> {
    let mut blocks = Vec::with_capacity(3);
    for entry in fs::read_dir("./fixtures/blocks")? {
        let entry = entry?;
        let data = fs::read(entry.path())?;
        blocks.push(
            generated::ConfirmedBlock::decode(data.as_slice())?
                .try_into()
                .unwrap(),
        )
    }
    Ok(blocks)
}

criterion_group!(
    benches,
    account::bench_encode_accounts,
    entry::bench_encode_entries
);

criterion_main!(benches);

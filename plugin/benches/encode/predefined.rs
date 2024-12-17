use std::{fs, io};

use prost_011::Message;
use solana_storage_proto::convert::generated;
use solana_transaction_status::ConfirmedBlock;

pub fn load_predefined_blocks() -> io::Result<Vec<ConfirmedBlock>> {
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

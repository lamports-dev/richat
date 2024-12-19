use {
    prost_011::Message,
    solana_sdk::clock::Slot,
    solana_storage_proto::convert::generated,
    solana_transaction_status::ConfirmedBlock,
    std::{fs, io},
};

pub fn load_predefined_blocks() -> io::Result<Vec<(Slot, ConfirmedBlock)>> {
    let mut blocks = Vec::with_capacity(3);
    for entry in fs::read_dir("./fixtures/blocks")? {
        let entry = entry?;
        let data = fs::read(entry.path())?;
        let block = generated::ConfirmedBlock::decode(data.as_slice())?
            .try_into()
            .expect("failed to parse block");
        blocks.push((0, block)) // TODO: slot
    }
    Ok(blocks)
}

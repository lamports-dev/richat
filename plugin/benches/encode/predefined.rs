use {
    prost_011::Message, solana_sdk::clock::Slot, solana_storage_proto::convert::generated,
    solana_transaction_status::ConfirmedBlock, std::fs,
};

pub fn load_predefined_blocks() -> Vec<(Slot, ConfirmedBlock)> {
    fs::read_dir("./fixtures/blocks")
        .expect("failed to read dir with fixtures")
        .map(|entry| {
            let entry = entry.expect("failed to read dir entry");
            let path = entry.path();

            let file_name = path
                .file_name()
                .expect("failed to get file name of fixture");
            let extension = path
                .extension()
                .expect("failed to get extension of fxiture");
            let slot = file_name.to_str().expect("failed to stringify file_name")
                [0..extension.len()]
                .parse::<u64>()
                .expect("failed to parse file name");

            let data = fs::read(path).expect("failed to read fixture");
            let block = generated::ConfirmedBlock::decode(data.as_slice())
                .expect("failed to decode fixture")
                .try_into()
                .expect("failed to parse block");

            (slot, block)
        })
        .collect::<Vec<_>>()
}

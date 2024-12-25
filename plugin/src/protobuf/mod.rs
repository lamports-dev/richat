mod encoding;
mod message;

pub use message::ProtobufMessage;

#[cfg(test)]
mod tests {
    use {
        super::ProtobufMessage,
        agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaAccountInfoV3,
        prost::Message, solana_sdk::pubkey::Pubkey,
    };

    mod predefined {
        use {
            prost_011::Message, solana_sdk::clock::Slot, solana_storage_proto::convert::generated,
            solana_transaction_status::ConfirmedBlock, std::fs,
        };

        pub fn load_predefined_blocks() -> Vec<(Slot, ConfirmedBlock)> {
            fs::read_dir("./fixtures/blocks")
                .expect("failed to read `fixtures` directory")
                .map(|entry| {
                    let entry = entry.expect("failed to read entry directory");
                    let path = entry.path();

                    let file_name = path.file_name().expect("failed to get fixture file name");
                    let extension = path.extension().expect("failed to get fixture extension");
                    let slot = file_name.to_str().expect("failed to stringify file name")
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
    }

    #[derive(Clone, Message)]
    pub struct Account {
        #[prost(bytes = "vec", tag = "1")]
        pubkey: Vec<u8>,
        #[prost(uint64, tag = "2")]
        lamports: u64,
        #[prost(bytes = "vec", tag = "3")]
        owner: Vec<u8>,
        #[prost(bool, tag = "4")]
        executable: bool,
        #[prost(uint64, tag = "5")]
        rent_epoch: u64,
        #[prost(bytes = "vec", tag = "6")]
        data: Vec<u8>,
        #[prost(uint64, tag = "7")]
        write_version: u64,
        #[prost(bytes = "vec", tag = "8")]
        signature: Vec<u8>,
    }

    #[derive(Message)]
    pub struct AccountMessage {
        #[prost(message, tag = "1")]
        account: Option<Account>,
        #[prost(uint64, tag = "2")]
        slot: u64,
    }

    pub fn generate_accounts() -> Vec<Account> {
        const PUBKEY: Pubkey =
            Pubkey::from_str_const("28Dncoh8nmzXYEGLUcBA5SUw5WDwDBn15uUCwrWBbyuu");
        const OWNER: Pubkey =
            Pubkey::from_str_const("5jrPJWVGrFvQ2V9wRZC3kHEZhxo9pmMir15x73oHT6mn");

        let mut accounts = Vec::new();
        for lamports in [0, 8123] {
            for executable in [true, false] {
                for rent_epoch in [0, 4242] {
                    for data in [
                        vec![],
                        vec![42; 165],
                        vec![42; 1024],
                        vec![42; 2 * 1024 * 1024],
                    ] {
                        for write_version in [0, 1] {
                            accounts.push(Account {
                                pubkey: PUBKEY.to_bytes().to_vec(),
                                lamports,
                                owner: OWNER.to_bytes().to_vec(),
                                executable,
                                rent_epoch,
                                data: data.to_owned(),
                                write_version,
                                signature: vec![],
                            })
                        }
                    }
                }
            }
        }
        accounts
    }

    #[test]
    pub fn test_decode_account() {
        let mut buf = Vec::new();
        let accounts_data = generate_accounts();
        let accounts = accounts_data
            .iter()
            .map(|account| ReplicaAccountInfoV3 {
                pubkey: &account.pubkey,
                lamports: account.lamports,
                owner: &account.owner,
                executable: account.executable,
                rent_epoch: account.rent_epoch,
                data: &account.data,
                write_version: account.write_version,
                txn: None,
            })
            .collect::<Vec<_>>();
        let protobuf_messages = accounts
            .iter()
            .enumerate()
            .map(|(slot, account)| {
                let slot = slot as u64;
                super::ProtobufMessage::Account { slot, account }
            })
            .collect::<Vec<_>>();
        for protobuf_message in protobuf_messages {
            protobuf_message.encode(&mut buf);
            let decoded = AccountMessage::decode(buf.as_slice())
                .expect("failed to decode `AccountMessage` from buf");
            let ProtobufMessage::Account {
                slot: protobuf_slot,
                account: protobuf_account,
            } = protobuf_message
            else {
                panic!("failed to get `::Account` from ProtobufMessage")
            };
            let decoded_slot = decoded.slot;
            let decoded_account = decoded
                .account
                .expect("failed to get `Account` from `AccountMessage`");
            assert_eq!(decoded_slot, protobuf_slot);
            assert_eq!(decoded_account.pubkey.as_slice(), protobuf_account.pubkey);
            assert_eq!(decoded_account.lamports, protobuf_account.lamports);
            assert_eq!(decoded_account.owner.as_slice(), protobuf_account.owner);
            assert_eq!(decoded_account.executable, protobuf_account.executable);
            assert_eq!(decoded_account.rent_epoch, protobuf_account.rent_epoch);
            assert_eq!(decoded_account.data.as_slice(), protobuf_account.data);
            assert_eq!(
                decoded_account.write_version,
                protobuf_account.write_version
            );
            buf.clear();
        }
    }

    #[test]
    pub fn test_decode_block_meta() {}

    #[test]
    pub fn test_decode_entry() {}

    #[test]
    pub fn test_decode_slot() {}

    #[test]
    pub fn test_decode_transaction() {}
}

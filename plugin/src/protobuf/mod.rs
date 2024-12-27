mod encoding;
mod message;

pub use message::ProtobufMessage;

#[cfg(test)]
mod tests {
    use {
        super::ProtobufMessage,
        agave_geyser_plugin_interface::geyser_plugin_interface::{
            ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaEntryInfoV2,
        },
        predefined::load_predefined_blocks,
        prost::{Enumeration, Message},
        solana_sdk::{hash::Hash, pubkey::Pubkey},
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
                ProtobufMessage::Account { slot, account }
            })
            .collect::<Vec<_>>();
        for protobuf_message in protobuf_messages {
            let mut buf = Vec::new();
            protobuf_message.encode(&mut buf);
            if let ProtobufMessage::Account { slot, account } = protobuf_message {
                let decoded = AccountMessage::decode(buf.as_slice())
                    .expect("failed to decode `AccountMessage` from buf");
                let decoded_account = decoded
                    .account
                    .expect("failed to get `Account` from `AccountMessage`");
                assert_eq!(decoded.slot, slot);
                assert_eq!(decoded_account.pubkey.as_slice(), account.pubkey);
                assert_eq!(decoded_account.lamports, account.lamports);
                assert_eq!(decoded_account.owner.as_slice(), account.owner);
                assert_eq!(decoded_account.executable, account.executable);
                assert_eq!(decoded_account.rent_epoch, account.rent_epoch);
                assert_eq!(decoded_account.data.as_slice(), account.data);
                assert_eq!(decoded_account.write_version, account.write_version)
            }
        }
    }

    #[derive(Debug, Enumeration)]
    #[repr(i32)]
    pub enum RewardType {
        Fee = 1,
        Rent = 2,
        Staking = 3,
        Voting = 4,
    }

    #[derive(Message)]
    pub struct Reward {
        #[prost(string, tag = "1")]
        pub pubkey: String,
        #[prost(int64, tag = "2")]
        pub lamports: i64,
        #[prost(uint64, tag = "3")]
        pub post_balance: u64,
        #[prost(enumeration = "RewardType", tag = "4")]
        pub reward_type: i32,
        #[prost(uint32, optional, tag = "5")]
        pub commission: Option<u32>,
    }

    impl From<Reward> for solana_transaction_status::Reward {
        fn from(reward: Reward) -> Self {
            solana_transaction_status::Reward {
                pubkey: reward.pubkey,
                lamports: reward.lamports,
                post_balance: reward.post_balance,
                reward_type: match reward.reward_type {
                    1 => Some(solana_transaction_status::RewardType::Fee),
                    2 => Some(solana_transaction_status::RewardType::Rent),
                    3 => Some(solana_transaction_status::RewardType::Staking),
                    4 => Some(solana_transaction_status::RewardType::Voting),
                    _ => None,
                },
                commission: reward.commission.map(|commission| commission as u8),
            }
        }
    }

    #[derive(Message)]
    pub struct RewardsAndNumPartitions {
        #[prost(message, repeated, tag = "1")]
        rewards: Vec<Reward>,
        #[prost(message, tag = "2")]
        num_partitions: Option<NumPartitions>,
    }

    #[derive(Message)]
    pub struct UnixTimestamp {
        #[prost(int64, tag = "1")]
        timestamp: i64,
    }

    #[derive(Message)]
    pub struct BlockHeight {
        #[prost(uint64, tag = "1")]
        block_height: u64,
    }

    #[derive(Message)]
    pub struct BlockMeta {
        #[prost(uint64, tag = "1")]
        slot: u64,
        #[prost(string, tag = "2")]
        blockhash: String,
        #[prost(message, tag = "3")]
        rewards: Option<RewardsAndNumPartitions>,
        #[prost(message, tag = "4")]
        block_time: Option<UnixTimestamp>,
        #[prost(message, tag = "5")]
        block_height: Option<BlockHeight>,
        #[prost(uint64, tag = "6")]
        parent_slot: u64,
        #[prost(string, tag = "7")]
        parent_blockhash: String,
        #[prost(uint64, tag = "8")]
        executed_transaction_count: u64,
        #[prost(uint64, tag = "9")]
        entry_count: u64,
    }

    #[test]
    pub fn test_decode_block_meta() {
        let blocks = load_predefined_blocks();

        let rewards_and_num_partitions = blocks
            .iter()
            .map(
                |(_slot, block)| solana_transaction_status::RewardsAndNumPartitions {
                    rewards: block.rewards.to_owned(),
                    num_partitions: block.num_partitions,
                },
            )
            .collect::<Vec<_>>();
        let block_metas = blocks
            .iter()
            .zip(rewards_and_num_partitions.iter())
            .map(
                |((slot, block), rewards_and_num_partitions)| ReplicaBlockInfoV4 {
                    parent_slot: block.parent_slot,
                    slot: *slot,
                    parent_blockhash: &block.previous_blockhash,
                    blockhash: &block.blockhash,
                    rewards: rewards_and_num_partitions,
                    block_time: block.block_time,
                    block_height: block.block_height,
                    executed_transaction_count: 0,
                    entry_count: 0,
                },
            )
            .collect::<Vec<_>>();
        let protobuf_messages = block_metas
            .iter()
            .map(|blockinfo| ProtobufMessage::BlockMeta { blockinfo })
            .collect::<Vec<_>>();
        for protobuf_message in protobuf_messages {
            let mut buf = Vec::new();
            protobuf_message.encode(&mut buf);
            if let ProtobufMessage::BlockMeta { blockinfo } = protobuf_message {
                let decoded = BlockMeta::decode(buf.as_slice())
                    .expect("failed to decode `BlockMeta` from buf");
                assert_eq!(decoded.slot, blockinfo.slot);
                assert_eq!(&decoded.blockhash, blockinfo.blockhash);
                assert_eq!(
                    decoded.rewards.map(|rewards| rewards
                        .rewards
                        .into_iter()
                        .map(Into::into)
                        .collect()),
                    Some(blockinfo.rewards.rewards.to_owned())
                ); // TODO: rewards
                assert_eq!(
                    decoded.block_time.map(|block_time| block_time.timestamp),
                    blockinfo.block_time
                );
                assert_eq!(
                    decoded
                        .block_height
                        .map(|block_height| block_height.block_height),
                    blockinfo.block_height
                );
                assert_eq!(decoded.parent_slot, blockinfo.parent_slot);
                assert_eq!(&decoded.parent_blockhash, blockinfo.parent_blockhash);
                assert_eq!(
                    decoded.executed_transaction_count,
                    blockinfo.executed_transaction_count
                );
                assert_eq!(decoded.entry_count, blockinfo.entry_count)
            }
        }
    }

    #[derive(Message)]
    pub struct Entry {
        #[prost(uint64, tag = "1")]
        pub slot: u64,
        #[prost(uint64, tag = "2")]
        pub index: u64,
        #[prost(uint64, tag = "3")]
        pub num_hashes: u64,
        #[prost(bytes = "vec", tag = "4")]
        pub hash: Vec<u8>,
        #[prost(uint64, tag = "5")]
        pub executed_transaction_count: u64,
        #[prost(uint64, tag = "6")]
        pub starting_transaction_index: u64,
    }

    pub fn generate_entries() -> [ReplicaEntryInfoV2<'static>; 2] {
        const FIRST_ENTRY_HASH: Hash = Hash::new_from_array([98; 32]);
        const SECOND_ENTRY_HASH: Hash = Hash::new_from_array([42; 32]);
        [
            ReplicaEntryInfoV2 {
                slot: 299888121,
                index: 42,
                num_hashes: 128,
                hash: FIRST_ENTRY_HASH.as_ref(),
                executed_transaction_count: 32,
                starting_transaction_index: 1000,
            },
            ReplicaEntryInfoV2 {
                slot: 299888121,
                index: 0,
                num_hashes: 16,
                hash: SECOND_ENTRY_HASH.as_ref(),
                executed_transaction_count: 32,
                starting_transaction_index: 1000,
            },
        ]
    }

    #[test]
    pub fn test_decode_entry() {
        let entries = generate_entries();
        let protobuf_messages = entries
            .iter()
            .map(|entry| ProtobufMessage::Entry { entry })
            .collect::<Vec<_>>();
        for protobuf_message in protobuf_messages {
            let mut buf = Vec::new();
            protobuf_message.encode(&mut buf);
            if let ProtobufMessage::Entry { entry } = protobuf_message {
                let decoded =
                    Entry::decode(buf.as_slice()).expect("failed to decode `Entry` from buf");
                assert_eq!(decoded.slot, entry.slot);
                assert_eq!(decoded.index, entry.index as u64);
                assert_eq!(decoded.num_hashes, entry.num_hashes);
                assert_eq!(decoded.hash.as_slice(), entry.hash);
                assert_eq!(
                    decoded.executed_transaction_count,
                    entry.executed_transaction_count
                );
                assert_eq!(
                    decoded.starting_transaction_index,
                    entry.starting_transaction_index as u64
                )
            }
        }
    }

    #[derive(Message)]
    pub struct Slot {
        #[prost(uint64, tag = "1")]
        slot: u64,
        #[prost(uint64, optional, tag = "2")]
        parent: Option<u64>,
        #[prost(enumeration = "SlotStatus", tag = "3")]
        status: i32,
    }

    #[derive(Debug, Enumeration)]
    #[repr(i32)]
    pub enum SlotStatus {
        Processed = 0,
        Rooted = 1,
        Confirmed = 2,
        FirstShredReceived = 3,
        Completed = 4,
        CreatedBank = 5,
        Dead = 6,
    }

    #[test]
    pub fn test_decode_slot() {
        let message = ProtobufMessage::Slot {
            slot: 0,
            parent: Some(1),
            status: &agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus::Processed,
        };
        let mut buf = Vec::new();
        message.encode(&mut buf);
        let decoded = Slot::decode(buf.as_slice()).expect("failed to decode `Slot` from buf");
        assert_eq!(decoded.slot, 0);
        assert_eq!(decoded.parent, Some(1));
        assert_eq!(decoded.status, 0)
    }

    /*
    super::message_header::encode_message_header(1, message.header, buf);
    super::pubkeys::encode_pubkeys(2, &message.account_keys, buf);
    super::recent_blockhash::encode_recent_blockhash(
        3,
        message.recent_blockhash.as_ref(),
        buf,
    );
    super::compiled_instructions::encode_compiled_instructions(
        4,
        &message.instructions,
        buf,
    );
    super::versioned::encode_versioned(5, true, buf);
    super::address_table_lookups::encode_address_table_lookups(
        6,
        &message.address_table_lookups,
        buf,
    )
    */

    #[derive(Message)]
    pub struct MessageAddressTableLookup {
        #[prost(bytes = "vec", tag = "1")]
        account_key: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        writable_indexes: Vec<u8>,
        #[prost(bytes = "vec", tag = "3")]
        readonly_indexes: Vec<u8>,
    }

    #[derive(Message)]
    pub struct CompiledInstruction {
        #[prost(uint32, tag = "1")]
        program_id_index: u32,
        #[prost(bytes = "vec", tag = "2")]
        accounts: Vec<u8>,
        #[prost(bytes = "vec", tag = "3")]
        data: Vec<u8>,
    }

    #[derive(Message)]
    pub struct MessageHeader {
        #[prost(uint32, tag = "1")]
        num_required_signatures: u32,
        #[prost(uint32, tag = "2")]
        num_readonly_signed_accounts: u32,
        #[prost(uint32, tag = "3")]
        num_readonly_unsigned_accounts: u32,
    }

    #[derive(Message)]
    pub struct LoadedMessage {
        #[prost(message, tag = "1")]
        header: Option<MessageHeader>,
        #[prost(bytes = "vec", repeated, tag = "2")]
        account_keys: Vec<Vec<u8>>,
        #[prost(bytes = "vec", tag = "3")]
        recent_blockhash: Vec<u8>,
        #[prost(message, repeated, tag = "4")]
        compiled_instructions: Vec<CompiledInstruction>,
        #[prost(bool, tag = "5")]
        versioned: bool,
        #[prost(message, tag = "6")]
        address_table_lookup: Option<MessageAddressTableLookup>,
    }

    #[derive(Message)]
    pub struct SanitizedTransaction {
        #[prost(message, tag = "1")]
        message: Option<LoadedMessage>,
        #[prost(bytes = "vec", repeated, tag = "2")]
        signatures: Vec<Vec<u8>>,
    }

    #[derive(Message)]
    pub struct TransactionReturnData {
        #[prost(bytes = "vec", tag = "1")]
        program_id: Vec<u8>,
        #[prost(bytes = "vec", tag = "2")]
        data: Vec<u8>,
    }

    #[derive(Message)]
    pub struct UiTokenAmount {
        #[prost(double, optional, tag = "1")]
        ui_amount: Option<f64>,
        #[prost(uint32, tag = "2")]
        decimals: u32,
        #[prost(string, tag = "3")]
        amount: String,
        #[prost(string, tag = "4")]
        ui_amount_string: String,
    }

    #[derive(Message)]
    pub struct TransactionTokenBalance {
        #[prost(uint32, tag = "1")]
        account_index: u32,
        #[prost(string, tag = "2")]
        mint: String,
        #[prost(message, tag = "3")]
        ui_token_amount: Option<UiTokenAmount>,
        #[prost(string, tag = "4")]
        owner: String,
        #[prost(string, tag = "5")]
        program_id: String,
    }

    #[derive(Message)]
    pub struct InnerInstruction {
        #[prost(message, tag = "1")]
        instruction: Option<CompiledInstruction>,
        #[prost(uint32, optional, tag = "2")]
        stack_height: Option<u32>,
    }

    #[derive(Message)]
    pub struct InnerInstructions {
        #[prost(uint32, tag = "1")]
        index: u32,
        #[prost(message, tag = "2")]
        instruction: Option<InnerInstruction>,
    }

    #[derive(Message)]
    pub struct TransactionStatusMeta {
        #[prost(uint64, tag = "2")]
        fee: u64,
        #[prost(uint64, repeated, tag = "3")]
        pre_balances: Vec<u64>,
        #[prost(uint64, repeated, tag = "4")]
        post_balances: Vec<u64>,
        #[prost(message, repeated, tag = "5")]
        inner_instructions: Vec<InnerInstructions>,
        #[prost(string, repeated, tag = "6")]
        log_messages: Vec<String>,
        #[prost(message, repeated, tag = "7")]
        pre_token_balances: Vec<TransactionTokenBalance>,
        #[prost(message, repeated, tag = "8")]
        post_token_balances: Vec<TransactionTokenBalance>,
        #[prost(message, repeated, tag = "9")]
        rewards: Vec<Reward>,
        #[prost(bool, tag = "10")]
        inner_instructions_is_none: bool,
        #[prost(bool, tag = "11")]
        log_messages_is_none: bool,
        #[prost(bytes = "vec", repeated, tag = "12")]
        loaded_writable_addresses: Vec<Vec<u8>>,
        #[prost(bytes = "vec", repeated, tag = "13")]
        loaded_readonly_addresses: Vec<Vec<u8>>,
        #[prost(message, tag = "14")]
        return_data: Option<TransactionReturnData>,
        #[prost(bool, tag = "15")]
        return_data_is_none: bool,
        #[prost(uint64, optional, tag = "16")]
        compute_units_consumed: Option<u64>,
    }

    #[derive(Message)]
    pub struct ReplicaTransaction {
        #[prost(bytes = "vec", tag = 1)]
        signature: Vec<u8>,
        #[prost(bool, tag = "2")]
        is_vote: bool,
        #[prost(message, tag = "3")]
        transaction: Option<SanitizedTransaction>,
        #[prost(message, tag = "4")]
        transaction_status_meta: Option<TransactionStatusMeta>,
        #[prost(uint64, tag = "5")]
        index: u64,
    }

    #[derive(Message)]
    pub struct Transaction {
        #[prost(message, tag = "1")]
        transaction: Option<ReplicaTransaction>,
        #[prost(uint64, tag = "2")]
        slot: u64,
    }

    #[test]
    pub fn test_decode_transaction() {}
}

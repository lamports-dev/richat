#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoV4,
    arbitrary::Arbitrary,
    libfuzzer_sys::fuzz_target,
    prost::{Enumeration, Message},
    richat_plugin::protobuf::ProtobufMessage,
    solana_transaction_status::{RewardType, RewardsAndNumPartitions},
};

#[derive(Clone, Message, PartialEq)]
pub struct Reward {
    #[prost(string, tag = "1")]
    pub pubkey: String,
    #[prost(int64, tag = "2")]
    pub lamports: i64,
    #[prost(uint64, tag = "3")]
    pub post_balance: u64,
    #[prost(enumeration = "FuzzRewardType", tag = "4")]
    pub reward_type: i32,
    #[prost(string, tag = "5")]
    pub commission: String,
}

#[derive(Clone, Message, PartialEq)]
pub struct Rewards {
    #[prost(message, repeated, tag = "1")]
    rewards: Vec<Reward>,
    #[prost(uint64, optional, tag = "2")]
    num_partitions: Option<u64>,
}

#[derive(Message)]
pub struct BlockMeta {
    #[prost(uint64, tag = "6")]
    parent_slot: u64,
    #[prost(string, tag = "7")]
    parent_blockhash: String,
    #[prost(uint64, tag = "1")]
    slot: u64,
    #[prost(string, tag = "2")]
    blockhash: String,
    #[prost(message, optional, tag = "3")]
    rewards: Option<Rewards>,
    #[prost(message, optional, tag = "4")]
    block_time: Option<i64>,
    #[prost(message, optional, tag = "5")]
    block_height: Option<u64>,
    #[prost(uint64, tag = "8")]
    executed_transaction_count: u64,
    #[prost(uint64, tag = "9")]
    entry_count: u64,
}

#[derive(Clone, Copy, Arbitrary, Debug, Enumeration)]
#[repr(i32)]
pub enum FuzzRewardType {
    Fee = 1,
    Rent = 2,
    Staking = 3,
    Voting = 4,
}

impl Into<RewardType> for FuzzRewardType {
    fn into(self) -> RewardType {
        match self {
            FuzzRewardType::Fee => RewardType::Fee,
            FuzzRewardType::Rent => RewardType::Rent,
            FuzzRewardType::Staking => RewardType::Staking,
            FuzzRewardType::Voting => RewardType::Voting,
        }
    }
}

#[derive(Arbitrary, Debug)]
pub struct FuzzReward {
    pubkey: String,
    lamports: i64,
    post_balance: u64,
    reward_type: Option<FuzzRewardType>,
    commission: Option<u8>,
}

#[derive(Arbitrary, Debug)]
pub struct FuzzBlockMeta<'a> {
    parent_slot: u64,
    parent_blockhash: &'a str,
    slot: u64,
    blockhash: &'a str,
    rewards: Vec<FuzzReward>,
    num_partitions: Option<u64>,
    block_time: Option<i64>,
    block_height: Option<u64>,
    executed_transaction_count: u64,
    entry_count: u64,
}

fuzz_target!(|fuzz_blockmeta: FuzzBlockMeta| {
    let mut buf = Vec::new();
    let rewards_and_num_partitions = RewardsAndNumPartitions {
        rewards: fuzz_blockmeta
            .rewards
            .iter()
            .map(|reward| solana_transaction_status::Reward {
                pubkey: reward.pubkey.to_owned(),
                lamports: reward.lamports,
                post_balance: reward.post_balance,
                reward_type: reward.reward_type.map(Into::into),
                commission: reward.commission,
            })
            .collect(),
        num_partitions: fuzz_blockmeta.num_partitions,
    };
    let blockinfo = ReplicaBlockInfoV4 {
        parent_slot: fuzz_blockmeta.parent_slot,
        parent_blockhash: fuzz_blockmeta.parent_blockhash,
        slot: fuzz_blockmeta.slot,
        blockhash: fuzz_blockmeta.blockhash,
        rewards: &rewards_and_num_partitions,
        block_time: fuzz_blockmeta.block_time,
        block_height: fuzz_blockmeta.block_height,
        executed_transaction_count: fuzz_blockmeta.executed_transaction_count,
        entry_count: fuzz_blockmeta.entry_count,
    };
    let message = ProtobufMessage::BlockMeta {
        blockinfo: &blockinfo,
    };
    message.encode(&mut buf);
    assert!(!buf.is_empty());

    let decoded = BlockMeta::decode(buf.as_slice()).expect("failed to decode `BlockMeta` from buf");
    assert_eq!(decoded.parent_slot, fuzz_blockmeta.parent_slot);
    assert_eq!(&decoded.parent_blockhash, fuzz_blockmeta.parent_blockhash);
    assert_eq!(decoded.slot, fuzz_blockmeta.slot);
    assert_eq!(&decoded.blockhash, fuzz_blockmeta.blockhash);
    let from_blockmeta_rewards = fuzz_blockmeta
        .rewards
        .iter()
        .map(|reward| Reward {
            pubkey: reward.pubkey.to_owned(),
            lamports: reward.lamports,
            post_balance: reward.post_balance,
            reward_type: reward
                .reward_type
                .map_or(0, |reward_type| reward_type as i32),
            commission: reward
                .commission
                .map_or(0u8.to_string(), |commission| commission.to_string()),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        decoded
            .rewards
            .as_ref()
            .map_or(Vec::default(), |rewards| rewards.rewards.to_owned()),
        from_blockmeta_rewards
    );
    assert_eq!(
        decoded.rewards.map_or(0, |reward| reward.num_partitions()),
        fuzz_blockmeta.num_partitions.unwrap_or_default()
    );
    assert_eq!(decoded.block_time, fuzz_blockmeta.block_time);
    assert_eq!(decoded.block_height, fuzz_blockmeta.block_height);
    assert_eq!(
        decoded.executed_transaction_count,
        fuzz_blockmeta.executed_transaction_count
    );
    assert_eq!(decoded.entry_count, fuzz_blockmeta.entry_count)
});

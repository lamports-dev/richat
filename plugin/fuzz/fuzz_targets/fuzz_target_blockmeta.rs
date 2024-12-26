#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoV4,
    arbitrary::Arbitrary,
    libfuzzer_sys::fuzz_target,
    richat_plugin::protobuf::ProtobufMessage,
    solana_transaction_status::{Reward, RewardType, RewardsAndNumPartitions},
};

#[derive(Arbitrary, Debug)]
pub enum FuzzRewardType {
    Fee,
    Rent,
    Staking,
    Voting,
}

impl From<FuzzRewardType> for RewardType {
    fn from(fuzz: FuzzRewardType) -> Self {
        match fuzz {
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
            .into_iter()
            .map(|reward| Reward {
                pubkey: reward.pubkey,
                lamports: reward.lamports,
                post_balance: reward.post_balance,
                reward_type: reward.reward_type.map(Into::into),
                commission: reward.commission,
            })
            .collect(),
        num_partitions: fuzz_blockmeta.num_partitions,
    };
    let message = ProtobufMessage::BlockMeta {
        blockinfo: &ReplicaBlockInfoV4 {
            parent_slot: fuzz_blockmeta.parent_slot,
            parent_blockhash: fuzz_blockmeta.parent_blockhash,
            slot: fuzz_blockmeta.slot,
            blockhash: fuzz_blockmeta.blockhash,
            rewards: &rewards_and_num_partitions,
            block_time: fuzz_blockmeta.block_time,
            block_height: fuzz_blockmeta.block_height,
            executed_transaction_count: fuzz_blockmeta.executed_transaction_count,
            entry_count: fuzz_blockmeta.entry_count,
        },
    };
    message.encode(&mut buf);
    assert!(!buf.is_empty())
});

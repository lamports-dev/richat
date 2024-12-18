#![no_main]

use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaBlockInfoV4;
use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use richat_plugin::protobuf::ProtobufMessage;
use solana_transaction_status::{Reward, RewardType, RewardsAndNumPartitions};

#[derive(Arbitrary, Debug)]
pub enum FuzzRewardType {
    Fee,
    Rent,
    Staking,
    Voting,
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
pub struct BlockMeta<'a> {
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

fuzz_target!(|blockmeta: BlockMeta| {
    let mut buf = Vec::new();
    let rewards_and_num_partitions = RewardsAndNumPartitions {
        rewards: blockmeta
            .rewards
            .into_iter()
            .map(|reward| Reward {
                pubkey: reward.pubkey,
                lamports: reward.lamports,
                post_balance: reward.post_balance,
                reward_type: reward.reward_type.map(|reward_type| match reward_type {
                    FuzzRewardType::Fee => RewardType::Fee,
                    FuzzRewardType::Rent => RewardType::Rent,
                    FuzzRewardType::Staking => RewardType::Staking,
                    FuzzRewardType::Voting => RewardType::Voting,
                }),
                commission: reward.commission,
            })
            .collect(),
        num_partitions: blockmeta.num_partitions,
    };
    let message = ProtobufMessage::BlockMeta {
        blockinfo: &ReplicaBlockInfoV4 {
            parent_slot: blockmeta.parent_slot,
            parent_blockhash: blockmeta.parent_blockhash,
            slot: blockmeta.slot,
            blockhash: blockmeta.blockhash,
            rewards: &rewards_and_num_partitions,
            block_time: blockmeta.block_time,
            block_height: blockmeta.block_height,
            executed_transaction_count: blockmeta.executed_transaction_count,
            entry_count: blockmeta.entry_count,
        },
    };
    let encoded = message.encode(&mut buf);
    assert!(!encoded.is_empty())
});

use prost::{
    bytes::BufMut,
    encoding::{encode_key, encode_varint, encoded_len_varint, key_len, WireType},
};

pub use self::{
    account::Account, block_meta::BlockMeta, entry::Entry, slot::Slot, transaction::Transaction,
};

mod account;
mod block_meta;
mod entry;
mod slot;
mod transaction;

mod proto {
    pub mod convert_to {
        use solana_sdk::clock::UnixTimestamp;
        use solana_transaction_status::{Reward, RewardType};

        pub const fn create_reward_type(reward_type: Option<RewardType>) -> super::RewardType {
            match reward_type {
                Some(RewardType::Fee) => super::RewardType::Fee,
                Some(RewardType::Rent) => super::RewardType::Rent,
                Some(RewardType::Staking) => super::RewardType::Staking,
                Some(RewardType::Voting) => super::RewardType::Voting,
                _ => super::RewardType::Unspecified,
            }
        }

        pub const fn create_num_partitions(num_partitions: u64) -> super::NumPartitions {
            super::NumPartitions { num_partitions }
        }

        pub fn create_reward(reward: &Reward) -> super::Reward {
            super::Reward {
                pubkey: reward.pubkey.clone(), // TODO: try to remove allocation
                lamports: reward.lamports,
                post_balance: reward.post_balance,
                reward_type: create_reward_type(reward.reward_type) as i32,
                commission: reward.commission.map(|c| c.to_string()).unwrap_or_default(), // TODO: try to remove allocation
            }
        }

        pub fn create_rewards(rewards: &[Reward]) -> Vec<super::Reward> {
            rewards.iter().map(create_reward).collect()
        }

        pub fn create_rewards_obj(
            rewards: &[Reward],
            num_partitions: Option<u64>,
        ) -> super::Rewards {
            super::Rewards {
                rewards: create_rewards(rewards),
                num_partitions: num_partitions.map(create_num_partitions),
            }
        }

        pub const fn create_block_height(block_height: u64) -> super::BlockHeight {
            super::BlockHeight { block_height }
        }

        pub const fn create_timestamp(timestamp: UnixTimestamp) -> super::UnixTimestamp {
            super::UnixTimestamp { timestamp }
        }
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Reward {
        #[prost(string, tag = "1")]
        pub pubkey: String,
        #[prost(int64, tag = "2")]
        pub lamports: i64,
        #[prost(uint64, tag = "3")]
        pub post_balance: u64,
        #[prost(enumeration = "RewardType", tag = "4")]
        pub reward_type: i32,
        #[prost(string, tag = "5")]
        pub commission: String,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct Rewards {
        #[prost(message, repeated, tag = "1")]
        pub rewards: Vec<Reward>,
        #[prost(message, optional, tag = "2")]
        pub num_partitions: Option<NumPartitions>,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct UnixTimestamp {
        #[prost(int64, tag = "1")]
        pub timestamp: i64,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct BlockHeight {
        #[prost(uint64, tag = "1")]
        pub block_height: u64,
    }

    #[derive(PartialEq, ::prost::Message)]
    pub struct NumPartitions {
        #[prost(uint64, tag = "1")]
        pub num_partitions: u64,
    }

    #[derive(Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum RewardType {
        Unspecified = 0,
        Fee = 1,
        Rent = 2,
        Staking = 3,
        Voting = 4,
    }

    impl RewardType {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub const fn as_str_name(&self) -> &'static str {
            match self {
                RewardType::Unspecified => "Unspecified",
                RewardType::Fee => "Fee",
                RewardType::Rent => "Rent",
                RewardType::Staking => "Staking",
                RewardType::Voting => "Voting",
            }
        }

        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> Option<Self> {
            match value {
                "Unspecified" => Some(Self::Unspecified),
                "Fee" => Some(Self::Fee),
                "Rent" => Some(Self::Rent),
                "Staking" => Some(Self::Staking),
                "Voting" => Some(Self::Voting),
                _ => None,
            }
        }
    }
}

#[inline]
pub fn field_encoded_len(tag: u32, len: usize) -> usize {
    key_len(tag) + len + encoded_len_varint(len as u64)
}

#[inline]
fn bytes_encode(tag: u32, value: &[u8], buf: &mut impl BufMut) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(value.len() as u64, buf);
    buf.put(value)
}

#[inline]
pub fn bytes_encode_repeated(tag: u32, values: &[&[u8]], buf: &mut impl BufMut) {
    for value in values {
        bytes_encode(tag, value, buf)
    }
}

#[inline]
pub fn bytes_encoded_len(tag: u32, value: &[u8]) -> usize {
    field_encoded_len(tag, value.len())
}

#[inline]
pub fn bytes_encoded_len_repeated(tag: u32, values: &[&[u8]]) -> usize {
    key_len(tag) * values.len()
        + values
            .iter()
            .map(|value| value.len() + encoded_len_varint(value.len() as u64))
            .sum::<usize>()
}

#[inline]
pub fn str_encode_repeated(tag: u32, values: &[&str], buf: &mut impl BufMut) {
    for value in values {
        bytes_encode(tag, value.as_ref(), buf)
    }
}

#[inline]
pub fn str_encoded_len_repeated(tag: u32, values: &[&str]) -> usize {
    key_len(tag) * values.len()
        + values
            .iter()
            .map(|value| value.len() + encoded_len_varint(value.len() as u64))
            .sum::<usize>()
}

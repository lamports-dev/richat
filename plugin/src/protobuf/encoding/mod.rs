pub use self::{
    account::Account, block_meta::BlockMeta, entry::Entry, slot::Slot, transaction::Transaction,
};
use {
    prost::{
        bytes::BufMut,
        encoding::{self, encode_key, encode_varint, encoded_len_varint, key_len, WireType},
    },
    solana_transaction_status::{Reward, RewardType},
};

mod account;
mod block_meta;
mod entry;
mod slot;
mod transaction;

pub fn encode_rewards(tag: u32, rewards: &[Reward], buf: &mut impl BufMut) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(rewards_encoded_len(tag, rewards) as u64, buf);
    for reward in rewards {
        encode_reward(reward, buf)
    }
}

pub fn rewards_encoded_len(tag: u32, rewards: &[Reward]) -> usize {
    iter_encoded_len(tag, rewards.iter().map(reward_encoded_len), rewards.len())
}

pub const fn reward_type_as_i32(reward_type: Option<RewardType>) -> i32 {
    match reward_type {
        None => 0,
        Some(RewardType::Fee) => 1,
        Some(RewardType::Rent) => 2,
        Some(RewardType::Staking) => 3,
        Some(RewardType::Voting) => 4,
    }
}

pub fn encode_reward(reward: &Reward, buf: &mut impl BufMut) {
    const NUM_STRINGS: [&str; 256] = [
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16",
        "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31",
        "32", "33", "34", "35", "36", "37", "38", "39", "40", "41", "42", "43", "44", "45", "46",
        "47", "48", "49", "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "60", "61",
        "62", "63", "64", "65", "66", "67", "68", "69", "70", "71", "72", "73", "74", "75", "76",
        "77", "78", "79", "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "90", "91",
        "92", "93", "94", "95", "96", "97", "98", "99", "100", "101", "102", "103", "104", "105",
        "106", "107", "108", "109", "110", "111", "112", "113", "114", "115", "116", "117", "118",
        "119", "120", "121", "122", "123", "124", "125", "126", "127", "128", "129", "130", "131",
        "132", "133", "134", "135", "136", "137", "138", "139", "140", "141", "142", "143", "144",
        "145", "146", "147", "148", "149", "150", "151", "152", "153", "154", "155", "156", "157",
        "158", "159", "160", "161", "162", "163", "164", "165", "166", "167", "168", "169", "170",
        "171", "172", "173", "174", "175", "176", "177", "178", "179", "180", "181", "182", "183",
        "184", "185", "186", "187", "188", "189", "190", "191", "192", "193", "194", "195", "196",
        "197", "198", "199", "200", "201", "202", "203", "204", "205", "206", "207", "208", "209",
        "210", "211", "212", "213", "214", "215", "216", "217", "218", "219", "220", "221", "222",
        "223", "224", "225", "226", "227", "228", "229", "230", "231", "232", "233", "234", "235",
        "236", "237", "238", "239", "240", "241", "242", "243", "244", "245", "246", "247", "248",
        "249", "250", "251", "252", "253", "254", "255",
    ];

    const fn u8_to_static_str(num: u8) -> &'static str {
        NUM_STRINGS[num as usize]
    }

    encoding::string::encode(1, &reward.pubkey, buf);
    encoding::int64::encode(2, &reward.lamports, buf);
    encoding::uint64::encode(3, &reward.post_balance, buf);
    encoding::int32::encode(4, &reward_type_as_i32(reward.reward_type), buf);
    if let Some(commission) = reward.commission {
        bytes_encode(5, u8_to_static_str(commission).as_ref(), buf);
    }
}

pub fn reward_encoded_len(reward: &Reward) -> usize {
    encoding::string::encoded_len(1, &reward.pubkey)
        + encoding::int64::encoded_len(2, &reward.lamports)
        + encoding::uint64::encoded_len(3, &reward.post_balance)
        + encoding::int32::encoded_len(4, &reward_type_as_i32(reward.reward_type))
        + reward
            .commission
            .map_or(0, |commission| bytes_encoded_len(5, &[commission]))
}

pub fn iter_encoded_len(tag: u32, iter: impl Iterator<Item = usize>, len: usize) -> usize {
    key_len(tag) * len
        + iter
            .map(|len| len + encoded_len_varint(len as u64))
            .sum::<usize>()
}

#[inline]
pub fn field_encoded_len(tag: u32, len: usize) -> usize {
    key_len(tag) + len + encoded_len_varint(len as u64)
}

#[inline]
pub fn bytes_encode(tag: u32, value: &[u8], buf: &mut impl BufMut) {
    encode_key(tag, WireType::LengthDelimited, buf);
    encode_varint(value.len() as u64, buf);
    buf.put(value)
}

#[inline]
pub fn bytes_encoded_len(tag: u32, value: &[u8]) -> usize {
    field_encoded_len(tag, value.len())
}

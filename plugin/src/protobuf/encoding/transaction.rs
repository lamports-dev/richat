use agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaTransactionInfoV2;
use prost::encoding::{self, message};
use solana_sdk::clock::Slot;

use super::bytes_encoded_len;

#[derive(Debug)]
pub struct Transaction<'a> {
    slot: Slot,
    transaction: &'a ReplicaTransactionInfoV2<'a>,
}

impl<'a> Transaction<'a> {
    pub const fn new(slot: Slot, transaction: &'a ReplicaTransactionInfoV2<'a>) -> Self {
        Self { slot, transaction }
    }
    fn transaction_encoded_len(&self) -> usize {
        /*
        let index = self.transaction.index as u64;

        bytes_encoded_len(1u32, self.transaction.signature.as_ref())
            + encoding::bool::encoded_len(2u32, &self.transaction.is_vote)
            + message::encoded_len(3u32, &self.transaction.transaction.)
            + message::encoded_len(4u32, &self.transaction.transaction_status_meta)
            + encoding::uint64::encoded_len(5u32, &index)
        */
        0
    }
}

pub struct MessageTransactionInfo {}
pub struct TransactionStatusMeta {}

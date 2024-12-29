use {
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    thiserror::Error,
    yellowstone_grpc_proto::geyser::CommitmentLevel,
};

#[derive(Debug, Error)]
pub enum MessageError {
    //
}

#[derive(Debug, Clone)]
pub enum Message {
    Account(MessageAccount),
    Slot(MessageSlot),
    Transaction(MessageTransaction),
    Entry(MessageEntry),
    BlockMeta(MessageBlockMeta),
    Block(MessageBlock),
}

impl Message {
    pub fn decode(_buffer: Vec<u8>) -> Result<Message, MessageError> {
        todo!()
    }

    pub fn get_slot(&self) -> Slot {
        todo!()
    }

    pub fn size(&self) -> usize {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct MessageAccount {
    pub pubkey: Pubkey,
    pub write_version: u64,
}

#[derive(Debug, Clone)]
pub struct MessageSlot {
    pub slot: Slot,
    pub commitment: CommitmentLevel,
}

#[derive(Debug, Clone)]
pub struct MessageTransaction {
    //
}

#[derive(Debug, Clone)]
pub struct MessageEntry {
    //
}

#[derive(Debug, Clone)]
pub struct MessageBlockMeta {
    pub slot: Slot,
    pub blockhash: String,
    pub block_height: Slot,
    pub executed_transaction_count: u64,
    pub entry_count: u64,
}

#[derive(Debug, Clone)]
pub struct MessageBlock {
    pub meta: MessageBlockMeta,
    pub accounts: Vec<MessageAccount>,
    pub transactions: Vec<MessageTransaction>,
    pub entries: Vec<MessageEntry>,
}

impl MessageBlock {
    pub fn new(
        meta: &MessageBlockMeta,
        accounts: Vec<MessageAccount>,
        transactions: Vec<MessageTransaction>,
        entries: Vec<MessageEntry>,
    ) -> Self {
        Self {
            meta: meta.clone(),
            accounts,
            transactions,
            entries,
        }
    }
}

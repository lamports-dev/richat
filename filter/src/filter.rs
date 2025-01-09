use {
    crate::{
        config::{ConfigFilter, ConfigFilterCommitment},
        message::Message,
    },
    solana_sdk::commitment_config::CommitmentLevel,
    std::ops::Range,
};

#[derive(Debug)]
pub struct Filter {
    // accounts: FilterAccounts,
    // slots: FilterSlots,
    // transactions: FilterTransactions,
    // transactions_status: FilterTransactions,
    // entries: FilterEntries,
    // blocks: FilterBlocks,
    // blocks_meta: FilterBlocksMeta,
    // accounts_data_slice: FilterAccountsDataSlice,
    pub commitment: ConfigFilterCommitment,
}

impl Filter {
    pub fn new(config: ConfigFilter) -> Result<Self, ()> {
        Ok(Self {
            commitment: config
                .commitment
                .unwrap_or(ConfigFilterCommitment::Processed),
        })
    }

    pub fn get_updates(
        &self,
        message: &Message,
        commitment: CommitmentLevel,
    ) -> () {
        todo!()
    }
}

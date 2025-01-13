use {
    crate::{
        config::{
            ConfigFilter, ConfigFilterAccounts, ConfigFilterAccountsDataSlice,
            ConfigFilterAccountsFilter, ConfigFilterAccountsFilterLamports, ConfigFilterBlocks,
            ConfigFilterCommitment, ConfigFilterSlots, ConfigFilterTransactions, MAX_DATA_SIZE,
            MAX_FILTERS,
        },
        message::{
            Message, MessageAccount, MessageBlock, MessageBlockCreatedAt, MessageBlockMeta,
            MessageEntry, MessageSlot, MessageTransaction,
        },
        protobuf::SubscribeUpdateMessage,
    },
    arrayvec::ArrayVec,
    prost::Message as _,
    richat_proto::geyser::{
        subscribe_update::UpdateOneof, CommitmentLevel as CommitmentLevelProto,
        SubscribeUpdateAccount, SubscribeUpdateAccountInfo, SubscribeUpdateBlock,
        SubscribeUpdateSlot, SubscribeUpdateTransaction, SubscribeUpdateTransactionStatus,
    },
    smallvec::{smallvec_inline, SmallVec},
    solana_sdk::{commitment_config::CommitmentLevel, pubkey::Pubkey, signature::Signature},
    spl_token_2022::{generic_token_account::GenericTokenAccount, state::Account as TokenAccount},
    std::{
        borrow::Borrow,
        collections::{HashMap, HashSet},
        ops::{Not, Range},
        sync::Arc,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FilterName(Arc<String>);

impl AsRef<str> for FilterName {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for FilterName {
    #[inline]
    fn borrow(&self) -> &str {
        &self.0[..]
    }
}

#[derive(Debug, Default)]
struct FilterNames {
    names: HashSet<FilterName>,
}

impl FilterNames {
    fn get(&mut self, name: &str) -> FilterName {
        match self.names.get(name) {
            Some(name) => name.clone(),
            None => {
                let name = FilterName(Arc::new(name.into()));
                self.names.insert(name.clone());
                name
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct Filter {
    slots: FilterSlots,
    accounts: FilterAccounts,
    accounts_data_slices: FilterAccountDataSlices,
    transactions: FilterTransactions,
    transactions_status: FilterTransactions,
    entries: FilterEntries,
    blocks_meta: FilterBlocksMeta,
    blocks: FilterBlocks,
    commitment: ConfigFilterCommitment,
}

impl Default for Filter {
    fn default() -> Self {
        Self {
            slots: FilterSlots::default(),
            accounts: FilterAccounts::default(),
            accounts_data_slices: FilterAccountDataSlices::default(),
            transactions: FilterTransactions {
                filter_type: FilterTransactionsType::Transaction,
                filters: HashMap::default(),
            },
            transactions_status: FilterTransactions {
                filter_type: FilterTransactionsType::TransactionStatus,
                filters: HashMap::default(),
            },
            entries: FilterEntries::default(),
            blocks_meta: FilterBlocksMeta::default(),
            blocks: FilterBlocks::default(),
            commitment: ConfigFilterCommitment::default(),
        }
    }
}

impl Filter {
    pub fn new(config: &ConfigFilter) -> Self {
        let mut names = FilterNames::default();
        Self {
            slots: FilterSlots::new(&mut names, &config.slots),
            accounts: FilterAccounts::new(&mut names, &config.accounts),
            accounts_data_slices: FilterAccountDataSlices::new(&config.accounts_data_slice),
            transactions: FilterTransactions::new(
                &mut names,
                &config.transactions,
                FilterTransactionsType::Transaction,
            ),
            transactions_status: FilterTransactions::new(
                &mut names,
                &config.transactions_status,
                FilterTransactionsType::TransactionStatus,
            ),
            entries: FilterEntries::new(&mut names, &config.entries),
            blocks_meta: FilterBlocksMeta::new(&mut names, &config.blocks_meta),
            blocks: FilterBlocks::new(&mut names, &config.blocks),
            commitment: config
                .commitment
                .unwrap_or(ConfigFilterCommitment::Processed),
        }
    }

    pub const fn commitment(&self) -> ConfigFilterCommitment {
        self.commitment
    }

    pub fn get_updates<'a>(
        &'a self,
        message: &'a Message,
        commitment: CommitmentLevel,
    ) -> SmallVec<[FilteredUpdate<'a>; 2]> {
        let mut vec = SmallVec::<[FilteredUpdate; 2]>::new();
        match message {
            Message::Slot(message) => {
                if let Some(update) = self.slots.get_update(message, commitment) {
                    vec.push(update);
                }
            }
            Message::Account(message) => {
                if let Some(update) = self
                    .accounts
                    .get_update(message, &self.accounts_data_slices)
                {
                    vec.push(update);
                }
            }
            Message::Transaction(message) => {
                if let Some(update) = self.transactions.get_update(message) {
                    vec.push(update);
                }
                if let Some(update) = self.transactions_status.get_update(message) {
                    vec.push(update);
                }
            }
            Message::Entry(message) => {
                if let Some(update) = self.entries.get_update(message) {
                    vec.push(update);
                }
            }
            Message::BlockMeta(message) => {
                if let Some(update) = self.blocks_meta.get_update(message) {
                    vec.push(update);
                }
            }
            Message::Block(message) => {
                for update in self.blocks.get_updates(message, &self.accounts_data_slices) {
                    vec.push(update);
                }
            }
        }
        vec
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct FilterSlotsInner {
    filter_by_commitment: bool,
}

impl FilterSlotsInner {
    fn new(filter: ConfigFilterSlots) -> Self {
        Self {
            filter_by_commitment: filter.filter_by_commitment.unwrap_or_default(),
        }
    }
}

#[derive(Debug, Default, Clone)]
struct FilterSlots {
    filters: HashMap<FilterName, FilterSlotsInner>,
}

impl FilterSlots {
    fn new(names: &mut FilterNames, configs: &HashMap<String, ConfigFilterSlots>) -> Self {
        Self {
            filters: configs
                .iter()
                .map(|(name, filter)| (names.get(name), FilterSlotsInner::new(*filter)))
                .collect(),
        }
    }

    fn get_update<'a>(
        &'a self,
        message: &'a MessageSlot,
        commitment: CommitmentLevel,
    ) -> Option<FilteredUpdate<'a>> {
        let msg_commitment = message.commitment();

        let filters = self
            .filters
            .iter()
            .filter_map(|(name, inner)| {
                if !inner.filter_by_commitment
                    || ((msg_commitment == CommitmentLevelProto::Processed
                        && commitment == CommitmentLevel::Processed)
                        || (msg_commitment == CommitmentLevelProto::Confirmed
                            && commitment == CommitmentLevel::Confirmed)
                        || (msg_commitment == CommitmentLevelProto::Finalized
                            && commitment == CommitmentLevel::Finalized))
                {
                    Some(name.as_ref())
                } else {
                    None
                }
            })
            .collect::<FilteredUpdateFilters>();

        filters.is_empty().not().then(|| FilteredUpdate {
            filters,
            filtered_update: FilteredUpdateType::Slot { message },
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterAccountsLamports {
    Eq(u64),
    Ne(u64),
    Lt(u64),
    Gt(u64),
}

impl From<ConfigFilterAccountsFilterLamports> for FilterAccountsLamports {
    fn from(cmp: ConfigFilterAccountsFilterLamports) -> Self {
        match cmp {
            ConfigFilterAccountsFilterLamports::Eq(value) => Self::Eq(value),
            ConfigFilterAccountsFilterLamports::Ne(value) => Self::Ne(value),
            ConfigFilterAccountsFilterLamports::Lt(value) => Self::Lt(value),
            ConfigFilterAccountsFilterLamports::Gt(value) => Self::Gt(value),
        }
    }
}

impl FilterAccountsLamports {
    const fn is_match(self, lamports: u64) -> bool {
        match self {
            Self::Eq(value) => value == lamports,
            Self::Ne(value) => value != lamports,
            Self::Lt(value) => value > lamports,
            Self::Gt(value) => value < lamports,
        }
    }
}

#[derive(Debug, Default, Clone)]
struct FilterAccountsState {
    memcmp: ArrayVec<(usize, ArrayVec<u8, MAX_DATA_SIZE>), MAX_FILTERS>,
    datasize: Option<usize>,
    token_account_state: bool,
    lamports: ArrayVec<FilterAccountsLamports, MAX_FILTERS>,
}

impl FilterAccountsState {
    fn new(filters: &[ConfigFilterAccountsFilter]) -> Self {
        let mut me = Self::default();
        for filter in filters {
            match filter {
                ConfigFilterAccountsFilter::Memcmp { offset, data } => {
                    me.memcmp.push((*offset, data.iter().cloned().collect()));
                }
                ConfigFilterAccountsFilter::DataSize(datasize) => {
                    me.datasize = Some(*datasize as usize);
                }
                ConfigFilterAccountsFilter::TokenAccountState => {
                    me.token_account_state = true;
                }
                ConfigFilterAccountsFilter::Lamports(value) => {
                    me.lamports.push((*value).into());
                }
            }
        }
        me
    }

    fn is_match(&self, lamports: u64, data: &[u8]) -> bool {
        if matches!(self.datasize, Some(datasize) if data.len() != datasize) {
            return false;
        }
        if self.token_account_state && !TokenAccount::valid_account_data(data) {
            return false;
        }
        if self.lamports.iter().any(|f| !f.is_match(lamports)) {
            return false;
        }
        for (offset, bytes) in self.memcmp.iter() {
            if data.len() < *offset + bytes.len() {
                return false;
            }
            let data = &data[*offset..*offset + bytes.len()];
            if data != bytes.as_slice() {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone)]
struct FilterAccountsInner {
    account: HashSet<Pubkey>,
    owner: HashSet<Pubkey>,
    filters: Option<FilterAccountsState>,
    nonempty_txn_signature: Option<bool>,
}

#[derive(Debug, Default, Clone)]
struct FilterAccounts {
    filters: HashMap<FilterName, FilterAccountsInner>,
}

impl FilterAccounts {
    fn new(names: &mut FilterNames, configs: &HashMap<String, ConfigFilterAccounts>) -> Self {
        let mut me = Self::default();
        for (name, filter) in configs {
            me.filters.insert(
                names.get(name),
                FilterAccountsInner {
                    account: filter.account.iter().copied().collect(),
                    owner: filter.owner.iter().copied().collect(),
                    filters: if filter.filters.is_empty() {
                        None
                    } else {
                        Some(FilterAccountsState::new(&filter.filters))
                    },
                    nonempty_txn_signature: filter.nonempty_txn_signature,
                },
            );
        }
        me
    }

    fn get_update<'a>(
        &'a self,
        message: &'a MessageAccount,
        data_slices: &'a FilterAccountDataSlices,
    ) -> Option<FilteredUpdate<'a>> {
        let msg_pubkey = message.pubkey();
        let msg_owner = message.owner();
        let msg_lamports = message.lamports();
        let msg_data = message.data();
        let msg_nonempty_txn_signature = message.nonempty_txn_signature();

        let filters = self
            .filters
            .iter()
            .filter_map(|(name, filter)| {
                if !filter.account.is_empty() && !filter.account.contains(msg_pubkey) {
                    return None;
                }

                if !filter.owner.is_empty() && !filter.owner.contains(msg_owner) {
                    return None;
                }

                if let Some(filters) = &filter.filters {
                    if !filters.is_match(msg_lamports, msg_data) {
                        return None;
                    }
                }

                if let Some(nonempty_txn_signature) = filter.nonempty_txn_signature {
                    if nonempty_txn_signature != msg_nonempty_txn_signature {
                        return None;
                    }
                }

                Some(name.as_ref())
            })
            .collect::<FilteredUpdateFilters>();

        filters.is_empty().not().then(|| FilteredUpdate {
            filters,
            filtered_update: FilteredUpdateType::Account {
                message,
                data_slices,
            },
        })
    }
}

#[derive(Debug, Default, Clone)]
pub struct FilterAccountDataSlices(SmallVec<[Range<usize>; 4]>);

impl FilterAccountDataSlices {
    fn new(data_slices: &[ConfigFilterAccountsDataSlice]) -> Self {
        let mut vec = SmallVec::new();
        for data_slice in data_slices {
            vec.push(Range {
                start: data_slice.offset as usize,
                end: (data_slice.offset + data_slice.length) as usize,
            })
        }
        Self(vec)
    }

    pub fn get_slice(&self, source: &[u8]) -> Vec<u8> {
        if self.0.is_empty() {
            source.to_vec()
        } else {
            let mut data = Vec::with_capacity(self.0.iter().map(|ds| ds.end - ds.start).sum());
            for data_slice in self.0.iter() {
                if source.len() >= data_slice.end {
                    data.extend_from_slice(&source[data_slice.start..data_slice.end]);
                }
            }
            data
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterTransactionsType {
    Transaction,
    TransactionStatus,
}

#[derive(Debug, Clone)]
struct FilterTransactionsInner {
    vote: Option<bool>,
    failed: Option<bool>,
    signature: Option<Signature>,
    account_include: HashSet<Pubkey>,
    account_exclude: HashSet<Pubkey>,
    account_required: HashSet<Pubkey>,
}

#[derive(Debug, Clone)]
struct FilterTransactions {
    filter_type: FilterTransactionsType,
    filters: HashMap<FilterName, FilterTransactionsInner>,
}

impl FilterTransactions {
    fn new(
        names: &mut FilterNames,
        configs: &HashMap<String, ConfigFilterTransactions>,
        filter_type: FilterTransactionsType,
    ) -> Self {
        let mut filters = HashMap::new();
        for (name, filter) in configs {
            filters.insert(
                names.get(name),
                FilterTransactionsInner {
                    vote: filter.vote,
                    failed: filter.failed,
                    signature: filter.signature,
                    account_include: filter.account_include.iter().copied().collect(),
                    account_exclude: filter.account_exclude.iter().copied().collect(),
                    account_required: filter.account_required.iter().copied().collect(),
                },
            );
        }
        Self {
            filter_type,
            filters,
        }
    }

    fn get_update<'a>(&'a self, message: &'a MessageTransaction) -> Option<FilteredUpdate<'a>> {
        let msg_vote = message.vote();
        let msg_failed = message.failed();
        let msg_signature = message.signature();
        let msg_account_keys = message.account_keys();

        let filters = self
            .filters
            .iter()
            .filter_map(|(name, filter)| {
                if let Some(is_vote) = filter.vote {
                    if is_vote != msg_vote {
                        return None;
                    }
                }

                if let Some(is_failed) = filter.failed {
                    if is_failed != msg_failed {
                        return None;
                    }
                }

                if let Some(signature) = &filter.signature {
                    if signature != msg_signature {
                        return None;
                    }
                }

                if !filter.account_include.is_empty()
                    && filter
                        .account_include
                        .intersection(msg_account_keys)
                        .next()
                        .is_none()
                {
                    return None;
                }

                if !filter.account_exclude.is_empty()
                    && filter
                        .account_exclude
                        .intersection(msg_account_keys)
                        .next()
                        .is_some()
                {
                    return None;
                }

                if !filter.account_required.is_empty()
                    && !filter.account_required.is_subset(msg_account_keys)
                {
                    return None;
                }

                Some(name.as_ref())
            })
            .collect::<FilteredUpdateFilters>();

        filters.is_empty().not().then(|| FilteredUpdate {
            filters,
            filtered_update: match self.filter_type {
                FilterTransactionsType::Transaction => FilteredUpdateType::Transaction { message },
                FilterTransactionsType::TransactionStatus => {
                    FilteredUpdateType::TransactionStatus { message }
                }
            },
        })
    }
}

#[derive(Debug, Default, Clone)]
struct FilterEntries {
    filters: Vec<FilterName>,
}

impl FilterEntries {
    fn new(names: &mut FilterNames, configs: &HashSet<String>) -> Self {
        Self {
            filters: configs.iter().map(|name| names.get(name)).collect(),
        }
    }

    fn get_update<'a>(&'a self, message: &'a MessageEntry) -> Option<FilteredUpdate<'a>> {
        let filters = self
            .filters
            .iter()
            .map(|f| f.as_ref())
            .collect::<FilteredUpdateFilters>();

        filters.is_empty().not().then(|| FilteredUpdate {
            filters,
            filtered_update: FilteredUpdateType::Entry { message },
        })
    }
}

#[derive(Debug, Default, Clone)]
struct FilterBlocksMeta {
    filters: Vec<FilterName>,
}

impl FilterBlocksMeta {
    fn new(names: &mut FilterNames, configs: &HashSet<String>) -> Self {
        Self {
            filters: configs.iter().map(|name| names.get(name)).collect(),
        }
    }

    fn get_update<'a>(&'a self, message: &'a MessageBlockMeta) -> Option<FilteredUpdate<'a>> {
        let filters = self
            .filters
            .iter()
            .map(|f| f.as_ref())
            .collect::<FilteredUpdateFilters>();

        filters.is_empty().not().then(|| FilteredUpdate {
            filters,
            filtered_update: FilteredUpdateType::BlockMeta { message },
        })
    }
}

#[derive(Debug, Clone)]
struct FilterBlocksInner {
    account_include: HashSet<Pubkey>,
    include_transactions: Option<bool>,
    include_accounts: Option<bool>,
    include_entries: Option<bool>,
}

#[derive(Debug, Default, Clone)]
struct FilterBlocks {
    filters: HashMap<FilterName, FilterBlocksInner>,
}

impl FilterBlocks {
    fn new(names: &mut FilterNames, configs: &HashMap<String, ConfigFilterBlocks>) -> Self {
        let mut me = Self::default();
        for (name, filter) in configs {
            me.filters.insert(
                names.get(name),
                FilterBlocksInner {
                    account_include: filter.account_include.iter().copied().collect(),
                    include_transactions: filter.include_transactions,
                    include_accounts: filter.include_accounts,
                    include_entries: filter.include_entries,
                },
            );
        }
        me
    }

    fn get_updates<'a>(
        &'a self,
        message: &'a MessageBlock,
        data_slices: &'a FilterAccountDataSlices,
    ) -> impl Iterator<Item = FilteredUpdate<'a>> {
        self.filters.iter().map(|(name, filter)| {
            let accounts = if filter.include_accounts == Some(true) {
                message
                    .accounts
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, account)| {
                        if !filter.account_include.is_empty()
                            && !filter.account_include.contains(account.pubkey())
                        {
                            None
                        } else {
                            Some(idx)
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![]
            };

            let transactions = if matches!(filter.include_transactions, None | Some(true)) {
                message
                    .transactions
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, tx)| {
                        if !filter.account_include.is_empty()
                            && filter
                                .account_include
                                .intersection(tx.account_keys())
                                .next()
                                .is_none()
                        {
                            None
                        } else {
                            Some(idx)
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![]
            };

            FilteredUpdate {
                filters: smallvec_inline![name.as_ref(); 8],
                filtered_update: FilteredUpdateType::Block {
                    message,
                    accounts,
                    transactions,
                    entries: filter.include_entries == Some(true),
                    data_slices,
                },
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct FilteredUpdate<'a> {
    pub filters: FilteredUpdateFilters<'a>,
    pub filtered_update: FilteredUpdateType<'a>,
}

impl<'a> FilteredUpdate<'a> {
    pub fn encode(&self) -> Vec<u8> {
        match &self.filtered_update {
            FilteredUpdateType::Slot { message } => match message {
                MessageSlot::Limited => todo!(),
                MessageSlot::Prost {
                    slot,
                    parent,
                    commitment,
                    dead_error,
                    created_at,
                } => SubscribeUpdateMessage {
                    filters: &self.filters,
                    update: UpdateOneof::Slot(SubscribeUpdateSlot {
                        slot: *slot,
                        parent: *parent,
                        status: *commitment as i32,
                        dead_error: dead_error.clone(),
                    }),
                    created_at: *created_at,
                }
                .encode_to_vec(),
            },
            FilteredUpdateType::Account {
                message,
                data_slices,
            } => match message {
                MessageAccount::Limited => todo!(),
                MessageAccount::Prost {
                    account,
                    slot,
                    is_startup,
                    created_at,
                    ..
                } => SubscribeUpdateMessage {
                    filters: &self.filters,
                    update: UpdateOneof::Account(SubscribeUpdateAccount {
                        account: Some(SubscribeUpdateAccountInfo {
                            pubkey: account.pubkey.clone(),
                            lamports: account.lamports,
                            owner: account.owner.clone(),
                            executable: account.executable,
                            rent_epoch: account.rent_epoch,
                            data: data_slices.get_slice(&account.data),
                            write_version: account.write_version,
                            txn_signature: account.txn_signature.clone(),
                        }),
                        slot: *slot,
                        is_startup: *is_startup,
                    }),
                    created_at: *created_at,
                }
                .encode_to_vec(),
            },
            FilteredUpdateType::Transaction { message } => match message {
                MessageTransaction::Limited => todo!(),
                MessageTransaction::Prost {
                    transaction,
                    slot,
                    created_at,
                    ..
                } => SubscribeUpdateMessage {
                    filters: &self.filters,
                    update: UpdateOneof::Transaction(SubscribeUpdateTransaction {
                        transaction: Some(transaction.clone()),
                        slot: *slot,
                    }),
                    created_at: *created_at,
                }
                .encode_to_vec(),
            },
            FilteredUpdateType::TransactionStatus { message } => match message {
                MessageTransaction::Limited => todo!(),
                MessageTransaction::Prost {
                    signature,
                    error,
                    transaction,
                    slot,
                    created_at,
                    ..
                } => SubscribeUpdateMessage {
                    filters: &self.filters,
                    update: UpdateOneof::TransactionStatus(SubscribeUpdateTransactionStatus {
                        slot: *slot,
                        signature: signature.as_ref().to_vec(),
                        is_vote: transaction.is_vote,
                        index: transaction.index,
                        err: error.clone(),
                    }),
                    created_at: *created_at,
                }
                .encode_to_vec(),
            },
            FilteredUpdateType::Entry { message } => match message {
                MessageEntry::Limited => todo!(),
                MessageEntry::Prost { entry, created_at } => SubscribeUpdateMessage {
                    filters: &self.filters,
                    update: UpdateOneof::Entry(entry.clone()),
                    created_at: *created_at,
                }
                .encode_to_vec(),
            },
            FilteredUpdateType::BlockMeta { message } => match message {
                MessageBlockMeta::Limited => todo!(),
                MessageBlockMeta::Prost {
                    block_meta,
                    created_at,
                } => SubscribeUpdateMessage {
                    filters: &self.filters,
                    update: UpdateOneof::BlockMeta(block_meta.clone()),
                    created_at: *created_at,
                }
                .encode_to_vec(),
            },
            FilteredUpdateType::Block {
                message,
                accounts,
                transactions,
                entries,
                data_slices,
            } => match message.created_at {
                MessageBlockCreatedAt::Limited => todo!(),
                MessageBlockCreatedAt::Prost(created_at) => {
                    let block_meta = match message.block_meta.as_ref() {
                        MessageBlockMeta::Limited => unreachable!(),
                        MessageBlockMeta::Prost { block_meta, .. } => block_meta,
                    };

                    SubscribeUpdateMessage {
                        filters: &self.filters,
                        update: UpdateOneof::Block(SubscribeUpdateBlock {
                            slot: block_meta.slot,
                            blockhash: block_meta.blockhash.clone(),
                            rewards: block_meta.rewards.clone(),
                            block_time: block_meta.block_time,
                            block_height: block_meta.block_height,
                            parent_slot: block_meta.parent_slot,
                            parent_blockhash: block_meta.parent_blockhash.clone(),
                            executed_transaction_count: block_meta.executed_transaction_count,
                            transactions: transactions
                                .iter()
                                .map(|idx| match message.transactions[*idx].as_ref() {
                                    MessageTransaction::Limited => unreachable!(),
                                    MessageTransaction::Prost { transaction, .. } => {
                                        transaction.clone()
                                    }
                                })
                                .collect(),
                            updated_account_count: message.accounts.len() as u64,
                            accounts: accounts
                                .iter()
                                .map(|idx| match message.accounts[*idx].as_ref() {
                                    MessageAccount::Limited => unreachable!(),
                                    MessageAccount::Prost { account, .. } => {
                                        SubscribeUpdateAccountInfo {
                                            pubkey: account.pubkey.clone(),
                                            lamports: account.lamports,
                                            owner: account.owner.clone(),
                                            executable: account.executable,
                                            rent_epoch: account.rent_epoch,
                                            data: data_slices.get_slice(&account.data),
                                            write_version: account.write_version,
                                            txn_signature: account.txn_signature.clone(),
                                        }
                                    }
                                })
                                .collect(),
                            entries_count: block_meta.entries_count,
                            entries: if *entries {
                                message
                                    .entries
                                    .iter()
                                    .map(|entry| match entry.as_ref() {
                                        MessageEntry::Limited => unreachable!(),
                                        MessageEntry::Prost { entry, .. } => entry.clone(),
                                    })
                                    .collect()
                            } else {
                                vec![]
                            },
                        }),
                        created_at,
                    }
                    .encode_to_vec()
                }
            },
        }
    }
}

pub type FilteredUpdateFilters<'a> = SmallVec<[&'a str; 8]>;

#[derive(Debug, Clone)]
pub enum FilteredUpdateType<'a> {
    Slot {
        message: &'a MessageSlot,
    },
    Account {
        message: &'a MessageAccount,
        data_slices: &'a FilterAccountDataSlices,
    },
    Transaction {
        message: &'a MessageTransaction,
    },
    TransactionStatus {
        message: &'a MessageTransaction,
    },
    Entry {
        message: &'a MessageEntry,
    },
    BlockMeta {
        message: &'a MessageBlockMeta,
    },
    Block {
        message: &'a MessageBlock,
        accounts: Vec<usize>,
        transactions: Vec<usize>,
        entries: bool,
        data_slices: &'a FilterAccountDataSlices,
    },
}

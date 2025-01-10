use {
    crate::{
        config::{
            ConfigFilter, ConfigFilterAccounts, ConfigFilterAccountsDataSlice,
            ConfigFilterAccountsFilter, ConfigFilterAccountsFilterLamports, ConfigFilterBlocks,
            ConfigFilterCommitment, ConfigFilterSlots, ConfigFilterTransactions,
        },
        message::{
            MessageValues, MessageValuesAccount, MessageValuesSlot, MessageValuesTransaction,
        },
    },
    smallvec::SmallVec,
    solana_sdk::{commitment_config::CommitmentLevel, pubkey::Pubkey, signature::Signature},
    spl_token_2022::{generic_token_account::GenericTokenAccount, state::Account as TokenAccount},
    std::{
        borrow::Borrow,
        collections::{HashMap, HashSet},
        ops::Range,
        sync::Arc,
    },
    yellowstone_grpc_proto::geyser::CommitmentLevel as CommitmentLevelProto,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FilterName(Arc<String>);

impl AsRef<str> for FilterName {
    #[inline]
    fn as_ref(&self) -> &str {
        &self.0
    }
}

// impl Deref for FilterName {
//     type Target = str;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

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

    pub fn get_updates(
        &self,
        message: &MessageValues,
        commitment: CommitmentLevel,
    ) -> SmallVec<[FilteredUpdate; 2]> {
        let mut vec = SmallVec::<[FilteredUpdate; 2]>::new();
        match message {
            MessageValues::Slot(values) => {
                vec.push(self.slots.get_update(values, commitment));
            }
            MessageValues::Account(values) => {
                vec.push(self.accounts.get_update(values, &self.accounts_data_slices));
            }
            MessageValues::Transaction(values) => {
                vec.push(self.transactions.get_update(values));
                vec.push(self.transactions_status.get_update(values));
            }
            MessageValues::Entry => {
                vec.push(self.entries.get_update());
            }
            MessageValues::BlockMeta => {
                vec.push(self.blocks_meta.get_update());
            }
        }
        vec.into_iter().filter(|u| !u.is_empty()).collect()
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

    fn get_update(
        &self,
        values: &MessageValuesSlot,
        commitment: CommitmentLevel,
    ) -> FilteredUpdate {
        let filters = self
            .filters
            .iter()
            .filter_map(|(name, inner)| {
                if !inner.filter_by_commitment
                    || ((values.commitment == CommitmentLevelProto::Processed
                        && commitment == CommitmentLevel::Processed)
                        || (values.commitment == CommitmentLevelProto::Confirmed
                            && commitment == CommitmentLevel::Confirmed)
                        || (values.commitment == CommitmentLevelProto::Finalized
                            && commitment == CommitmentLevel::Finalized))
                {
                    Some(name.as_ref())
                } else {
                    None
                }
            })
            .collect();

        FilteredUpdate {
            filters,
            filtered_update: FilteredUpdateType::Slot,
        }
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
    memcmp: SmallVec<[(usize, Vec<u8>); 4]>,
    datasize: Option<usize>,
    token_account_state: bool,
    lamports: SmallVec<[FilterAccountsLamports; 4]>,
}

impl FilterAccountsState {
    fn new(filters: &[ConfigFilterAccountsFilter]) -> Self {
        let mut me = Self::default();
        for filter in filters {
            match filter {
                ConfigFilterAccountsFilter::Memcmp { offset, data } => {
                    me.memcmp.push((*offset, data.clone()));
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
            if data != bytes {
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
        values: &MessageValuesAccount,
        data_slices: &'a FilterAccountDataSlices,
    ) -> FilteredUpdate<'a> {
        let filters = self
            .filters
            .iter()
            .filter_map(|(name, filter)| {
                if !filter.account.is_empty() && !filter.account.contains(&values.pubkey) {
                    return None;
                }

                if !filter.owner.is_empty() && !filter.owner.contains(&values.owner) {
                    return None;
                }

                if let Some(filters) = &filter.filters {
                    if !filters.is_match(values.lamports, values.data) {
                        return None;
                    }
                }

                if let Some(nonempty_txn_signature) = filter.nonempty_txn_signature {
                    if nonempty_txn_signature != values.nonempty_txn_signature {
                        return None;
                    }
                }

                Some(name.as_ref())
            })
            .collect();

        FilteredUpdate {
            filters,
            filtered_update: FilteredUpdateType::Account(data_slices),
        }
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

    fn get_update(&self, values: &MessageValuesTransaction) -> FilteredUpdate {
        let filters = self
            .filters
            .iter()
            .filter_map(|(name, filter)| {
                if let Some(is_vote) = filter.vote {
                    if is_vote != values.vote {
                        return None;
                    }
                }

                if let Some(is_failed) = filter.failed {
                    if is_failed != values.failed {
                        return None;
                    }
                }

                if let Some(signature) = &filter.signature {
                    if signature != &values.signature {
                        return None;
                    }
                }

                if !filter.account_include.is_empty()
                    && filter
                        .account_include
                        .intersection(values.account_keys)
                        .next()
                        .is_none()
                {
                    return None;
                }

                if !filter.account_exclude.is_empty()
                    && filter
                        .account_exclude
                        .intersection(values.account_keys)
                        .next()
                        .is_some()
                {
                    return None;
                }

                if !filter.account_required.is_empty()
                    && !filter.account_required.is_subset(values.account_keys)
                {
                    return None;
                }

                Some(name.as_ref())
            })
            .collect();

        FilteredUpdate {
            filters,
            filtered_update: match self.filter_type {
                FilterTransactionsType::Transaction => FilteredUpdateType::Transaction,
                FilterTransactionsType::TransactionStatus => FilteredUpdateType::TransactionStatus,
            },
        }
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

    fn get_update(&self) -> FilteredUpdate {
        FilteredUpdate {
            filters: self.filters.iter().map(|f| f.as_ref()).collect(),
            filtered_update: FilteredUpdateType::Entry,
        }
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

    fn get_update(&self) -> FilteredUpdate {
        FilteredUpdate {
            filters: self.filters.iter().map(|f| f.as_ref()).collect(),
            filtered_update: FilteredUpdateType::BlockMeta,
        }
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
}

#[derive(Debug, Clone)]
pub struct FilteredUpdate<'a> {
    pub filters: SmallVec<[&'a str; 8]>,
    pub filtered_update: FilteredUpdateType<'a>,
}

impl<'a> FilteredUpdate<'a> {
    fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }
}

#[derive(Debug, Clone)]
pub enum FilteredUpdateType<'a> {
    Slot,
    Account(&'a FilterAccountDataSlices),
    Transaction,
    TransactionStatus,
    Entry,
    BlockMeta,
    Block(&'a FilterAccountDataSlices),
}

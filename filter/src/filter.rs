use {
    crate::{
        config::{
            ConfigFilter, ConfigFilterAccounts, ConfigFilterAccountsDataSlice,
            ConfigFilterAccountsFilter, ConfigFilterAccountsFilterLamports, ConfigFilterBlocks,
            ConfigFilterCommitment, ConfigFilterSlots, ConfigFilterTransactions,
        },
        message::Message,
    },
    smallvec::SmallVec,
    solana_sdk::{commitment_config::CommitmentLevel, pubkey::Pubkey, signature::Signature},
    std::{
        borrow::Borrow,
        collections::{HashMap, HashSet},
        ops::Range,
        sync::Arc,
    },
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FilterName(Arc<String>);

// impl AsRef<str> for FilterName {
//     #[inline]
//     fn as_ref(&self) -> &str {
//         &self.0
//     }
// }

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

    pub fn get_updates(&self, message: &Message, commitment: CommitmentLevel) -> () {
        todo!()
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
}

#[derive(Debug, Default, Clone)]
struct FilterAccounts {
    nonempty_txn_signature: Vec<(FilterName, Option<bool>)>,
    nonempty_txn_signature_required: HashSet<FilterName>,
    account: HashMap<Pubkey, HashSet<FilterName>>,
    account_required: HashSet<FilterName>,
    owner: HashMap<Pubkey, HashSet<FilterName>>,
    owner_required: HashSet<FilterName>,
    filters: Vec<(FilterName, FilterAccountsState)>,
}

impl FilterAccounts {
    fn new(names: &mut FilterNames, configs: &HashMap<String, ConfigFilterAccounts>) -> Self {
        let mut me = Self::default();
        for (name, config) in configs {
            me.nonempty_txn_signature
                .push((names.get(name), config.nonempty_txn_signature));
            if config.nonempty_txn_signature.is_some() {
                me.nonempty_txn_signature_required.insert(names.get(name));
            }

            Self::set(
                &mut me.account,
                &mut me.account_required,
                &name,
                names,
                &config.account,
            );
            Self::set(
                &mut me.owner,
                &mut me.owner_required,
                &name,
                names,
                &config.owner,
            );

            me.filters
                .push((names.get(name), FilterAccountsState::new(&config.filters)));
        }
        me
    }

    fn set(
        map: &mut HashMap<Pubkey, HashSet<FilterName>>,
        map_required: &mut HashSet<FilterName>,
        name: &str,
        names: &mut FilterNames,
        pubkeys: &[Pubkey],
    ) {
        let mut required = false;
        for pubkey in pubkeys {
            if map.entry(*pubkey).or_default().insert(names.get(name)) {
                required = true;
            }
        }

        if required {
            map_required.insert(names.get(name));
        }
    }
}

#[derive(Debug, Default, Clone)]
struct FilterAccountsState {
    memcmp: Vec<(usize, Vec<u8>)>,
    datasize: Option<usize>,
    token_account_state: bool,
    lamports: Vec<FilterAccountsLamports>,
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

#[derive(Debug, Default, Clone)]
struct FilterAccountDataSlices(SmallVec<[Range<usize>; 4]>);

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
                    signature: filter.signature.clone(),
                    account_include: filter.account_include.iter().cloned().collect(),
                    account_exclude: filter.account_exclude.iter().cloned().collect(),
                    account_required: filter.account_required.iter().cloned().collect(),
                },
            );
        }
        Self {
            filter_type,
            filters,
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
                    account_include: filter.account_include.iter().cloned().collect(),
                    include_transactions: filter.include_transactions,
                    include_accounts: filter.include_accounts,
                    include_entries: filter.include_entries,
                },
            );
        }
        me
    }
}

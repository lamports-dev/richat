use {
    base64::{engine::general_purpose::STANDARD as base64_engine, Engine},
    richat_proto::geyser::{
        subscribe_request_filter_accounts_filter::Filter as AccountsFilterDataOneof,
        subscribe_request_filter_accounts_filter_lamports::Cmp as AccountsFilterLamports,
        subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
        CommitmentLevel as CommitmentLevelProto, SubscribeRequest,
        SubscribeRequestAccountsDataSlice, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterLamports,
        SubscribeRequestFilterAccountsFilterMemcmp, SubscribeRequestFilterBlocks,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
    },
    richat_shared::config::{
        deserialize_maybe_signature, deserialize_num_str, deserialize_pubkey_set,
        deserialize_pubkey_vec,
    },
    serde::{
        de::{self, Deserializer},
        Deserialize, Serialize,
    },
    solana_sdk::{
        commitment_config::CommitmentLevel,
        pubkey::{ParsePubkeyError, Pubkey},
        signature::{ParseSignatureError, Signature},
    },
    std::collections::{HashMap, HashSet},
    thiserror::Error,
};

pub const MAX_FILTERS: usize = 4;
pub const MAX_DATA_SIZE: usize = 128;
pub const MAX_DATA_BASE58_SIZE: usize = 175;
pub const MAX_DATA_BASE64_SIZE: usize = 172;

#[derive(Debug, Error)]
pub enum ConfigLimitsError {
    #[error("Filter name exceeds limit, max {max}")]
    FilterNameOverflow { max: usize },
    #[error("Max amount of filters/data_slices reached, only {max} allowed")]
    FiltersOverflow { max: usize },
    #[error("Subscribe on full stream with `any` is not allowed, at least one filter required")]
    EmptyNotAllowed,
    #[error("Max amount of Pubkeys is reached, only {max} allowed")]
    PubkeysOverflow { max: usize },
    #[error("Pubkey {pubkey} in filters is not allowed")]
    PubkeyNotAllowed { pubkey: Pubkey },
    #[error("Too much filters provided; max: {max}")]
    TooMuchFilters { max: usize },
    #[error("Filter data is too large, max size: {MAX_DATA_SIZE}")]
    FilterDataOverflow,
    #[error("Datasize used more than once")]
    DatasizeDuplicated,
    #[error("Failed to create filter: data slices out of order")]
    DataSliceOutOfOrder,
    #[error("Failed to create filter: data slices overlapped")]
    DataSliceOverlap,
    #[error("`include_{0}` is not allowed")]
    BlocksNotAllowed(&'static str),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigLimits {
    pub name_max: usize,
    pub slots: ConfigLimitsSlots,
    pub accounts: ConfigLimitsAccounts,
    pub transactions: ConfigLimitsTransactions,
    pub transactions_status: ConfigLimitsTransactions,
    pub entries: ConfigLimitsEntries,
    pub blocks_meta: ConfigLimitsBlocksMeta,
    pub blocks: ConfigLimitsBlocks,
}

impl Default for ConfigLimits {
    fn default() -> Self {
        Self {
            name_max: 128,
            slots: Default::default(),
            accounts: Default::default(),
            transactions: Default::default(),
            transactions_status: Default::default(),
            entries: Default::default(),
            blocks_meta: Default::default(),
            blocks: Default::default(),
        }
    }
}

impl ConfigLimits {
    const fn check_name_max(len: usize, max: usize) -> Result<(), ConfigLimitsError> {
        if len <= max {
            Ok(())
        } else {
            Err(ConfigLimitsError::FilterNameOverflow { max })
        }
    }

    fn check_names_max<'a>(
        names: impl Iterator<Item = &'a String>,
        max: usize,
    ) -> Result<(), ConfigLimitsError> {
        for key in names {
            Self::check_name_max(key.len(), max)?;
        }
        Ok(())
    }

    const fn check_max(len: usize, max: usize) -> Result<(), ConfigLimitsError> {
        if len <= max {
            Ok(())
        } else {
            Err(ConfigLimitsError::FiltersOverflow { max })
        }
    }

    const fn check_any(is_empty: bool, any: bool) -> Result<(), ConfigLimitsError> {
        if !is_empty || any {
            Ok(())
        } else {
            Err(ConfigLimitsError::EmptyNotAllowed)
        }
    }

    const fn check_pubkey_max(len: usize, max: usize) -> Result<(), ConfigLimitsError> {
        if len <= max {
            Ok(())
        } else {
            Err(ConfigLimitsError::PubkeysOverflow { max })
        }
    }

    fn check_pubkey_reject(
        pubkey: &Pubkey,
        set: &HashSet<Pubkey>,
    ) -> Result<(), ConfigLimitsError> {
        if !set.contains(pubkey) {
            Ok(())
        } else {
            Err(ConfigLimitsError::PubkeyNotAllowed { pubkey: *pubkey })
        }
    }

    pub fn check_filter(&self, filter: &ConfigFilter) -> Result<(), ConfigLimitsError> {
        self.slots.check_filter(self.name_max, &filter.slots)?;
        self.accounts
            .check_filter(self.name_max, &filter.accounts, &filter.accounts_data_slice)?;
        self.transactions
            .check_filter(self.name_max, &filter.transactions)?;
        self.transactions_status
            .check_filter(self.name_max, &filter.transactions_status)?;
        self.entries.check_filter(self.name_max, &filter.entries)?;
        self.blocks_meta
            .check_filter(self.name_max, &filter.blocks_meta)?;
        self.blocks.check_filter(self.name_max, &filter.blocks)?;

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigLimitsSlots {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max: usize,
}

impl Default for ConfigLimitsSlots {
    fn default() -> Self {
        Self { max: usize::MAX }
    }
}

impl ConfigLimitsSlots {
    pub fn check_filter(
        &self,
        name_max: usize,
        filters: &HashMap<String, ConfigFilterSlots>,
    ) -> Result<(), ConfigLimitsError> {
        ConfigLimits::check_names_max(filters.keys(), name_max)?;
        ConfigLimits::check_max(filters.len(), self.max)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigLimitsAccounts {
    pub max: usize,
    pub any: bool,
    pub account_max: usize,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub account_reject: HashSet<Pubkey>,
    pub owner_max: usize,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub owner_reject: HashSet<Pubkey>,
    pub data_slice_max: usize,
}

impl Default for ConfigLimitsAccounts {
    fn default() -> Self {
        Self {
            max: usize::MAX,
            any: true,
            account_max: usize::MAX,
            account_reject: HashSet::new(),
            owner_max: usize::MAX,
            owner_reject: HashSet::new(),
            data_slice_max: usize::MAX,
        }
    }
}

impl ConfigLimitsAccounts {
    pub fn check_filter(
        &self,
        name_max: usize,
        filters: &HashMap<String, ConfigFilterAccounts>,
        data_slices: &[ConfigFilterAccountsDataSlice],
    ) -> Result<(), ConfigLimitsError> {
        ConfigLimits::check_names_max(filters.keys(), name_max)?;
        ConfigLimits::check_max(filters.len(), self.max)?;

        for filter in filters.values() {
            ConfigLimits::check_any(
                filter.account.is_empty() && filter.owner.is_empty(),
                self.any,
            )?;
            ConfigLimits::check_pubkey_max(filter.account.len(), self.account_max)?;
            ConfigLimits::check_pubkey_max(filter.owner.len(), self.owner_max)?;

            for pubkey in filter.account.iter() {
                ConfigLimits::check_pubkey_reject(pubkey, &self.account_reject)?;
            }
            for pubkey in filter.owner.iter() {
                ConfigLimits::check_pubkey_reject(pubkey, &self.owner_reject)?;
            }

            if filter.filters.len() > MAX_FILTERS {
                return Err(ConfigLimitsError::TooMuchFilters { max: MAX_FILTERS });
            }
            let mut datasize_defined = false;
            for filter in filter.filters.iter() {
                match filter {
                    ConfigFilterAccountsFilter::Memcmp {
                        offset: _offset,
                        data,
                    } => {
                        if data.len() > MAX_DATA_SIZE {
                            return Err(ConfigLimitsError::FilterDataOverflow);
                        }
                    }
                    ConfigFilterAccountsFilter::DataSize(_) => {
                        if datasize_defined {
                            return Err(ConfigLimitsError::DatasizeDuplicated);
                        }
                        datasize_defined = true;
                    }
                    ConfigFilterAccountsFilter::TokenAccountState => {}
                    ConfigFilterAccountsFilter::Lamports(_) => {}
                }
            }
        }

        ConfigLimits::check_max(data_slices.len(), self.data_slice_max)?;
        for (i, ds_a) in data_slices.iter().enumerate() {
            // check order
            for ds_b in data_slices[i + 1..].iter() {
                if ds_a.offset > ds_b.offset {
                    return Err(ConfigLimitsError::DataSliceOutOfOrder);
                }
            }

            // check overlap
            for slice_b in data_slices[0..i].iter() {
                if ds_a.offset < slice_b.offset + slice_b.length {
                    return Err(ConfigLimitsError::DataSliceOverlap);
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigLimitsTransactions {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max: usize,
    pub any: bool,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub account_include_max: usize,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub account_include_reject: HashSet<Pubkey>,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub account_exclude_max: usize,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub account_required_max: usize,
}

impl Default for ConfigLimitsTransactions {
    fn default() -> Self {
        Self {
            max: usize::MAX,
            any: true,
            account_include_max: usize::MAX,
            account_include_reject: HashSet::new(),
            account_exclude_max: usize::MAX,
            account_required_max: usize::MAX,
        }
    }
}

impl ConfigLimitsTransactions {
    pub fn check_filter(
        &self,
        name_max: usize,
        filters: &HashMap<String, ConfigFilterTransactions>,
    ) -> Result<(), ConfigLimitsError> {
        ConfigLimits::check_names_max(filters.keys(), name_max)?;
        ConfigLimits::check_max(filters.len(), self.max)?;

        for filter in filters.values() {
            ConfigLimits::check_any(
                filter.vote.is_none()
                    && filter.failed.is_none()
                    && filter.account_include.is_empty()
                    && filter.account_exclude.is_empty()
                    && filter.account_required.is_empty(),
                self.any,
            )?;
            ConfigLimits::check_pubkey_max(filter.account_include.len(), self.account_include_max)?;
            ConfigLimits::check_pubkey_max(filter.account_exclude.len(), self.account_exclude_max)?;
            ConfigLimits::check_pubkey_max(
                filter.account_required.len(),
                self.account_required_max,
            )?;

            for pubkey in filter.account_include.iter() {
                ConfigLimits::check_pubkey_reject(pubkey, &self.account_include_reject)?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigLimitsEntries {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max: usize,
}

impl Default for ConfigLimitsEntries {
    fn default() -> Self {
        Self { max: usize::MAX }
    }
}

impl ConfigLimitsEntries {
    pub fn check_filter(
        &self,
        name_max: usize,
        filters: &HashSet<String>,
    ) -> Result<(), ConfigLimitsError> {
        ConfigLimits::check_names_max(filters.iter(), name_max)?;
        ConfigLimits::check_max(filters.len(), self.max)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigLimitsBlocksMeta {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max: usize,
}

impl Default for ConfigLimitsBlocksMeta {
    fn default() -> Self {
        Self { max: usize::MAX }
    }
}

impl ConfigLimitsBlocksMeta {
    pub fn check_filter(
        &self,
        name_max: usize,
        filters: &HashSet<String>,
    ) -> Result<(), ConfigLimitsError> {
        ConfigLimits::check_names_max(filters.iter(), name_max)?;
        ConfigLimits::check_max(filters.len(), self.max)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigLimitsBlocks {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max: usize,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub account_include_max: usize,
    pub account_include_any: bool,
    #[serde(deserialize_with = "deserialize_pubkey_set")]
    pub account_include_reject: HashSet<Pubkey>,
    pub include_transactions: bool,
    pub include_accounts: bool,
    pub include_entries: bool,
}

impl Default for ConfigLimitsBlocks {
    fn default() -> Self {
        Self {
            max: usize::MAX,
            account_include_max: usize::MAX,
            account_include_any: true,
            account_include_reject: HashSet::new(),
            include_transactions: true,
            include_accounts: true,
            include_entries: true,
        }
    }
}

impl ConfigLimitsBlocks {
    pub fn check_filter(
        &self,
        name_max: usize,
        filters: &HashMap<String, ConfigFilterBlocks>,
    ) -> Result<(), ConfigLimitsError> {
        ConfigLimits::check_names_max(filters.keys(), name_max)?;
        ConfigLimits::check_max(filters.len(), self.max)?;

        for filter in filters.values() {
            ConfigLimits::check_any(filter.account_include.is_empty(), self.account_include_any)?;
            ConfigLimits::check_pubkey_max(filter.account_include.len(), self.account_include_max)?;
            if !(filter.include_transactions == Some(false) || self.include_transactions) {
                return Err(ConfigLimitsError::BlocksNotAllowed("transactions"));
            }
            if !(matches!(filter.include_accounts, None | Some(false)) || self.include_accounts) {
                return Err(ConfigLimitsError::BlocksNotAllowed("accounts"));
            }
            if !(matches!(filter.include_entries, None | Some(false)) || self.include_accounts) {
                return Err(ConfigLimitsError::BlocksNotAllowed("entries"));
            }
            for pubkey in filter.account_include.iter() {
                ConfigLimits::check_pubkey_reject(pubkey, &self.account_include_reject)?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ConfigFilterError {
    #[error(transparent)]
    Limits(#[from] ConfigLimitsError),
    #[error("Field `{0}` should be defined")]
    FieldNotDefined(&'static str),
    #[error("Token account state value is invalid")]
    TokenAccountStateInvalid,
    #[error("Invalid base58 encoding")]
    InvalidBase58,
    #[error("Invalid base64 encoding")]
    InvalidBase64,
    #[error("Invalid pubkey `{0}`: {1}")]
    Pubkey(String, ParsePubkeyError),
    #[error("Invalid signature `{0}`: {1}")]
    Signature(String, ParseSignatureError),
    #[error("Unknown commitment level: {0}")]
    UnknownCommitment(i32),
    #[error("Commitment {0:?} not allowed, only processed/confirmed/finalized are supported")]
    InvalidCommitment(CommitmentLevelProto),
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigFilter {
    pub slots: HashMap<String, ConfigFilterSlots>,
    pub accounts: HashMap<String, ConfigFilterAccounts>,
    pub accounts_data_slice: Vec<ConfigFilterAccountsDataSlice>,
    pub transactions: HashMap<String, ConfigFilterTransactions>,
    pub transactions_status: HashMap<String, ConfigFilterTransactions>,
    pub entries: HashSet<String>,
    pub blocks_meta: HashSet<String>,
    pub blocks: HashMap<String, ConfigFilterBlocks>,
    pub commitment: Option<ConfigFilterCommitment>,
}

impl ConfigFilter {
    fn try_conv_map<T, C>(map: HashMap<String, T>) -> Result<HashMap<String, C>, ConfigFilterError>
    where
        C: TryFrom<T, Error = ConfigFilterError>,
    {
        map.into_iter()
            .map(|(key, value)| value.try_into().map(|value| (key, value)))
            .collect::<Result<_, _>>()
    }

    fn conv_map<C, T>(map: HashMap<String, C>) -> HashMap<String, T>
    where
        T: From<C>,
    {
        map.into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect()
    }

    fn conv_set<T: Default>(set: HashSet<String>) -> HashMap<String, T> {
        set.into_iter().map(|key| (key, T::default())).collect()
    }

    fn parse_vec_pubkeys(pubkeys: Vec<String>) -> Result<Vec<Pubkey>, ConfigFilterError> {
        pubkeys
            .into_iter()
            .map(|pubkey| {
                pubkey
                    .parse()
                    .map_err(|error| ConfigFilterError::Pubkey(pubkey, error))
            })
            .collect::<Result<Vec<_>, _>>()
    }

    fn conv_vec_pubkeys(pubkeys: Vec<Pubkey>) -> Vec<String> {
        pubkeys.into_iter().map(|pk| pk.to_string()).collect()
    }
}

impl TryFrom<SubscribeRequest> for ConfigFilter {
    type Error = ConfigFilterError;

    fn try_from(value: SubscribeRequest) -> Result<Self, Self::Error> {
        Ok(Self {
            slots: Self::try_conv_map(value.slots)?,
            accounts: Self::try_conv_map(value.accounts)?,
            accounts_data_slice: value
                .accounts_data_slice
                .into_iter()
                .map(|ds| ds.try_into())
                .collect::<Result<_, _>>()?,
            transactions: Self::try_conv_map(value.transactions)?,
            transactions_status: Self::try_conv_map(value.transactions_status)?,
            entries: value.entry.into_keys().collect(),
            blocks_meta: value.blocks_meta.into_keys().collect(),
            blocks: Self::try_conv_map(value.blocks)?,
            commitment: value.commitment.map(|value| value.try_into()).transpose()?,
        })
    }
}

impl From<ConfigFilter> for SubscribeRequest {
    fn from(value: ConfigFilter) -> Self {
        SubscribeRequest {
            slots: ConfigFilter::conv_map(value.slots),
            accounts: ConfigFilter::conv_map(value.accounts),
            accounts_data_slice: value
                .accounts_data_slice
                .into_iter()
                .map(|ds| ds.into())
                .collect(),
            transactions: ConfigFilter::conv_map(value.transactions),
            transactions_status: ConfigFilter::conv_map(value.transactions_status),
            entry: ConfigFilter::conv_set(value.entries),
            blocks_meta: ConfigFilter::conv_set(value.blocks_meta),
            blocks: ConfigFilter::conv_map(value.blocks),
            commitment: value.commitment.map(|c| c.into()),
            ping: None,
            from_slot: None,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigFilterSlots {
    pub filter_by_commitment: Option<bool>,
}

impl TryFrom<SubscribeRequestFilterSlots> for ConfigFilterSlots {
    type Error = ConfigFilterError;

    fn try_from(value: SubscribeRequestFilterSlots) -> Result<Self, Self::Error> {
        Ok(Self {
            filter_by_commitment: value.filter_by_commitment,
        })
    }
}

impl From<ConfigFilterSlots> for SubscribeRequestFilterSlots {
    fn from(value: ConfigFilterSlots) -> Self {
        Self {
            filter_by_commitment: value.filter_by_commitment,
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigFilterAccounts {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub account: Vec<Pubkey>,
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub owner: Vec<Pubkey>,
    pub filters: Vec<ConfigFilterAccountsFilter>,
    pub nonempty_txn_signature: Option<bool>,
}

impl TryFrom<SubscribeRequestFilterAccounts> for ConfigFilterAccounts {
    type Error = ConfigFilterError;

    fn try_from(value: SubscribeRequestFilterAccounts) -> Result<Self, Self::Error> {
        let account = ConfigFilter::parse_vec_pubkeys(value.account)?;
        let owner = ConfigFilter::parse_vec_pubkeys(value.owner)?;

        if value.filters.len() > MAX_FILTERS {
            return Err(ConfigLimitsError::TooMuchFilters { max: MAX_FILTERS }.into());
        }

        let filters = value
            .filters
            .into_iter()
            .map(|filter| filter.try_into())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            account,
            owner,
            filters,
            nonempty_txn_signature: value.nonempty_txn_signature,
        })
    }
}

impl From<ConfigFilterAccounts> for SubscribeRequestFilterAccounts {
    fn from(value: ConfigFilterAccounts) -> Self {
        Self {
            account: ConfigFilter::conv_vec_pubkeys(value.account),
            owner: ConfigFilter::conv_vec_pubkeys(value.owner),
            filters: value.filters.into_iter().map(Into::into).collect(),
            nonempty_txn_signature: value.nonempty_txn_signature,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub enum ConfigFilterAccountsFilter {
    Memcmp { offset: usize, data: Vec<u8> },
    DataSize(u64),
    TokenAccountState,
    Lamports(ConfigFilterAccountsFilterLamports),
}

impl<'de> Deserialize<'de> for ConfigFilterAccountsFilter {
    fn deserialize<D>(deserializer: D) -> Result<ConfigFilterAccountsFilter, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Debug, Deserialize)]
        enum ConfigMemcmpData {
            Str(String),
            Bytes(Vec<u8>),
        }

        #[derive(Debug, Deserialize)]
        enum Config {
            Memcmp {
                offset: usize,
                data: ConfigMemcmpData,
            },
            DataSize(u64),
            TokenAccountState,
            Lamports(ConfigFilterAccountsFilterLamports),
        }

        Ok(match Config::deserialize(deserializer)? {
            Config::Memcmp { offset, data } => {
                let data = match data {
                    ConfigMemcmpData::Str(data) => match &data[0..7] {
                        "base64" => base64_engine.decode(data).map_err(de::Error::custom)?,
                        "base58" => bs58::decode(data).into_vec().map_err(de::Error::custom)?,
                        _ => data.as_bytes().to_vec(),
                    },
                    ConfigMemcmpData::Bytes(data) => data,
                };
                Self::Memcmp { offset, data }
            }
            Config::DataSize(size) => Self::DataSize(size),
            Config::TokenAccountState => Self::TokenAccountState,
            Config::Lamports(value) => Self::Lamports(value),
        })
    }
}

impl TryFrom<SubscribeRequestFilterAccountsFilter> for ConfigFilterAccountsFilter {
    type Error = ConfigFilterError;

    fn try_from(value: SubscribeRequestFilterAccountsFilter) -> Result<Self, Self::Error> {
        let value = value
            .filter
            .ok_or(ConfigFilterError::FieldNotDefined("filter"))?;

        Ok(match value {
            AccountsFilterDataOneof::Memcmp(memcmp) => {
                let value = memcmp
                    .data
                    .ok_or(ConfigFilterError::FieldNotDefined("data"))?;
                let data = match &value {
                    AccountsFilterMemcmpOneof::Bytes(data) => data.clone(),
                    AccountsFilterMemcmpOneof::Base58(data) => {
                        if data.len() > MAX_DATA_BASE58_SIZE {
                            return Err(ConfigLimitsError::FilterDataOverflow.into());
                        }
                        bs58::decode(data)
                            .into_vec()
                            .map_err(|_| ConfigFilterError::InvalidBase58)?
                    }
                    AccountsFilterMemcmpOneof::Base64(data) => {
                        if data.len() > MAX_DATA_BASE64_SIZE {
                            return Err(ConfigLimitsError::FilterDataOverflow.into());
                        }
                        base64_engine
                            .decode(data)
                            .map_err(|_| ConfigFilterError::InvalidBase64)?
                    }
                };
                if data.len() > MAX_DATA_SIZE {
                    return Err(ConfigLimitsError::FilterDataOverflow.into());
                }

                Self::Memcmp {
                    offset: memcmp.offset as usize,
                    data,
                }
            }
            AccountsFilterDataOneof::Datasize(size) => Self::DataSize(size),
            AccountsFilterDataOneof::TokenAccountState(value) => {
                if !value {
                    return Err(ConfigFilterError::TokenAccountStateInvalid);
                }
                Self::TokenAccountState
            }
            AccountsFilterDataOneof::Lamports(lamports) => Self::Lamports(lamports.try_into()?),
        })
    }
}

impl From<ConfigFilterAccountsFilter> for SubscribeRequestFilterAccountsFilter {
    fn from(value: ConfigFilterAccountsFilter) -> Self {
        Self {
            filter: Some(match value {
                ConfigFilterAccountsFilter::Memcmp { offset, data } => {
                    AccountsFilterDataOneof::Memcmp(SubscribeRequestFilterAccountsFilterMemcmp {
                        offset: offset as u64,
                        data: Some(AccountsFilterMemcmpOneof::Bytes(data)),
                    })
                }
                ConfigFilterAccountsFilter::DataSize(size) => {
                    AccountsFilterDataOneof::Datasize(size)
                }
                ConfigFilterAccountsFilter::TokenAccountState => {
                    AccountsFilterDataOneof::TokenAccountState(true)
                }
                ConfigFilterAccountsFilter::Lamports(value) => {
                    AccountsFilterDataOneof::Lamports(value.into())
                }
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub enum ConfigFilterAccountsFilterLamports {
    Eq(u64),
    Ne(u64),
    Lt(u64),
    Gt(u64),
}

impl TryFrom<SubscribeRequestFilterAccountsFilterLamports> for ConfigFilterAccountsFilterLamports {
    type Error = ConfigFilterError;

    fn try_from(value: SubscribeRequestFilterAccountsFilterLamports) -> Result<Self, Self::Error> {
        let value = value.cmp.ok_or(ConfigFilterError::FieldNotDefined("cmp"))?;

        Ok(match value {
            AccountsFilterLamports::Eq(value) => Self::Eq(value),
            AccountsFilterLamports::Ne(value) => Self::Ne(value),
            AccountsFilterLamports::Lt(value) => Self::Lt(value),
            AccountsFilterLamports::Gt(value) => Self::Gt(value),
        })
    }
}

impl From<ConfigFilterAccountsFilterLamports> for SubscribeRequestFilterAccountsFilterLamports {
    fn from(value: ConfigFilterAccountsFilterLamports) -> Self {
        Self {
            cmp: Some(match value {
                ConfigFilterAccountsFilterLamports::Eq(value) => AccountsFilterLamports::Eq(value),
                ConfigFilterAccountsFilterLamports::Ne(value) => AccountsFilterLamports::Ne(value),
                ConfigFilterAccountsFilterLamports::Lt(value) => AccountsFilterLamports::Lt(value),
                ConfigFilterAccountsFilterLamports::Gt(value) => AccountsFilterLamports::Gt(value),
            }),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigFilterAccountsDataSlice {
    pub offset: u64,
    pub length: u64,
}

impl TryFrom<SubscribeRequestAccountsDataSlice> for ConfigFilterAccountsDataSlice {
    type Error = ConfigFilterError;

    fn try_from(value: SubscribeRequestAccountsDataSlice) -> Result<Self, Self::Error> {
        Ok(Self {
            offset: value.offset,
            length: value.length,
        })
    }
}

impl From<ConfigFilterAccountsDataSlice> for SubscribeRequestAccountsDataSlice {
    fn from(value: ConfigFilterAccountsDataSlice) -> Self {
        Self {
            offset: value.offset,
            length: value.length,
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigFilterTransactions {
    pub vote: Option<bool>,
    pub failed: Option<bool>,
    #[serde(deserialize_with = "deserialize_maybe_signature")]
    pub signature: Option<Signature>,
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub account_include: Vec<Pubkey>,
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub account_exclude: Vec<Pubkey>,
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub account_required: Vec<Pubkey>,
}

impl TryFrom<SubscribeRequestFilterTransactions> for ConfigFilterTransactions {
    type Error = ConfigFilterError;

    fn try_from(value: SubscribeRequestFilterTransactions) -> Result<Self, Self::Error> {
        Ok(Self {
            vote: value.vote,
            failed: value.failed,
            signature: value
                .signature
                .map(|sig| {
                    sig.parse()
                        .map_err(|error| ConfigFilterError::Signature(sig, error))
                })
                .transpose()?,
            account_include: ConfigFilter::parse_vec_pubkeys(value.account_include)?,
            account_exclude: ConfigFilter::parse_vec_pubkeys(value.account_exclude)?,
            account_required: ConfigFilter::parse_vec_pubkeys(value.account_required)?,
        })
    }
}

impl From<ConfigFilterTransactions> for SubscribeRequestFilterTransactions {
    fn from(value: ConfigFilterTransactions) -> Self {
        Self {
            vote: value.vote,
            failed: value.failed,
            signature: value.signature.map(|sig| sig.to_string()),
            account_include: ConfigFilter::conv_vec_pubkeys(value.account_include),
            account_exclude: ConfigFilter::conv_vec_pubkeys(value.account_exclude),
            account_required: ConfigFilter::conv_vec_pubkeys(value.account_required),
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigFilterBlocks {
    #[serde(deserialize_with = "deserialize_pubkey_vec")]
    pub account_include: Vec<Pubkey>,
    pub include_transactions: Option<bool>,
    pub include_accounts: Option<bool>,
    pub include_entries: Option<bool>,
}

impl TryFrom<SubscribeRequestFilterBlocks> for ConfigFilterBlocks {
    type Error = ConfigFilterError;

    fn try_from(value: SubscribeRequestFilterBlocks) -> Result<Self, Self::Error> {
        Ok(Self {
            account_include: ConfigFilter::parse_vec_pubkeys(value.account_include)?,
            include_transactions: value.include_transactions,
            include_accounts: value.include_accounts,
            include_entries: value.include_entries,
        })
    }
}

impl From<ConfigFilterBlocks> for SubscribeRequestFilterBlocks {
    fn from(value: ConfigFilterBlocks) -> Self {
        Self {
            account_include: ConfigFilter::conv_vec_pubkeys(value.account_include),
            include_transactions: value.include_transactions,
            include_accounts: value.include_accounts,
            include_entries: value.include_entries,
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum ConfigFilterCommitment {
    #[default]
    Processed,
    Confirmed,
    Finalized,
}

impl TryFrom<i32> for ConfigFilterCommitment {
    type Error = ConfigFilterError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        CommitmentLevelProto::try_from(value)
            .map_err(|_error| ConfigFilterError::UnknownCommitment(value))?
            .try_into()
    }
}

impl TryFrom<CommitmentLevelProto> for ConfigFilterCommitment {
    type Error = ConfigFilterError;

    fn try_from(value: CommitmentLevelProto) -> Result<Self, Self::Error> {
        match value {
            CommitmentLevelProto::Processed => Ok(Self::Processed),
            CommitmentLevelProto::Confirmed => Ok(Self::Confirmed),
            CommitmentLevelProto::Finalized => Ok(Self::Finalized),
            value => Err(ConfigFilterError::InvalidCommitment(value)),
        }
    }
}

impl From<ConfigFilterCommitment> for i32 {
    fn from(value: ConfigFilterCommitment) -> Self {
        (match value {
            ConfigFilterCommitment::Processed => CommitmentLevelProto::Processed,
            ConfigFilterCommitment::Confirmed => CommitmentLevelProto::Confirmed,
            ConfigFilterCommitment::Finalized => CommitmentLevelProto::Finalized,
        }) as i32
    }
}

impl From<ConfigFilterCommitment> for CommitmentLevel {
    fn from(value: ConfigFilterCommitment) -> Self {
        match value {
            ConfigFilterCommitment::Processed => Self::Processed,
            ConfigFilterCommitment::Confirmed => Self::Confirmed,
            ConfigFilterCommitment::Finalized => Self::Finalized,
        }
    }
}

use {
    crate::config::{deserialize_pubkey_set, ConfigAppsWorkers},
    richat_shared::{
        config::deserialize_num_str, transports::grpc::ConfigGrpcServer as ConfigAppGrpcServer,
    },
    serde::{de::Deserializer, Deserialize},
    solana_sdk::pubkey::Pubkey,
    std::{collections::HashSet, time::Duration},
    thiserror::Error,
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpc {
    pub server: ConfigAppGrpcServer,
    pub workers: ConfigAppsWorkers,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub stream_channel_capacity: usize,
    pub unary: ConfigAppsGrpcUnary,
    pub filters: ConfigAppsFilters,
    #[serde(deserialize_with = "ConfigAppsGrpc::deserialize_x_token")]
    pub x_token: HashSet<Vec<u8>>,
}

impl Default for ConfigAppsGrpc {
    fn default() -> Self {
        Self {
            server: ConfigAppGrpcServer::default(),
            workers: ConfigAppsWorkers::default(),
            stream_channel_capacity: 100_000,
            unary: ConfigAppsGrpcUnary::default(),
            filters: ConfigAppsFilters::default(),
            x_token: HashSet::default(),
        }
    }
}

impl ConfigAppsGrpc {
    pub fn deserialize_x_token<'de, D>(deserializer: D) -> Result<HashSet<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        Vec::<&str>::deserialize(deserializer)
            .map(|vec| vec.into_iter().map(|s| s.as_bytes().to_vec()).collect())
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpcUnary {
    pub enabled: bool,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub requests_queue_size: usize,
}

impl Default for ConfigAppsGrpcUnary {
    fn default() -> Self {
        Self {
            enabled: true,
            requests_queue_size: 100,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsFilters {
    /// Filter names config
    pub names: ConfigAppsFiltersNames,
    /// Limits for possible filters
    pub limits: ConfigAppsFiltersLimits,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsFiltersNames {
    /// Filter name size limit
    pub size_limit: usize,
    /// Number of cached filter names before doing cleanup
    pub set_size_limit: usize,
    /// Cleanup interval once filter names reached `set_size_limit`
    #[serde(with = "humantime_serde")]
    pub set_cleanup_interval: Duration,
}

impl Default for ConfigAppsFiltersNames {
    fn default() -> Self {
        Self {
            size_limit: 128,
            set_size_limit: 8_192,
            set_cleanup_interval: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Error)]
pub enum FilterLimitsCheckError {
    #[error("Max amount of filters/data_slices reached, only {max} allowed")]
    Max { max: usize },
    #[error("Subscribe on full stream with `any` is not allowed, at least one filter required")]
    Any,
    #[error("Max amount of Pubkeys reached, only {max} allowed")]
    MaxPubkey { max: usize },
    #[error("Pubkey {pubkey} in filters is not allowed")]
    PubkeyReject { pubkey: Pubkey },
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsFiltersLimits {
    pub accounts: ConfigAppsFiltersLimitsAccounts,
    pub slots: ConfigAppsFiltersLimitsSlots,
    pub transactions: ConfigAppsFiltersLimitsTransactions,
    pub transactions_status: ConfigAppsFiltersLimitsTransactions,
    pub entries: ConfigAppsFiltersLimitsEntries,
    pub blocks_meta: ConfigAppsFiltersLimitsBlocksMeta,
    pub blocks: ConfigAppsFiltersLimitsBlocks,
}

impl ConfigAppsFiltersLimits {
    pub const fn check_max(len: usize, max: usize) -> Result<(), FilterLimitsCheckError> {
        if len <= max {
            Ok(())
        } else {
            Err(FilterLimitsCheckError::Max { max })
        }
    }

    pub const fn check_any(is_empty: bool, any: bool) -> Result<(), FilterLimitsCheckError> {
        if !is_empty || any {
            Ok(())
        } else {
            Err(FilterLimitsCheckError::Any)
        }
    }

    pub const fn check_pubkey_max(len: usize, max: usize) -> Result<(), FilterLimitsCheckError> {
        if len <= max {
            Ok(())
        } else {
            Err(FilterLimitsCheckError::MaxPubkey { max })
        }
    }

    pub fn check_pubkey_reject(
        pubkey: &Pubkey,
        set: &HashSet<Pubkey>,
    ) -> Result<(), FilterLimitsCheckError> {
        if !set.contains(pubkey) {
            Ok(())
        } else {
            Err(FilterLimitsCheckError::PubkeyReject { pubkey: *pubkey })
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsFiltersLimitsAccounts {
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

impl Default for ConfigAppsFiltersLimitsAccounts {
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

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigAppsFiltersLimitsSlots {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max: usize,
}

impl Default for ConfigAppsFiltersLimitsSlots {
    fn default() -> Self {
        Self { max: usize::MAX }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigAppsFiltersLimitsTransactions {
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

impl Default for ConfigAppsFiltersLimitsTransactions {
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

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigAppsFiltersLimitsEntries {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max: usize,
}

impl Default for ConfigAppsFiltersLimitsEntries {
    fn default() -> Self {
        Self { max: usize::MAX }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigAppsFiltersLimitsBlocksMeta {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max: usize,
}

impl Default for ConfigAppsFiltersLimitsBlocksMeta {
    fn default() -> Self {
        Self { max: usize::MAX }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConfigAppsFiltersLimitsBlocks {
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

impl Default for ConfigAppsFiltersLimitsBlocks {
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

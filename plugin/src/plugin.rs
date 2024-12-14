use {
    crate::{
        channel::Sender, config::Config, metrics::PrometheusService, protobuf::ProtobufMessage,
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult,
        SlotStatus,
    },
    log::error,
    solana_sdk::clock::Slot,
    std::{
        sync::atomic::{AtomicU64, Ordering},
        time::Duration,
    },
    tokio::runtime::{Builder, Runtime},
};

#[derive(Debug)]
pub struct PluginInner {
    runtime: Runtime,
    messages: Sender,
    prometheus: PrometheusService,
}

impl PluginInner {
    fn new(config: Config) -> PluginResult<Self> {
        // Create Tokio runtime
        let mut builder = Builder::new_multi_thread();
        if let Some(worker_threads) = config.tokio.worker_threads {
            builder.worker_threads(worker_threads);
        }
        if let Some(cpus) = config.tokio.affinity.clone() {
            builder.on_thread_start(move || {
                affinity::set_thread_affinity(&cpus).expect("failed to set affinity")
            });
        }
        let runtime = builder
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicU64 = AtomicU64::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
                format!("richatPlugin{id:02}")
            })
            .enable_all()
            .build()
            .map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;

        // Create messages store
        let messages = Sender::new(config.channel);

        // Start prometheus server
        let prometheus = runtime.block_on(async move {
            let prometheus = PrometheusService::new(config.prometheus)
                .await
                .map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;
            Ok::<_, GeyserPluginError>(prometheus)
        })?;

        Ok(Self {
            runtime,
            messages,
            prometheus,
        })
    }
}

#[derive(Debug, Default)]
pub struct Plugin {
    inner: Option<PluginInner>,
}

impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"))
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        solana_logger::setup_with_default("info");
        let config = Config::load_from_file(config_file).inspect_err(|error| {
            error!("failed to load config: {error:?}");
        })?;

        // Setup logger from the config
        solana_logger::setup_with_default(&config.log.level);

        // Create inner
        self.inner = Some(PluginInner::new(config).inspect_err(|error| {
            error!("failed to load plugin from the config: {error:?}");
        })?);

        Ok(())
    }

    fn on_unload(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.prometheus.shutdown();
            inner.runtime.shutdown_timeout(Duration::from_secs(30));
        }
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        if !is_startup {
            let account = match account {
                ReplicaAccountInfoVersions::V0_0_1(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
                }
                ReplicaAccountInfoVersions::V0_0_2(_info) => {
                    unreachable!("ReplicaAccountInfoVersions::V0_0_2 is not supported")
                }
                ReplicaAccountInfoVersions::V0_0_3(info) => info,
            };

            let inner = self.inner.as_ref().expect("initialized");
            inner
                .messages
                .push(slot, ProtobufMessage::Account { slot, account });
        }

        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: Slot,
        parent: Option<u64>,
        status: &SlotStatus,
    ) -> PluginResult<()> {
        let inner = self.inner.as_ref().expect("initialized");
        inner.messages.push(
            slot,
            ProtobufMessage::Slot {
                slot,
                parent,
                status,
            },
        );

        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions<'_>,
        slot: u64,
    ) -> PluginResult<()> {
        let transaction = match transaction {
            ReplicaTransactionInfoVersions::V0_0_1(_info) => {
                unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
            }
            ReplicaTransactionInfoVersions::V0_0_2(info) => info,
        };

        let inner = self.inner.as_ref().expect("initialized");
        inner
            .messages
            .push(slot, ProtobufMessage::Transaction { slot, transaction });

        Ok(())
    }

    fn notify_entry(&self, entry: ReplicaEntryInfoVersions) -> PluginResult<()> {
        #[allow(clippy::infallible_destructuring_match)]
        let entry = match entry {
            ReplicaEntryInfoVersions::V0_0_1(_entry) => {
                unreachable!("ReplicaEntryInfoVersions::V0_0_1 is not supported")
            }
            ReplicaEntryInfoVersions::V0_0_2(entry) => entry,
        };

        let inner = self.inner.as_ref().expect("initialized");
        inner
            .messages
            .push(entry.slot, ProtobufMessage::Entry { entry });

        Ok(())
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions<'_>) -> PluginResult<()> {
        let blockinfo = match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(_info) => {
                unreachable!("ReplicaBlockInfoVersions::V0_0_1 is not supported")
            }
            ReplicaBlockInfoVersions::V0_0_2(_info) => {
                unreachable!("ReplicaBlockInfoVersions::V0_0_2 is not supported")
            }
            ReplicaBlockInfoVersions::V0_0_3(_info) => {
                unreachable!("ReplicaBlockInfoVersions::V0_0_3 is not supported")
            }
            ReplicaBlockInfoVersions::V0_0_4(info) => info,
        };

        let inner = self.inner.as_ref().expect("initialized");
        inner
            .messages
            .push(blockinfo.slot, ProtobufMessage::BlockMeta { blockinfo });

        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        true
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = Plugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}

use {
    crate::{
        channel::Sender,
        config::Config,
        metrics,
        protobuf::{ProtobufEncoder, ProtobufMessage},
        version::VERSION,
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
        ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult,
        SlotStatus,
    },
    futures::future::BoxFuture,
    log::error,
    richat_shared::{
        shutdown::Shutdown,
        transports::{grpc::GrpcServer, quic::QuicServer, tcp::TcpServer},
    },
    solana_sdk::clock::Slot,
    std::{fmt, time::Duration},
    tokio::{runtime::Runtime, task::JoinError},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginNotification {
    Slot,
    Account,
    Transaction,
    Entry,
    BlockMeta,
}

struct PluginTask(BoxFuture<'static, Result<(), JoinError>>);

unsafe impl Sync for PluginTask {}

impl fmt::Debug for PluginTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginTask").finish()
    }
}

#[derive(Debug)]
pub struct PluginInner {
    runtime: Runtime,
    messages: Sender,
    encoder: ProtobufEncoder,
    shutdown: Shutdown,
    tasks: Vec<(&'static str, PluginTask)>,
}

impl PluginInner {
    fn new(config: Config) -> PluginResult<Self> {
        // Create Tokio runtime
        let runtime = config
            .tokio
            .build_runtime("richatPlugin")
            .map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;

        // Create messages store
        let messages = Sender::new(config.channel);

        // Spawn servers
        let (messages, shutdown, tasks) = runtime
            .block_on(async move {
                let shutdown = Shutdown::new();
                let mut tasks = Vec::with_capacity(4);

                use metrics::{connections_total_add, connections_total_dec, ConnectionsTransport};

                // Start Quic
                if let Some(config) = config.quic {
                    tasks.push((
                        "Quic Server",
                        PluginTask(Box::pin(
                            QuicServer::spawn(
                                config,
                                messages.clone(),
                                || connections_total_add(ConnectionsTransport::Quic), // on_conn_new_cb
                                || connections_total_dec(ConnectionsTransport::Quic), // on_conn_drop_cb
                                shutdown.clone(),
                            )
                            .await?,
                        )),
                    ));
                }

                // Start Tcp
                if let Some(config) = config.tcp {
                    tasks.push((
                        "Tcp Server",
                        PluginTask(Box::pin(
                            TcpServer::spawn(
                                config,
                                messages.clone(),
                                || connections_total_add(ConnectionsTransport::Tcp), // on_conn_new_cb
                                || connections_total_dec(ConnectionsTransport::Tcp), // on_conn_drop_cb
                                shutdown.clone(),
                            )
                            .await?,
                        )),
                    ));
                }

                // Start gRPC
                if let Some(config) = config.grpc {
                    tasks.push((
                        "gRPC Server",
                        PluginTask(Box::pin(
                            GrpcServer::spawn(
                                config,
                                messages.clone(),
                                || connections_total_add(ConnectionsTransport::Grpc), // on_conn_new_cb
                                || connections_total_dec(ConnectionsTransport::Grpc), // on_conn_drop_cb
                                VERSION,
                                shutdown.clone(),
                            )
                            .await?,
                        )),
                    ));
                }

                // Start prometheus server
                if let Some(config) = config.prometheus {
                    tasks.push((
                        "Prometheus Server",
                        PluginTask(Box::pin(
                            metrics::spawn_server(config, shutdown.clone()).await?,
                        )),
                    ));
                }

                Ok::<_, anyhow::Error>((messages, shutdown, tasks))
            })
            .map_err(|error| GeyserPluginError::Custom(format!("{error:?}").into()))?;

        Ok(Self {
            runtime,
            messages,
            encoder: config.channel.encoder,
            shutdown,
            tasks,
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
            inner.messages.close();

            inner.shutdown.shutdown();
            inner.runtime.block_on(async {
                for (name, task) in inner.tasks {
                    if let Err(error) = task.0.await {
                        error!("failed to join `{name}` task: {error:?}");
                    }
                }
            });

            inner.runtime.shutdown_timeout(Duration::from_secs(10));
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
                .push(ProtobufMessage::Account { slot, account }, inner.encoder);
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
            ProtobufMessage::Slot {
                slot,
                parent,
                status,
            },
            inner.encoder,
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
        inner.messages.push(
            ProtobufMessage::Transaction { slot, transaction },
            inner.encoder,
        );

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
            .push(ProtobufMessage::Entry { entry }, inner.encoder);

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
            .push(ProtobufMessage::BlockMeta { blockinfo }, inner.encoder);

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

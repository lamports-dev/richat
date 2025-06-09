use {
    crate::{
        channel::{Messages, ParsedMessage},
        metrics,
        pubsub::{
            notification::{
                RpcBlockUpdate, RpcNotification, RpcNotifications, RpcTransactionUpdate,
            },
            solana::{SubscribeConfig, SubscribeConfigHashId, SubscribeMethod},
            ClientId, SubscriptionId,
        },
    },
    ::metrics::gauge,
    foldhash::quality::RandomState,
    prost_types::Timestamp,
    rayon::{
        iter::{IntoParallelIterator, ParallelIterator},
        ThreadPoolBuilder,
    },
    richat_filter::message::{MessageSlot, MessageTransaction},
    richat_proto::{convert_from, geyser::SlotStatus},
    richat_shared::five8::{pubkey_encode, signature_encode},
    solana_account_decoder::encode_ui_account,
    solana_rpc_client_api::response::{
        ProcessedSignatureResult, RpcKeyedAccount, RpcLogsResponse, RpcSignatureResult, SlotInfo,
        SlotTransactionStats, SlotUpdate,
    },
    solana_sdk::{
        clock::Slot, commitment_config::CommitmentLevel, signature::Signature,
        transaction::TransactionError,
    },
    std::{
        collections::{hash_map::Entry as HashMapEntry, BTreeMap, HashMap, HashSet},
        sync::Arc,
        thread,
        time::Duration,
    },
    tokio::sync::{mpsc, oneshot},
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ClientRequest {
    Subscribe {
        client_id: ClientId,
        config: SubscribeConfig,
        tx: oneshot::Sender<(SubscriptionId, SubscribeMethod)>,
    },
    Unsubscribe {
        client_id: ClientId,
        subscription_id: SubscriptionId,
        tx: oneshot::Sender<bool>,
    },
    Remove {
        client_id: ClientId,
    },
}

#[derive(Debug)]
struct SubscriptionInfo {
    id: SubscriptionId,
    config_hash: SubscribeConfigHashId,
    config: SubscribeConfig,
    clients: HashSet<ClientId>,
}

#[derive(Debug, Default)]
struct Subscriptions {
    subscription_id: SubscriptionId,
    subscriptions: HashMap<
        SubscribeConfigHashId,
        (CommitmentLevel, SubscribeMethod, SubscriptionId),
        RandomState,
    >,
    subscriptions_per_client: HashMap<
        ClientId,
        HashMap<SubscriptionId, (CommitmentLevel, SubscribeMethod), RandomState>,
        RandomState,
    >,
    subscriptions_per_method: HashMap<
        (CommitmentLevel, SubscribeMethod),
        HashMap<SubscriptionId, SubscriptionInfo, RandomState>,
        RandomState,
    >,
}

impl Subscriptions {
    fn get_subscriptions(
        &self,
        commitment: CommitmentLevel,
        method: SubscribeMethod,
    ) -> Option<impl Iterator<Item = &SubscriptionInfo>> {
        self.subscriptions_per_method
            .get(&(commitment, method))
            .map(|map| map.values())
    }

    fn update(
        &mut self,
        request: ClientRequest,
        signatures: &mut CachedSignatures,
        notifications: &mut RpcNotifications,
    ) {
        match request {
            ClientRequest::Subscribe {
                client_id,
                config,
                tx,
            } => {
                let method = config.method();
                let subscription_id = self.subscribe(client_id, config, signatures, notifications);
                let _ = tx.send((subscription_id, method));
            }
            ClientRequest::Unsubscribe {
                client_id,
                subscription_id,
                tx,
            } => {
                let removed = self.unsubscribe(client_id, subscription_id);
                let _ = tx.send(removed);
            }
            ClientRequest::Remove { client_id } => self.remove_client(client_id),
        }
    }

    fn subscribe(
        &mut self,
        client_id: ClientId,
        config: SubscribeConfig,
        signatures: &mut CachedSignatures,
        notifications: &mut RpcNotifications,
    ) -> SubscriptionId {
        let config_hash = config.get_hash_id();
        match self.subscriptions.entry(config_hash) {
            HashMapEntry::Occupied(entry) => {
                let (commitment, method, subscription_id) = entry.get();

                self.subscriptions_per_client
                    .entry(client_id)
                    .or_default()
                    .insert(*subscription_id, (*commitment, *method));

                self.subscriptions_per_method
                    .get_mut(&(*commitment, *method))
                    .expect("subscriptions storage inconsistent, subscribe #1")
                    .get_mut(subscription_id)
                    .expect("subscriptions storage inconsistent, subscribe #2")
                    .clients
                    .insert(client_id);

                *subscription_id
            }
            HashMapEntry::Vacant(entry) => {
                let subscription_id = self.subscription_id;
                self.subscription_id += 1;

                let mut is_final = false;
                if let SubscribeConfig::Signature {
                    signature,
                    commitment,
                } = &config
                {
                    if let Some((slot, err)) = signatures.get(signature, commitment.commitment) {
                        is_final = true;
                        let json = RpcNotification::serialize_with_context(
                            "signatureNotification",
                            subscription_id,
                            slot,
                            RpcSignatureResult::ProcessedSignature(ProcessedSignatureResult {
                                err,
                            }),
                        );
                        notifications.push(
                            subscription_id,
                            SubscribeMethod::Signature,
                            is_final,
                            json,
                        );
                    }
                }

                if !is_final {
                    let commitment = config.commitment();
                    let method = config.method();
                    entry.insert((commitment, method, subscription_id));

                    // add subscription info for client
                    self.subscriptions_per_client
                        .entry(client_id)
                        .or_default()
                        .insert(subscription_id, (commitment, method));

                    // create subscription info
                    self.subscriptions_per_method
                        .entry((commitment, method))
                        .or_default()
                        .insert(
                            subscription_id,
                            SubscriptionInfo {
                                id: subscription_id,
                                config_hash,
                                config,
                                clients: HashSet::new(),
                            },
                        );
                }

                subscription_id
            }
        }
    }

    fn unsubscribe(&mut self, client_id: ClientId, subscription_id: SubscriptionId) -> bool {
        if let Some((commitment, method)) = self
            .subscriptions_per_client
            .get_mut(&client_id)
            .and_then(|map| map.remove(&subscription_id))
        {
            self.remove_client_subscription(commitment, method, subscription_id, client_id);
            true
        } else {
            false
        }
    }

    fn remove_client(&mut self, client_id: ClientId) {
        if let Some(map) = self.subscriptions_per_client.remove(&client_id) {
            for (subscription_id, (commitment, method)) in map.into_iter() {
                self.remove_client_subscription(commitment, method, subscription_id, client_id);
            }
        }
    }

    fn remove_client_subscription(
        &mut self,
        commitment: CommitmentLevel,
        method: SubscribeMethod,
        subscription_id: SubscriptionId,
        client_id: ClientId,
    ) {
        // get subscription info and remove client id
        let subscriotions = self
            .subscriptions_per_method
            .get_mut(&(commitment, method))
            .expect("subscriptions storage inconsistent, remove client subscription #1");
        let subscriotion_info = subscriotions
            .get_mut(&subscription_id)
            .expect("subscriptions storage inconsistent, remove client subscription #2");
        assert!(
            subscriotion_info.clients.remove(&client_id),
            "subscriptions storage inconsistent, remove client subscription #3"
        );

        // drop subscription if no clients left
        if subscriotion_info.clients.is_empty() {
            assert!(
                self.subscriptions
                    .remove(&subscriotion_info.config_hash)
                    .is_some(),
                "subscriptions storage inconsistent, remove client subscription #4"
            );
            subscriotions.remove(&subscription_id);
        }
    }

    fn remove_subscription(&mut self, config_hash: SubscribeConfigHashId) {
        let (commitment, method, subscription_id) = self
            .subscriptions
            .remove(&config_hash)
            .expect("subscriptions storage inconsistent, remove subscription #1");
        let subscriotion = self
            .subscriptions_per_method
            .get_mut(&(commitment, method))
            .expect("subscriptions storage inconsistent, remove subscription #2")
            .remove(&subscription_id)
            .expect("subscriptions storage inconsistent, remove subscription #3");
        for client_id in subscriotion.clients {
            self.subscriptions_per_client
                .get_mut(&client_id)
                .expect("subscriptions storage inconsistent, remove subscription #4")
                .remove(&subscription_id)
                .expect("subscriptions storage inconsistent, remove subscription #5");
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn subscriptions_worker(
    messages: Messages,
    mut clients_rx: mpsc::Receiver<ClientRequest>,
    workers_count: usize,
    workers_affinity: Option<Vec<usize>>,
    max_clients_request_per_tick: usize,
    max_messages_per_commitment_per_tick: usize,
    mut notifications: RpcNotifications,
    signatures_cache_max: usize,
    signatures_cache_slots_max: usize,
) -> anyhow::Result<()> {
    // Subscriptions storage
    let mut subscriptions = Subscriptions::default();

    // Messages head
    let receiver = messages.to_receiver();
    let mut head_processed = messages.get_current_tail(CommitmentLevel::Processed);
    let mut head_confirmed = messages.get_current_tail(CommitmentLevel::Processed);
    let mut head_finalized = messages.get_current_tail(CommitmentLevel::Processed);

    // Subscriptions filters pool
    let workers = ThreadPoolBuilder::new()
        .num_threads(workers_count)
        .spawn_handler(move |thread| {
            let workers_affinity = workers_affinity.clone();
            thread::Builder::new()
                .name(format!("richatPSubWrk{:02}", thread.index()))
                .spawn(move || {
                    if let Some(cpus) = workers_affinity {
                        affinity_linux::set_thread_affinity(cpus.into_iter())
                            .expect("failed to set affinity");
                    }
                    thread.run()
                })?;
            Ok(())
        })
        .build()?;

    // Signatures cache
    let mut signatures = CachedSignatures::new(signatures_cache_max, signatures_cache_slots_max);

    // main loop
    let mut slot_finalized = 0;
    let mut slots_stats = BTreeMap::<Slot, SlotTransactionStatsItem>::new();
    let mut messages_extra = vec![];
    loop {
        messages_extra.clear();

        // Update subscriptions from clients
        for _ in 0..max_clients_request_per_tick {
            match clients_rx.try_recv() {
                Ok(request) => subscriptions.update(request, &mut signatures, &mut notifications),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => return Ok(()), // means shutdown
            };
        }

        // Collect messages from channels
        let mut jobs = Vec::with_capacity(max_messages_per_commitment_per_tick * 3 * 3);
        let mut messages_processed = Vec::with_capacity(max_messages_per_commitment_per_tick);
        let mut messages_confirmed = Vec::with_capacity(max_messages_per_commitment_per_tick);
        let mut messages_finalized = Vec::with_capacity(max_messages_per_commitment_per_tick);
        for (commitment, head, messages) in [
            (
                CommitmentLevel::Processed,
                &mut head_processed,
                &mut messages_processed,
            ),
            (
                CommitmentLevel::Confirmed,
                &mut head_confirmed,
                &mut messages_confirmed,
            ),
            (
                CommitmentLevel::Finalized,
                &mut head_finalized,
                &mut messages_finalized,
            ),
        ] {
            while messages.len() < max_messages_per_commitment_per_tick {
                if let Some(message) = receiver.try_recv(commitment, *head)? {
                    *head += 1;
                    // ignore Slot messages for any commitment except processed
                    if commitment != CommitmentLevel::Processed {
                        if let ParsedMessage::Slot(_) = &message {
                            continue;
                        }
                    }
                    messages.push(message);
                } else {
                    break;
                }
            }
            for message in messages.iter() {
                if commitment == CommitmentLevel::Processed {
                    if let Some((message, stats)) = match &message {
                        ParsedMessage::Slot(message)
                            if message.status() == SlotStatus::SlotProcessed =>
                        {
                            gauge!(metrics::PUBSUB_SLOT, "commitment" => "processed")
                                .set(message.slot() as f64);
                            slots_stats
                                .entry(message.slot())
                                .or_default()
                                .add_created_at(message.slot(), message.created_at().into())
                        }
                        ParsedMessage::Slot(message)
                            if message.status() == SlotStatus::SlotDead =>
                        {
                            signatures.dead_slot(message.slot());
                            None
                        }
                        ParsedMessage::Slot(message)
                            if message.status() == SlotStatus::SlotConfirmed =>
                        {
                            gauge!(metrics::PUBSUB_SLOT, "commitment" => "confirmed")
                                .set(message.slot() as f64);
                            signatures.set_confirmed(message.slot());
                            None
                        }
                        ParsedMessage::Slot(message)
                            if message.status() == SlotStatus::SlotFinalized =>
                        {
                            gauge!(metrics::PUBSUB_SLOT, "commitment" => "finalized")
                                .set(message.slot() as f64);
                            signatures.set_finalized(message.slot());
                            slot_finalized = message.slot();
                            loop {
                                match slots_stats.keys().next().copied() {
                                    Some(slot_min) if slot_min <= slot_finalized => {
                                        slots_stats.remove(&slot_min);
                                    }
                                    _ => break,
                                }
                            }
                            None
                        }
                        ParsedMessage::Transaction(message) => {
                            signatures.add_signature(message);
                            slots_stats
                                .entry(message.slot())
                                .or_default()
                                .add_tx(message.failed())
                        }
                        ParsedMessage::Entry(message) => slots_stats
                            .entry(message.slot())
                            .or_default()
                            .add_entry(message.executed_transaction_count()),
                        ParsedMessage::BlockMeta(message) => {
                            slots_stats.entry(message.slot()).or_default().add_meta(
                                message.executed_transaction_count(),
                                message.entries_count(),
                            )
                        }
                        _ => None,
                    } {
                        messages_extra.push((message, stats));
                    }
                }

                for method in SubscribeMethod::get_message_methods(message) {
                    if let Some(subscriptions) =
                        subscriptions.get_subscriptions(commitment, *method)
                    {
                        for subscription in subscriptions {
                            jobs.push((*method, message, subscription, None));
                        }
                    }
                }
            }
        }
        for (message, stats) in messages_extra.iter() {
            let method = SubscribeMethod::SlotsUpdates;
            if let Some(subscriptions) =
                subscriptions.get_subscriptions(CommitmentLevel::Processed, method)
            {
                for subscription in subscriptions {
                    jobs.push((method, message, subscription, Some(*stats)));
                }
            }
        }

        if jobs.is_empty() {
            thread::sleep(Duration::from_micros(1));
            continue;
        }

        // Filter messages
        let new_notifications = workers.install(|| {
            jobs.into_par_iter()
                .filter_map(|(method, message, subscription, extra_info)| {
                    match (method, message) {
                        (SubscribeMethod::Account, ParsedMessage::Account(message)) => {
                            if let Some((encoding, data_slice)) =
                                subscription.config.filter_account(message)
                            {
                                let json = RpcNotification::serialize_with_context(
                                    "accountNotification",
                                    subscription.id,
                                    message.slot(),
                                    encode_ui_account(
                                        message.pubkey(),
                                        message.as_ref(),
                                        encoding,
                                        None,
                                        data_slice,
                                    ),
                                );
                                return Some((subscription, false, json));
                            }
                        }
                        (SubscribeMethod::Program, ParsedMessage::Account(message)) => {
                            if let Some((encoding, data_slice)) =
                                subscription.config.filter_program(message)
                            {
                                let json = RpcNotification::serialize_with_context(
                                    "programNotification",
                                    subscription.id,
                                    message.slot(),
                                    &RpcKeyedAccount {
                                        pubkey: pubkey_encode(&(*message.pubkey()).to_bytes()), // TODO: use `.as_bytes()` from 2.2
                                        account: encode_ui_account(
                                            message.pubkey(),
                                            message.as_ref(),
                                            encoding,
                                            None,
                                            data_slice,
                                        ),
                                    },
                                );
                                return Some((subscription, false, json));
                            }
                        }
                        (SubscribeMethod::Logs, ParsedMessage::Transaction(message)) => {
                            if let Some((err, logs)) = subscription.config.filter_logs(message) {
                                let json = RpcNotification::serialize_with_context(
                                    "logsNotification",
                                    subscription.id,
                                    message.slot(),
                                    &RpcLogsResponse {
                                        signature: signature_encode(&(*message.signature()).into()),
                                        err,
                                        logs,
                                    },
                                );
                                return Some((subscription, false, json));
                            }
                        }
                        (SubscribeMethod::Signature, ParsedMessage::Transaction(message)) => {
                            if let Some(err) = subscription.config.filter_signature(message) {
                                let json = RpcNotification::serialize_with_context(
                                    "signatureNotification",
                                    subscription.id,
                                    message.slot(),
                                    RpcSignatureResult::ProcessedSignature(
                                        ProcessedSignatureResult { err },
                                    ),
                                );
                                return Some((subscription, true, json));
                            }
                        }
                        (SubscribeMethod::Slot, ParsedMessage::Slot(message)) => {
                            if message.status() == SlotStatus::SlotCreatedBank {
                                let json = RpcNotification::serialize(
                                    "slotNotification",
                                    subscription.id,
                                    SlotInfo {
                                        slot: message.slot(),
                                        parent: message.parent().unwrap_or_default(),
                                        root: slot_finalized,
                                    },
                                );
                                return Some((subscription, false, json));
                            }
                        }
                        (SubscribeMethod::SlotsUpdates, ParsedMessage::Slot(message)) => {
                            let json = RpcNotification::serialize(
                                "slotsUpdatesNotification",
                                subscription.id,
                                match message.status() {
                                    SlotStatus::SlotFirstShredReceived => {
                                        SlotUpdate::FirstShredReceived {
                                            slot: message.slot(),
                                            timestamp: message.created_at().as_millis(),
                                        }
                                    }
                                    SlotStatus::SlotCompleted => SlotUpdate::Completed {
                                        slot: message.slot(),
                                        timestamp: message.created_at().as_millis(),
                                    },
                                    SlotStatus::SlotCreatedBank => SlotUpdate::CreatedBank {
                                        slot: message.slot(),
                                        parent: message.parent().unwrap_or_default(),
                                        timestamp: message.created_at().as_millis(),
                                    },
                                    SlotStatus::SlotProcessed => SlotUpdate::Frozen {
                                        slot: message.slot(),
                                        timestamp: message.created_at().as_millis(),
                                        stats: extra_info?,
                                    },
                                    SlotStatus::SlotDead => SlotUpdate::Dead {
                                        slot: message.slot(),
                                        timestamp: message.created_at().as_millis(),
                                        err: message
                                            .dead_error()
                                            .map(|de| de.to_owned())
                                            .unwrap_or_default(),
                                    },
                                    SlotStatus::SlotConfirmed => {
                                        SlotUpdate::OptimisticConfirmation {
                                            slot: message.slot(),
                                            timestamp: message.created_at().as_millis(),
                                        }
                                    }
                                    SlotStatus::SlotFinalized => SlotUpdate::Root {
                                        slot: message.slot(),
                                        timestamp: message.created_at().as_millis(),
                                    },
                                },
                            );
                            return Some((subscription, false, json));
                        }
                        (SubscribeMethod::Block, ParsedMessage::Block(message)) => {
                            if let Some((encoding, options)) =
                                subscription.config.filter_block(message)
                            {
                                let json = RpcNotification::serialize_with_context(
                                    "blockNotification",
                                    subscription.id,
                                    message.slot(),
                                    RpcBlockUpdate::new(message, encoding, options),
                                );
                                return Some((subscription, false, json));
                            }
                        }
                        (SubscribeMethod::Root, ParsedMessage::Slot(message)) => {
                            if message.status() == SlotStatus::SlotFinalized {
                                let json = RpcNotification::serialize(
                                    "rootNotification",
                                    subscription.id,
                                    message.slot(),
                                );
                                return Some((subscription, false, json));
                            }
                        }
                        (SubscribeMethod::Transaction, ParsedMessage::Transaction(message)) => {
                            if let Some((
                                encoding,
                                transaction_details,
                                show_rewards,
                                max_supported_transaction_version,
                            )) = subscription.config.filter_transaction(message)
                            {
                                let json = RpcNotification::serialize_with_context(
                                    "transactionNotification",
                                    subscription.id,
                                    message.slot(),
                                    RpcTransactionUpdate::new(
                                        message,
                                        encoding,
                                        transaction_details,
                                        show_rewards,
                                        max_supported_transaction_version,
                                    ),
                                );
                                return Some((subscription, false, json));
                            }
                        }
                        _ => {}
                    };
                    None
                })
                .map(|(subscription, is_final, json)| {
                    (
                        subscription.config_hash,
                        subscription.config.method(),
                        subscription.id,
                        is_final,
                        json,
                    )
                })
                .collect::<Vec<_>>()
        });

        for (subscription_config_hash, subscription_method, subscription_id, is_final, json) in
            new_notifications
        {
            notifications.push(subscription_id, subscription_method, is_final, json);
            if is_final {
                subscriptions.remove_subscription(subscription_config_hash);
            }
        }
    }
}

#[derive(Debug)]
struct CachedSignature {
    slot: Slot,
    err: Option<TransactionError>,
}

#[derive(Debug)]
struct CachedSignatures {
    signatures: HashMap<Signature, CachedSignature, RandomState>,
    signatures_max: usize,
    slots: BTreeMap<Slot, Vec<Signature>>,
    slots_max: usize,
    slot_confirmed: Slot,
    slot_finalized: Slot,
}

impl CachedSignatures {
    fn new(signatures_max: usize, slots_max: usize) -> Self {
        Self {
            signatures: HashMap::with_capacity_and_hasher(signatures_max, RandomState::default()),
            signatures_max,
            slots: BTreeMap::new(),
            slots_max,
            slot_confirmed: 0,
            slot_finalized: 0,
        }
    }

    fn get(
        &self,
        signature: &Signature,
        commitment: CommitmentLevel,
    ) -> Option<(Slot, Option<TransactionError>)> {
        if let Some(cached) = self.signatures.get(signature) {
            let valid_cache = match commitment {
                CommitmentLevel::Processed => true,
                CommitmentLevel::Confirmed if self.slot_confirmed >= cached.slot => true,
                CommitmentLevel::Finalized if self.slot_finalized >= cached.slot => true,
                _ => false,
            };
            if valid_cache {
                return Some((cached.slot, cached.err.clone()));
            }
        }
        None
    }

    fn add_signature(&mut self, message: &MessageTransaction) {
        if let Ok(err) = convert_from::create_tx_error(message.error().as_ref()) {
            while self.signatures.len() >= self.signatures_max || self.slots.len() >= self.slots_max
            {
                let Some((_slot, signatures)) = self.slots.pop_first() else {
                    break;
                };
                for signature in signatures {
                    self.signatures.remove(&signature);
                }
            }

            self.signatures.insert(
                *message.signature(),
                CachedSignature {
                    slot: message.slot(),
                    err,
                },
            );
            self.slots
                .entry(message.slot())
                .or_default()
                .push(*message.signature());

            gauge!(metrics::PUBSUB_CACHED_SIGNATURES_TOTAL).set(self.signatures.len() as f64);
        }
    }

    fn dead_slot(&mut self, slot: Slot) {
        for signature in self.slots.remove(&slot).unwrap_or_default() {
            self.signatures.remove(&signature);
        }
        gauge!(metrics::PUBSUB_CACHED_SIGNATURES_TOTAL).set(self.signatures.len() as f64);
    }

    fn set_confirmed(&mut self, slot: Slot) {
        self.slot_confirmed = slot;
    }

    fn set_finalized(&mut self, slot: Slot) {
        self.slot_finalized = slot;
    }
}

type SlotTransactionStatsItemResult = Option<(ParsedMessage, SlotTransactionStats)>;

#[derive(Debug, Default)]
struct SlotTransactionStatsItem {
    slot: Slot,
    created_at: Timestamp,

    meta: bool,
    num_transactions: u64,
    num_entries: u64,

    num_tx_success: u64,
    num_tx_failed: u64,

    num_entries_received: u64,
    max_tx_per_entry: u64,
}

impl SlotTransactionStatsItem {
    fn add_created_at(
        &mut self,
        slot: Slot,
        created_at: Timestamp,
    ) -> SlotTransactionStatsItemResult {
        self.slot = slot;
        self.created_at = created_at;
        self.try_create()
    }

    fn add_tx(&mut self, failed: bool) -> SlotTransactionStatsItemResult {
        if failed {
            self.num_tx_failed += 1;
        } else {
            self.num_tx_success += 1;
        }
        self.try_create()
    }

    fn add_entry(&mut self, executed_transaction_count: u64) -> SlotTransactionStatsItemResult {
        self.num_entries_received += 1;
        self.max_tx_per_entry = self.max_tx_per_entry.max(executed_transaction_count);
        self.try_create()
    }

    fn add_meta(
        &mut self,
        num_transactions: u64,
        num_entries: u64,
    ) -> SlotTransactionStatsItemResult {
        self.meta = true;
        self.num_transactions = num_transactions;
        self.num_entries = num_entries;
        self.try_create()
    }

    fn try_create(&self) -> SlotTransactionStatsItemResult {
        if self.slot != 0
            && self.meta
            && self.num_transactions == self.num_tx_success + self.num_tx_failed
            && self.num_entries == self.num_entries_received
        {
            let message = MessageSlot::Prost {
                slot: self.slot,
                parent: None,
                status: SlotStatus::SlotProcessed,
                dead_error: None,
                created_at: self.created_at,
                size: 0,
            };
            let stats = SlotTransactionStats {
                num_transaction_entries: self.num_entries,
                num_successful_transactions: self.num_tx_success,
                num_failed_transactions: self.num_tx_failed,
                max_transactions_per_entry: self.max_tx_per_entry,
            };
            return Some((ParsedMessage::Slot(Arc::new(message)), stats));
        }
        None
    }
}

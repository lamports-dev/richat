use {
    crate::{
        channel::{Messages, ParsedMessage},
        pubsub::{
            notification::{RpcNotification, RpcNotifications},
            solana::{SubscribeConfig, SubscribeConfigHashId, SubscribeMethod},
            ClientId, SubscriptionId,
        },
    },
    rayon::{
        iter::{IntoParallelIterator, ParallelIterator},
        ThreadPoolBuilder,
    },
    solana_account_decoder::encode_ui_account,
    solana_sdk::commitment_config::CommitmentLevel,
    std::{
        collections::{hash_map::Entry as HashMapEntry, HashMap, HashSet},
        thread,
    },
    tokio::sync::{mpsc, oneshot},
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ClientRequest {
    Subscribe {
        client_id: ClientId,
        config: SubscribeConfig,
        tx: oneshot::Sender<SubscriptionId>,
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
    subscriptions:
        HashMap<SubscribeConfigHashId, (CommitmentLevel, SubscribeMethod, SubscriptionId)>,
    subscriptions_per_client:
        HashMap<ClientId, HashMap<SubscriptionId, (CommitmentLevel, SubscribeMethod)>>,
    subscriptions_per_method:
        HashMap<(CommitmentLevel, SubscribeMethod), HashMap<SubscriptionId, SubscriptionInfo>>,
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

    fn update(&mut self, request: ClientRequest) {
        match request {
            ClientRequest::Subscribe {
                client_id,
                config,
                tx,
            } => {
                let subscription_id = self.subscribe(client_id, config);
                let _ = tx.send(subscription_id);
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

    fn subscribe(&mut self, client_id: ClientId, config: SubscribeConfig) -> SubscriptionId {
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

pub fn subscriptions_worker(
    messages: Messages,
    mut clients_rx: mpsc::Receiver<ClientRequest>,
    workers_count: usize,
    workers_affinity: Option<Vec<usize>>,
    max_clients_request_per_tick: usize,
    max_messages_per_commitment_per_tick: usize,
    mut notifications: RpcNotifications,
) -> anyhow::Result<()> {
    // Subscriptions storage
    let mut subscriptions = Subscriptions::default();

    // Messages head
    let receiver = messages.to_receiver();
    let mut head_processed = messages
        .get_current_tail(CommitmentLevel::Processed, None)
        .ok_or(anyhow::anyhow!("failed to get head position for processed"))?;
    let mut head_confirmed = messages
        .get_current_tail(CommitmentLevel::Processed, None)
        .ok_or(anyhow::anyhow!("failed to get head position for confirmed"))?;
    let mut head_finalized = messages
        .get_current_tail(CommitmentLevel::Processed, None)
        .ok_or(anyhow::anyhow!("failed to get head position for finalized"))?;

    // Subscriptions filters pool
    let workers = ThreadPoolBuilder::new()
        .num_threads(workers_count)
        .spawn_handler(move |thread| {
            let workers_affinity = workers_affinity.clone();
            thread::Builder::new()
                .name(format!("richatPSubWrk{:02}", thread.index()))
                .spawn(move || {
                    if let Some(cpus) = workers_affinity {
                        affinity::set_thread_affinity(cpus).expect("failed to set affinity");
                    }
                    thread.run()
                })?;
            Ok(())
        })
        .build()?;

    loop {
        // Update subscriptions from clients
        for _ in 0..max_clients_request_per_tick {
            match clients_rx.try_recv() {
                Ok(request) => subscriptions.update(request),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => return Ok(()), // means shutdown
            };
        }

        // Collect messages from channels
        let mut jobs = Vec::with_capacity(0);
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
            for _ in 0..max_messages_per_commitment_per_tick {
                if let Some(message) = receiver.try_recv(commitment, *head)? {
                    *head += 1;
                    messages.push(message);
                } else {
                    break;
                }
            }
            for message in messages.iter() {
                for method in SubscribeMethod::get_message_methods(message) {
                    if let Some(subscriptions) =
                        subscriptions.get_subscriptions(commitment, *method)
                    {
                        for subscription in subscriptions {
                            jobs.push((*method, message, subscription));
                        }
                    }
                }
            }
        }

        // Filter messages
        let new_notifications = workers.install(|| {
            jobs.into_par_iter()
                .filter_map(|(method, message, subscription)| {
                    match (message, method) {
                        (ParsedMessage::Account(message), SubscribeMethod::Account) => {
                            if let Some((encoding, data_slice)) =
                                subscription.config.filter_account(message.pubkey())
                            {
                                let json = RpcNotification::serialize_with_context(
                                    message.slot(),
                                    &encode_ui_account(
                                        message.pubkey(),
                                        message.as_ref(),
                                        encoding,
                                        None,
                                        data_slice,
                                    ),
                                );
                                return Some((
                                    subscription.config_hash,
                                    subscription.id,
                                    false,
                                    json,
                                ));
                            }
                        }
                        _ => {}
                    }
                    None
                })
                .collect::<Vec<_>>()
        });

        for (subscription_config_hash, subscription_id, is_final, json) in new_notifications {
            notifications.push(subscription_id, is_final, json);
            if is_final {
                subscriptions.remove_subscription(subscription_config_hash);
            }
        }
    }
}

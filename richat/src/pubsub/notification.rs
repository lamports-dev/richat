use {
    crate::pubsub::SubscriptionId,
    serde::Serialize,
    solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext},
    solana_sdk::clock::Slot,
    std::{
        collections::VecDeque,
        sync::{Arc, Weak},
    },
    tokio::sync::broadcast,
};

#[derive(Debug, Clone)]
pub struct RpcNotification {
    pub subscription_id: SubscriptionId,
    pub is_final: bool,
    pub json: Weak<String>,
}

impl RpcNotification {
    pub fn serialize<T: Serialize>(value: &T) -> Arc<String> {
        let json = serde_json::to_string(value).expect("json serialization never fail");
        Arc::new(json)
    }

    pub fn serialize_with_context<T: Serialize>(slot: Slot, value: &T) -> Arc<String> {
        Self::serialize(&RpcResponse {
            context: RpcResponseContext {
                slot,
                api_version: None,
            },
            value,
        })
    }
}

#[derive(Debug)]
pub struct RpcNotifications {
    items: VecDeque<Arc<String>>,
    bytes_total: usize,
    max_len: usize,
    max_bytes: usize,
    sender: broadcast::Sender<RpcNotification>,
}

impl RpcNotifications {
    pub fn new(
        max_len: usize,
        max_bytes: usize,
        sender: broadcast::Sender<RpcNotification>,
    ) -> Self {
        Self {
            items: VecDeque::with_capacity(max_len + 1),
            bytes_total: 0,
            max_len,
            max_bytes,
            sender,
        }
    }

    pub fn push(&mut self, subscription_id: SubscriptionId, is_final: bool, json: Arc<String>) {
        let notification = RpcNotification {
            subscription_id,
            is_final,
            json: Arc::downgrade(&json),
        };
        let _ = self.sender.send(notification);

        self.bytes_total += json.len();
        self.items.push_back(json);

        while self.bytes_total > self.max_bytes || self.items.len() > self.max_len {
            let item = self
                .items
                .pop_front()
                .expect("RpcNotifications item should exists");
            self.bytes_total -= item.len();
        }
    }
}

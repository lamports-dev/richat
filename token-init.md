# tokenInitSubscribe

In summer 2021 [Tritone One](https://triton.one/) introduced [transactionSubscribe](https://github.com/solana-foundation/solana-improvement-documents/pull/69). This was an obvious addition to existing subscribe methods in [Whirligig](https://docs.triton.one/project-yellowstone/whirligig-websockets) — a project that consumes [Geyser gRPC](https://github.com/rpcpool/yellowstone-grpc) stream and implements Agave PubSub interface. Later, [Helius](https://www.helius.dev/) implemented [SIMD-0065](https://github.com/solana-foundation/solana-improvement-documents/pull/69) spec in their [Enhanced Websockets](https://www.helius.dev/docs/enhanced-websockets/transaction-subscribe).

It looked like all methods in Agave Pubsub + `transactionSubscribe` allowed to build great Solana Wallets but we still don't have coverage of the case when somebody creates Token Account for you, that's why wallets sometimes need to call `getTokenAccountsByOwner` for syncing. I decided to fill this gap and move wallets from polling same data to getting events with a new method `tokenInitSubscribe`.

## Spec

### Abstract Rust code for Agave

```rust
#[derive(Debug, Serialize)]
pub struct RpcTokenInitResponse {
    pub accounts: Vec<String>,
    pub signature: String,
    pub failed: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReqTokenInitConfig {
    #[serde(flatten)]
    pub commitment: Option<CommitmentConfig>,
}

#[allow(clippy::needless_return)]
#[rpc]
pub trait RpcSolPubSub {
    type Metadata;

    #[pubsub(
        subscription = "tokenInitNotification",
        subscribe,
        name = "tokenInitSubscribe"
    )]
    fn token_init_subscribe(
        &self,
        meta: Self::Metadata,
        subscriber: Subscriber<RpcResponse<RpcTokenInitResponse>>,
        pubkey_str: String,
        config: Option<ReqTokenInitConfig>,
    );

    #[pubsub(
        subscription = "tokenInitNotification",
        unsubscribe,
        name = "tokenInitUnsubscribe"
    )]
    fn token_init_unsubscribe(
        &self,
        meta: Option<Self::Metadata>,
        id: PubSubSubscriptionId,
    ) -> Result<bool>;
}
```

## Possible syncing process in abstract Solana Wallet

For simplicity I propose to request only confirmed data (and optionally show spinning while they are not finalized).

1. Subscribe to Account updates with `accountSubscribe`, `transactionSubscribe` and `tokenInitSubscribe`
2. Subscribe to slots with `slotsUpdatesSubscribe`, monitor confirmed and finalized
3. Request all Token Accounts with `getTokenAccountsByOwner` with `minContextSlot` (should be the slot that you received from WebSocket connection from `slotsUpdates` subscription, i.e. we request data only at least from the moment when we already subscribed on them)
4. Subscribe to all Token Accounts with `accountSubscribe` and re-subscribe with `transactionSubscribe` (`transactionSubscribe` allows multiple addresses in one subscription)
5. Request current state for Account and all Token Accounts with `getMultipleAccounts` with `minContextSlot`
6. Request history for Account and all Token Accounts with `getSignaturesForAddress` with `minContextSlot`
7. If an account update for Token Account is received with owner as System Program and lamports as zero then Token Account is closed (alternatively you can parse transaction from `transactionSubscribe` and parse inner instructions to lookup `CloseAccount` instruction)
8. If a message from `tokenInitSubscribe` is received wallet needs:
    1. Subscribe to the new Token Account with `accountSubscribe` and re-subscribe with `transactionSubscribe`
    2. Request current state with `getAccountInfo` with `minContextSlot`
    3. Request history with `getSignaturesForAddress` with `minContextSlot`

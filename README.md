# richat

Next iteration of [Yellowstone Dragon's Mouth / Geyser gRPC](https://github.com/rpcpool/yellowstone-grpc) that was originally developed and currently maintained by [Triton One](https://triton.one/). `Richat` includes code derived from `Dragon's Mouth` (copyright `Triton One Limited`) with significant architecture changes.

## Sponsored by

## Licensing

Default license for any file in this project is `AGPL-3.0-only`, except files in next directories that licensed under `Apache-2.0`:

- `client`
- `richat`
- `shared`

## Richat support and extended version

Any issue not related with bugs / features related topics would be closed. If you struggle to load plugin or have some questions how to use crates please drop your question in Telegram group: [https://t.me/lamportsdev](https://t.me/lamportsdev)

In addition to open-source version there also enterprise version with ability to downstream full stream. To get more info please send email to: [customers@lamports.dev](mailto:customers@lamports.dev)

```mermaid
flowchart LR
    P[plugin] -->|full stream| R1(richat)
    R1 -->|full stream| R2(richat)
    R2 -->|filtered stream| C1(client)
    R1 -->|filtered stream| C2(client)
    R1 -->|filtered stream| C3(client)
    R2 -->|filtered stream| C4(client)
```

## Components

- `cli` — CLI client for full stream, gRPC stream with filters, simple Solana PubSub
- `client` — library for building consumers
- `filter` — library for filtering geyser messages
- `plugin-agave` — Agave validator geyser plugin https://docs.anza.xyz/validator/geyser
- `proto` — library with proto files, re-imports structs from crate `yellowstone-grpc-proto`
- `richat` — app with full stream consumer and producers: gRPC (`Dragon's Mouth`), Solana PubSub
- `shared` — shared code between components (except `client`)

## Releases

#### Branches

- `master` — development branch
- `agave-v2.1` — development branch for agave v2.1
- `agave-v2.0` — development branch for agave v2.0

#### Tags

- `cli-v0.0.0`
- `client-v0.0.0`
- `filter-v0.0.0`
- `plugin-agave-v0.0.0`
- `plugin-agave-v0.0.0+solana.2.1.5`
- `proto-v0.0.0`
- `richat-v0.0.0`
- `richat-v0.0.0+solana.2.1.5`
- `shared-v0.0.0`

At one moment of time we can support more than one agave version (like v2.0 and v2.1), as result we can have two different major supported versions of every component, for example: `cli-v1.y.z` for `agave-v2.0` and `cli-v2.y.z` for `agave-v2.1`. In addition to standard version `plugin-agave` and `richat` can one or more tags with pinned solana version.

## List of RPC providers with Dragon's Mouth support

- `GetBlock` — https://getblock.io/
- `Helius` — https://www.helius.dev/
- `OrbitFlare` — https://orbitflare.com/
- `QuickNode` — https://www.quicknode.com/
- `Shyft` — https://shyft.to/
- `Triton One` — https://triton.one/

If your RPC provider not in the list, please open Issue / PR!

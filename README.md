# richat

Next iteration of [Yellowstone Dragon's Mouth / Geyser gRPC](https://github.com/rpcpool/yellowstone-grpc) that was originally developed and currently maintained by [Triton One](https://triton.one/). `Richat` is heavily based on `Dragon's Mouth` code with significant architecture changes.

## Sponsored by

## Licensing

Default license for any file in this project is `AGPL-3.0-only`, except files in next directories that licensed under `Apache-2.0`:

- `client`
- `richat`
- `shared`

## Components

- `cli` — CLI client for full stream, gRPC stream with filters, simple Solana PubSub
- `client` — library for building consumers
- `plugin` — validator geyser plugin https://docs.anza.xyz/validator/geyser
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
- `plugin-v0.0.0`
- `plugin-v0.0.0+solana.2.1.5`
- `richat-v0.0.0`
- `richat-v0.0.0+solana.2.1.5`

At one moment of time we can support more than one agave version (like v2.0 and v2.1), as result we can have two different major supported versions of every component, for example: `cli-v1.y.z` for `agave-v2.0` and `cli-v2.y.z` for `agave-v2.1`. In addition to standard version `plugin` and `richat` can one or more tags with pinned solana version.

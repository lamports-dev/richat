# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note:** Version 0 of Semantic Versioning is handled differently from version 1 and above.
The minor version will be incremented upon a breaking change and the patch version will be incremented for features.

## [Unreleased]

### Fixes

### Features

### Breaking

## 2025-07-10

- richat-cli-v5.0.0
- richat-client-v4.0.0
- richat-filter-v4.0.0
- richat-plugin-agave-v4.0.0
- richat-proto-v4.0.0
- richat-v4.0.0
- richat-shared-v4.0.0

### Features

- plugin: bump to agave 2.3 ([#128](https://github.com/lamports-dev/richat/pull/128))

## 2025-07-10

- richat-cli-v4.6.0
- richat-client-v3.6.0
- richat-filter-v3.6.0
- richat-plugin-agave-v3.6.0
- richat-proto-v3.2.0
- richat-v3.7.0
- richat-shared-v3.6.0

### Fixes

- filter: fix memcmp decode data in the config ([#118](https://github.com/lamports-dev/richat/pull/118))
- richat: do not send Block message to connected richat ([#122](https://github.com/lamports-dev/richat/pull/122))
- plugin: disable accounts on snapshot loading ([#124](https://github.com/lamports-dev/richat/pull/124))
- richat: use flag instead of counter in client state ([#125](https://github.com/lamports-dev/richat/pull/125))

### Features

- proto: add version to quic response ([#115](https://github.com/lamports-dev/richat/pull/115))
- richat: use jemalloc ([#117](https://github.com/lamports-dev/richat/pull/117))
- shared: parse affinity with stride in range ([#120](https://github.com/lamports-dev/richat/pull/120))
- richat: use Mutex instead of RwLock in channels ([#126](https://github.com/lamports-dev/richat/pull/126))
- plugin: remove direct bytes dep ([#127](https://github.com/lamports-dev/richat/pull/127))

### Breaking

- richat: replay only processed commitment ([#116](https://github.com/lamports-dev/richat/pull/116))

## 2025-05-30

- richat-cli-v4.5.0
- richat-client-v3.5.0
- richat-filter-v3.5.0
- richat-plugin-agave-v3.5.0
- richat-proto-v3.1.0
- richat-v3.6.0
- richat-shared-v3.5.0

### Fixes

- richat: receive all slot statuses for confirmed / finalized ([#113](https://github.com/lamports-dev/richat/pull/113))

### Features

- richat: impl `SubscribeReplayInfo` ([#109](https://github.com/lamports-dev/richat/pull/109))
- richat: faster account messages deduper ([#110](https://github.com/lamports-dev/richat/pull/110))
- richat: remove tcp ([#111](https://github.com/lamports-dev/richat/pull/111))
- richat: use foldhash for performance ([#112](https://github.com/lamports-dev/richat/pull/112))
- shared: affinity by relative cores ([#114](https://github.com/lamports-dev/richat/pull/114))

## 2025-05-03

- richat-cli-v4.4.0
- richat-client-v3.4.0
- richat-filter-v3.4.0
- richat-plugin-agave-v3.4.0
- richat-v3.5.0
- richat-shared-v3.4.0

### Features

- richat: detailed mismatch error message ([#107](https://github.com/lamports-dev/richat/pull/107))
- shared: `accept Vec<u8>` instead of `serde_json::Value` ([#107](https://github.com/lamports-dev/richat/pull/107))

## 2025-04-21

- richat-cli-v4.3.0
- richat-client-v3.3.0
- richat-filter-v3.3.0
- richat-plugin-agave-v3.3.0
- richat-v3.4.0
- richat-shared-v3.3.0

### Features

- shared: add jsonrpc feature ([#104](https://github.com/lamports-dev/richat/pull/104))

## 2025-04-12

- richat-cli-v4.2.0
- richat-client-v3.2.0
- richat-filter-v3.2.0
- richat-plugin-agave-v3.2.0
- richat-v3.3.0
- richat-shared-v3.2.0

### Features

- richat: use metrics.rs ([#101](https://github.com/lamports-dev/richat/pull/101))

## 2025-04-09

- richat-cli-v4.1.0
- richat-client-v3.1.0
- richat-filter-v3.1.0
- richat-plugin-agave-v3.1.0
- richat-v3.2.0
- richat-shared-v3.1.0

### Features

- shared: support ready and health endpoints on metrics server ([#97](https://github.com/lamports-dev/richat/pull/97))
- shared: use affinity only on linux ([#99](https://github.com/lamports-dev/richat/pull/99))

## 2025-03-19

- richat-v3.1.0

### Fixes

- richat: do not update head on new filter with same commitment ([#88](https://github.com/lamports-dev/richat/pull/88))
- richat: push messages even after SlotStatus ([#89](https://github.com/lamports-dev/richat/pull/89))

### Features

- richat: add metrics ([#90](https://github.com/lamports-dev/richat/pull/90))

## 2025-03-12

- cli-v4.0.0
- client-v3.0.0
- filter-v3.0.0
- plugin-agave-v3.0.0
- proto-v3.0.0
- richat-v3.0.0
- shared-v3.0.0

### Breaking

- solana: upgrade to v2.2 ([#85](https://github.com/lamports-dev/richat/pull/85))

## 2025-03-12

- richat-cli-v3.0.0
- richat-filter-v2.4.1

### Breaking

- cli: add dragon's mouth support ([#83](https://github.com/lamports-dev/richat/pull/83))

## 2025-03-06

- richat-cli-v2.2.2
- richat-plugin-agave-v2.1.1
- richat-v2.3.1

### Fixes

- rustls: install CryptoProvider ([#82](https://github.com/lamports-dev/richat/pull/82))

## 2025-02-22

- richat-shared-v2.5.0

### Features

- shared: add features ([#77](https://github.com/lamports-dev/richat/pull/77))

## 2025-02-20

- richat-cli-v2.2.1
- richat-filter-v2.4.0
- richat-v2.3.0
- richat-shared-v2.4.0

### Features

- shared: use `five8` to encode/decode ([#70](https://github.com/lamports-dev/richat/pull/70))
- shared: use `slab` for `Shutdown` ([#75](https://github.com/lamports-dev/richat/pull/75))

### Breaking

- filter: encode with slices ([#73](https://github.com/lamports-dev/richat/pull/73))
- richat: remove parser thread ([#74](https://github.com/lamports-dev/richat/pull/74))
- richat: support multiple sources ([#76](https://github.com/lamports-dev/richat/pull/76))

## 2025-02-11

- richat-cli-v2.2.0
- richat-client-v2.2.0
- richat-filter-v2.3.0
- richat-plugin-agave-v2.1.0
- richat-proto-v2.1.0
- richat-v2.2.0
- richat-shared-v2.3.0

### Fixes

- richat: remove extra lock on clients queue ([#49](https://github.com/lamports-dev/richat/pull/49))
- plugin-agave: set `nodelay` correctly for Tcp ([#53](https://github.com/lamports-dev/richat/pull/53))
- richat: add minimal sleep ([#54](https://github.com/lamports-dev/richat/pull/54))
- richat: consume dragons mouth stream ([#62](https://github.com/lamports-dev/richat/pull/62))
- richat: fix slot status ([#66](https://github.com/lamports-dev/richat/pull/66))

### Features

- cli: add bin `richat-track` ([#51](https://github.com/lamports-dev/richat/pull/51))
- richat: change logs and metrics in the config ([#64](https://github.com/lamports-dev/richat/pull/64))
- richat: add solana pubsub ([#65](https://github.com/lamports-dev/richat/pull/65))
- richat: add metrics (backport of [private#5](https://github.com/lamports-dev/richat-private/pull/5)) ([#69](https://github.com/lamports-dev/richat/pull/69))

### Breaking

- plugin-agave,richat: remove `max_slots` ([#68](https://github.com/lamports-dev/richat/pull/68))

## 2025-01-22

- filter-v2.2.0
- richat-v2.1.0

### Features

- richat: add metrics (backport of [private#1](https://github.com/lamports-dev/richat-private/pull/1)) ([#44](https://github.com/lamports-dev/richat/pull/44))
- richat: add downstream server (backport of [private#3](https://github.com/lamports-dev/richat-private/pull/3)) ([#46](https://github.com/lamports-dev/richat/pull/46))

## 2025-01-19

- cli-v2.1.0
- client-v2.1.0
- filter-v2.1.0
- richat-v2.0.0
- shared-v2.1.0

### Features

- richat: impl gRPC dragon's mouth ([#42](https://github.com/lamports-dev/richat/pull/42))

## 2025-01-14

- cli-v2.0.0
- cli-v1.0.0
- client-v2.0.0
- client-v1.0.0
- filter-v2.0.0
- filter-v1.0.0
- plugin-agave-v2.0.0
- plugin-agave-v1.0.0
- proto-v2.0.0
- proto-v1.0.0
- shared-v2.0.0
- shared-v1.0.0

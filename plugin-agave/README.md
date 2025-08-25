# Development

To run plugin with `solana-test-validator`:

```
cp config.json config.dev.json && vim config.dev.json
cargo build -p richat-plugin-agave --lib --release --features="rustls-install-default-provider" && solana-test-validator --geyser-plugin-config plugin-agave/config.dev.json
```

If you run plugin on mainnet validator do not try to do it in `debug` mode, validator would start fall behind.

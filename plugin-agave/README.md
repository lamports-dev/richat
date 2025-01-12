# Development

To run plugin with `solana-test-validator`:

```
cp config.json config-test.json && vim config-test.json
cargo build -p richat-plugin-agave --lib --release && solana-test-validator --geyser-plugin-config plugin-agave/config-test.json
```

If you run plugin on mainnet validator do not try to do it in `debug` mode, validator would start fall behind.

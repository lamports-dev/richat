# Development

To run plugin with `solana-test-validator`:

```
cp config.json config-test.json && vim config-test.json
cargo build -p richat-plugin --lib --release && solana-test-validator --geyser-plugin-config plugin/config-test.json
```

If you run plugin on mainnet validator do not try to do it in `debug` mode, validator would start fall behind.

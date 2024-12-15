# Development

To run plugin with `solana-test-validator`:

```
cp config.json config-test.json && vim config-test.json
cargo build -p richat-plugin --lib --release && solana-test-validator --geyser-plugin-config plugin/config-test.json
```

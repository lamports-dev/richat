ci-all: \
	ci-fmt \
	ci-cargo-deny \
	ci-clippy \
	ci-clippy-fuzz \
	ci-check \
	ci-test

ci-fmt:
	cargo +nightly fmt --check

ci-cargo-deny:
	cargo deny check advisories

ci-clippy:
	cargo clippy --workspace --all-targets -- -Dwarnings

ci-clippy-fuzz:
	cd plugin-agave/fuzz && cargo clippy --workspace --all-targets -- -Dwarnings

PACKAGES=richat-cli richat-client richat-filter richat-plugin-agave richat richat-shared
ci-check:
	for package in $(PACKAGES) ; do \
		echo cargo check -p $$package --all-targets ; \
		cargo check -p $$package --all-targets ; \
	done
	cargo check -p richat-plugin-agave --all-targets --all-features
	cargo check -p richat-shared --all-targets --no-default-features --features="config"
	cargo check -p richat-shared --all-targets --no-default-features --features="five8"
	cargo check -p richat-shared --all-targets --no-default-features --features="metrics"
	cargo check -p richat-shared --all-targets --no-default-features --features="shutdown"
	cargo check -p richat-shared --all-targets --no-default-features --features="transports"
	cargo check -p richat-shared --all-targets --no-default-features --features="version"

ci-test:
	cargo test --all-targets

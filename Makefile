ci-all: \
	ci-cargo-deny \
	ci-clippy \
	ci-clippy-fuzz \
	ci-check \
	ci-test

ci-cargo-deny:
	cargo deny check advisories

ci-clippy:
	cargo clippy --workspace --all-targets -- -Dwarnings

ci-clippy-fuzz:
	cd plugin/fuzz && cargo clippy --workspace --all-targets -- -Dwarnings

PACKAGES=richat-cli richat-client richat-plugin richat richat-shared
ci-check:
	for package in $(PACKAGES) ; do \
		echo cargo check -p $$package --all-targets ; \
		cargo check -p $$package --all-targets ; \
	done

ci-test:
	cargo test --all-targets

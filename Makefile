.PHONY: dev build check check-rs check-fe engine-dev engine-build engine-check check-all test-shared test-engine test-ui test-all coverage-rs coverage-rs-lcov coverage-rs-summary install clean docker-up docker-down observability-up observability-down

# Development
dev:
	bun run tauri dev

# Build production binary
build:
	bun run tauri build

# Run all checks (Rust + Frontend)
check: check-rs check-fe

check-all: check engine-check

check-rs:
	cd src-tauri && cargo check

coverage-rs:
	cd src-tauri && (cargo llvm-cov --version >/dev/null 2>&1 || cargo install cargo-llvm-cov --locked)
	cd src-tauri && rustup component add llvm-tools-preview --toolchain $$(rustup show active-toolchain | awk '{print $$1}')
	cd src-tauri && cargo llvm-cov --workspace --tests --html --output-dir coverage/html

coverage-rs-lcov:
	cd src-tauri && (cargo llvm-cov --version >/dev/null 2>&1 || cargo install cargo-llvm-cov --locked)
	cd src-tauri && rustup component add llvm-tools-preview --toolchain $$(rustup show active-toolchain | awk '{print $$1}')
	cd src-tauri && cargo llvm-cov --workspace --tests --lcov --output-path coverage/lcov.info

coverage-rs-summary:
	cd src-tauri && (cargo llvm-cov --version >/dev/null 2>&1 || cargo install cargo-llvm-cov --locked)
	cd src-tauri && rustup component add llvm-tools-preview --toolchain $$(rustup show active-toolchain | awk '{print $$1}')
	cd src-tauri && cargo llvm-cov --workspace --tests --summary-only

check-fe:
	bun run check

engine-dev:
	cargo run -p crossql-engine

engine-build:
	cargo build -p crossql-engine --release

engine-check:
	cargo check -p crossql-engine
	cargo clippy -p crossql-engine

test-shared:
	cargo test -p crossql-shared

test-engine:
	cargo test -p crossql-engine

test-ui:
	bun run test

test-all: test-shared test-engine test-ui

# Install frontend dependencies
install:
	bun install

# Clean build artifacts
clean:
	cd src-tauri && cargo clean
	rm -rf build .svelte-kit

# Docker (test data sources)
docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

observability-up:
	docker compose --profile engine --profile observability up -d

observability-down:
	docker compose --profile engine --profile observability down

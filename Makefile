.PHONY: dev build check check-rs check-fe install clean docker-up docker-down

# Development
dev:
	bun run tauri dev

# Build production binary
build:
	bun run tauri build

# Run all checks (Rust + Frontend)
check: check-rs check-fe

check-rs:
	cd src-tauri && cargo check

check-fe:
	bun run check

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

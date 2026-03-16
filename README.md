# CrossQL

A desktop query federation engine for visually joining and analyzing data across multiple heterogeneous databases.

Built with **Tauri 2 + Svelte 5 + Polars (Rust)**.
Designed and structured by me, with implementation assisted by AI tools.

## What It Does

Connect to multiple databases, design pipelines on a drag-and-drop canvas, join data across sources, and export the result as a Parquet file — all locally, no cloud required.

## Features

- **Visual pipeline canvas** — drag-and-drop nodes, connect edges
- **Cross-database joins** — join tables from different databases on different hosts
- **Transform nodes** — filter, select, rename, cast, derived columns, schema mapping
- **Per-node progress** — live status ring (idle → running → done) during execution
- **Parquet output** — with compression options (Snappy, Zstd, Gzip, Lz4)
- **SQL query on output** — query your parquet results using SQL (Polars engine)
- **Pipeline save/load** — persist pipelines as `.etl.json` files
- **Schema preview** — inspect column names and types before running

## Architecture

CrossQL has two components:
- **Desktop App (Tauri + Svelte)** for designing pipelines and submitting jobs
- **Remote Engine (`crossql-engine`)** for queueing, running, and exposing run status/metrics

```text
┌───────────────────────────────┐
│ CrossQL Desktop (Tauri/Svelte)│
│ - Pipeline editor             │
│ - Remote execution tab        │
└───────────────┬───────────────┘
                │ HTTP (Bearer API key)
                ▼
┌───────────────────────────────┐
│ crossql-engine (Axum/Rust)    │
│ - /pipeline/submit            │
│ - /pipeline/:id/status        │
│ - /runs, /status, /metrics    │
│ - runs.json persistence        │
└───────────────┬───────────────┘
                │
                ▼
       Output files + run history
```

## Supported Sources

| Source | Driver | Protocol |
|---|---|---|
| PostgreSQL | `sqlx` | TCP |
| MySQL | `sqlx` | TCP |
| MongoDB | `mongodb` crate | TCP |
| Cassandra | `cdrs-tokio` | CQL |
| CSV | Polars CSV reader | Local file |
| Parquet | Polars Parquet scanner | Local file |

## Prerequisites

- [Bun](https://bun.sh) — JavaScript runtime & package manager
- [Rust](https://rustup.rs) — install via `rustup`

## Quick Start

```bash
# Install dependencies
make install

# Start test databases
make docker-up

# Run in development mode
make dev

# Run checks
make check
```

## Engine Setup

The remote engine requires these environment variables:

- `CROSSQL_API_KEY` (required): bearer token expected by every request
- `CROSSQL_OUTPUT_DIR` (required): output root used for artifacts and `runs.json`
- `CROSSQL_PORT` (optional, default `7070`): HTTP listen port

Run engine in development:

```bash
export CROSSQL_API_KEY=test-key
export CROSSQL_OUTPUT_DIR=/tmp/crossql-output
export CROSSQL_PORT=7070
make engine-dev
```

Build release binary:

```bash
make engine-build
./target/release/crossql-engine
```

Validate engine-only checks:

```bash
make engine-check
```

## Desktop Remote Usage

1. Start `crossql-engine` and keep it running.
2. Open CrossQL desktop app (`make dev`).
3. Open **Remote Execution** tab.
4. Enter engine URL and API key, then click **Connect**.
5. Return to **Pipeline Editor** and click **Send to Engine**.
6. Monitor active runs/history from **Remote Execution**.

## Testing Data Sources

A `docker-compose.yaml` is included for spinning up local test instances of PostgreSQL, MySQL, MongoDB, and Cassandra.

```bash
make docker-up    # start all databases
make docker-down  # stop all databases
```

> [!CAUTION]
> **This is a hobby/experimental project.** All data is pulled into memory before joins are executed. Avoid `SELECT * FROM large_table` on production databases — always filter at the source with `WHERE` clauses and `LIMIT` to control memory usage. Using `SELECT *` is acceptable only against test/local databases.

## Prometheus and Grafana

`crossql-engine` exposes `GET /metrics` in Prometheus text format. Prometheus and Grafana are now included as optional services in the existing `docker-compose.yaml` under the `observability` profile.

How to run:

1. Set engine API key in your shell so Prometheus can authenticate:
   - `export CROSSQL_API_KEY=test-key`
2. Start engine + observability stack from the existing compose file:
   - `docker compose --profile engine --profile observability up -d`
3. Open:
   - Prometheus: `http://localhost:9090`
   - Grafana: `http://localhost:3000` (default login: `admin` / `admin`)
4. In Grafana, add Prometheus data source URL:
   - `http://prometheus:9090`
5. Query metrics such as:
   - `crossql_active_pipelines`
   - `crossql_chunks_processed_total`
   - `crossql_pipeline_duration_seconds_sum`
   - `crossql_memory_bytes_used`

## License

MIT

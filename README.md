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

## Testing Data Sources

A `docker-compose.yaml` is included for spinning up local test instances of PostgreSQL, MySQL, MongoDB, and Cassandra.

```bash
make docker-up    # start all databases
make docker-down  # stop all databases
```

> [!CAUTION]
> **This is a hobby/experimental project.** All data is pulled into memory before joins are executed. Avoid `SELECT * FROM large_table` on production databases — always filter at the source with `WHERE` clauses and `LIMIT` to control memory usage. Using `SELECT *` is acceptable only against test/local databases.

## License

MIT

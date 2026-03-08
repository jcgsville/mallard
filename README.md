# mallard

Forward-only schema migrations for DuckDB, with heavy inspiration from Graphile Migrate:

- raw SQL migrations
- forward-only workflow
- CLI-first developer experience

## Getting started

```bash
cargo run -- init
```

Expected output:

```text
Created /path/to/project/mallard.toml
Prepared /path/to/project/migrations/committed
Prepared /path/to/project/migrations/current.sql
```

This bootstraps the project layout and writes a default `mallard.toml` with:

- config discovery via `mallard.toml`
- `${VAR}` and `${VAR:-default}` environment interpolation
- a `migrations/current.sql` authoring file from day one

## Initial direction

The project is currently scaffolded as a small Rust CLI using `clap` and `duckdb-rs`.
From here, a natural next step is to add commands like:

- `init`
- `migrate`
- `status`

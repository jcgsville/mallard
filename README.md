# mallard

Forward-only schema migrations for DuckDB, with heavy inspiration from Graphile Migrate:

- raw SQL migrations
- forward-only workflow
- CLI-first developer experience

## Getting started

```bash
cargo run -- init --db-path ./dev.duckdb
```

Expected output:

```text
Connected to DuckDB at ./dev.duckdb
```

## Initial direction

The project is currently scaffolded as a small Rust CLI using `clap` and `duckdb-rs`.
From here, a natural next step is to add commands like:

- `init`
- `new`
- `migrate`
- `status`

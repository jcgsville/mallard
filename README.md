# mallard

Mallard is a forward-only DuckDB schema migration CLI.

It is built around a simple workflow:

- committed migrations live in `migrations/committed`
- one editable current migration lives in `migrations/current.sql` or `migrations/current/**/*.sql`
- committed migrations are replayed into the target database in order
- the current migration is validated separately before it is committed

Mallard takes heavy inspiration from the SQL-first approach of Graphile Migrate, but targets DuckDB.

## Install and run

From this repository:

```bash
cargo run -- --help
```

Or build the binary:

```bash
cargo build
./target/debug/mallard --help
```

## Quick start

Initialize a project:

```bash
cargo run -- init
```

That creates a default `mallard.toml` if one does not already exist, plus:

```text
migrations/
  committed/
  current.sql
  fixtures/
```

Write SQL into `migrations/current.sql`, then iterate on it locally:

```bash
cargo run -- watch
```

Commit it into the forward-only history:

```bash
cargo run -- commit
```

Apply committed migrations to the main database:

```bash
cargo run -- migrate
```

## Core concepts

### Committed migrations

Committed migrations are immutable SQL files in `migrations/committed`. Mallard requires them to:

- use contiguous numeric filenames such as `000001.sql`
- include header metadata for previous hash and current hash
- form a valid hash chain
- contain a non-empty SQL body

Mallard treats these files as the authoritative forward-only history.

### Current migration

The current migration is the editable work-in-progress migration. You can author it in either form:

- `migrations/current.sql`
- `migrations/current/**/*.sql`

Directory mode is loaded recursively in sorted path order. You cannot use both forms at the same time.

### Main and shadow databases

- the main database is the configured `database_path`
- the shadow database is the configured `shadow_path`

Mallard uses these databases for different jobs.

- `main` is your normal development database
- `shadow` is a disposable validation database; Mallard recreates it when it wants a clean replay from committed history

Shadow validation means:

- replay all committed migrations from scratch into a fresh shadow database
- compile and apply the current migration on top of that clean baseline
- fail if either stage errors

That gives Mallard a trustworthy "can this build cleanly from the committed history plus current migration?" check.

What Mallard does not do yet:

- compare main and shadow schemas for drift
- diff the resulting schema against an expected snapshot
- perform deeper semantic validation beyond successful replay and execution

Future improvements may include stronger schema verification, drift detection, or schema snapshot checks, but the shadow database is primarily a clean execution and commit-validation target.

### Migration idempotency

By default, `watch` reruns the current migration against the main development database on every change:

```bash
mallard watch
```

Why this works:

- `watch` first brings the main database up to date with committed migrations
- it then reapplies the current migration on top of the existing main database state each time files change

That means your work-in-progress migration should be idempotent to enable iteration. `migrations/current.sql` should be safe to run repeatedly and still converge on the schema you want.

When plain `create ...` statements are not enough, an explicit undo/redo pattern is often the simplest approach:

```sql
drop table if exists people;

create table people (
  id integer primary key,
  name text
);
```

## Configuration

Mallard discovers `mallard.toml` by walking upward from the current working directory. Every command also accepts:

```text
--config <PATH>
```

All relative paths are resolved from the config file directory.

### Supported config fields

```toml
version = 1
database_path = "dev.duckdb"
shadow_path = ".mallard/shadow.duckdb"
migrations_dir = "migrations"
internal_schema = "mallard"
manage_metadata = true

[placeholders]
APP_SCHEMA = "main"
```

- `version`: must be `1`
- `database_path`: main DuckDB file path
- `shadow_path`: shadow DuckDB file path
- `migrations_dir`: root migration directory
- `internal_schema`: schema name for Mallard metadata tables
- `manage_metadata`: when `true`, Mallard creates and manages its metadata table; when `false`, the table must already exist
- `[placeholders]`: raw text substitutions used during compile and execution

### Config environment interpolation

String config values support:

- `${VAR}`
- `${VAR:-default}`

## SQL authoring features

### Fixture includes

The current migration supports include directives:

```sql
--! include fixtures/base_tables.sql
```

Behavior:

- include paths must resolve to files under `migrations/fixtures`
- include cycles are rejected
- including the same fixture file more than once anywhere in the current migration is rejected
- includes are expanded inline before validation, compile output, and commit output

Includes are only an authoring feature for the current migration. They are expanded away when a migration is committed.

### Placeholders

Placeholders use `:NAME` syntax:

```sql
create schema if not exists :APP_SCHEMA;
```

Behavior:

- names must start with `_` or an uppercase ASCII letter
- remaining characters may be uppercase ASCII letters, digits, or `_`
- replacement is raw text substitution
- unknown placeholders fail compilation or execution
- placeholder values are not SQL-escaped automatically

## Commands

All commands support `--config <PATH>`.

### `mallard init`

Bootstraps a Mallard project.

Behavior:

- if `--config` is provided and the file does not exist, Mallard writes the default config there
- if `--config` is provided and the file already exists, Mallard reuses it
- without `--config`, Mallard searches upward from the working directory for an existing `mallard.toml`
- if no config is found, Mallard creates `mallard.toml` in the working directory
- creates `committed/`, `fixtures/`, and `current.sql` under the configured `migrations_dir`

Example:

```bash
mallard init
```

### `mallard migrate`

Applies committed migrations to the main database.

Behavior:

- loads and validates committed migrations from `migrations/committed`
- verifies the applied database history matches the committed prefix on disk
- applies only pending committed migrations, in order, each in its own transaction
- does not apply the current migration

Example:

```bash
mallard migrate
```

### `mallard commit`

Validates the current migration, writes the next committed migration, and clears the current migration source.

Behavior:

- loads the current migration from file mode or directory mode
- expands fixture includes
- rejects empty current SQL
- rejects current migration lines that start with committed header syntax like `--! `
- recreates the shadow database
- replays all committed migrations into shadow
- resolves placeholders and runs the current migration in a transaction on shadow
- writes the next committed migration file on success
- clears `current.sql` or removes the source files in `current/`

Important details:

- includes are expanded into the committed file
- placeholders are not baked into the committed file; they are resolved later at execution time
- committed filenames use the next sequence number like `000001.sql`

Example:

```bash
mallard commit
```

### `mallard uncommit`

Moves the latest unapplied, committed migration back into the current migration.

Behavior:

- refuses to uncommit a migration that has already been applied to the main database
- restores the committed SQL body into `current.sql` or `current/<filename>.sql`
- deletes the committed migration file

This command changes files on disk only. It does not roll back database state. Thus, you should never uncommit a migration that was already applied to a production database.

Example:

```bash
mallard uncommit
```

### `mallard compile [--output <PATH>]`

Compiles the current migration without executing it.

Behavior:

- expands includes
- resolves placeholders
- prints compiled SQL to stdout when `--output` is omitted
- writes compiled SQL to the provided file when `--output` is set

Example:

```bash
mallard compile --output build/current.sql
```

### `mallard run`

Brings the main database up to date, and runs the current migration against it. In general, `watch` is better suited for development, but this command can be useful in cases where a one-off run of the current migration is useful.

Behavior:

- compiles the current migration with includes and placeholders
- treats empty current SQL as a no-op
- applies the current migration in a transaction

Example:

```bash
mallard run
```

### `mallard watch [--interval-ms <MS>]`

Polls migration inputs and reruns the `run` flow when files change.

Flags:

- `--interval-ms <MS>`: polling interval in milliseconds, default `1000`

Behavior:

- performs one run immediately on startup
- watches `current.sql`, `current/`, `committed/`, and `fixtures/` using polling
- reruns when file path, size, or modified time changes
- by default, Mallard does not reset the main database between reruns
- current migrations should therefore be written to tolerate repeated execution while watching
- does not watch `mallard.toml`

Examples:

```bash
mallard watch
mallard watch --interval-ms 500
```

### `mallard status`

Reports whether committed migrations are pending and whether the current migration has content.

Behavior:

- loads committed migrations and current migration state
- reads database metadata if the main database exists
- validates that applied history matches the committed prefix
- prints two booleans:
  - pending committed migrations
  - current migration has changes

Exit codes:

- `0`: no pending committed migrations and no current changes
- `1`: pending committed migrations only
- `2`: current migration has changes only
- `3`: both are true

Example:

```bash
mallard status
```

### `mallard reset --force`

Destroys and rebuilds the main database from committed migrations.

Behavior:

- requires `--force`
- deletes the main database file and its `.wal`
- reruns `migrate`
- does not apply the current migration
- does not touch the shadow database

Example:

```bash
mallard reset --force
```

## Typical workflow

```bash
mallard init

# edit migrations/current.sql
mallard run
mallard commit
mallard migrate
mallard status
```

## Notes and guardrails

- Mallard is forward-only; it does not generate down migrations
- `uncommit` only works for the latest committed migration and only before that migration reaches the main database
- `reset` is destructive and intentionally gated behind `--force`
- `run` can leave newly committed migrations applied even if the current migration later fails
- placeholder values are raw SQL text; quote or escape them yourself when needed

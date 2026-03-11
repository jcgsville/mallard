# Mallard

Mallard is a forward-only DuckDB schema migration CLI.

It is built around a simple workflow:

- start `mallard watch` and iterate on `migrations/current.sql` in development
- commit with `mallard commit` once you're happy with a migration
- run `mallard apply` to deploy your migrations to production

Mallard takes heavy inspiration from the SQL-first approach of Graphile Migrate, but Mallard targets
DuckDB.

As with most open source software, I make no claims or guarantees as to the correctness or security
properties of Mallard. It's pretty simple, so you can test it and dump the source into an LLM of
your choice to validate it your self. The documentation below represents the intended behavior. If
you observe a deviation from the intended behavior, a detailed bug report filed via GitHub Issues is
welcome 🙂

## Install

Mallard ships as prebuilt binaries through GitHub Releases.

### Install on MacOS and Linux

Install the latest release to `~/.local/bin`:

```bash
curl -fsSL https://github.com/jcgsville/mallard/releases/latest/download/install.sh | sh
```

Use `--version` to install a specific version:

```bash
curl -fsSL https://github.com/jcgsville/mallard/releases/download/v0.1.0/install.sh | sh -s -- --version 0.0.2
```

Use `--to` to change the install directory:

```bash
curl -fsSL https://github.com/jcgsville/mallard/releases/latest/download/install.sh | sh -s -- --to /usr/local/bin
```

### Install on Windows

Download the `x86_64-pc-windows-msvc` zip asset from GitHub Releases, extract it, and place
`mallard.exe` in the extracted folder somewhere on your `PATH`.

### Build From Source

Mallard is a Rust CLI. If you prefer a local build, use Rust 1.86 or newer:

```bash
cargo run -- --help
```

Or build the binary directly:

```bash
cargo build --release
./target/release/mallard --help
```

### Supported Release Targets

- `x86_64-unknown-linux-gnu`
- `x86_64-apple-darwin`
- `aarch64-apple-darwin`
- `x86_64-pc-windows-msvc`

Release assets include platform archives, a release-scoped `install.sh`, and matching SHA-256
checksum files.

If you have a use case for another release target, let us know with a GitHub Issue.

## Quick Start

Initialize a project:

```bash
mallard init
```

That creates a default `mallard.toml` if one does not already exist, plus the necessary directory
structure:

```text
migrations/
  committed/
  current.sql
  fixtures/
```

Use the watch command to iterate on your first migration:

```bash
mallard watch
```

Write the SQL for your first migration in `migrations/current.sql`. Check the output of the watch
command to see if it is working.

Commit your migration:

```bash
mallard commit
```

Apply committed migrations to a shared database:

```bash
mallard apply
```

## Core Concepts

### Committed Migrations

Committed migrations are immutable SQL files in `migrations/committed`. Mallard requires them to:

- use contiguous numeric filenames such as `000001.sql`
- include header metadata for previous hash and current hash
- form a valid hash chain with the previous migrations
- contain a non-empty SQL body

Mallard treats these files as the authoritative forward-only history of the database schema.

### Current Migration

The current migration is the editable work-in-progress migration in `migrations/current.sql`.

### Main & Shadow Databases

- the main database is the configured `database_path`
- the shadow database is the configured `shadow_path`

Mallard uses these databases for different jobs.

- `main` is your normal development database
- `shadow` is a disposable validation database. Mallard recreates it when it wants a clean replay
  from committed history

Shadow validation means:

- replay all committed migrations from scratch into a fresh shadow database
- compile and apply the current migration on top of that clean baseline
- fail if either stage errors

That gives Mallard a trustworthy "can this build cleanly from the committed history plus current
migration?" check.

What Mallard does not do yet:

- compare main and shadow schemas for drift
- diff the resulting schema against an expected snapshot
- perform deeper semantic validation beyond successful replay and execution

Future improvements may include stronger schema verification, drift detection, or schema snapshot
checks, but the shadow database is primarily a clean execution and commit-validation target.

### Migration Idempotency

By default, `watch` reruns the current migration against the main development database on every
change:

```bash
mallard watch
```

That means your work-in-progress migration should be idempotent to enable iteration. i.e.
`migrations/current.sql` should be safe to run repeatedly and still converge on the schema you want.

For example, instead of just `create`ing a table, your migration should `drop table if exists`
first:

```sql
drop table if exists people;

create table people (
  id integer primary key,
  name text
);
```

## Configuration

Mallard discovers `mallard.toml` by walking upward from the current working directory. Every command
also accepts:

```text
--config <PATH>
```

All relative paths are resolved from the config file directory.

### Supported Config Fields

```toml
version = 1
database_path = "dev.duckdb"
shadow_path = "shadow.duckdb"
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
- `manage_metadata`: when `true`, Mallard creates and manages its metadata table; when `false`, the
  table must already exist
- `[placeholders]`: raw text substitutions used during compile and execution

### Config Environment Interpolation

String config values support environment interpolation with the following syntax:

- `${VAR}`
- `${VAR:-default}`

## SQL Authoring Features

### Fixture Includes

The current migration supports include directives:

```sql
--! include fixtures/base_tables.sql
```

Include directives help your git history to better track the iteration of stateless objects in the
database like [DuckDB Macros](https://duckdb.org/docs/stable/sql/statements/create_macro). If you
have an include file for a macro, you can more easily see the history of the macro's changes by
looking at the git history of that file.

We do not recommend managing stateful resources with macros.

Behavior:

- include paths must resolve to files under `migrations/fixtures`
- include files can recursively include other files
- include cycles are rejected
- including the same fixture file more than once anywhere in the current migration is rejected
- includes are expanded inline before validation, compile output, and and commit output

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
- without `--config`, Mallard searches upward from the working directory for an existing
  `mallard.toml`
- if no config is found, Mallard creates `mallard.toml` in the working directory
- creates `committed/`, `fixtures/`, and `current.sql` under the configured `migrations_dir`

### `mallard migrate`

Applies committed migrations to the main database.

Behavior:

- loads and validates committed migrations from `migrations/committed`
- verifies the applied database history matches the committed prefix on disk
- applies only pending committed migrations, in order, each in its own transaction
- does not apply the current migration

### `mallard commit`

Validates the current migration, writes the next committed migration, and clears the current
migration source.

Behavior:

- loads the current migration from `current.sql`
- expands fixture includes
- recreates the shadow database
- replays all committed migrations against the shadow database
- runs the current migration with placeholders in a transaction against the shadow shadow database
- writes the next committed migration file using the next sequence number, like `000013.sql`
- clears `current.sql`

### `mallard uncommit`

Moves the latest unapplied, committed migration back into the current migration as long as it has
not already been applied to the main database.

Behavior:

- restores the committed SQL body into `current.sql`
- deletes the committed migration file

This command changes files on disk only. It does not roll back database state. Thus, you should
never uncommit a migration that was already applied to a production database.

### `mallard compile [--output <PATH>]`

Compiles the current migration without executing it.

Behavior:

- expands includes
- resolves placeholders
- prints compiled SQL to stdout when `--output` is omitted
- writes compiled SQL to the provided file when `--output` is set

Note that the compiled SQL includes resolved placeholder values. Committed migrations keep the
placeholders references, and only resolve them when migrations are applied.

### `mallard run`

Applies all pending, committed migrations, and then runs the current migration against the main
database. In general, `watch` is better suited for development, but this command can be useful in
cases where a one-off run of the current migration is useful.

### `mallard watch [--interval-ms <MS>]`

Updates the main database, polls migration inputs, and reruns the current migration when files
change.

Flags:

- `--interval-ms <MS>`: polling interval in milliseconds, default `1000`

Behavior:

- applies all pending, committed migrations against the main database
- applies the current migration against the main database
- watches `current.sql`, `committed/`, and `fixtures/` using polling
- re-applies the current migration when file path, size, or modified time changes

### `mallard status`

Reports whether any committed migrations are pending application and whether the current migration
has content.

Behavior:

- loads and validates the committed migrations and current migration state
- prints two booleans:
  - pending committed migrations
  - current migration has changes

Exit codes:

- `0`: no pending committed migrations and no current changes
- `1`: pending committed migrations only
- `2`: current migration has changes only
- `3`: both are true

### `mallard reset --force`

Destroys and rebuilds the main database from committed migrations.

Behavior:

- requires `--force`
- deletes the main database file and its `.wal`
- reruns `migrate`
- does not apply the current migration
- does not touch the shadow database

`mallard reset` is extremely destructive, and should never be run against a production database

## Typical Workflow on Existing Mallard Project

```bash
mallard watch

# edit migrations/current.sql until you're happy

mallard commit

# Push your changes to git branch

# In your continuous deployment tool:
mallard migrate
```

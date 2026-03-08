# Mallard Design Doc

## Overview

Mallard is a forward-only schema migration CLI for DuckDB.

It is heavily inspired by Graphile Migrate:

- SQL-first migrations
- forward-only workflow
- committed migration hash chain
- migration state stored inside the database in a separate internal schema
- shadow database support for validation and replay checks when using the full development workflow
- CLI-first workflow

Mallard intentionally diverges where Graphile Migrate depends on PostgreSQL-specific behavior that does not map cleanly to DuckDB.

## Goals

- Make DuckDB schema changes auditable, deterministic, and easy to review.
- Keep migrations as raw SQL.
- Favor roll-forward fixes over down migrations.
- Preserve a strict linear committed history.
- Store execution state inside the target database, not only on disk.
- Feel familiar to Graphile Migrate users.

## Non-Goals

- Automatic rollback generation.
- ORM-specific abstractions.
- Full PostgreSQL feature parity.
- Reproducing PostgreSQL-only Graphile internals where DuckDB needs a different approach.

## Core Principles

1. Migrations are plain SQL files.
2. Production runs committed migrations only.
3. Committed migrations are append-only and hash-verified.
4. A current migration exists for local iteration and future Graphile-like workflows.
5. Mallard owns its internal metadata schema in the database.
6. DuckDB-native behavior wins over Graphile compatibility when the two conflict.

## High-Level Model

Mallard has two classes of migration state:

### Filesystem state

Project migration files live in a migrations directory.

Target layout:

```text
migrations/
  committed/
    000001-init.sql
    000002-add-users.sql
  current.sql
  fixtures/
```

Notes:

- `migrations/current.sql` is created by `mallard init`.
- In early milestones, `current.sql` may exist before all Graphile-style workflows around it are implemented.
- `fixtures/` is optional and can be added later if Mallard supports include directives.

### Database state

Mallard stores migration metadata in a dedicated internal schema inside the DuckDB database.

Default schema name:

```sql
mallard
```

This name is configurable in `mallard.toml`.

Recommended metadata table:

```sql
create schema if not exists mallard;

create table if not exists mallard.migrations (
  filename text primary key,
  hash text not null unique,
  previous_hash text,
  applied_at timestamp not null default current_timestamp
);
```

Notes:

- `previous_hash` forms a hash chain over committed migrations.
- Mallard should verify that on-disk committed migrations form one linear sequence.
- Mallard should verify that database-applied migrations match the on-disk chain before applying new ones.

## Why a Separate Schema

This follows the Graphile Migrate model and keeps internal bookkeeping out of application schemas.

Benefits:

- easy introspection
- clear separation of concerns
- fewer naming collisions
- easier future schema versioning for Mallard itself

## Config Design

## Recommended Default

Use a standalone `mallard.toml`.

Config discovery order:

1. `--config <path>`
2. nearest `mallard.toml` walking upward from the working directory

For now, Mallard supports only `mallard.toml`.

## Why TOML

Compared to `.gmrc`:

- more readable for nested config
- comments are natural
- better fit for Rust tooling
- avoids JSON5 and JS config complexity
- easier to validate strictly

## Recommended Initial Config Shape

```toml
version = 1

[database]
path = "${MALLARD_DB_PATH:-dev.duckdb}"

[shadow]
path = "${MALLARD_SHADOW_PATH:-.mallard/shadow.duckdb}"

[migrations]
dir = "migrations"
internal_schema = "mallard"

[placeholders]
APP_SCHEMA = "main"
```

Rules:

- support `${VAR}` and `${VAR:-default}` env interpolation in string values
- resolve relative paths from the config file directory
- placeholder keys are written without a leading `:` in TOML
- SQL references placeholders with `:NAME`
- `internal_schema` defaults to `mallard` but may be overridden

Example:

```toml
[placeholders]
APP_SCHEMA = "main"
```

then in SQL:

```sql
create schema if not exists :APP_SCHEMA;
```

## CLI Surface

## Target Command Set

Mallard should aim for a Graphile-like command set:

- `init`
- `migrate`
- `watch`
- `commit`
- `uncommit`
- `status`
- `reset`
- `compile`
- `run`

Mallard will intentionally omit `new`.

Reason:

- if Mallard follows the Graphile model, `current.sql` is the authoring surface
- `commit` turns that current migration into the next committed migration
- a separate `new` command adds little value in that model

## Phased Delivery

### Phase 1

- `init`
- `migrate`
- `status`

### Phase 2

- `commit`
- current migration parsing and validation
- shadow database replay validation

### Phase 3

- `watch`
- `compile`
- `run`
- placeholders
- hooks/actions
- include support via `fixtures/`

### Phase 4

- `uncommit`
- optional multifile current migration
- advanced validation and ergonomics

## Command Semantics

### `mallard init`

Creates:

- `mallard.toml`
- `migrations/`
- `migrations/committed/`
- `migrations/current.sql`

It may also create a hidden working directory later, for example `.mallard/`, if Mallard needs local lock or shadow helpers.

It does not need a root connection like Graphile Migrate because DuckDB is file-backed.

### `mallard migrate`

- opens the target DuckDB file
- ensures the internal schema exists
- reads committed migrations from disk
- verifies hash chain and filename ordering
- compares against `<internal_schema>.migrations`
- applies pending committed migrations in order
- records each applied migration in `<internal_schema>.migrations`

This is the production-safe command.

### `mallard status`

Target behavior:

- bit `1`: committed migrations exist on disk that are not yet applied
- bit `2`: `current.sql` is non-empty

If both are true, exit code is `3`.

This mirrors Graphile Migrate closely and gives Mallard a stable shape early, even if `commit` and `watch` land later.

### `mallard commit`

Future behavior:

- read `current.sql`
- validate it against a fresh shadow database
- write a new committed migration file
- reset `current.sql`
- optionally apply the new committed migration to the main database if needed

### `mallard watch`

Future behavior:

- apply committed migrations
- apply current migration
- re-run on file changes

This is the Graphile-like development loop, but it should come after the committed path is solid.

### `mallard uncommit`

Future behavior only.

This should be deferred because it mutates local migration state and is harder to make safe.

### `mallard reset`

DuckDB adaptation:

- close all connections
- delete or replace the database file
- recreate it by opening it again
- re-run committed migrations

This is destructive and should require explicit confirmation flags.

## Migration File Format

Mallard should copy Graphile Migrate's committed migration headers closely.

Example:

```sql
--! Previous: e3b0c44298fc1c149afbf4c8996fb924...
--! Hash: a4c4aeb92c20500f364b12b3771ef3a1...
--! Message: add users table

create table users (
  id bigint primary key,
  email text not null unique
);
```

Rules:

- file body is trimmed, then exactly one trailing newline is written
- committed files are immutable in normal operation
- hash is computed from body plus previous hash
- filenames are zero-padded and lexically sortable

Recommended filename shape:

```text
000001-init.sql
000002-add-users.sql
```

## Hash and History Rules

Mallard should enforce:

- committed migrations must be a single linear sequence
- each committed file stores `previous_hash`
- each committed file stores `hash`
- applied history in the database must match the prefix of on-disk history exactly
- editing a committed migration should fail validation

Possible future escape hatch:

- `--! AllowInvalidHash`

But this should not be in v1.

## Current Migration Model

Mallard should keep the Graphile concept of a current migration.

`migrations/current.sql` is:

- editable
- not yet committed
- intended for iterative development
- eventually transformed into the next committed migration by `mallard commit`

Why create it from the start:

- it makes the long-term workflow visible immediately
- it keeps the project layout stable from day one
- it avoids changing `init` output later

Even before `commit` and `watch` exist, `status` can still report whether `current.sql` is empty or non-empty.

## Shadow Database Design

Mallard should preserve the Graphile idea of a shadow database, but adapt the mechanism.

### Graphile concept

Graphile uses a separate PostgreSQL database to validate committed and current migrations.

### Mallard adaptation

Use a separate DuckDB file path, for example:

```text
.mallard/shadow.duckdb
```

Behavior:

- delete and recreate the shadow file when validating
- apply committed migrations from scratch
- optionally apply `current.sql`
- use this to validate that the schema is buildable from a clean state

The shadow database is optional for the basic `mallard migrate` production path, but it is expected for Graphile-style development features such as `commit`, `watch`, and replay validation.

Important:

- do not compare raw `.duckdb` file bytes
- for now, shadow validation is replay validation only: if the shadow database can be rebuilt cleanly, the migration passes the first safety check

## DuckDB-Specific Differences from Graphile Migrate

## 1. No PostgreSQL advisory locks

Graphile uses DB-native advisory locking.

DuckDB does not offer the same model, and only supports one writer process cleanly.

Mallard should rely on DuckDB's built-in single-writer locking for migration execution in v1 rather than adding a separate CLI lock file.

## 2. No root or superuser database flow

Graphile needs root connections to create and drop databases.

DuckDB databases are files.

Mallard reset semantics are file operations, not SQL `CREATE DATABASE` or `DROP DATABASE`.

Therefore Mallard does not need:

- `rootConnectionString`
- root-only actions
- database owner concepts in the Graphile sense

## 3. No PostgreSQL catalog queries

Graphile inspects `pg_namespace`, `pg_class`, and other PostgreSQL catalogs.

Mallard should use DuckDB metadata via:

- `information_schema`
- DuckDB metadata functions
- direct known-table existence queries in the internal schema

## 4. Different DDL behavior

DuckDB supports transactional DDL, which is good.

But some PostgreSQL assumptions do not carry over:

- no PostgreSQL `search_path` semantics to depend on
- no `SET LOCAL` pattern to copy directly
- some `ALTER TABLE` features are more limited
- `ADD CONSTRAINT` and `DROP CONSTRAINT` support is limited
- some PostgreSQL syntax and operators differ

Mallard docs should explicitly tell users: write DuckDB SQL, not PostgreSQL SQL.

## 5. Different concurrency model

DuckDB supports one writer process, not Graphile-style shared multi-process write patterns.

Mallard should:

- assume a single active migration writer
- fail fast when DuckDB reports the database is busy or locked
- provide clear error messages for busy databases

## Transaction Model

Default rule:

- each committed migration runs inside a transaction

For v1 and early phases:

- do not support `--! no-transaction`

Reason:

- this is largely a PostgreSQL escape hatch
- Mallard should start simple until a real DuckDB need appears

## Placeholder Model

Mallard should support text substitution placeholders, inspired by Graphile Migrate.

Example:

```sql
create schema if not exists :APP_SCHEMA;
```

Configured via:

```toml
[placeholders]
APP_SCHEMA = "main"
```

Rules:

- placeholder syntax is `:NAME`
- names are uppercase alphanumeric plus underscore
- replacement is textual
- Mallard does not auto-escape values
- placeholders are resolved before execution

## Actions and Hooks

Graphile has rich actions.

Mallard should defer most of this until later.

If and when added, keep them DuckDB-native:

- SQL hook files
- command hooks
- target selection such as `main`, `shadow`, or both

Do not add PostgreSQL-shaped hooks such as root-only hooks in v1.

## Recommended Internal Validation Rules

On every `migrate`:

1. load and sort committed migration files
2. verify filename pattern
3. verify hash chain on disk
4. ensure internal schema and table exist
5. read applied migrations from the database
6. ensure applied history matches the prefix of on-disk history
7. apply remaining files in order
8. insert a metadata row after each successful migration

Failure cases should include:

- missing committed file
- modified committed file
- divergent applied history
- duplicate filename
- duplicate hash
- busy database

## Initial Rust Architecture

Suggested modules:

- `config`
- `project`
- `migration_files`
- `migration_hash`
- `migration_state`
- `duckdb_client`
- `commands/init`
- `commands/migrate`
- `commands/status`

Recommended crates to add later:

- `serde`
- `toml`
- `sha2`
- `walkdir`
- `notify` for watch mode

## Implementation Roadmap

### Milestone 1: project and config plumbing

- parse `mallard.toml`
- env interpolation
- config discovery
- project directory bootstrap
- generate `migrations/current.sql`

### Milestone 2: committed migration engine

- committed filename parser
- hash generation and verification
- internal schema creation
- applied migration lookup
- `migrate`

### Milestone 3: status and safety rails

- pending committed detection
- non-empty `current.sql` detection
- destructive reset confirmation
- better error messages

### Milestone 4: current migration workflow

- `commit`
- shadow replay validation
- `current.sql` reset behavior
- stronger current migration formatting and validation

### Milestone 5: developer ergonomics

- `watch`
- includes
- placeholders
- hooks
- compile and run helpers

### Milestone 6: advanced local-history tooling

- `uncommit`
- multifile current migration
- stronger replay and drift tooling

## Open Questions

1. How strict should Mallard be about formatting and comments in `current.sql` before `commit` exists?
2. What is the minimal useful hook model once actions are introduced?

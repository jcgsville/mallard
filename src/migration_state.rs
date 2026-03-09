use anyhow::Result;
use duckdb::{params, Connection};

use crate::config::SqlIdentifier;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedMigration {
    pub filename: String,
    pub hash: String,
    pub previous_hash: Option<String>,
}

pub fn ensure_metadata_storage(
    connection: &Connection,
    internal_schema: &SqlIdentifier,
) -> Result<()> {
    let schema = internal_schema.quoted();
    connection.execute_batch(&format!(
        "CREATE SCHEMA IF NOT EXISTS {schema};\
         CREATE TABLE IF NOT EXISTS {schema}.migrations (\
             filename TEXT PRIMARY KEY,\
             hash TEXT NOT NULL UNIQUE,\
             previous_hash TEXT,\
             applied_at TIMESTAMP NOT NULL DEFAULT current_timestamp\
         );"
    ))?;
    Ok(())
}

pub fn metadata_table_exists(
    connection: &Connection,
    internal_schema: &SqlIdentifier,
) -> Result<bool> {
    let count: i64 = connection.query_row(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = 'migrations'",
        [internal_schema.as_str()],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

pub fn load_applied_migrations(
    connection: &Connection,
    internal_schema: &SqlIdentifier,
) -> Result<Vec<AppliedMigration>> {
    let sql = format!(
        "SELECT filename, hash, previous_hash FROM {}.migrations ORDER BY filename ASC",
        internal_schema.quoted()
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok(AppliedMigration {
            filename: row.get(0)?,
            hash: row.get(1)?,
            previous_hash: row.get(2)?,
        })
    })?;

    let mut applied = Vec::new();
    for row in rows {
        applied.push(row?);
    }

    Ok(applied)
}

pub fn load_applied_migrations_if_present(
    connection: &Connection,
    internal_schema: &SqlIdentifier,
) -> Result<Vec<AppliedMigration>> {
    if metadata_table_exists(connection, internal_schema)? {
        load_applied_migrations(connection, internal_schema)
    } else {
        Ok(Vec::new())
    }
}

pub fn record_applied_migration(
    connection: &Connection,
    internal_schema: &SqlIdentifier,
    migration: &AppliedMigration,
) -> Result<()> {
    let sql = format!(
        "INSERT INTO {}.migrations (filename, hash, previous_hash) VALUES (?, ?, ?)",
        internal_schema.quoted()
    );
    connection.execute(
        &sql,
        params![migration.filename, migration.hash, migration.previous_hash],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use duckdb::Connection;

    use super::{
        ensure_metadata_storage, load_applied_migrations, load_applied_migrations_if_present,
        metadata_table_exists, record_applied_migration, AppliedMigration,
    };
    use crate::config::SqlIdentifier;

    #[test]
    fn creates_and_reads_metadata_rows() {
        let connection = Connection::open_in_memory().unwrap();
        let schema = SqlIdentifier::parse("mallard", "schema").unwrap();
        ensure_metadata_storage(&connection, &schema).unwrap();
        record_applied_migration(
            &connection,
            &schema,
            &AppliedMigration {
                filename: "000001-init.sql".to_string(),
                hash: "a".repeat(64),
                previous_hash: None,
            },
        )
        .unwrap();

        let applied = load_applied_migrations(&connection, &schema).unwrap();

        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].filename, "000001-init.sql");
    }

    #[test]
    fn returns_empty_when_metadata_table_is_missing() {
        let connection = Connection::open_in_memory().unwrap();
        let schema = SqlIdentifier::parse("mallard", "schema").unwrap();

        assert!(!metadata_table_exists(&connection, &schema).unwrap());
        assert!(load_applied_migrations_if_present(&connection, &schema)
            .unwrap()
            .is_empty());
    }
}

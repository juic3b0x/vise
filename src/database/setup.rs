//! Blocking SQLite initialization, identity checks, and atomic migrations.
use super::{APPLICATION_ID, DOWNLOADS_SCHEMA, SCHEMA, SCHEMA_VERSION, State, read_records};
use crate::error::{Error, Result};
use rusqlite::{Connection, TransactionBehavior};
use std::{path::PathBuf, time::Duration};

pub(super) fn open(path: PathBuf) -> Result<State> {
    let mut connection = Connection::open(path)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    let version = validate_identity(&connection)?;
    configure(&connection)?;
    migrate(&mut connection, version)?;
    check_integrity(&connection)?;
    let saved = read_records(&mut connection)?
        .into_iter()
        .map(|record| (record.id, record))
        .collect();
    Ok(State { connection, saved })
}

fn validate_identity(connection: &Connection) -> Result<i32> {
    let application: i32 =
        connection.pragma_query_value(None, "application_id", |row| row.get(0))?;
    let version: i32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
    if version > SCHEMA_VERSION || (application != 0 && application != APPLICATION_ID) {
        return Err(Error::Invalid(
            "unsupported or foreign SQLite journal".into(),
        ));
    }
    if version == 0 {
        let tables: i64 = connection.query_row(
            "SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |row| row.get(0),
        )?;
        if tables != 0 || application != 0 {
            return Err(Error::Invalid(
                "refusing to initialize an existing non-Vise database".into(),
            ));
        }
    } else if !(1..=SCHEMA_VERSION).contains(&version) || application != APPLICATION_ID {
        return Err(Error::Invalid(
            "invalid Vise SQLite journal identity".into(),
        ));
    }
    Ok(version)
}

fn configure(connection: &Connection) -> Result<()> {
    connection.pragma_update(None, "foreign_keys", "ON")?;
    let mode: String = connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(Error::Invalid(
            "SQLite journal requires WAL support on the output filesystem".into(),
        ));
    }
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "wal_autocheckpoint", 1000)?;
    Ok(())
}

fn migrate(connection: &mut Connection, version: i32) -> Result<()> {
    match version {
        0 => initialize(connection),
        1 => migrate_v1(connection),
        2 | 3 => migrate_metadata(connection, version),
        _ => Ok(()),
    }
}

fn initialize(connection: &mut Connection) -> Result<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(DOWNLOADS_SCHEMA)?;
    transaction.execute_batch(SCHEMA)?;
    transaction.pragma_update(None, "application_id", APPLICATION_ID)?;
    transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

fn migrate_v1(connection: &mut Connection) -> Result<()> {
    // Rebuild the parent to extend its CHECK constraint. Foreign keys
    // are disabled only outside the migration transaction; chunk rows
    // and their references keep their original table name throughout.
    connection.pragma_update(None, "foreign_keys", "OFF")?;
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    transaction.execute_batch(
        &DOWNLOADS_SCHEMA.replace("CREATE TABLE downloads (", "CREATE TABLE downloads_new ("),
    )?;
    transaction.execute_batch("INSERT INTO downloads_new(id,url,output_path,total_size,status,created_at,updated_at,ranged,validator,error) SELECT id,url,output_path,total_size,status,created_at,updated_at,ranged,validator,error FROM downloads;
        DROP TABLE downloads; ALTER TABLE downloads_new RENAME TO downloads;
        CREATE INDEX downloads_status ON downloads(status);")?;
    let violation = transaction
        .prepare("PRAGMA foreign_key_check")?
        .exists([])?;
    if violation {
        return Err(Error::Invalid(
            "foreign-key violation during journal migration".into(),
        ));
    }
    transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    transaction.commit()?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    Ok(())
}

fn migrate_metadata(connection: &mut Connection, version: i32) -> Result<()> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if version == 2 {
        transaction.execute_batch("ALTER TABLE downloads ADD COLUMN proxy_url TEXT")?;
    }
    transaction.execute_batch("ALTER TABLE downloads ADD COLUMN cookies_file TEXT")?;
    transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    transaction.commit()?;
    Ok(())
}

fn check_integrity(connection: &Connection) -> Result<()> {
    let check: String = connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
    if check != "ok" {
        return Err(Error::Invalid(format!(
            "SQLite journal integrity check failed: {check}"
        )));
    }
    Ok(())
}

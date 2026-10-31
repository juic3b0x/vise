//! Blocking SQLite initialization, identity checks, and atomic migrations.
use super::{APPLICATION_ID, DOWNLOADS_SCHEMA, SCHEMA, SCHEMA_VERSION, State, read_records};
use crate::error::{Error, Result};
use rusqlite::{Connection, TransactionBehavior};
use std::{path::PathBuf, time::Duration};

pub(super) fn open(path: PathBuf) -> Result<State> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(&path)?;
    let mut connection = Connection::open(&path)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    let version = validate_identity(&connection)?;
    configure(&connection)?;
    migrate(&mut connection, version, &path)?;
    check_integrity(&connection)?;
    let saved = read_records(&mut connection)?
        .into_iter()
        .map(|record| (record.id, record))
        .collect();
    let credentialed = super::credential_ids(&saved);
    Ok(State {
        connection,
        saved,
        credentialed,
    })
}

pub(super) fn validate_identity(connection: &Connection) -> Result<i32> {
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
            "SQLite journal requires WAL support on the home filesystem".into(),
        ));
    }
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "wal_autocheckpoint", 1000)?;
    Ok(())
}

pub(super) fn migrate(
    connection: &mut Connection,
    version: i32,
    path: &std::path::Path,
) -> Result<()> {
    match version {
        0 => initialize(connection),
        1..=4 => super::migration::upgrade_paths(connection, version, path),
        5 => {
            let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            tx.execute_batch("ALTER TABLE downloads ADD COLUMN request_credentials TEXT;")?;
            tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
            tx.commit()?;
            Ok(())
        }
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

pub(super) fn check_integrity(connection: &Connection) -> Result<()> {
    let check: String = connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
    if check != "ok" {
        return Err(Error::Invalid(format!(
            "SQLite journal integrity check failed: {check}"
        )));
    }
    Ok(())
}

/// Check at every credential-bearing write, including unchanged checkpoints.
#[cfg(unix)]
pub(super) fn credentials_permissions(path: &std::path::Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    for suffix in ["", "-wal", "-shm"] {
        let mut name = path.as_os_str().to_os_string();
        name.push(suffix);
        let metadata = match std::fs::metadata(std::path::PathBuf::from(name)) {
            Err(error) if !suffix.is_empty() && error.kind() == std::io::ErrorKind::NotFound => {
                continue;
            }
            result => result?,
        };
        if metadata.permissions().mode() & 0o7777 != 0o600 {
            return Err(Error::Invalid("refusing to persist request credentials: journal and WAL/SHM permissions must be 0600".into()));
        }
    }
    Ok(())
}
#[cfg(windows)]
pub(super) fn credentials_permissions(_path: &std::path::Path) -> Result<()> {
    static WARNING: std::sync::Once = std::sync::Once::new();
    WARNING.call_once(||tracing::warn!("request credentials are persisted in SQLite; protect the home directory with Windows account ACLs"));
    Ok(())
}

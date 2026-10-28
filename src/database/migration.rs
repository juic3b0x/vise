//! Legacy schema conversion and idempotent imports into the global journal.
use super::{
    DOWNLOADS_SCHEMA, Database, SCHEMA_VERSION, decode_download, read_chunks, read_records,
    snapshots, write_rows,
};
use crate::{
    error::{Error, Result},
    model::Record,
    paths,
    rate_limiter::RateLimit,
};
use rusqlite::{
    Connection, OpenFlags, OptionalExtension, Transaction, TransactionBehavior, params,
};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::Duration,
};

pub(super) fn upgrade_paths(connection: &mut Connection, version: i32, path: &Path) -> Result<()> {
    let directory = paths::parent(path)?;
    let output = if directory.file_name().is_some_and(|name| name == ".vise") {
        paths::parent(directory)?
    } else {
        directory
    };
    let output = std::fs::canonicalize(output)?;
    // Disable foreign-key actions only outside the atomic parent-table rebuild.
    connection.pragma_update(None, "foreign_keys", "OFF")?;
    let result = upgrade_transaction(connection, version, &output);
    connection.pragma_update(None, "foreign_keys", "ON")?;
    result
}

fn upgrade_transaction(connection: &mut Connection, version: i32, output: &Path) -> Result<()> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut records = legacy_downloads(&tx, version, output)?;
    read_chunks(&tx, &mut records)?;
    tx.execute_batch(
        &DOWNLOADS_SCHEMA.replace("CREATE TABLE downloads (", "CREATE TABLE downloads_new ("),
    )?;
    tx.execute_batch("DROP TABLE downloads; ALTER TABLE downloads_new RENAME TO downloads; CREATE INDEX downloads_status ON downloads(status);")?;
    write_rows(&tx, records.values())?;
    if tx.prepare("PRAGMA foreign_key_check")?.exists([])? {
        return Err(Error::Invalid(
            "foreign-key violation during path migration".into(),
        ));
    }
    tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
    tx.commit()?;
    Ok(())
}

fn legacy_downloads(
    tx: &Transaction<'_>,
    version: i32,
    output: &Path,
) -> Result<BTreeMap<uuid::Uuid, Record>> {
    let checksums = if version >= 2 {
        "expected_sha256,expected_md5,actual_sha256,actual_md5,verified_at"
    } else {
        "NULL,NULL,NULL,NULL,NULL"
    };
    let proxy = if version >= 3 { "proxy_url" } else { "NULL" };
    let cookies = if version >= 4 { "cookies_file" } else { "NULL" };
    let query = format!(
        "SELECT id,url,output_path,total_size,status,created_at,ranged,validator,error,{checksums},{proxy},{cookies},'','',NULL FROM downloads ORDER BY id"
    );
    let mut statement = tx.prepare(&query)?;
    let mut rows = statement.query([])?;
    let mut records = BTreeMap::new();
    while let Some(row) = rows.next()? {
        let mut record = decode_download(row)?;
        paths::resolve_legacy(&mut record, output)?;
        records.insert(record.id, record);
    }
    Ok(records)
}

impl Database {
    pub(crate) async fn source_imported(&self, source: String) -> Result<bool> {
        self.call(move |state| imported(&state.connection, &source))
            .await
    }

    /// Import all rows, the initial rate, and the source marker in one commit.
    /// Destination/UUID conflicts abort the import without replacing history.
    pub(crate) async fn import_source(
        &self,
        source: String,
        records: Vec<Record>,
        rate: Option<RateLimit>,
    ) -> Result<()> {
        let path = self.path.clone();
        self.call(move |state| {
            if records.iter().any(|r| r.request_credentials.is_some()) {
                super::setup::credentials_permissions(&path)?;
            }
            if imported(&state.connection, &source)? {
                return Ok(());
            }
            let records = snapshots(records)?;
            if records.keys().any(|id| state.saved.contains_key(id)) {
                return Err(Error::Conflict(
                    "legacy journal contains a task ID already present in the global journal"
                        .into(),
                ));
            }
            let tx = state
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            write_rows(&tx, records.values())?;
            if let Some(rate) = rate {
                tx.execute(
                    "INSERT OR IGNORE INTO metadata(key,value) VALUES('limit_rate',?1)",
                    [rate.bytes_per_second().unwrap_or(0).to_string()],
                )?;
            }
            tx.execute("INSERT INTO metadata(key,value) VALUES(?1,'1')", [source])?;
            tx.commit()?;
            state.saved.extend(records);
            Ok(())
        })
        .await
    }

    /// Snapshot a legacy WAL database through SQLite's backup API. Migrations
    /// run only in memory; source databases and their WAL files stay untouched.
    pub(crate) async fn legacy_snapshot(path: PathBuf) -> Result<(Vec<Record>, Option<RateLimit>)> {
        tokio::task::spawn_blocking(move || {
            let source = Connection::open_with_flags(&path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            source.busy_timeout(Duration::from_secs(5))?;
            let version = super::setup::validate_identity(&source)?;
            if version == 0 {
                return Err(Error::Invalid(
                    "legacy SQLite journal is not initialized".into(),
                ));
            }
            let mut copy = Connection::open_in_memory()?;
            rusqlite::backup::Backup::new(&source, &mut copy)?.run_to_completion(
                128,
                Duration::from_millis(10),
                None,
            )?;
            super::setup::check_integrity(&copy)?;
            super::setup::migrate(&mut copy, version, &path)?;
            let rate: Option<String> = copy
                .query_row(
                    "SELECT value FROM metadata WHERE key='limit_rate'",
                    [],
                    |row| row.get(0),
                )
                .optional()?;
            let rate = rate.map(|value| value.parse()).transpose()?;
            Ok((read_records(&mut copy)?, rate))
        })
        .await
        .map_err(|error| Error::Worker(error.to_string()))?
    }
}

fn imported(connection: &Connection, source: &str) -> Result<bool> {
    Ok(connection
        .query_row(
            "SELECT value FROM metadata WHERE key=?1",
            params![source],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .as_deref()
        == Some("1"))
}

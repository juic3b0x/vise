//! Durable, normalized SQLite state journal. Every SQLite operation (including
//! mutex acquisition and connection teardown) runs outside Tokio's async workers.
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use uuid::Uuid;

use crate::{
    error::{Error, Result},
    model::{Chunk, Download, Record, timestamp_millis},
};

const APPLICATION_ID: i32 = 0x5649_5345; // VISE
const SCHEMA_VERSION: i32 = 3;
pub const FILE_NAME: &str = "state.sqlite3";

const DOWNLOADS_SCHEMA: &str = r#"
CREATE TABLE downloads (
    id          TEXT PRIMARY KEY NOT NULL,
    url         TEXT NOT NULL,
    output_path TEXT NOT NULL UNIQUE,
    total_size  INTEGER CHECK (total_size IS NULL OR total_size >= 0),
    status      TEXT NOT NULL CHECK (status IN ('queued','probing','downloading','verifying','paused','completed','failed','corrupt')),
    created_at  INTEGER NOT NULL CHECK (created_at >= 0),
    updated_at  INTEGER NOT NULL CHECK (updated_at >= 0),
    ranged      INTEGER NOT NULL CHECK (ranged IN (0,1)),
    validator   TEXT,
    error       TEXT,
    expected_sha256 TEXT CHECK (expected_sha256 IS NULL OR length(expected_sha256)=64),
    expected_md5 TEXT CHECK (expected_md5 IS NULL OR length(expected_md5)=32),
    actual_sha256 TEXT CHECK (actual_sha256 IS NULL OR length(actual_sha256)=64),
    actual_md5 TEXT CHECK (actual_md5 IS NULL OR length(actual_md5)=32),
    verified_at INTEGER CHECK (verified_at IS NULL OR verified_at>=0),
    proxy_url TEXT
) STRICT;
"#;
const SCHEMA: &str = r#"
CREATE INDEX downloads_status ON downloads(status);
CREATE TABLE chunks (
    download_id      TEXT NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,
    chunk_index      INTEGER NOT NULL CHECK (chunk_index >= 0),
    start_byte       INTEGER NOT NULL CHECK (start_byte >= 0),
    end_byte         INTEGER CHECK (end_byte IS NULL OR end_byte >= start_byte),
    downloaded_bytes INTEGER NOT NULL CHECK (downloaded_bytes >= 0 AND
                         (end_byte IS NULL OR downloaded_bytes <= end_byte - start_byte)),
    status          TEXT NOT NULL CHECK (status IN ('queued','downloading','paused','completed','failed')),
    PRIMARY KEY (download_id, chunk_index)
) STRICT;
CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL) STRICT;
"#;

struct State {
    connection: Connection,
    // Durable snapshots only; transient speed samples are excluded. Unchanged
    // downloads don't cause row rewrites or idle WAL transactions.
    saved: BTreeMap<Uuid, Record>,
}

struct Shared(Mutex<Option<State>>);
impl Drop for Shared {
    fn drop(&mut self) {
        let state = self
            .0
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn_blocking(move || drop(state));
        } else {
            drop(state);
        }
    }
}

#[derive(Clone)]
pub struct Database {
    state: Arc<Shared>,
    path: Arc<PathBuf>,
    gate: Arc<tokio::sync::Mutex<()>>,
}

impl Database {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let open_path = path.clone();
        let state = tokio::task::spawn_blocking(move || -> Result<State> {
            let mut connection = Connection::open(open_path)?;
            connection.busy_timeout(Duration::from_secs(5))?;
            let application: i32 = connection.pragma_query_value(None, "application_id", |row| row.get(0))?;
            let version: i32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
            if version > SCHEMA_VERSION || (application != 0 && application != APPLICATION_ID) {
                return Err(Error::Invalid("unsupported or foreign SQLite journal".into()));
            }
            if version == 0 {
                let tables: i64 = connection.query_row("SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'", [], |row| row.get(0))?;
                if tables != 0 || application != 0 { return Err(Error::Invalid("refusing to initialize an existing non-Vise database".into())); }
            } else if !(1..=SCHEMA_VERSION).contains(&version) || application != APPLICATION_ID {
                return Err(Error::Invalid("invalid Vise SQLite journal identity".into()));
            }
            connection.pragma_update(None, "foreign_keys", "ON")?;
            let mode: String = connection.query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))?;
            if !mode.eq_ignore_ascii_case("wal") { return Err(Error::Invalid("SQLite journal requires WAL support on the output filesystem".into())); }
            connection.pragma_update(None, "synchronous", "FULL")?;
            connection.pragma_update(None, "wal_autocheckpoint", 1000)?;
            if version == 0 {
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(DOWNLOADS_SCHEMA)?;
                transaction.execute_batch(SCHEMA)?;
                transaction.pragma_update(None, "application_id", APPLICATION_ID)?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
            } else if version == 1 {
                // Rebuild the parent to extend its CHECK constraint. Foreign keys
                // are disabled only outside the migration transaction; chunk rows
                // and their references keep their original table name throughout.
                connection.pragma_update(None, "foreign_keys", "OFF")?;
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch(&DOWNLOADS_SCHEMA.replace("CREATE TABLE downloads (", "CREATE TABLE downloads_new ("))?;
                transaction.execute_batch("INSERT INTO downloads_new(id,url,output_path,total_size,status,created_at,updated_at,ranged,validator,error) SELECT id,url,output_path,total_size,status,created_at,updated_at,ranged,validator,error FROM downloads;
                    DROP TABLE downloads; ALTER TABLE downloads_new RENAME TO downloads;
                    CREATE INDEX downloads_status ON downloads(status);")?;
                let violation = transaction.prepare("PRAGMA foreign_key_check")?.exists([])?;
                if violation { return Err(Error::Invalid("foreign-key violation during journal migration".into())); }
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
                connection.pragma_update(None, "foreign_keys", "ON")?;
            } else if version == 2 {
                let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
                transaction.execute_batch("ALTER TABLE downloads ADD COLUMN proxy_url TEXT")?;
                transaction.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                transaction.commit()?;
            }
            let check: String = connection.query_row("PRAGMA quick_check(1)", [], |row| row.get(0))?;
            if check != "ok" { return Err(Error::Invalid(format!("SQLite journal integrity check failed: {check}"))); }
            let saved = read_records(&mut connection)?.into_iter().map(|record| (record.id, record)).collect();
            Ok(State { connection, saved })
        }).await.map_err(|error| Error::Worker(error.to_string()))??;
        Ok(Self {
            state: Arc::new(Shared(Mutex::new(Some(state)))),
            path: Arc::new(path),
            gate: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    async fn call<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut State) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let state = self.state.clone();
        // Move this permit INTO the blocking operation. Cancelling its async
        // caller cannot let a newer snapshot overtake a still-running commit.
        let permit = self.gate.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let mut guard = state
                .0
                .lock()
                .map_err(|_| Error::Worker("SQLite connection mutex poisoned".into()))?;
            operation(
                guard
                    .as_mut()
                    .ok_or_else(|| Error::Worker("SQLite journal is closed".into()))?,
            )
        })
        .await
        .map_err(|error| Error::Worker(error.to_string()))?
    }

    /// Read a transaction-consistent durable view. Active UI snapshots come from
    /// Manager's live cache, while restart recovery always loads these SQL rows.
    pub async fn list(&self) -> Result<Vec<Download>> {
        self.call(|state| {
            Ok(read_records(&mut state.connection)?
                .iter()
                .map(Record::snapshot)
                .collect())
        })
        .await
    }

    pub(crate) async fn load(&self) -> Result<Vec<Record>> {
        self.call(|state| read_records(&mut state.connection)).await
    }

    pub(crate) async fn load_rate_limit(&self) -> Result<crate::rate_limiter::RateLimit> {
        self.call(|state| {
            let value: Option<String> = state
                .connection
                .query_row(
                    "SELECT value FROM metadata WHERE key='limit_rate'",
                    [],
                    |row| row.get(0),
                )
                .optional()?;
            value.as_deref().unwrap_or("off").parse()
        })
        .await
    }

    pub(crate) async fn save_rate_limit(&self, rate: crate::rate_limiter::RateLimit) -> Result<()> {
        self.call(move |state| {
            let transaction = state.connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute("INSERT INTO metadata(key,value) VALUES ('limit_rate',?1) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [rate.bytes_per_second().unwrap_or(0).to_string()])?;
            transaction.commit()?;
            Ok(())
        }).await
    }

    pub(crate) async fn needs_legacy_import(&self) -> Result<bool> {
        self.call(|state| Ok(!import_done(&state.connection)?))
            .await
    }

    /// Import and the completion marker commit together. An empty import also
    /// records completion, preventing stale JSON from resurrecting deleted tasks.
    pub(crate) async fn import_legacy(&self, records: Vec<Record>) -> Result<()> {
        self.call(move |state| {
            if import_done(&state.connection)? {
                return Ok(());
            }
            let count: i64 =
                state
                    .connection
                    .query_row("SELECT COUNT(*) FROM downloads", [], |row| row.get(0))?;
            if count != 0 {
                return Err(Error::Invalid(
                    "refusing legacy import into a nonempty SQLite journal".into(),
                ));
            }
            let records = snapshots(records)?;
            let transaction = state
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            write_rows(&transaction, records.values())?;
            transaction.execute(
                "INSERT INTO metadata(key,value) VALUES ('legacy_import_complete','1')",
                [],
            )?;
            transaction.commit()?;
            state.saved = records;
            Ok(())
        })
        .await
    }

    /// One atomic snapshot transaction covers parent rows, every changed segment,
    /// segment layout resets, and cascading task deletion. The engine synchronizes
    /// partial files before this method and serializes resets with checkpoints.
    pub(crate) async fn persist(&self, records: Vec<Record>) -> Result<()> {
        self.call(move |state| {
            let records = snapshots(records)?;
            if state.saved == records {
                return Ok(());
            }
            let transaction = state
                .connection
                .transaction_with_behavior(TransactionBehavior::Immediate)?;
            for id in state.saved.keys().filter(|id| !records.contains_key(id)) {
                transaction.execute("DELETE FROM downloads WHERE id=?1", [id.to_string()])?;
            }
            write_rows(
                &transaction,
                records
                    .values()
                    .filter(|record| state.saved.get(&record.id) != Some(record)),
            )?;
            transaction.commit()?;
            state.saved = records;
            Ok(())
        })
        .await
    }
}

fn import_done(connection: &Connection) -> Result<bool> {
    Ok(connection
        .query_row(
            "SELECT value FROM metadata WHERE key='legacy_import_complete'",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .as_deref()
        == Some("1"))
}

fn snapshots(records: Vec<Record>) -> Result<BTreeMap<Uuid, Record>> {
    let mut snapshots = BTreeMap::new();
    for mut record in records {
        record.speed = 0.0;
        if snapshots.insert(record.id, record).is_some() {
            return Err(Error::Invalid(
                "duplicate download ID in journal snapshot".into(),
            ));
        }
    }
    Ok(snapshots)
}

fn sql_number(value: u64) -> Result<i64> {
    value.try_into().map_err(|_| {
        Error::Invalid("download offset exceeds SQLite's signed 64-bit integer range".into())
    })
}

fn unsigned(value: i64) -> Result<u64> {
    value
        .try_into()
        .map_err(|_| Error::Invalid("negative byte offset in SQLite journal".into()))
}

fn write_rows<'a>(
    transaction: &Transaction<'_>,
    records: impl Iterator<Item = &'a Record>,
) -> Result<()> {
    let mut download = transaction.prepare_cached(r#"
        INSERT INTO downloads(id,url,output_path,total_size,status,created_at,updated_at,ranged,validator,error,expected_sha256,expected_md5,actual_sha256,actual_md5,verified_at,proxy_url)
        VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16)
        ON CONFLICT(id) DO UPDATE SET url=excluded.url, output_path=excluded.output_path,
            total_size=excluded.total_size, status=excluded.status, updated_at=excluded.updated_at,
            ranged=excluded.ranged, validator=excluded.validator, error=excluded.error,
            expected_sha256=excluded.expected_sha256, expected_md5=excluded.expected_md5,
            actual_sha256=excluded.actual_sha256, actual_md5=excluded.actual_md5, verified_at=excluded.verified_at,
            proxy_url=excluded.proxy_url
    "#)?;
    let mut chunk = transaction.prepare_cached(r#"
        INSERT INTO chunks(download_id,chunk_index,start_byte,end_byte,downloaded_bytes,status)
        VALUES (?1,?2,?3,?4,?5,?6)
        ON CONFLICT(download_id,chunk_index) DO UPDATE SET start_byte=excluded.start_byte,
            end_byte=excluded.end_byte, downloaded_bytes=excluded.downloaded_bytes, status=excluded.status
    "#)?;
    for record in records {
        let id = record.id.to_string();
        download.execute(params![
            id,
            record.url,
            record.filename,
            record.total_bytes.map(sql_number).transpose()?,
            record.status.as_str(),
            record.created_at,
            timestamp_millis(),
            i64::from(record.ranged),
            record.validator,
            record.error,
            record.checksums.expected_sha256,
            record.checksums.expected_md5,
            record.checksums.actual_sha256,
            record.checksums.actual_md5,
            record.checksums.verified_at,
            record
                .proxy_url
                .as_ref()
                .map(crate::proxy::ProxyUrl::as_str)
        ])?;
        transaction.execute(
            "DELETE FROM chunks WHERE download_id=?1 AND chunk_index>=?2",
            params![id, sql_number(record.chunks.len() as u64)?],
        )?;
        for (index, segment) in record.chunks.iter().enumerate() {
            chunk.execute(params![
                id,
                sql_number(index as u64)?,
                sql_number(segment.start)?,
                segment.end.map(sql_number).transpose()?,
                sql_number(segment.written)?,
                segment.status.as_str()
            ])?;
        }
    }
    Ok(())
}

fn read_records(connection: &mut Connection) -> Result<Vec<Record>> {
    let transaction = connection.transaction()?;
    let mut records = BTreeMap::new();
    {
        let mut statement = transaction.prepare("SELECT id,url,output_path,total_size,status,created_at,ranged,validator,error,expected_sha256,expected_md5,actual_sha256,actual_md5,verified_at,proxy_url FROM downloads ORDER BY id")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let id = Uuid::parse_str(&id)
                .map_err(|error| Error::Invalid(format!("invalid journal download ID: {error}")))?;
            let status: String = row.get(4)?;
            records.insert(
                id,
                Record {
                    id,
                    url: row.get(1)?,
                    filename: row.get(2)?,
                    proxy_url: row
                        .get::<_, Option<String>>(14)?
                        .map(|value| value.parse())
                        .transpose()?,
                    total_bytes: row.get::<_, Option<i64>>(3)?.map(unsigned).transpose()?,
                    status: status.parse()?,
                    created_at: row.get(5)?,
                    ranged: row.get(6)?,
                    validator: row.get(7)?,
                    error: row.get(8)?,
                    checksums: crate::checksum::Checksums {
                        expected_sha256: row.get(9)?,
                        expected_md5: row.get(10)?,
                        actual_sha256: row.get(11)?,
                        actual_md5: row.get(12)?,
                        verified_at: row.get(13)?,
                    },
                    chunks: Vec::new(),
                    speed: 0.0,
                },
            );
        }
    }
    {
        let mut statement = transaction.prepare("SELECT download_id,chunk_index,start_byte,end_byte,downloaded_bytes,status FROM chunks ORDER BY download_id,chunk_index")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let id = Uuid::parse_str(&id)
                .map_err(|error| Error::Invalid(format!("invalid journal chunk ID: {error}")))?;
            let record = records
                .get_mut(&id)
                .ok_or_else(|| Error::Invalid("orphan chunk in SQLite journal".into()))?;
            let index = unsigned(row.get(1)?)?;
            if index != record.chunks.len() as u64 {
                return Err(Error::Invalid(
                    "non-contiguous chunk indices in SQLite journal".into(),
                ));
            }
            let status: String = row.get(5)?;
            record.chunks.push(Chunk {
                start: unsigned(row.get(2)?)?,
                end: row.get::<_, Option<i64>>(3)?.map(unsigned).transpose()?,
                written: unsigned(row.get(4)?)?,
                status: status.parse()?,
            });
        }
    }
    transaction.commit()?;
    Ok(records.into_values().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ChunkStatus, Status};

    #[tokio::test]
    async fn migrates_v1_atomically_without_losing_chunks_or_configuration() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FILE_NAME);
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch("CREATE TABLE downloads(id TEXT PRIMARY KEY NOT NULL,url TEXT NOT NULL,output_path TEXT NOT NULL UNIQUE,total_size INTEGER,status TEXT NOT NULL CHECK(status IN ('queued','probing','downloading','paused','completed','failed')),created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,ranged INTEGER NOT NULL,validator TEXT,error TEXT) STRICT;").unwrap();
        connection.execute_batch(SCHEMA).unwrap();
        connection
            .pragma_update(None, "application_id", APPLICATION_ID)
            .unwrap();
        connection.pragma_update(None, "user_version", 1).unwrap();
        let original = record();
        connection
            .execute(
                "INSERT INTO downloads VALUES(?1,?2,?3,1024,'paused',1234,1234,1,'\"v1\"',NULL)",
                params![original.id.to_string(), original.url, original.filename],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO chunks VALUES(?1,0,0,1024,256,'paused')",
                [original.id.to_string()],
            )
            .unwrap();
        connection
            .execute("INSERT INTO metadata VALUES('limit_rate','1048576')", [])
            .unwrap();
        drop(connection);
        let database = Database::open(&path).await.unwrap();
        assert_eq!(database.load().await.unwrap(), vec![original.clone()]);
        assert_eq!(
            database.load_rate_limit().await.unwrap().bytes_per_second(),
            Some(1048576)
        );
        database
            .call(|state| {
                assert_eq!(
                    state
                        .connection
                        .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?,
                    i64::from(SCHEMA_VERSION)
                );
                assert!(
                    !state
                        .connection
                        .prepare("PRAGMA foreign_key_check")?
                        .exists([])?
                );
                assert_eq!(
                    state
                        .connection
                        .pragma_query_value(None, "foreign_keys", |row| row.get::<_, i64>(0))?,
                    1
                );
                Ok(())
            })
            .await
            .unwrap();
        let mut corrupt = original;
        corrupt.status = Status::Corrupt;
        corrupt.checksums.expected_sha256 = Some("0".repeat(64));
        corrupt.checksums.actual_sha256 = Some("1".repeat(64));
        database.persist(vec![corrupt.clone()]).await.unwrap();
        assert_eq!(database.load().await.unwrap(), vec![corrupt]);
    }

    fn record() -> Record {
        Record {
            id: Uuid::new_v4(),
            url: "https://example.com/file.bin".into(),
            filename: "file.bin".into(),
            proxy_url: None,
            status: Status::Paused,
            total_bytes: Some(1024),
            ranged: true,
            validator: Some("\"v1\"".into()),
            chunks: vec![Chunk {
                start: 0,
                end: Some(1024),
                written: 256,
                status: ChunkStatus::Paused,
            }],
            created_at: 1234,
            error: None,
            checksums: Default::default(),
            speed: 0.0,
        }
    }

    #[tokio::test]
    async fn normalized_schema_round_trips_and_cascades() {
        let dir = tempfile::tempdir().unwrap();
        let database = Database::open(dir.path().join(FILE_NAME)).await.unwrap();
        database.import_legacy(Vec::new()).await.unwrap();
        let record = record();
        database.persist(vec![record.clone()]).await.unwrap();
        assert_eq!(database.load().await.unwrap(), vec![record.clone()]);
        assert_eq!(database.list().await.unwrap()[0].created_at, 1234);
        database.call(|state| {
            assert_eq!(state.connection.pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))?, "wal");
            assert_eq!(state.connection.pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))?, 2);
            assert_eq!(state.connection.pragma_query_value(None, "foreign_keys", |row| row.get::<_, i64>(0))?, 1);
            assert_eq!(state.connection.query_row("SELECT chunk_index,start_byte,end_byte,downloaded_bytes,status FROM chunks", [], |row|
                Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?, row.get::<_, i64>(2)?, row.get::<_, i64>(3)?, row.get::<_, String>(4)?)))?,
                (0, 0, 1024, 256, "paused".into()));
            Ok(())
        }).await.unwrap();
        database.persist(Vec::new()).await.unwrap();
        database
            .call(|state| {
                assert_eq!(
                    state
                        .connection
                        .query_row("SELECT COUNT(*) FROM chunks", [], |row| row
                            .get::<_, i64>(0))?,
                    0
                );
                Ok(())
            })
            .await
            .unwrap();
        assert!(!database.needs_legacy_import().await.unwrap());
    }

    #[tokio::test]
    async fn constraint_failure_rolls_back_parent_and_all_chunks() {
        let dir = tempfile::tempdir().unwrap();
        let database = Database::open(dir.path().join(FILE_NAME)).await.unwrap();
        let mut original = record();
        original.chunks = vec![
            Chunk {
                start: 0,
                end: Some(512),
                written: 128,
                status: crate::model::ChunkStatus::Paused,
            },
            Chunk {
                start: 512,
                end: Some(1024),
                written: 128,
                status: crate::model::ChunkStatus::Paused,
            },
        ];
        database.persist(vec![original.clone()]).await.unwrap();
        let mut invalid = original.clone();
        invalid.status = Status::Completed;
        invalid.chunks[0].written = 256;
        invalid.chunks[1].written = 1024; // Failure after parent AND first chunk update.
        assert!(matches!(
            database.persist(vec![invalid]).await,
            Err(Error::Database(_))
        ));
        assert_eq!(database.load().await.unwrap(), vec![original.clone()]);
        let mut changed = original;
        changed.chunks[0].written = 512;
        database.persist(vec![changed.clone()]).await.unwrap();
        assert_eq!(database.load().await.unwrap(), vec![changed]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn database_lock_waits_do_not_block_async_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let database = Database::open(dir.path().join(FILE_NAME)).await.unwrap();
        let external = Connection::open(database.path()).unwrap();
        external.execute_batch("BEGIN IMMEDIATE").unwrap();
        let worker = database.clone();
        let pending = tokio::spawn(async move { worker.persist(vec![record()]).await });
        tokio::time::timeout(
            Duration::from_millis(200),
            tokio::time::sleep(Duration::from_millis(30)),
        )
        .await
        .unwrap();
        assert!(!pending.is_finished());
        external.execute_batch("ROLLBACK").unwrap();
        pending.await.unwrap().unwrap();
        assert_eq!(database.list().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cancellation_does_not_allow_commits_to_overtake_each_other() {
        let dir = tempfile::tempdir().unwrap();
        let database = Database::open(dir.path().join(FILE_NAME)).await.unwrap();
        let (started, entered) = tokio::sync::oneshot::channel();
        let (release, released) = std::sync::mpsc::channel();
        let worker = database.clone();
        let first = tokio::spawn(async move {
            worker
                .call(move |_| {
                    let _ = started.send(());
                    released.recv().unwrap();
                    Ok(())
                })
                .await
        });
        entered.await.unwrap();
        first.abort();
        let worker = database.clone();
        let next = tokio::spawn(async move { worker.persist(vec![record()]).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(!next.is_finished());
        release.send(()).unwrap();
        next.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn migrates_v2_and_round_trips_authenticated_proxy_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FILE_NAME);
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch("CREATE TABLE downloads(id TEXT PRIMARY KEY NOT NULL,url TEXT NOT NULL,output_path TEXT NOT NULL UNIQUE,total_size INTEGER,status TEXT NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,ranged INTEGER NOT NULL,validator TEXT,error TEXT,expected_sha256 TEXT,expected_md5 TEXT,actual_sha256 TEXT,actual_md5 TEXT,verified_at INTEGER) STRICT;").unwrap();
        connection.execute_batch(SCHEMA).unwrap();
        connection
            .pragma_update(None, "application_id", APPLICATION_ID)
            .unwrap();
        connection.pragma_update(None, "user_version", 2).unwrap();
        let mut original = record();
        original.checksums.expected_sha256 = Some("0".repeat(64));
        connection.execute("INSERT INTO downloads VALUES(?1,?2,?3,1024,'paused',1234,1234,1,'\"v1\"',NULL,?4,NULL,NULL,NULL,NULL)", params![original.id.to_string(), original.url, original.filename, original.checksums.expected_sha256]).unwrap();
        connection
            .execute(
                "INSERT INTO chunks VALUES(?1,0,0,1024,256,'paused')",
                [original.id.to_string()],
            )
            .unwrap();
        drop(connection);
        let database = Database::open(&path).await.unwrap();
        assert_eq!(database.load().await.unwrap(), vec![original.clone()]);
        original.proxy_url = Some(
            "socks5h://alice:p%40ssword@proxy.example:1080"
                .parse()
                .unwrap(),
        );
        database.persist(vec![original.clone()]).await.unwrap();
        assert_eq!(database.load().await.unwrap(), vec![original.clone()]);
        let public = database.list().await.unwrap().remove(0);
        assert!(public.proxy_authenticated);
        assert_eq!(
            public.proxy_url.as_deref(),
            Some("socks5h://proxy.example:1080")
        );
        assert!(!serde_json::to_string(&public).unwrap().contains("ssword"));
        database
            .call(|state| {
                assert_eq!(
                    state
                        .connection
                        .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))?,
                    i64::from(SCHEMA_VERSION)
                );
                assert!(
                    !state
                        .connection
                        .prepare("PRAGMA foreign_key_check")?
                        .exists([])?
                );
                Ok(())
            })
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn rejects_foreign_databases_and_future_schema_versions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(FILE_NAME);
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("CREATE TABLE unrelated(value TEXT)")
            .unwrap();
        drop(connection);
        assert!(Database::open(&path).await.is_err());
        let path = dir.path().join("future.sqlite3");
        let connection = Connection::open(&path).unwrap();
        connection
            .pragma_update(None, "application_id", APPLICATION_ID)
            .unwrap();
        connection.pragma_update(None, "user_version", 999).unwrap();
        drop(connection);
        assert!(Database::open(path).await.is_err());
    }
}

//! Read-only logical snapshots; neither equality nor diagnostics depend on WAL layout.
use rusqlite::{Connection, OpenFlags, types::ValueRef};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fmt, path::Path, time::Duration};

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Snapshot {
    user_version: i64,
    tables: BTreeMap<String, Table>,
    pub download_ids: Vec<String>,
}

#[derive(PartialEq, Eq)]
struct Table {
    schema: Option<String>,
    columns: Vec<String>,
    rows: Vec<Vec<u8>>,
}

// Rows include private request_credentials bytes. Never derive Debug for Table
// or print SQL values on a mismatch: report only counts, lengths and a digest.
impl fmt::Debug for Table {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut digest = Sha256::new();
        let mut bytes = 0;
        for value in self
            .schema
            .iter()
            .map(String::as_bytes)
            .chain(self.columns.iter().map(String::as_bytes))
            .chain(self.rows.iter().map(Vec::as_slice))
        {
            bytes += value.len();
            digest.update((value.len() as u64).to_le_bytes());
            digest.update(value);
        }
        output
            .debug_struct("Table")
            .field("rows", &self.rows.len())
            .field("bytes", &bytes)
            .field("sha256", &format_args!("{:x}", digest.finalize()))
            .finish()
    }
}

impl Snapshot {
    pub fn row_counts(&self) -> BTreeMap<&str, usize> {
        self.tables
            .iter()
            .map(|(name, table)| (name.as_str(), table.rows.len()))
            .collect()
    }
}

pub(super) fn snapshot(path: &Path) -> Snapshot {
    let mut connection =
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    connection.busy_timeout(Duration::from_secs(5)).unwrap();
    // One read transaction covers schema, version, IDs and every table, including
    // committed WAL frames. Opening read-only must not migrate or initialize it.
    let transaction = connection.transaction().unwrap();
    let user_version = transaction
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    let definitions = transaction
        .prepare("SELECT name,sql FROM sqlite_master WHERE type='table' ORDER BY name")
        .unwrap()
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get(1)?)))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let tables = definitions
        .into_iter()
        .map(|(name, schema)| {
            let table = read_table(&transaction, &name, schema);
            (name, table)
        })
        .collect();
    let download_ids = transaction
        .prepare("SELECT id FROM downloads ORDER BY id")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    transaction.commit().unwrap();
    Snapshot {
        user_version,
        tables,
        download_ids,
    }
}

fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn read_table(connection: &Connection, name: &str, schema: Option<String>) -> Table {
    let mut columns = connection
        .prepare(&format!("PRAGMA table_xinfo({})", identifier(name)))
        .unwrap()
        .query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, i64>(5)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    // PK ordinal handles composite keys; all-column tie breakers also cover
    // tables without a PK and SQLite's nullable non-integer primary keys.
    columns.sort_by_key(|(_, pk)| if *pk == 0 { i64::MAX } else { *pk });
    let order = columns
        .iter()
        .map(|(column, _)| identifier(column))
        .collect::<Vec<_>>()
        .join(",");
    let mut query = connection
        .prepare(&format!(
            "SELECT * FROM {} ORDER BY {order}",
            identifier(name)
        ))
        .unwrap();
    let names = query
        .column_names()
        .into_iter()
        .map(str::to_owned)
        .collect();
    let count = query.column_count();
    let rows = query
        .query_map([], |row| {
            let mut bytes = Vec::new();
            for index in 0..count {
                encode(&mut bytes, row.get_ref(index)?);
            }
            Ok(bytes)
        })
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    Table {
        schema,
        columns: names,
        rows,
    }
}

fn encode(output: &mut Vec<u8>, value: ValueRef<'_>) {
    let (tag, bytes) = match value {
        ValueRef::Null => (0, Vec::new()),
        ValueRef::Integer(value) => (1, value.to_le_bytes().to_vec()),
        ValueRef::Real(value) => (2, value.to_bits().to_le_bytes().to_vec()),
        ValueRef::Text(value) => (3, value.to_vec()),
        ValueRef::Blob(value) => (4, value.to_vec()),
    };
    output.push(tag);
    output.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    output.extend_from_slice(&bytes);
}

#[test]
fn logical_journal_snapshot_survives_wal_checkpoint() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state.sqlite3");
    let writer = Connection::open(&path).unwrap();
    writer
        .execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
        PRAGMA user_version=6;
        CREATE TABLE downloads(id TEXT PRIMARY KEY, request_credentials TEXT);
        INSERT INTO downloads VALUES ('saved','private');
        CREATE TABLE extra(part INTEGER, seq INTEGER, value BLOB, PRIMARY KEY(part,seq));
        INSERT INTO extra VALUES (2,1,x'0001'),(1,2,NULL);
        CREATE TABLE unkeyed(value TEXT);
        INSERT INTO unkeyed VALUES ('b'),('a');",
        )
        .unwrap();
    let before = snapshot(&path);
    writer
        .execute_batch(
            "PRAGMA wal_checkpoint(TRUNCATE);
        DELETE FROM unkeyed; INSERT INTO unkeyed VALUES ('a'),('b');",
        )
        .unwrap();
    assert_eq!(snapshot(&path), before);
}

#[test]
fn logical_journal_snapshot_detects_changes_without_printing_credentials() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("state.sqlite3");
    let writer = Connection::open(&path).unwrap();
    writer
        .execute_batch(
            "CREATE TABLE downloads(id TEXT PRIMARY KEY, request_credentials TEXT);
        CREATE TABLE extra(value INTEGER); INSERT INTO extra VALUES (1);",
        )
        .unwrap();
    let secret = "private-journal-sentinel-never-print";
    writer
        .execute("INSERT INTO downloads VALUES ('saved',?1)", [secret])
        .unwrap();
    let before = snapshot(&path);
    assert!(!format!("{before:?}").contains(secret));
    writer
        .execute(
            "UPDATE downloads SET request_credentials=?1",
            [format!("{secret}-changed")],
        )
        .unwrap();
    let changed = snapshot(&path);
    assert_ne!(changed, before);
    assert!(!format!("{changed:?}").contains(secret));
    writer.execute_batch("UPDATE extra SET value=2").unwrap();
    assert_ne!(snapshot(&path), changed);
    let before_version = snapshot(&path);
    writer.pragma_update(None, "user_version", 6).unwrap();
    assert_ne!(snapshot(&path), before_version);
}

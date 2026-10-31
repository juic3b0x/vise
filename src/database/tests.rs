use super::*;
use crate::model::{ChunkStatus, Status};
use std::time::Duration;

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
    let mut original = record();
    crate::paths::resolve_legacy(&mut original, &std::fs::canonicalize(dir.path()).unwrap())
        .unwrap();
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
    database
        .persist_records(vec![corrupt.clone()])
        .await
        .unwrap();
    assert_eq!(database.load().await.unwrap(), vec![corrupt]);
}

pub(super) fn record() -> Record {
    let id = Uuid::new_v4();
    let output = std::env::temp_dir().join(id.to_string());
    Record {
        id,
        output_path: output.join("file.bin"),
        part_path: output.join(".vise").join(format!("{id}.part")),
        url: "https://example.com/file.bin".into(),
        filename: "file.bin".into(),
        proxy_url: None,
        cookies_file: None,
        request_credentials: None,
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
    database
        .import_source("unit-test".into(), Vec::new(), None)
        .await
        .unwrap();
    let record = record();
    database
        .persist_records(vec![record.clone()])
        .await
        .unwrap();
    assert_eq!(database.load().await.unwrap(), vec![record.clone()]);
    assert_eq!(database.list().await.unwrap()[0].created_at, 1234);
    database
        .call(|state| {
            assert_eq!(
                state
                    .connection
                    .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))?,
                "wal"
            );
            assert_eq!(
                state
                    .connection
                    .pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))?,
                2
            );
            assert_eq!(
                state
                    .connection
                    .pragma_query_value(None, "foreign_keys", |row| row.get::<_, i64>(0))?,
                1
            );
            assert_eq!(
                state.connection.query_row(
                    "SELECT chunk_index,start_byte,end_byte,downloaded_bytes,status FROM chunks",
                    [],
                    |row| Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, String>(4)?
                    ))
                )?,
                (0, 0, 1024, 256, "paused".into())
            );
            Ok(())
        })
        .await
        .unwrap();
    database.persist_records(Vec::new()).await.unwrap();
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
    assert!(database.source_imported("unit-test".into()).await.unwrap());
}

#[tokio::test]
async fn legacy_import_conflicts_roll_back_rows_rate_and_source_marker() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::open(dir.path().join(FILE_NAME)).await.unwrap();
    let original = record();
    database
        .persist_records(vec![original.clone()])
        .await
        .unwrap();
    let other = record();
    let mut conflicting = record();
    let destination = conflicting.output_path.clone();
    let staging = conflicting.part_path.clone();
    conflicting.output_path = original.output_path.clone();
    conflicting.part_path = crate::paths::parent(&original.output_path)
        .unwrap()
        .join(".vise")
        .join(format!("{}.part", conflicting.id));
    assert!(
        database
            .import_source(
                "source".into(),
                vec![other.clone(), conflicting.clone()],
                Some("2M".parse().unwrap())
            )
            .await
            .is_err()
    );
    assert_eq!(database.load().await.unwrap(), vec![original.clone()]);
    assert!(!database.source_imported("source".into()).await.unwrap());
    assert_eq!(
        database.load_rate_limit().await.unwrap().bytes_per_second(),
        None
    );
    conflicting.output_path = destination;
    conflicting.part_path = staging;
    database
        .import_source(
            "source".into(),
            vec![other, conflicting],
            Some("2M".parse().unwrap()),
        )
        .await
        .unwrap();
    assert_eq!(database.list().await.unwrap().len(), 3);
    assert!(database.source_imported("source".into()).await.unwrap());
    database.persist_records(Vec::new()).await.unwrap();
    database
        .import_source("source".into(), vec![original], None)
        .await
        .unwrap();
    assert!(database.list().await.unwrap().is_empty());
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
    database
        .persist_records(vec![original.clone()])
        .await
        .unwrap();
    let mut invalid = original.clone();
    invalid.status = Status::Completed;
    invalid.chunks[0].written = 256;
    invalid.chunks[1].written = 1024; // Failure after parent AND first chunk update.
    assert!(matches!(
        database.persist_records(vec![invalid]).await,
        Err(Error::Database(_))
    ));
    assert_eq!(database.load().await.unwrap(), vec![original.clone()]);
    let mut changed = original;
    changed.chunks[0].written = 512;
    database
        .persist_records(vec![changed.clone()])
        .await
        .unwrap();
    assert_eq!(database.load().await.unwrap(), vec![changed]);
}

#[tokio::test(flavor = "current_thread")]
async fn database_lock_waits_do_not_block_async_runtime() {
    let dir = tempfile::tempdir().unwrap();
    let database = Database::open(dir.path().join(FILE_NAME)).await.unwrap();
    let external = Connection::open(database.path()).unwrap();
    external.execute_batch("BEGIN IMMEDIATE").unwrap();
    let worker = database.clone();
    let pending = tokio::spawn(async move { worker.persist_records(vec![record()]).await });
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
    let next = tokio::spawn(async move { worker.persist_records(vec![record()]).await });
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
    crate::paths::resolve_legacy(&mut original, &std::fs::canonicalize(dir.path()).unwrap())
        .unwrap();
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
    database
        .persist_records(vec![original.clone()])
        .await
        .unwrap();
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
async fn migrates_v3_and_round_trips_cookie_paths_without_changing_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(FILE_NAME);
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(&DOWNLOADS_SCHEMA.replace(
            ",\n    filename TEXT NOT NULL,\n    part_path TEXT NOT NULL UNIQUE",
            "",
        ))
        .unwrap();
    connection
        .execute_batch("ALTER TABLE downloads DROP COLUMN cookies_file")
        .unwrap();
    connection.execute_batch(SCHEMA).unwrap();
    connection
        .pragma_update(None, "application_id", APPLICATION_ID)
        .unwrap();
    connection.pragma_update(None, "user_version", 3).unwrap();
    let mut original = record();
    crate::paths::resolve_legacy(&mut original, &std::fs::canonicalize(dir.path()).unwrap())
        .unwrap();
    connection.execute("INSERT INTO downloads(id,url,output_path,total_size,status,created_at,updated_at,ranged,validator) VALUES(?1,?2,?3,1024,'paused',1234,1234,1,'\"v1\"')", params![original.id.to_string(), original.url, original.filename]).unwrap();
    connection
        .execute(
            "INSERT INTO chunks VALUES(?1,0,0,1024,256,'paused')",
            [original.id.to_string()],
        )
        .unwrap();
    drop(connection);
    let database = Database::open(&path).await.unwrap();
    assert_eq!(database.load().await.unwrap(), vec![original.clone()]);
    original.cookies_file = Some(dir.path().join("cookies.txt"));
    database
        .persist_records(vec![original.clone()])
        .await
        .unwrap();
    let reopened = Database::open(&path).await.unwrap();
    assert_eq!(reopened.load().await.unwrap(), vec![original]);
    reopened
        .call(|state| {
            assert_eq!(
                state
                    .connection
                    .pragma_query_value(None, "user_version", |row| row.get::<_, i32>(0))?,
                SCHEMA_VERSION
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

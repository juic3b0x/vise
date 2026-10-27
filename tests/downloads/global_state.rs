//! Cross-directory history, legacy imports, and global engine ownership.
use super::fixture::{self, Fixture, Mode, progress, terminal};
use std::{path::Path, time::Duration};
use uuid::Uuid;
use vise::{
    engine::{Config, Manager},
    model::Status,
    rate_limiter::RateLimit,
};

fn config(home: &Path, output: &Path) -> Config {
    Config {
        home_dir: Some(home.into()),
        output_dir: output.into(),
        connections_per_download: 1,
        ..Config::default()
    }
}

async fn assert_part_removed(path: &Path) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match tokio::fs::symlink_metadata(path).await {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) => panic!("cannot inspect staging path {}: {error}", path.display()),
                Ok(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "staging path still exists after completion: {}",
            path.display()
        )
    });
}

#[tokio::test]
async fn changing_output_preserves_destinations_offsets_history_and_the_global_limit() {
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("test-home");
    let first_dir = root.path().join("first");
    let second_dir = root.path().join("second");
    let first = Manager::open(config(&home, &first_dir)).await.unwrap();
    let old = first.add(&fixture.url, Some("same.bin")).await.unwrap();
    progress(&first, old.id).await;
    first.pause(old.id).await.unwrap();
    let saved = first.get(old.id).await.unwrap().downloaded_bytes;
    let limit: RateLimit = "12M".parse().unwrap();
    first.set_rate_limit(limit).await.unwrap();
    first.shutdown().await.unwrap();
    drop(first);
    let second = Manager::open(config(&home, &second_dir)).await.unwrap();
    assert_eq!(
        second.database_path(),
        home.canonicalize().unwrap().join(".vise/state.sqlite3")
    );
    assert_eq!(
        second.rate_configuration().limit_bytes_per_second,
        limit.bytes_per_second()
    );
    let recovered = second.get(old.id).await.unwrap();
    assert_eq!(recovered.downloaded_bytes, saved);
    assert_eq!(
        recovered.output_path,
        first_dir.canonicalize().unwrap().join("same.bin")
    );
    assert!(recovered.part_path.is_absolute());
    let new = second.add(&fixture.url, Some("same.bin")).await.unwrap();
    second.resume_all().await.unwrap();
    for task in [old, new] {
        let done = terminal(&second, task.id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        assert_eq!(
            tokio::fs::read(&done.output_path).await.unwrap(),
            fixture.state.data
        );
        assert_part_removed(&done.part_path).await;
    }
    assert_eq!(second.list().await.len(), 2);
    assert!(!first_dir.join(".vise/state.sqlite3").exists());
    assert!(!second_dir.join(".vise/state.sqlite3").exists());
    second.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn completion_event_observes_committed_output_and_no_staging_file() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::open(config(
        &root.path().join("home"),
        &root.path().join("output"),
    ))
    .await
    .unwrap();
    let mut events = manager.subscribe_events();
    let task = manager.add(&fixture.url, None).await.unwrap();
    let done = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let event = events.recv().await.unwrap();
            if event.kind == vise::events::EventKind::Completed {
                let done = event.download.unwrap();
                if done.id == task.id {
                    break done;
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(done.status, Status::Completed);
    assert_eq!(
        tokio::fs::read(&done.output_path).await.unwrap(),
        fixture.state.data
    );
    assert_part_removed(&done.part_path).await;
    assert_part_removed(&done.part_path.with_extension("published")).await;
    let database = vise::database::Database::open(manager.database_path())
        .await
        .unwrap();
    let saved = database
        .list()
        .await
        .unwrap()
        .into_iter()
        .find(|item| item.id == done.id)
        .unwrap();
    assert_eq!(saved.status, Status::Completed);
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn failed_completion_commit_emits_no_success_and_recovers_the_published_file() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let root = tempfile::tempdir().unwrap();
    let settings = config(&root.path().join("home"), &root.path().join("output"));
    let first = Manager::open(settings.clone()).await.unwrap();
    // Arm the failure before any worker can publish, independent of fixture
    // timing or scheduler load. Resume behavior is exercised by recovery below.
    let connection = rusqlite::Connection::open(first.database_path()).unwrap();
    connection
        .execute_batch(
            "CREATE TRIGGER reject_completion BEFORE UPDATE OF status ON downloads
        WHEN NEW.status='completed' BEGIN SELECT RAISE(ABORT, 'injected completion failure'); END;",
        )
        .unwrap();
    let mut events = first.subscribe_events();
    let task = first.add(&fixture.url, None).await.unwrap();
    let failed = terminal(&first, task.id).await;
    assert_eq!(failed.status, Status::Failed);
    assert!(
        failed
            .error
            .as_deref()
            .unwrap()
            .contains("injected completion failure")
    );
    first.shutdown().await.unwrap();
    while let Ok(event) = events.try_recv() {
        assert_ne!(event.kind, vise::events::EventKind::Completed);
    }
    assert_part_removed(&failed.part_path).await;
    let evidence = failed.part_path.with_extension("published");
    assert!(same_file::is_same_file(&failed.output_path, &evidence).unwrap());
    connection
        .execute_batch("DROP TRIGGER reject_completion;")
        .unwrap();
    drop(connection);
    drop(first);
    let requests = fixture.state.ranges.lock().unwrap().len();
    let recovered = Manager::open(settings).await.unwrap();
    assert_eq!(
        recovered.get(task.id).await.unwrap().status,
        Status::Completed
    );
    assert_part_removed(&failed.part_path).await;
    assert_part_removed(&evidence).await;
    assert_eq!(
        tokio::fs::read(&failed.output_path).await.unwrap(),
        fixture.state.data
    );
    assert_eq!(requests, fixture.state.ranges.lock().unwrap().len());
    recovered.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn renamed_publication_recovers_on_either_side_of_the_sql_commit() {
    let fixture = Fixture::start(Mode::Ranges).await;
    for (status, restore_started) in [
        ("downloading", false),
        ("downloading", true),
        ("completed", false),
    ] {
        let root = tempfile::tempdir().unwrap();
        let settings = config(&root.path().join("home"), &root.path().join("output"));
        let first = Manager::open(settings.clone()).await.unwrap();
        let task = first.add(&fixture.url, None).await.unwrap();
        let done = terminal(&first, task.id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        first.shutdown().await.unwrap();
        let database_path = first.database_path().to_path_buf();
        drop(first);
        let evidence = done.part_path.with_extension("published");
        tokio::fs::hard_link(&done.output_path, &evidence)
            .await
            .unwrap();
        if restore_started {
            tokio::fs::hard_link(&evidence, &done.part_path)
                .await
                .unwrap();
        }
        let connection = rusqlite::Connection::open(database_path).unwrap();
        connection
            .execute(
                "UPDATE downloads SET status=?1 WHERE id=?2",
                [status, &task.id.to_string()],
            )
            .unwrap();
        drop(connection);
        let requests = fixture.state.ranges.lock().unwrap().len();
        let manager = Manager::open(settings).await.unwrap();
        assert_eq!(
            manager.get(task.id).await.unwrap().status,
            Status::Completed
        );
        assert_part_removed(&done.part_path).await;
        assert_part_removed(&evidence).await;
        assert_eq!(
            tokio::fs::read(&done.output_path).await.unwrap(),
            fixture.state.data
        );
        assert_eq!(requests, fixture.state.ranges.lock().unwrap().len());
        manager.shutdown().await.unwrap();
    }
    fixture.close().await;
}

#[tokio::test]
async fn batch_exit_status_ignores_paused_history_and_keeps_the_saved_global_rate() {
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("test-home");
    let output = root.path().join("first");
    let first = Manager::open(config(&home, &output)).await.unwrap();
    let old = first.add(&fixture.url, None).await.unwrap();
    progress(&first, old.id).await;
    first.pause(old.id).await.unwrap();
    first.set_rate_limit("4M".parse().unwrap()).await.unwrap();
    first.shutdown().await.unwrap();
    drop(first);
    let mut command = fixture::command(root.path());
    command
        .args(["--batch", "-o"])
        .arg(root.path().join("second"))
        .arg(&fixture.url);
    let result = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let manager = Manager::open(config(&home, &output)).await.unwrap();
    assert_eq!(manager.get(old.id).await.unwrap().status, Status::Paused);
    assert_eq!(
        manager.rate_configuration().limit_bytes_per_second,
        Some(4 * 1024 * 1024)
    );
    assert_eq!(manager.list().await.len(), 2);
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn temporarily_missing_destination_preserves_offsets_until_it_returns() {
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let output = root.path().join("first");
    let alternate = root.path().join("second");
    let manager = Manager::open(config(&home, &output)).await.unwrap();
    let task = manager.add(&fixture.url, None).await.unwrap();
    progress(&manager, task.id).await;
    manager.pause(task.id).await.unwrap();
    let saved = manager.get(task.id).await.unwrap().downloaded_bytes;
    manager.shutdown().await.unwrap();
    drop(manager);
    let offline = root.path().join("offline");
    std::fs::rename(&output, &offline).unwrap();
    let manager = Manager::open(config(&home, &alternate)).await.unwrap();
    assert_eq!(manager.get(task.id).await.unwrap().downloaded_bytes, saved);
    assert_eq!(manager.get(task.id).await.unwrap().status, Status::Failed);
    manager.shutdown().await.unwrap();
    drop(manager);
    std::fs::rename(offline, &output).unwrap();
    let manager = Manager::open(config(&home, &alternate)).await.unwrap();
    manager.resume_all().await.unwrap();
    let done = terminal(&manager, task.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(
        tokio::fs::read(done.output_path).await.unwrap(),
        fixture.state.data
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn global_lock_and_private_permissions_apply_across_output_directories() {
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let first = Manager::open(config(&home, &root.path().join("first")))
        .await
        .unwrap();
    let error = Manager::open(config(&home, &root.path().join("second")))
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("global engine"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(home.join(".vise"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            std::fs::metadata(home.join(".vise/state.sqlite3"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    first.shutdown().await.unwrap();
}

fn legacy_sqlite(
    output: &Path,
    fixture: &Fixture,
    id: Uuid,
    written: usize,
) -> rusqlite::Connection {
    let directory = output.join(".vise");
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join(format!("{id}.part"));
    std::fs::write(&path, &fixture.state.data[..written]).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .unwrap()
        .set_len(fixture.state.data.len() as u64)
        .unwrap();
    let connection = rusqlite::Connection::open(directory.join("state.sqlite3")).unwrap();
    connection.execute_batch("PRAGMA journal_mode=WAL;
        CREATE TABLE downloads(id TEXT PRIMARY KEY NOT NULL,url TEXT NOT NULL,output_path TEXT NOT NULL UNIQUE,total_size INTEGER,status TEXT NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,ranged INTEGER NOT NULL,validator TEXT,error TEXT,expected_sha256 TEXT,expected_md5 TEXT,actual_sha256 TEXT,actual_md5 TEXT,verified_at INTEGER,proxy_url TEXT,cookies_file TEXT) STRICT;
        CREATE TABLE chunks(download_id TEXT NOT NULL REFERENCES downloads(id) ON DELETE CASCADE,chunk_index INTEGER NOT NULL,start_byte INTEGER NOT NULL,end_byte INTEGER,downloaded_bytes INTEGER NOT NULL,status TEXT NOT NULL,PRIMARY KEY(download_id,chunk_index)) STRICT;
        CREATE TABLE metadata(key TEXT PRIMARY KEY NOT NULL,value TEXT NOT NULL) STRICT;
        INSERT INTO metadata VALUES('limit_rate','2097152');
        PRAGMA application_id=1447646021; PRAGMA user_version=4;
    ").unwrap();
    connection.execute("INSERT INTO downloads VALUES(?1,?2,'legacy.bin',?3,'downloading',1234,1234,1,'\"version-0\"',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)", rusqlite::params![id.to_string(), fixture.url, fixture.state.data.len() as i64]).unwrap();
    connection
        .execute(
            "INSERT INTO chunks VALUES(?1,0,0,?2,?3,'downloading')",
            rusqlite::params![
                id.to_string(),
                fixture.state.data.len() as i64,
                written as i64
            ],
        )
        .unwrap();
    connection
}

#[tokio::test]
async fn sqlite_wal_import_preserves_offsets_and_never_resurrects_deleted_tasks() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let output = root.path().join("legacy");
    let id = Uuid::new_v4();
    let written = 256 * 1024;
    let source = legacy_sqlite(&output, &fixture, id, written);
    let manager = Manager::open(config(&home, &output)).await.unwrap();
    let restored = manager.get(id).await.unwrap();
    assert_eq!(restored.downloaded_bytes, written as u64);
    assert_eq!(restored.created_at, 1234);
    assert_eq!(
        manager.rate_configuration().limit_bytes_per_second,
        Some(2 * 1024 * 1024)
    );
    manager.resume(id).await.unwrap();
    let done = terminal(&manager, id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(
        fixture.state.ranges.lock().unwrap().first().unwrap().0,
        written
    );
    assert_eq!(
        source
            .pragma_query_value(None, "user_version", |row| row.get::<_, i32>(0))
            .unwrap(),
        4
    );
    manager.delete(id).await.unwrap();
    manager.shutdown().await.unwrap();
    drop(manager);
    let manager = Manager::open(config(&home, &output)).await.unwrap();
    assert!(manager.list().await.is_empty());
    assert_eq!(
        source
            .query_row("SELECT COUNT(*) FROM downloads", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn imports_multiple_legacy_directories_without_replacing_global_history() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let root = tempfile::tempdir().unwrap();
    let home = root.path().join("home");
    let mut ids = Vec::new();
    for name in ["first", "second"] {
        let output = root.path().join(name);
        let id = Uuid::new_v4();
        ids.push(id);
        let source = legacy_sqlite(&output, &fixture, id, 1024);
        let manager = Manager::open(config(&home, &output)).await.unwrap();
        assert_eq!(manager.list().await.len(), ids.len());
        for id in &ids {
            assert_eq!(manager.get(*id).await.unwrap().downloaded_bytes, 1024);
        }
        manager.shutdown().await.unwrap();
        drop(source);
    }
    fixture.close().await;
}

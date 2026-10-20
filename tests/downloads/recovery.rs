use super::fixture::{Fixture, Mode, manager, progress, terminal};
use std::time::Duration;
use uuid::Uuid;
use vise::engine::Config;
use vise::engine::Manager;
use vise::model::Status;

#[tokio::test]
async fn pause_resume_survives_process_restart() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let first_manager = manager(&dir, 2).await;
    let item = first_manager.add(&fixture.url, None).await.unwrap();
    progress(&first_manager, item.id).await;
    first_manager.pause(item.id).await.unwrap();
    let paused = first_manager.get(item.id).await.unwrap();
    assert_eq!(paused.status, Status::Paused);
    assert!(paused.downloaded_bytes > 0);
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        first_manager.get(item.id).await.unwrap().downloaded_bytes,
        paused.downloaded_bytes
    );
    let database = vise::database::Database::open(dir.path().join(".vise/state.sqlite3"))
        .await
        .unwrap();
    let durable = database.list().await.unwrap();
    assert_eq!(durable[0].downloaded_bytes, paused.downloaded_bytes);
    assert_eq!(durable[0].created_at, paused.created_at);
    assert_eq!(durable[0].status, Status::Paused);
    drop(database);
    let connection = rusqlite::Connection::open(dir.path().join(".vise/state.sqlite3")).unwrap();
    let expected: Vec<(usize, usize)> = connection.prepare("SELECT start_byte+downloaded_bytes,end_byte-1 FROM chunks WHERE downloaded_bytes < end_byte-start_byte ORDER BY chunk_index").unwrap()
        .query_map([], |row| Ok((row.get::<_, i64>(0)? as usize, row.get::<_, i64>(1)? as usize))).unwrap().collect::<Result<_, _>>().unwrap();
    drop(connection);
    first_manager.shutdown().await.unwrap();
    drop(first_manager);
    let manager = manager(&dir, 2).await;
    assert_eq!(
        manager.get(item.id).await.unwrap().downloaded_bytes,
        paused.downloaded_bytes
    );
    let before = fixture.state.ranges.lock().unwrap().len();
    manager.resume(item.id).await.unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(
        tokio::fs::read(dir.path().join(done.filename))
            .await
            .unwrap(),
        fixture.state.data
    );
    let mut actual = fixture.state.ranges.lock().unwrap()[before..].to_vec();
    let mut expected = expected;
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        actual, expected,
        "resume must not request already-journaled bytes"
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn abrupt_process_death_restores_durable_offsets_and_finishes_exactly() {
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let dir = tempfile::tempdir().unwrap();
    let mut process = super::fixture::command(dir.path())
        .args(["--batch", "--connections", "1", "--output"])
        .arg(dir.path())
        .arg(&fixture.url)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let saved = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let path = dir.path().join(".vise/state.sqlite3");
            let saved = tokio::task::spawn_blocking(move || -> Option<(Uuid, u64)> {
                let connection = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).ok()?;
                connection.busy_timeout(Duration::from_millis(100)).ok()?;
                connection.query_row("SELECT d.id, COALESCE(SUM(c.downloaded_bytes),0) FROM downloads d LEFT JOIN chunks c ON c.download_id=d.id GROUP BY d.id", [], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
                }).ok().and_then(|(id, bytes)| id.parse().ok().and_then(|id| u64::try_from(bytes).ok().map(|bytes| (id, bytes))))
            }).await.unwrap();
            if let Some((id, written)) = saved {
                if written > 0 && written < fixture.state.data.len() as u64 { return (id, written); }
            }
            if process.try_wait().unwrap().is_some() {
                panic!("child exited before a progress checkpoint");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    process.kill().unwrap();
    process.wait().unwrap();
    let (id, written) = saved.unwrap();
    let manager = manager(&dir, 1).await;
    let recovered = manager.get(id).await.unwrap();
    assert_eq!(recovered.status, Status::Paused);
    assert!(recovered.downloaded_bytes >= written);
    manager.resume(id).await.unwrap();
    let done = terminal(&manager, id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(
        tokio::fs::read(dir.path().join(done.filename))
            .await
            .unwrap(),
        fixture.state.data
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn lifecycle_events_are_ordered_and_deletion_is_durable() {
    use vise::events::EventKind;
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 2).await;
    let mut events = manager.subscribe_events();
    let item = manager.add(&fixture.url, None).await.unwrap();
    assert_eq!(events.recv().await.unwrap().kind, EventKind::Added);
    progress(&manager, item.id).await;
    manager.pause(item.id).await.unwrap();
    manager.resume(item.id).await.unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed);
    manager.delete(item.id).await.unwrap();
    let mut kinds = Vec::new();
    while let Ok(event) = events.try_recv() {
        kinds.push(event.kind);
    }
    for kind in [
        EventKind::PauseRequested,
        EventKind::Paused,
        EventKind::Resumed,
        EventKind::Completed,
        EventKind::DeleteRequested,
        EventKind::Deleted,
    ] {
        assert!(kinds.contains(&kind), "missing {kind:?}: {kinds:?}");
    }
    let completed = kinds
        .iter()
        .position(|kind| *kind == EventKind::Completed)
        .unwrap();
    let deleted = kinds
        .iter()
        .position(|kind| *kind == EventKind::Deleted)
        .unwrap();
    assert!(completed < deleted);
    assert!(dir.path().join(&done.filename).exists());
    let active = manager.add(&fixture.url, None).await.unwrap();
    progress(&manager, active.id).await;
    manager.pause(active.id).await.unwrap();
    let (resumed, removed) = tokio::join!(manager.resume(active.id), manager.delete(active.id));
    assert!(resumed.is_ok() || matches!(resumed, Err(vise::error::Error::NotFound(_))));
    removed.unwrap();
    assert!(manager.list().await.is_empty());
    assert!(
        !dir.path()
            .join(".vise")
            .join(format!("{}.part", active.id))
            .exists()
    );
    assert!(!dir.path().join(&active.filename).exists());
    manager.shutdown().await.unwrap();
    drop(manager);
    let manager = self::manager(&dir, 2).await;
    assert!(manager.list().await.is_empty());
    assert!(dir.path().join(done.filename).exists());
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn shutdown_waits_for_all_workers_even_when_checkpointing_fails() {
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 1).await;
    let active = manager.add(&fixture.url, None).await.unwrap();
    let queued = manager.add(&fixture.url, None).await.unwrap();
    progress(&manager, active.id).await;
    // Abort real SQLite updates to exercise transaction failure during shutdown.
    let connection = rusqlite::Connection::open(dir.path().join(".vise/state.sqlite3")).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_updates BEFORE UPDATE ON downloads BEGIN SELECT RAISE(ABORT, 'test checkpoint failure'); END;").unwrap();
    assert!(manager.shutdown().await.is_err());
    let snapshot = manager.get(active.id).await.unwrap();
    assert_eq!(snapshot.status, Status::Paused);
    assert_eq!(manager.get(queued.id).await.unwrap().status, Status::Paused);
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        manager.get(active.id).await.unwrap().downloaded_bytes,
        snapshot.downloaded_bytes
    );
    connection
        .execute_batch("DROP TRIGGER reject_updates")
        .unwrap();
    drop(connection);
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn legacy_json_is_imported_once_and_resumes_without_redownloading_saved_bytes() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join(".vise");
    tokio::fs::create_dir(&state_dir).await.unwrap();
    let id = Uuid::new_v4();
    let written = 256 * 1024;
    let part = state_dir.join(format!("{id}.part"));
    tokio::fs::write(&part, &fixture.state.data[..written])
        .await
        .unwrap();
    tokio::fs::OpenOptions::new()
        .write(true)
        .open(&part)
        .await
        .unwrap()
        .set_len(fixture.state.data.len() as u64)
        .await
        .unwrap();
    let legacy = serde_json::json!({"version":1,"downloads":[{
        "id":id,"url":fixture.url,"filename":"legacy.bin","status":"downloading",
        "total_bytes":fixture.state.data.len(),"ranged":true,"validator":"\"version-0\"",
        "chunks":[{"start":0,"end":fixture.state.data.len(),"written":written}],"error":null
    }]});
    tokio::fs::write(
        state_dir.join("state.json"),
        serde_json::to_vec(&legacy).unwrap(),
    )
    .await
    .unwrap();
    let manager = manager(&dir, 2).await;
    assert!(state_dir.join("state.json.migrated").exists());
    assert!(!state_dir.join("state.json").exists());
    let restored = manager.get(id).await.unwrap();
    assert_eq!(restored.status, Status::Paused);
    assert_eq!(restored.downloaded_bytes, written as u64);
    assert!(restored.created_at > 0);
    manager.resume(id).await.unwrap();
    let done = terminal(&manager, id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(fixture.state.ranges.lock().unwrap()[0].0, written);
    assert_eq!(
        tokio::fs::read(dir.path().join(&done.filename))
            .await
            .unwrap(),
        fixture.state.data
    );
    manager.delete(id).await.unwrap();
    manager.shutdown().await.unwrap();
    drop(manager);
    // Simulate a crash after import commit but before the JSON backup rename.
    tokio::fs::copy(
        state_dir.join("state.json.migrated"),
        state_dir.join("state.json"),
    )
    .await
    .unwrap();
    let manager = self::manager(&dir, 2).await;
    assert!(
        manager.list().await.is_empty(),
        "stale JSON must never resurrect deleted rows"
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn invalid_legacy_json_is_preserved_and_can_be_repaired() {
    let dir = tempfile::tempdir().unwrap();
    let state_dir = dir.path().join(".vise");
    tokio::fs::create_dir(&state_dir).await.unwrap();
    tokio::fs::write(state_dir.join("state.json"), b"invalid JSON")
        .await
        .unwrap();
    assert!(
        Manager::open(Config {
            output_dir: dir.path().into(),
            ..Default::default()
        })
        .await
        .is_err()
    );
    assert_eq!(
        tokio::fs::read(state_dir.join("state.json")).await.unwrap(),
        b"invalid JSON"
    );
    tokio::fs::write(
        state_dir.join("state.json"),
        br#"{"version":1,"downloads":[]}"#,
    )
    .await
    .unwrap();
    let manager = manager(&dir, 1).await;
    assert!(manager.list().await.is_empty());
    assert!(state_dir.join("state.json.migrated").exists());
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovers_a_published_file_before_sql_completion_without_any_http_transfer() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let first = manager(&dir, 2).await;
    let item = first.add(&fixture.url, None).await.unwrap();
    let done = terminal(&first, item.id).await;
    assert_eq!(done.status, Status::Completed);
    first.shutdown().await.unwrap();
    drop(first);
    let part = dir.path().join(".vise").join(format!("{}.part", item.id));
    tokio::fs::hard_link(dir.path().join(&item.filename), &part)
        .await
        .unwrap();
    let connection = rusqlite::Connection::open(dir.path().join(".vise/state.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE downloads SET status='downloading' WHERE id=?1",
            [item.id.to_string()],
        )
        .unwrap();
    drop(connection);
    let requests = fixture.state.ranges.lock().unwrap().len();
    let manager = manager(&dir, 2).await;
    assert_eq!(
        manager.get(item.id).await.unwrap().status,
        Status::Completed
    );
    assert!(!part.exists());
    let database = vise::database::Database::open(dir.path().join(".vise/state.sqlite3"))
        .await
        .unwrap();
    assert_eq!(database.list().await.unwrap()[0].status, Status::Completed);
    assert_eq!(requests, fixture.state.ranges.lock().unwrap().len());
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

use super::{tests::record, *};

#[tokio::test]
async fn failed_cancelled_and_ambiguous_commits_preserve_the_saved_cache() {
    let root = tempfile::tempdir().unwrap();
    let database = Database::open(root.path().join(FILE_NAME)).await.unwrap();
    let original = record();
    database
        .persist_records(vec![original.clone()])
        .await
        .unwrap();
    let mut changed = original.clone();
    changed.error = Some("changed".into());
    let external = Connection::open(database.path()).unwrap();
    external.execute_batch("CREATE TRIGGER refuse_cache BEFORE UPDATE ON downloads BEGIN SELECT RAISE(ABORT, 'injected failure'); END;").unwrap();
    assert!(
        database
            .persist_records(vec![changed.clone()])
            .await
            .is_err()
    );
    let cached = database
        .call(|state| Ok(state.saved.clone()))
        .await
        .unwrap();
    assert_eq!(cached.get(&original.id), Some(&original));
    external.execute_batch("DROP TRIGGER refuse_cache").unwrap();
    {
        let gate = database.gate.lock().await;
        let pending = database.persist_records(vec![changed.clone()]);
        tokio::pin!(pending);
        assert!(matches!(
            futures_util::poll!(&mut pending),
            std::task::Poll::Pending
        ));
        drop(gate);
        // Drop the pending caller before it can dispatch a blocking commit.
    }
    assert_eq!(database.load().await.unwrap(), vec![original.clone()]);
    assert_eq!(
        database
            .call(|state| Ok(state.saved.clone()))
            .await
            .unwrap()
            .get(&original.id),
        Some(&original)
    );
    database.reject_next_commit_ack();
    assert!(
        database
            .persist_records(vec![changed.clone()])
            .await
            .is_err()
    );
    assert_eq!(database.load().await.unwrap(), vec![changed.clone()]);
    assert_eq!(
        database
            .call(|state| Ok(state.saved.clone()))
            .await
            .unwrap()
            .get(&original.id),
        Some(&original)
    );
    database
        .persist_records(vec![changed.clone()])
        .await
        .unwrap();
    assert_eq!(
        database
            .call(|state| Ok(state.saved.clone()))
            .await
            .unwrap()
            .get(&original.id),
        Some(&changed)
    );
}

#[tokio::test]
async fn incremental_cache_keeps_unchanged_history_and_applies_deletions_atomically() {
    let root = tempfile::tempdir().unwrap();
    let database = Database::open(root.path().join(FILE_NAME)).await.unwrap();
    let first = record();
    let mut second = first.clone();
    second.id = Uuid::new_v4();
    second.filename = "second.bin".into();
    second.output_path = second.output_path.with_file_name("second.bin");
    second.part_path = second
        .part_path
        .with_file_name(format!("{}.part", second.id));
    database
        .persist_records(vec![first.clone(), second.clone()])
        .await
        .unwrap();
    let id = first.id;
    let pointer = database
        .call(move |state| Ok(state.saved[&id].url.as_ptr() as usize))
        .await
        .unwrap();
    second.error = Some("updated".into());
    database
        .persist_records(vec![first.clone(), second])
        .await
        .unwrap();
    database.persist_records(vec![first.clone()]).await.unwrap();
    assert_eq!(
        database
            .call(move |state| Ok(state.saved[&id].url.as_ptr() as usize))
            .await
            .unwrap(),
        pointer
    );
    assert_eq!(database.load().await.unwrap(), vec![first]);
}

#[tokio::test]
async fn cancelled_caller_cannot_advance_cache_ahead_of_an_inflight_commit() {
    let root = tempfile::tempdir().unwrap();
    let database = Database::open(root.path().join(FILE_NAME)).await.unwrap();
    let original = record();
    database
        .persist_records(vec![original.clone()])
        .await
        .unwrap();
    let mut changed = original.clone();
    changed.error = Some("committed after caller cancellation".into());
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    *database.before_commit.lock().unwrap() = Some((entered, wait));
    let worker = database.clone();
    let expected = changed.clone();
    let pending = tokio::spawn(async move { worker.persist_records(vec![changed]).await });
    ready.await.unwrap();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    let external = Connection::open(database.path()).unwrap();
    let durable: Option<String> = external
        .query_row("SELECT error FROM downloads", [], |row| row.get(0))
        .unwrap();
    assert_eq!(durable, original.error);
    // A submitted blocking transaction remains ordered and completes. Cancellation
    // cannot install provisional cache data or make a later call overtake it.
    release.send(()).unwrap();
    assert_eq!(database.load().await.unwrap(), vec![expected.clone()]);
    assert_eq!(
        database
            .call(|state| Ok(state.saved.clone()))
            .await
            .unwrap()
            .get(&expected.id),
        Some(&expected)
    );
}

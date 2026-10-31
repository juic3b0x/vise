use super::{tests::record, *};

#[tokio::test]
async fn deleted_saved_entries_follow_acknowledged_commits() {
    let root = tempfile::tempdir().unwrap();
    let database = Database::open(root.path().join(FILE_NAME)).await.unwrap();
    let first = record();
    let second = record();
    let originals = vec![first.clone(), second.clone()];
    database.persist_records(originals.clone()).await.unwrap();
    let original_cache = database
        .call(|state| Ok(state.saved.clone()))
        .await
        .unwrap();
    let external = Connection::open(database.path()).unwrap();
    let mut changed = second.clone();
    changed.error = Some("update in deleting transaction".into());

    // The delete executes before this rejected update. Both SQL tables and the
    // saved cache must retain their complete pre-transaction contents.
    external.execute_batch("CREATE TRIGGER refuse_deleting_update BEFORE UPDATE ON downloads BEGIN SELECT RAISE(ABORT, 'injected update after deletion'); END;").unwrap();
    assert!(database.persist_records(vec![changed]).await.is_err());
    assert_eq!(
        database
            .call(|state| Ok(state.saved.clone()))
            .await
            .unwrap(),
        original_cache,
        "failed deletion must retain the entire saved cache"
    );
    let durable = database.load().await.unwrap();
    assert_eq!(durable.len(), 2);
    assert!(originals.iter().all(|record| durable.contains(record)));
    external
        .execute_batch("DROP TRIGGER refuse_deleting_update")
        .unwrap();

    database
        .persist_records(vec![second.clone()])
        .await
        .unwrap();
    let expected_cache = BTreeMap::from([(second.id, second.clone())]);
    assert_eq!(
        database
            .call(|state| Ok(state.saved.clone()))
            .await
            .unwrap(),
        expected_cache,
        "successful deletion must remove the saved entry"
    );
    assert_eq!(database.load().await.unwrap(), vec![second.clone()]);
    let deleted_chunks: i64 = external
        .query_row(
            "SELECT COUNT(*) FROM chunks WHERE download_id=?1",
            [first.id.to_string()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(deleted_chunks, 0, "deletion must cascade to chunks");

    // An identical record must be inserted again, rather than skipped because
    // its old value survived in the saved cache.
    database.persist_records(originals.clone()).await.unwrap();
    let durable = database.load().await.unwrap();
    assert_eq!(
        durable.len(),
        2,
        "identical deleted record must be reinserted"
    );
    assert!(originals.iter().all(|record| durable.contains(record)));

    database.reject_next_commit_ack();
    assert!(
        database
            .persist_records(vec![second.clone()])
            .await
            .is_err()
    );
    assert_eq!(database.load().await.unwrap(), vec![second.clone()]);
    assert_eq!(
        database
            .call(|state| Ok(state.saved.clone()))
            .await
            .unwrap(),
        original_cache,
        "lost deletion acknowledgement must retain the saved cache"
    );
    database.persist_records(vec![second]).await.unwrap();
    assert_eq!(
        database
            .call(|state| Ok(state.saved.clone()))
            .await
            .unwrap(),
        expected_cache,
        "retry must reconcile the deleted saved entry"
    );
}

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

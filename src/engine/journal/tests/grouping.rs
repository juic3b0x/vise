//! Frozen-batch coverage, cancellation, reconciliation and exclusive-operation races.
use super::*;

#[tokio::test]
async fn five_ready_receipts_share_one_capture_and_commit_even_if_one_is_dropped() {
    use std::task::Poll;
    let (_root, manager, job) = completed_job().await;
    for index in 1..5 {
        let mut record = job.record.read().await.clone();
        record.id = Uuid::new_v4();
        record.filename = format!("file-{index}.bin");
        record.output_path = manager.0.config.output_dir.join(&record.filename);
        record.part_path = manager
            .0
            .config
            .output_dir
            .join(".vise")
            .join(format!("{}.part", record.id));
        manager.0.jobs.write().await.insert(
            record.id,
            Arc::new(crate::engine::Job::new(record, reqwest::Client::new())),
        );
    }
    manager.checkpoint().await.unwrap();
    for job in manager.0.jobs.read().await.values() {
        job.record.write().await.error = Some("five ready".into());
    }
    let guard = manager.0.checkpoint.lock().await;
    let (entered, release) = gate(&manager).await;
    let mut requests: Vec<_> = (0..5).map(|_| Box::pin(manager.checkpoint())).collect();
    for request in &mut requests {
        assert!(matches!(
            futures_util::poll!(request.as_mut()),
            Poll::Pending
        ));
    }
    drop(requests.remove(0));
    let mut first_live = requests.remove(0);
    drop(guard);
    tokio::select! {
        _ = entered => {},
        result = first_live.as_mut() => {
            assert!(result.is_ok(), "a dropped receipt cancelled shared work: {result:?}");
            panic!("receipt completed before the capture gate");
        }
    }
    // A second capture would consume this gate. It must remain armed after
    // every receipt in the first frozen group has settled.
    let (mut extra_capture, extra_release) = gate(&manager).await;
    let (first_sql, sql_release) = manager.0.database.gate_next_commit();
    release.send(()).unwrap();
    first_sql.await.unwrap();
    let (mut extra_commit, commit_release) = manager.0.database.gate_next_commit();
    sql_release.send(()).unwrap();
    assert!(
        first_live.await.is_ok(),
        "a dropped receipt cancelled shared work"
    );
    for mut request in requests {
        assert!(
            matches!(futures_util::poll!(request.as_mut()), Poll::Ready(Ok(()))),
            "five ready receipts did not settle together"
        );
    }
    assert!(
        matches!(
            extra_capture.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "five ready requests made an extra capture"
    );
    assert!(
        matches!(
            extra_commit.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "five ready requests made an extra SQLite commit"
    );
    assert!(manager.0.checkpoint_hook.lock().await.take().is_some());
    drop(extra_release);
    // Disarm the still-unused database gate with an unchanged checkpoint:
    // it cannot issue another transaction, so the hook stays untouched.
    let rows = manager.0.database.load().await.unwrap();
    assert_eq!(rows.len(), 5);
    assert!(
        rows.iter()
            .all(|r| r.error.as_deref() == Some("five ready"))
    );
    manager.shutdown().await.unwrap();
    drop(commit_release);
}

#[tokio::test]
async fn late_mutation_requires_its_own_frozen_batch_receipt() {
    use std::task::Poll;
    let (_root, manager, job) = completed_job().await;
    job.record.write().await.error = Some("first batch".into());
    let (entered, release) = gate(&manager).await;
    let worker = manager.clone();
    let first = tokio::spawn(async move { worker.checkpoint().await });
    entered.await.unwrap();
    job.record.write().await.error = Some("late batch".into());
    let mut late = Box::pin(manager.checkpoint());
    assert!(matches!(futures_util::poll!(late.as_mut()), Poll::Pending));
    let (next_entered, next_release) = gate(&manager).await;
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    assert!(
        futures_util::poll!(late.as_mut()).is_pending(),
        "late request borrowed an earlier receipt"
    );
    next_entered.await.unwrap();
    assert_eq!(
        manager.0.database.load().await.unwrap()[0].error.as_deref(),
        Some("first batch"),
        "checkpoint recaptured state after file synchronization"
    );
    assert!(
        matches!(futures_util::poll!(late.as_mut()), Poll::Pending),
        "late request borrowed an earlier receipt"
    );
    next_release.send(()).unwrap();
    late.await.unwrap();
    assert_eq!(
        manager.0.database.load().await.unwrap()[0].error.as_deref(),
        Some("late batch")
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn shared_sql_failure_fails_every_covered_receipt_without_advancing_cache() {
    let (_root, manager, job) = completed_job().await;
    let saved = job.completed_snapshot.lock().unwrap().clone().unwrap();
    job.record.write().await.error = Some("rejected group".into());
    let connection = rusqlite::Connection::open(manager.database_path()).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_group BEFORE UPDATE ON downloads BEGIN SELECT RAISE(ABORT, 'rejected group'); END;").unwrap();
    let guard = manager.0.checkpoint.lock().await;
    let mut requests: Vec<_> = (0..5).map(|_| Box::pin(manager.checkpoint())).collect();
    for request in &mut requests {
        assert!(futures_util::poll!(request.as_mut()).is_pending());
    }
    drop(guard);
    for result in futures_util::future::join_all(requests).await {
        assert!(result.unwrap_err().to_string().contains("rejected group"));
    }
    assert_eq!(manager.0.database.load().await.unwrap()[0].error, None);
    assert!(Arc::ptr_eq(
        job.completed_snapshot.lock().unwrap().as_ref().unwrap(),
        &saved
    ));
    connection
        .execute_batch("DROP TRIGGER reject_group")
        .unwrap();
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn uncertain_sql_outcome_must_reconcile_before_another_capture() {
    use std::sync::atomic::Ordering;
    let (_root, manager, job) = completed_job().await;
    job.record.write().await.error = Some("committed without acknowledgement".into());
    manager.0.database.reject_next_commit_ack();
    assert!(manager.checkpoint().await.is_err());
    manager
        .0
        .database
        .reject_reconciliation
        .store(true, Ordering::Release);
    job.record.write().await.error = Some("must wait for reconciliation".into());
    let (mut captured, capture_release) = gate(&manager).await;
    let result = tokio::select! {
        result = manager.checkpoint() => result,
        _ = &mut captured => {
            let _ = capture_release.send(());
            assert!(!manager.0.database.reject_reconciliation.load(Ordering::Acquire), "capture ran before reconciliation succeeded");
            return;
        }
    };
    assert!(result.is_err(), "an unresolved SQL outcome was bypassed");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("reconciliation failure"),
        "an unresolved SQL outcome was bypassed"
    );
    assert!(
        matches!(
            captured.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "capture ran before reconciliation succeeded"
    );
    assert!(manager.0.checkpoint_hook.lock().await.take().is_some());
    drop(capture_release);
    assert_eq!(
        manager.0.database.load().await.unwrap()[0].error.as_deref(),
        Some("committed without acknowledgement")
    );
    manager
        .0
        .database
        .reject_reconciliation
        .store(false, Ordering::Release);
    manager.checkpoint().await.unwrap();
    assert_eq!(
        manager.0.database.load().await.unwrap()[0].error.as_deref(),
        Some("must wait for reconciliation")
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn enrollment_during_idle_handoff_keeps_an_owner_and_shutdown_drains_it() {
    let (_root, manager, job) = completed_job().await;
    job.record.write().await.error = Some("before handoff".into());
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    *manager.0.journal_group.before_idle.lock().await = Some((entered, wait));
    let worker = manager.clone();
    let first = tokio::spawn(async move { worker.checkpoint().await });
    ready.await.unwrap();
    job.record.write().await.error = Some("during handoff".into());
    let mut late = Box::pin(manager.checkpoint());
    assert!(futures_util::poll!(late.as_mut()).is_pending());
    release.send(()).unwrap();
    first.await.unwrap().unwrap();
    manager.0.journal_group.assert_owned();
    manager.shutdown().await.unwrap();
    late.await.unwrap();
    manager.0.journal_group.assert_owned();
    assert_eq!(
        manager.0.database.load().await.unwrap()[0].error.as_deref(),
        Some("during handoff")
    );
}
#[tokio::test]
async fn deletion_waits_for_older_captured_snapshots_and_cannot_resurrect() {
    let (_root, manager, job) = completed_job().await;
    job.record.write().await.error = Some("captured before deletion".into());
    let id = job.record.read().await.id;
    let (entered, release) = gate(&manager).await;
    let worker = manager.clone();
    let captured = tokio::spawn(async move { worker.checkpoint().await });
    entered.await.unwrap();
    let (at_lock, ready) = tokio::sync::oneshot::channel();
    let (proceed, wait) = tokio::sync::oneshot::channel();
    *manager.0.delete_hook.lock().await = Some((at_lock, wait));
    let mut deletion = Box::pin(manager.delete(id));
    tokio::select! {
        _ = ready => {},
        result = deletion.as_mut() => panic!("deletion escaped its pre-lock gate: {result:?}"),
    }
    proceed.send(()).unwrap();
    assert!(futures_util::poll!(deletion.as_mut()).is_pending());
    assert!(
        manager.0.jobs.read().await.contains_key(&id),
        "deletion bypassed the checkpoint barrier"
    );
    release.send(()).unwrap();
    captured.await.unwrap().unwrap();
    deletion.await.unwrap();
    assert!(
        manager.0.database.load().await.unwrap().is_empty(),
        "an older snapshot resurrected a deleted job"
    );
    manager.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn directory_failure_fails_the_receipt_and_preserves_completed_cache() {
    let (root, manager, job) = completed_job().await;
    let saved = job.completed_snapshot.lock().unwrap().clone().unwrap();
    job.record.write().await.error = Some("SQL only".into());
    let (entered, release) = manager.0.database.gate_next_commit();
    let worker = manager.clone();
    let pending = tokio::spawn(async move { worker.checkpoint().await });
    entered.await.unwrap();
    let directory = manager.database_path().parent().unwrap();
    let backup = root.path().join("temporarily-moved-journal");
    std::fs::rename(directory, &backup).unwrap();
    std::fs::write(directory, b"not a directory").unwrap();
    release.send(()).unwrap();
    assert!(
        pending.await.unwrap().is_err(),
        "directory failure acknowledged success"
    );
    assert!(
        Arc::ptr_eq(
            job.completed_snapshot.lock().unwrap().as_ref().unwrap(),
            &saved
        ),
        "directory failure advanced completed cache"
    );
    std::fs::remove_file(directory).unwrap();
    std::fs::rename(backup, directory).unwrap();
    manager.shutdown().await.unwrap();
}

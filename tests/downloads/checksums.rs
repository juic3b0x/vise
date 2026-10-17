use super::fixture::{Fixture, Mode, expected_hashes, manager, manager_for_checksum, terminal};
use std::sync::atomic::Ordering;
use vise::checksum::ExpectedChecksums;
use vise::model::Status;

#[tokio::test]
async fn checksum_success_is_journaled_before_publication_for_ranges_streams_and_empty_files() {
    for mode in [Mode::Ranges, Mode::UnknownLength, Mode::Empty] {
        let fixture = Fixture::start(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 4).await;
        let expected = expected_hashes(if matches!(mode, Mode::Empty) {
            &[]
        } else {
            &fixture.state.data
        });
        let mut events = manager.subscribe_events();
        let item = manager
            .add_with_checksums(&fixture.url, None, expected.clone())
            .await
            .unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        assert_eq!(done.checksums.actual_sha256, expected.sha256);
        assert_eq!(done.checksums.actual_md5, expected.md5);
        assert!(done.checksums.verified_at.is_some());
        manager.shutdown().await.unwrap();
        assert!(dir.path().join(&done.filename).exists());
        assert!(!dir.path().join(format!(".vise/{}.part", item.id)).exists());
        let mut kinds = Vec::new();
        while let Ok(event) = events.try_recv() {
            kinds.push(event.kind);
        }
        let verifying = kinds
            .iter()
            .position(|kind| {
                matches!(
                    kind,
                    vise::events::EventKind::StateChanged {
                        to: Status::Verifying,
                        ..
                    }
                )
            })
            .unwrap();
        let verified = kinds
            .iter()
            .position(|kind| *kind == vise::events::EventKind::Verified)
            .unwrap();
        let completed = kinds
            .iter()
            .position(|kind| *kind == vise::events::EventKind::Completed)
            .unwrap();
        assert!(verifying < verified && verified < completed);
        drop(manager);
        let restored = manager_for_checksum(&dir).await;
        let saved = restored.get(item.id).await.unwrap();
        assert_eq!(saved.checksums, done.checksums);
        restored.shutdown().await.unwrap();
        fixture.close().await;
    }
}

#[tokio::test]
async fn mismatch_retains_staging_and_survives_restart_then_retry_redownloads_every_chunk() {
    let fixture = Fixture::start(Mode::Ranges).await;
    fixture.state.version.store(1, Ordering::SeqCst);
    let dir = tempfile::tempdir().unwrap();
    let original = manager(&dir, 4).await;
    let expected = expected_hashes(&fixture.state.data);
    let item = original
        .add_with_checksums(&fixture.url, None, expected.clone())
        .await
        .unwrap();
    let bad = terminal(&original, item.id).await;
    assert_eq!(bad.status, Status::Corrupt);
    let actual = expected_hashes(
        &fixture
            .state
            .data
            .iter()
            .map(|b| b.wrapping_add(1))
            .collect::<Vec<_>>(),
    );
    for value in [
        expected.sha256.as_ref().unwrap(),
        expected.md5.as_ref().unwrap(),
        actual.sha256.as_ref().unwrap(),
        actual.md5.as_ref().unwrap(),
    ] {
        assert!(bad.error.as_ref().unwrap().contains(value));
    }
    assert!(bad.checksums.verified_at.is_none());
    assert!(!dir.path().join(&bad.filename).exists());
    assert!(dir.path().join(format!(".vise/{}.part", item.id)).exists());
    original.shutdown().await.unwrap();
    drop(original);
    let restored = manager(&dir, 4).await;
    assert_eq!(restored.get(item.id).await.unwrap().status, Status::Corrupt);
    assert!(restored.resume(item.id).await.is_err());
    restored.resume_all().await.unwrap();
    assert_eq!(restored.get(item.id).await.unwrap().status, Status::Corrupt);
    fixture.state.version.store(0, Ordering::SeqCst);
    let before = fixture.state.ranges.lock().unwrap().len();
    restored.retry(item.id).await.unwrap();
    let done = terminal(&restored, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    let ranges = fixture.state.ranges.lock().unwrap()[before..].to_vec();
    assert_eq!(
        ranges
            .iter()
            .map(|(start, end)| end - start + 1)
            .sum::<usize>(),
        fixture.state.data.len()
    );
    assert_eq!(done.checksums.expected(), expected);
    assert_eq!(
        tokio::fs::read(dir.path().join(&done.filename))
            .await
            .unwrap(),
        fixture.state.data
    );
    restored.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn metadata_digests_verify_whole_representations_and_ignore_partial_message_digests() {
    for mode in [
        Mode::MetadataRepr,
        Mode::MetadataContent,
        Mode::PartialDigest,
    ] {
        let fixture = Fixture::start(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 2).await;
        let item = manager.add(&fixture.url, None).await.unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        if matches!(mode, Mode::PartialDigest) {
            assert!(done.checksums.expected().is_empty());
        } else {
            assert_eq!(
                done.checksums.expected(),
                expected_hashes(&fixture.state.data)
            );
            assert!(done.checksums.verified_at.is_some());
        }
        manager.shutdown().await.unwrap();
        fixture.close().await;
    }
}

#[tokio::test]
async fn explicit_hashes_override_metadata_and_md5_mismatch_alone_blocks_publication() {
    for mode in [Mode::MetadataRepr, Mode::Ranges] {
        let fixture = Fixture::start(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 2).await;
        let sha = if matches!(mode, Mode::MetadataRepr) {
            Some("0".repeat(64))
        } else {
            None
        };
        let md5 = if matches!(mode, Mode::Ranges) {
            Some("0".repeat(32))
        } else {
            None
        };
        let item = manager
            .add_with_checksums(
                &fixture.url,
                None,
                ExpectedChecksums::new(sha.as_deref(), md5.as_deref()).unwrap(),
            )
            .await
            .unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Corrupt);
        assert!(!dir.path().join(&done.filename).exists());
        if let Some(sha) = sha {
            assert_eq!(done.checksums.expected_sha256, Some(sha));
        }
        if let Some(md5) = md5 {
            assert_eq!(done.checksums.expected_md5, Some(md5));
        }
        manager.shutdown().await.unwrap();
        fixture.close().await;
    }
}

#[tokio::test]
async fn interrupted_publication_rechecks_hashes_before_recovering_or_retracting_the_target() {
    for tamper in [false, true] {
        let fixture = Fixture::start(Mode::Ranges).await;
        let dir = tempfile::tempdir().unwrap();
        let original = manager(&dir, 2).await;
        let item = original
            .add_with_checksums(&fixture.url, None, expected_hashes(&fixture.state.data))
            .await
            .unwrap();
        assert_eq!(terminal(&original, item.id).await.status, Status::Completed);
        original.shutdown().await.unwrap();
        drop(original);
        let part = dir.path().join(format!(".vise/{}.part", item.id));
        let target = dir.path().join(&item.filename);
        tokio::fs::hard_link(&target, &part).await.unwrap();
        let connection =
            rusqlite::Connection::open(dir.path().join(".vise/state.sqlite3")).unwrap();
        connection
            .execute(
                "UPDATE downloads SET status='verifying' WHERE id=?1",
                [item.id.to_string()],
            )
            .unwrap();
        drop(connection);
        if tamper {
            use tokio::io::AsyncWriteExt;
            let mut file = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&part)
                .await
                .unwrap();
            file.write_all(b"corrupt bytes").await.unwrap();
            file.sync_all().await.unwrap();
        }
        let requests = fixture.state.ranges.lock().unwrap().len();
        let restored = manager(&dir, 2).await;
        let saved = restored.get(item.id).await.unwrap();
        assert_eq!(
            saved.status,
            if tamper {
                Status::Corrupt
            } else {
                Status::Completed
            }
        );
        assert_eq!(part.exists(), tamper);
        assert_eq!(target.exists(), !tamper);
        assert_eq!(requests, fixture.state.ranges.lock().unwrap().len());
        assert_eq!(saved.checksums.verified_at.is_some(), !tamper);
        restored.shutdown().await.unwrap();
        fixture.close().await;
    }
}

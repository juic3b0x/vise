use super::fixture::{DownloadCase, Fixture, Mode, manager, progress, terminal};
use std::sync::atomic::Ordering;
use std::time::Duration;
use vise::engine::Config;
use vise::engine::Manager;
use vise::model::Status;

#[tokio::test]
async fn parallel_ranges_are_bounded_and_byte_exact() {
    let case = DownloadCase::start(Mode::Ranges, 4).await;
    let first = case.add().await;
    let second = case.add().await;
    assert_ne!(first.filename, second.filename);
    for id in [first.id, second.id] {
        case.assert_complete_bytes(id).await;
    }
    assert!(case.fixture.state.peak.load(Ordering::SeqCst) > 1);
    assert!(case.fixture.state.peak.load(Ordering::SeqCst) <= 4);
    case.close().await;
}

#[tokio::test]
async fn changed_validator_restarts_instead_of_splicing_versions() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 2).await;
    let item = manager.add(&fixture.url, None).await.unwrap();
    progress(&manager, item.id).await;
    manager.pause(item.id).await.unwrap();
    fixture.state.version.store(1, Ordering::SeqCst);
    manager.resume(item.id).await.unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    let expected: Vec<_> = fixture
        .state
        .data
        .iter()
        .map(|b| b.wrapping_add(1))
        .collect();
    assert_eq!(
        tokio::fs::read(dir.path().join(done.filename))
            .await
            .unwrap(),
        expected
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn retries_dropped_ranges_from_written_offset() {
    let case = DownloadCase::start(Mode::Truncated, 1).await;
    let item = case.add().await;
    case.assert_complete_bytes(item.id).await;
    assert!(
        case.fixture
            .state
            .ranges
            .lock()
            .unwrap()
            .iter()
            .any(|(start, _)| *start > 0)
    );
    case.close().await;
}

#[tokio::test]
async fn handles_ignored_ranges_chunked_streams_transient_errors_and_empty_files() {
    for mode in [
        Mode::IgnoreRanges,
        Mode::UnknownLength,
        Mode::Transient,
        Mode::Empty,
        Mode::NoValidator,
        Mode::TruncatedNoValidator,
    ] {
        let fixture = Fixture::start(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 4).await;
        let item = manager.add(&fixture.url, None).await.unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        let output = tokio::fs::read(dir.path().join(done.filename))
            .await
            .unwrap();
        if matches!(mode, Mode::Empty) {
            assert!(output.is_empty());
        } else {
            assert_eq!(output, fixture.state.data);
        }
        manager.shutdown().await.unwrap();
        fixture.close().await;
    }
}

#[tokio::test]
async fn permanent_errors_and_invalid_ranges_do_not_publish_output() {
    for mode in [Mode::NotFound, Mode::BadRange] {
        let fixture = Fixture::start(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 2).await;
        let item = manager.add(&fixture.url, None).await.unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Failed);
        assert!(done.error.is_some());
        assert!(!dir.path().join(done.filename).exists());
        manager.shutdown().await.unwrap();
        fixture.close().await;
    }
}

#[tokio::test]
async fn queued_and_stalled_downloads_pause_promptly() {
    let fixture = Fixture::start(Mode::Stalled).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 2).await;
    let first = manager.add(&fixture.url, None).await.unwrap();
    let second = manager.add(&fixture.url, None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    tokio::time::timeout(Duration::from_secs(1), manager.pause(second.id))
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), manager.pause(first.id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manager.get(first.id).await.unwrap().status, Status::Paused);
    assert_eq!(manager.get(second.id).await.unwrap().status, Status::Paused);
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn refuses_existing_outputs_and_concurrent_managers() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 2).await;
    assert!(
        Manager::open(Config {
            output_dir: dir.path().into(),
            home_dir: Some(dir.path().join("test-home")),
            ..Default::default()
        })
        .await
        .is_err()
    );
    tokio::fs::write(dir.path().join("keep.bin"), b"original")
        .await
        .unwrap();
    assert!(manager.add(&fixture.url, Some("keep.bin")).await.is_err());
    let item = manager.add(&fixture.url, Some("raced.bin")).await.unwrap();
    tokio::fs::write(dir.path().join("raced.bin"), b"external writer")
        .await
        .unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Failed);
    assert_eq!(
        tokio::fs::read(dir.path().join("raced.bin")).await.unwrap(),
        b"external writer"
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn stalled_reads_exhaust_a_bounded_retry_budget() {
    let fixture = Fixture::start(Mode::Stalled).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = Manager::open(Config {
        output_dir: dir.path().into(),
        home_dir: Some(dir.path().join("test-home")),
        concurrent_downloads: 1,
        connections_per_download: 1,
        min_split_size: 1024 * 1024,
        retries: 1,
        read_timeout: Duration::from_millis(100),
        limit_rate: None,
        proxy_url: None,
        cookies_file: None,
        request: Default::default(),
    })
    .await
    .unwrap();
    let item = manager.add(&fixture.url, None).await.unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Failed);
    assert_eq!(fixture.state.ranges.lock().unwrap().len(), 2);
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn missing_completed_file_is_redownloaded_without_reusing_stale_offsets() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let original = manager(&dir, 2).await;
    let item = original.add(&fixture.url, None).await.unwrap();
    let done = terminal(&original, item.id).await;
    assert_eq!(done.status, Status::Completed);
    original.shutdown().await.unwrap();
    drop(original);
    tokio::fs::remove_file(dir.path().join(&done.filename))
        .await
        .unwrap();
    let manager = manager(&dir, 2).await;
    assert_eq!(manager.get(item.id).await.unwrap().status, Status::Failed);
    manager.resume(item.id).await.unwrap();
    let done = terminal(&manager, item.id).await;
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
async fn stateless_downloads_reuse_connections_across_tasks() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let root = tempfile::tempdir().unwrap();
    let manager = manager(&root, 1).await;
    let first = manager.add(&fixture.url, None).await.unwrap();
    assert_eq!(terminal(&manager, first.id).await.status, Status::Completed);
    let previous = fixture.state.peers.lock().unwrap().clone();
    fixture.state.peers.lock().unwrap().clear();
    let second = manager.add(&fixture.url, None).await.unwrap();
    assert_eq!(
        terminal(&manager, second.id).await.status,
        Status::Completed
    );
    let next = fixture.state.peers.lock().unwrap().clone();
    assert!(
        !previous.is_disjoint(&next),
        "a completed task's pooled connection was not reused"
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

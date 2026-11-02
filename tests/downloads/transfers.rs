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
async fn retained_small_bodies_reuse_a_bounded_pool_across_many_tasks() {
    let case = DownloadCase::start(Mode::DelayedProbe, 2).await;
    for _ in 0..24 {
        let item = case.add().await;
        case.assert_complete_bytes(item.id).await;
    }
    assert!(case.fixture.state.peers.lock().unwrap().len() <= 2);
    assert_eq!(case.fixture.state.requests.load(Ordering::SeqCst), 24);
    case.close().await;
}

#[tokio::test]
async fn fresh_single_connection_uses_one_request_and_correct_bytes() {
    for mode in [
        Mode::Ranges,
        Mode::IgnoreRanges,
        Mode::UnknownLength,
        Mode::Empty,
        Mode::NoValidator,
        Mode::MetadataRepr,
        Mode::MetadataContent,
    ] {
        let case = DownloadCase::start(mode, 1).await;
        let item = case.add().await;
        if matches!(mode, Mode::Empty) {
            assert_eq!(case.wait(item.id).await.status, Status::Completed);
            assert!(tokio::fs::read(&item.output_path).await.unwrap().is_empty());
        } else {
            case.assert_complete_bytes(item.id).await;
        }
        assert_eq!(case.fixture.state.requests.load(Ordering::SeqCst), 1);
        case.close().await;
    }
}

#[tokio::test]
async fn fresh_single_connection_pool_stays_bounded_across_many_tasks() {
    let case = DownloadCase::start(Mode::DelayedProbe, 1).await;
    for _ in 0..24 {
        let item = case.add().await;
        case.assert_complete_bytes(item.id).await;
    }
    assert_eq!(case.fixture.state.peers.lock().unwrap().len(), 1);
    assert_eq!(case.fixture.state.requests.load(Ordering::SeqCst), 24);
    case.close().await;
}

#[tokio::test]
async fn interrupted_single_connection_reprobes_and_resumes_saved_offsets() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let original = manager(&dir, 1).await;
    let item = original.add(&fixture.url, None).await.unwrap();
    progress(&original, item.id).await;
    original.pause(item.id).await.unwrap();
    assert_eq!(fixture.state.requests.load(Ordering::SeqCst), 1);
    original.shutdown().await.unwrap();
    drop(original);
    let resumed = manager(&dir, 1).await;
    assert_eq!(resumed.get(item.id).await.unwrap().status, Status::Paused);
    resumed.resume(item.id).await.unwrap();
    let done = terminal(&resumed, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(
        tokio::fs::read(dir.path().join(done.filename))
            .await
            .unwrap(),
        fixture.state.data
    );
    assert_eq!(fixture.state.requests.load(Ordering::SeqCst), 3);
    assert!(
        fixture
            .state
            .ranges
            .lock()
            .unwrap()
            .iter()
            .any(|(start, _)| *start > 0)
    );
    resumed.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn default_budget_retains_small_bodies_and_safely_falls_back_without_identity() {
    for size in [0, 1, 1024 * 1024 - 1, 1024 * 1024, 1024 * 1024 + 1] {
        for mode in [
            Mode::Ranges,
            Mode::NoValidator,
            Mode::WeakValidator,
            Mode::IgnoreRanges,
            Mode::UnknownLength,
            Mode::MetadataRepr,
            Mode::MetadataContent,
            Mode::PartialDigest,
        ] {
            let fixture = Fixture::with_size(mode, Some(size)).await;
            let root = tempfile::tempdir().unwrap();
            let manager = manager(&root, Config::default().connections_per_download).await;
            let item = manager.add(&fixture.url, None).await.unwrap();
            let done = terminal(&manager, item.id).await;
            assert_eq!(done.status, Status::Completed, "{:?}", done.error);
            assert_eq!(
                tokio::fs::read(&done.output_path).await.unwrap(),
                fixture.state.data
            );
            let requests = fixture.state.requests.load(Ordering::SeqCst);
            if size <= 1024 * 1024
                || matches!(
                    mode,
                    Mode::IgnoreRanges | Mode::UnknownLength | Mode::MetadataContent
                )
            {
                assert_eq!(requests, 1, "size {size}");
            } else {
                assert_eq!(requests, 2, "size {size}");
            }
            let ranges = fixture.state.ranges.lock().unwrap().clone();
            if size > 0
                && !matches!(
                    mode,
                    Mode::IgnoreRanges | Mode::UnknownLength | Mode::MetadataContent
                )
            {
                assert_eq!(ranges[0], (0, (size - 1).min(1024 * 1024 - 1)));
                if size > 1024 * 1024 {
                    if matches!(mode, Mode::NoValidator | Mode::WeakValidator) {
                        assert_eq!(ranges[1], (0, size - 1));
                    } else {
                        assert_eq!(ranges[1], (1024 * 1024, size - 1));
                    }
                }
            }
            manager.shutdown().await.unwrap();
            fixture.close().await;
        }
    }
}

#[tokio::test]
async fn default_small_fixed_body_retries_and_remains_rate_limited() {
    let fixture = Fixture::with_size(Mode::Truncated, Some(1024 * 1024)).await;
    let root = tempfile::tempdir().unwrap();
    let manager = manager(&root, 8).await;
    manager.set_rate_limit("8M".parse().unwrap()).await.unwrap();
    let item = manager.add(&fixture.url, None).await.unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(
        tokio::fs::read(&done.output_path).await.unwrap(),
        fixture.state.data
    );
    assert_eq!(fixture.state.requests.load(Ordering::SeqCst), 2);
    assert!(
        fixture
            .state
            .ranges
            .lock()
            .unwrap()
            .iter()
            .any(|(start, _)| *start > 0)
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn retained_prefix_layout_is_exact_and_bounded_with_fixed_and_adaptive_workers() {
    for connections in [1, 4, 8] {
        for minimum in [0, 1024 * 1024] {
            for size in [
                0,
                1,
                1024 * 1024 - 1,
                1024 * 1024,
                1024 * 1024 + 1,
                8 * 1024 * 1024 + 137,
            ] {
                let fixture = Fixture::with_size(Mode::Ranges, Some(size)).await;
                let root = tempfile::tempdir().unwrap();
                let manager = Manager::open(Config {
                    home_dir: Some(root.path().join("test-home")),
                    output_dir: root.path().into(),
                    concurrent_downloads: 1,
                    connections_per_download: connections,
                    min_split_size: minimum,
                    ..Config::default()
                })
                .await
                .unwrap();
                let item = manager.add(&fixture.url, None).await.unwrap();
                let done = terminal(&manager, item.id).await;
                assert_eq!(done.status, Status::Completed, "{:?}", done.error);
                assert_eq!(
                    tokio::fs::read(&done.output_path).await.unwrap(),
                    fixture.state.data
                );
                assert!(fixture.state.peak.load(Ordering::SeqCst) <= connections);
                if size <= 1024 * 1024 || connections == 1 {
                    assert_eq!(fixture.state.requests.load(Ordering::SeqCst), 1);
                }
                let mut ranges = fixture.state.ranges.lock().unwrap().clone();
                ranges.sort_unstable();
                let mut cursor = 0;
                for (start, end) in ranges {
                    assert_eq!(
                        start, cursor,
                        "duplicated or missing payload at C={connections}, size={size}"
                    );
                    cursor = end + 1;
                }
                assert_eq!(cursor, size);
                manager.shutdown().await.unwrap();
                fixture.close().await;
            }
        }
    }
}

#[tokio::test]
async fn progressing_faults_beyond_the_prompt_allowance_keep_offsets_and_retry_budgets() {
    use vise::events::EventKind;
    for (mode, connections, faults, retries, completed, private) in [
        (Mode::BodyFaults, 1, 3, 3, true, false),
        (Mode::CleanBodyFaults, 1, 3, 3, true, true),
        (Mode::BodyFaults, 4, 64, 1, false, true),
        (Mode::BodyFaults, 8, 64, 1, false, false),
        (Mode::EmptyBodyFaults, 1, 2, 1, false, true),
        (Mode::BodyFaults, 1, 1, 0, false, false),
    ] {
        let fixture = Fixture::start(mode).await;
        fixture.state.body_faults.store(faults, Ordering::SeqCst);
        let root = tempfile::tempdir().unwrap();
        let request = if private {
            vise::net::credentials::RequestOverrides::from_cli(
                &["X-Secret: recovery-private-sentinel".into()],
                None,
                None,
            )
            .unwrap()
        } else {
            Default::default()
        };
        let manager = Manager::open(Config {
            output_dir: root.path().into(),
            home_dir: Some(root.path().join("home")),
            connections_per_download: connections,
            min_split_size: 0,
            retries,
            request,
            ..Default::default()
        })
        .await
        .unwrap();
        let mut events = manager.subscribe_events();
        let item = manager.add(&fixture.url, None).await.unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(
            done.status,
            if completed {
                Status::Completed
            } else {
                Status::Failed
            },
            "{:?}",
            done.error
        );
        if completed {
            assert_eq!(
                tokio::fs::read(&done.output_path).await.unwrap(),
                fixture.state.data
            );
            let ranges = fixture.state.ranges.lock().unwrap().clone();
            assert_eq!(ranges.len(), faults + 1);
            for (attempt, (start, _)) in ranges.iter().enumerate() {
                assert_eq!(*start, attempt * 64 * 1024);
            }
        } else {
            assert!(!done.output_path.exists());
        }
        let mut retried = 0;
        while let Ok(event) = events.try_recv() {
            assert!(!format!("{event:?}").contains("recovery-private-sentinel"));
            if let EventKind::Retrying { attempt, .. } = event.kind {
                assert!(attempt <= retries);
                retried += 1;
            }
        }
        assert!(retried <= retries as usize * done.chunks.len());
        if connections == 1 {
            assert_eq!(
                fixture.state.requests.load(Ordering::SeqCst),
                faults.min(retries as usize) + 1
            );
        }
        manager.shutdown().await.unwrap();
        fixture.close().await;
    }
}

#[tokio::test]
async fn retained_initial_bodies_reject_truncation_and_excess_without_publication() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for (body, length, valid) in [
        ("3\r\nabc\r\n0\r\n\r\n", "Transfer-Encoding: chunked", true),
        ("2\r\nab\r\n0\r\n\r\n", "Transfer-Encoding: chunked", false),
        (
            "4\r\nabcd\r\n0\r\n\r\n",
            "Transfer-Encoding: chunked",
            false,
        ),
        ("ab", "Content-Length: 3", false),
        ("abcd", "Content-Length: 4", false),
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/body", listener.local_addr().unwrap());
        let wire = format!(
            "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-2/3\r\nETag: \"body-test\"\r\n{length}\r\nConnection: close\r\n\r\n{body}"
        );
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") {
                request.push(socket.read_u8().await.unwrap());
            }
            assert!(
                String::from_utf8(request)
                    .unwrap()
                    .contains("bytes=0-1048575")
            );
            socket.write_all(wire.as_bytes()).await.unwrap();
        });
        let root = tempfile::tempdir().unwrap();
        let manager = Manager::open(Config {
            output_dir: root.path().into(),
            home_dir: Some(root.path().join("home")),
            retries: 0,
            ..Config::default()
        })
        .await
        .unwrap();
        let item = manager.add(&url, None).await.unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(
            done.status,
            if valid {
                Status::Completed
            } else {
                Status::Failed
            }
        );
        assert_eq!(done.output_path.exists(), valid);
        if valid {
            assert_eq!(tokio::fs::read(&done.output_path).await.unwrap(), b"abc");
        }
        manager.shutdown().await.unwrap();
        server.await.unwrap();
    }
}

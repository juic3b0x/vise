use super::fixture::{DownloadCase, Fixture, Mode, expected_hashes, manager, terminal};
use std::sync::atomic::Ordering;
use vise::{
    engine::{Config, Manager},
    model::Status,
};

fn coverage(item: &vise::model::Download) {
    let mut cursor = 0;
    for (index, c) in item.chunks.iter().enumerate() {
        assert_eq!(c.index, index);
        assert_eq!(c.start_byte, cursor);
        let end = c.end_byte.unwrap();
        assert!(c.downloaded_bytes <= end - c.start_byte);
        cursor = end;
    }
    assert_eq!(Some(cursor), item.total_bytes);
}

#[tokio::test]
async fn heterogeneous_tail_split_is_byte_exact() {
    let fixture = Fixture::start(Mode::Heterogeneous).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = manager(&dir, 4).await;
    let item = manager
        .add_with_checksums(&fixture.url, None, expected_hashes(&fixture.state.data))
        .await
        .unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert!(done.tail_splits > 0);
    assert!(done.chunks.len() > 4);
    coverage(&done);
    assert!(done.checksums.verified_at.is_some());
    assert_eq!(
        tokio::fs::read(&done.output_path).await.unwrap(),
        fixture.state.data
    );
    assert!(fixture.state.peak.load(Ordering::SeqCst) <= 4);
    assert!(
        fixture
            .state
            .if_ranges
            .lock()
            .unwrap()
            .iter()
            .all(|v| v.as_deref() == Some("\"version-0\""))
    );
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn split_validator_change_fails_without_publication() {
    let case = DownloadCase::start(Mode::SplitValidator, 4).await;
    let item = case.add().await;
    let done = case.wait(item.id).await;
    assert_eq!(done.status, Status::Failed);
    assert!(done.tail_splits > 0);
    assert!(!done.output_path.exists());
    assert!(done.part_path.exists());
    coverage(&done);
    case.close().await;
}

#[tokio::test]
async fn no_range_or_validator_never_splits() {
    for mode in [Mode::IgnoreRanges, Mode::UnknownLength, Mode::NoValidator] {
        let fixture = Fixture::start(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 4).await;
        let item = manager.add(&fixture.url, None).await.unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Completed);
        assert_eq!(done.tail_splits, 0);
        assert_eq!(done.chunks.len(), 1);
        assert!(fixture.state.peak.load(Ordering::SeqCst) <= 1);
        manager.shutdown().await.unwrap();
        fixture.close().await;
    }
}

#[tokio::test]
async fn extra_request_refusals_keep_ownership_and_retry_safely() {
    for mode in [Mode::Split429, Mode::Split503, Mode::SplitRefused] {
        let fixture = Fixture::start(mode).await;
        let dir = tempfile::tempdir().unwrap();
        let manager = manager(&dir, 4).await;
        let item = manager
            .add_with_checksums(&fixture.url, None, expected_hashes(&fixture.state.data))
            .await
            .unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        assert!(done.tail_splits > 0);
        coverage(&done);
        assert!(done.checksums.verified_at.is_some());
        manager.shutdown().await.unwrap();
        fixture.close().await;
    }
}

#[tokio::test]
async fn recovered_split_layout_respects_lower_connection_budget() {
    let fixture = Fixture::start(Mode::Heterogeneous).await;
    let dir = tempfile::tempdir().unwrap();
    let original = manager(&dir, 4).await;
    let item = original.add(&fixture.url, None).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if original.get(item.id).await.unwrap().tail_splits > 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    original.pause(item.id).await.unwrap();
    let paused = original.get(item.id).await.unwrap();
    coverage(&paused);
    original.shutdown().await.unwrap();
    drop(original);
    let resumed = Manager::open(Config {
        home_dir: Some(dir.path().join("test-home")),
        output_dir: dir.path().into(),
        connections_per_download: 1,
        min_split_size: 0,
        ..Config::default()
    })
    .await
    .unwrap();
    assert_eq!(resumed.get(item.id).await.unwrap().tail_splits, 0);
    resumed.resume(item.id).await.unwrap();
    let done = terminal(&resumed, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(
        tokio::fs::read(&done.output_path).await.unwrap(),
        fixture.state.data
    );
    coverage(&done);
    resumed.shutdown().await.unwrap();
    fixture.close().await;
}

mod credential_fixture {
    include!("credentials/fixture.rs");
}

#[tokio::test]
async fn split_requests_preserve_proxy_and_origin_rules() {
    use super::proxy_support::{Kind, TestProxy};
    use credential_fixture::{Capture, SECRET, options};
    for cross_origin in [false, true] {
        let source = Capture::start(Mode::Heterogeneous).await;
        let target = if cross_origin {
            Some(Capture::start(Mode::Heterogeneous).await)
        } else {
            None
        };
        *source.redirect.lock().unwrap() = Some(
            target
                .as_ref()
                .map_or_else(|| source.url.clone(), |server| server.url.clone()),
        );
        let proxy = TestProxy::start(Kind::Http, true).await;
        let root = tempfile::tempdir().unwrap();
        let manager = manager(&root, 4).await;
        let mut settings = options();
        settings.proxy_url = Some(proxy.url.parse().unwrap());
        settings.expected = expected_hashes(&source.origin.state.data);
        let item = manager
            .add_with_options(&source.url.replace("/file", "/redirect"), None, settings)
            .await
            .unwrap();
        let done = terminal(&manager, item.id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        assert!(done.tail_splits > 0);
        for (_, headers) in source.requests.lock().unwrap().iter() {
            assert_eq!(headers.get("x-secret").unwrap(), SECRET);
            assert!(headers.get("proxy-authorization").is_none());
        }
        if let Some(target) = &target {
            for (_, headers) in target.requests.lock().unwrap().iter() {
                assert!(headers.get("x-secret").is_none());
                assert!(headers.get("authorization").is_none());
                assert!(headers.get("cookie").is_none());
                assert_eq!(headers.get("referer").unwrap(), "http://source.example/");
            }
        }
        let rendered = serde_json::to_string(&done).unwrap();
        assert!(!rendered.contains(SECRET));
        assert!(!rendered.contains("p%40ssword"));
        let requests = proxy.requests.lock().unwrap().clone();
        assert!(requests.len() > 5);
        // The fixture records only requests that passed its required proxy auth.
        assert!(requests.iter().all(|r| r.method == "GET"));
        assert!(done.checksums.verified_at.is_some());
        manager.shutdown().await.unwrap();
        source.close().await;
        if let Some(target) = target {
            target.close().await;
        }
        proxy.close().await;
    }
}

#[tokio::test]
async fn retry_uses_revised_end() {
    let fixture = Fixture::start(Mode::SplitDonorRetry).await;
    let root = tempfile::tempdir().unwrap();
    let manager = manager(&root, 4).await;
    let item = manager
        .add_with_checksums(&fixture.url, None, expected_hashes(&fixture.state.data))
        .await
        .unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert!(done.tail_splits > 0);
    let original_start = fixture.state.data.len() * 3 / 4;
    let donor = done
        .chunks
        .iter()
        .find(|c| c.start_byte == original_start as u64)
        .unwrap();
    let donor_end = donor.end_byte.unwrap() as usize;
    let ranges = fixture.state.ranges.lock().unwrap().clone();
    assert!(
        ranges.iter().any(|&(start, end)| start > original_start
            && start < donor_end
            && end == donor_end - 1)
    );
    assert!(done.checksums.verified_at.is_some());
    coverage(&done);
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn split_pause_delete_shutdown_join_all_work() {
    for action in ["pause", "delete", "shutdown"] {
        let fixture = Fixture::start(Mode::Heterogeneous).await;
        let root = tempfile::tempdir().unwrap();
        let manager = manager(&root, 4).await;
        let item = manager.add(&fixture.url, None).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if manager.get(item.id).await.unwrap().tail_splits > 0 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        match action {
            "pause" => {
                manager.pause(item.id).await.unwrap();
                let before = manager.get(item.id).await.unwrap();
                assert_eq!(before.status, Status::Paused);
                tokio::task::yield_now().await;
                assert_eq!(
                    manager.get(item.id).await.unwrap().downloaded_bytes,
                    before.downloaded_bytes
                );
                coverage(&before);
            }
            "delete" => {
                manager.delete(item.id).await.unwrap();
                assert!(manager.get(item.id).await.is_err());
                assert!(!item.part_path.exists());
                assert!(
                    vise::database::Database::open(manager.database_path())
                        .await
                        .unwrap()
                        .list()
                        .await
                        .unwrap()
                        .is_empty()
                );
            }
            _ => {
                manager.shutdown().await.unwrap();
                assert_eq!(manager.get(item.id).await.unwrap().status, Status::Paused);
            }
        }
        assert!(!item.output_path.exists());
        manager.shutdown().await.unwrap();
        fixture.close().await;
    }
}

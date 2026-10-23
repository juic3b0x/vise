use super::fixture::{Fixture, Mode, manager, terminal};
use std::sync::atomic::Ordering;
use std::time::Duration;
use vise::engine::Config;
use vise::engine::Manager;
use vise::model::Status;
use vise::rate_limiter::RateConfiguration;
use vise::rate_limiter::RateLimit;

#[tokio::test]
async fn global_rate_budget_is_shared_by_parallel_files_and_chunks() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = Manager::open(Config {
        output_dir: dir.path().into(),
        home_dir: Some(dir.path().join("test-home")),
        concurrent_downloads: 2,
        connections_per_download: 2,
        limit_rate: Some("4M".parse().unwrap()),
        ..Config::default()
    })
    .await
    .unwrap();
    let started = tokio::time::Instant::now();
    let first = manager.add(&fixture.url, None).await.unwrap();
    let second = manager.add(&fixture.url, None).await.unwrap();
    for id in [first.id, second.id] {
        let done = terminal(&manager, id).await;
        assert_eq!(done.status, Status::Completed, "{:?}", done.error);
        assert_eq!(
            tokio::fs::read(dir.path().join(done.filename))
                .await
                .unwrap(),
            fixture.state.data
        );
    }
    assert!(
        started.elapsed() >= Duration::from_millis(3900),
        "each file must share the global 4 MiB/s budget"
    );
    assert!(fixture.state.peak.load(Ordering::SeqCst) > 1);
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[tokio::test]
async fn live_limits_preserve_the_open_response_and_do_not_trigger_idle_timeout() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let manager = Manager::open(Config {
        output_dir: dir.path().into(),
        home_dir: Some(dir.path().join("test-home")),
        connections_per_download: 1,
        retries: 0,
        read_timeout: Duration::from_millis(200),
        limit_rate: Some("8K".parse().unwrap()),
        ..Config::default()
    })
    .await
    .unwrap();
    let item = manager.add(&fixture.url, None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while manager.get(item.id).await.unwrap().downloaded_bytes == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    manager
        .set_rate_limit("256K".parse().unwrap())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    manager.set_rate_limit("1".parse().unwrap()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    manager.set_rate_limit(RateLimit::UNLIMITED).await.unwrap();
    let done = terminal(&manager, item.id).await;
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    assert_eq!(
        fixture.state.ranges.lock().unwrap().len(),
        1,
        "rate changes must retain the existing HTTP response"
    );
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
async fn rate_changes_are_durable_and_failed_commits_preserve_the_live_limit() {
    let dir = tempfile::tempdir().unwrap();
    let original = manager(&dir, 1).await;
    let rate: RateLimit = "2M".parse().unwrap();
    original.set_rate_limit(rate).await.unwrap();
    let connection =
        rusqlite::Connection::open(dir.path().join("test-home/.vise/state.sqlite3")).unwrap();
    connection.execute_batch("CREATE TRIGGER reject_rate BEFORE UPDATE ON metadata WHEN NEW.key = 'limit_rate' BEGIN SELECT RAISE(ABORT, 'intentional failure'); END;").unwrap();
    assert!(
        original
            .set_rate_limit("3M".parse().unwrap())
            .await
            .is_err()
    );
    assert_eq!(original.rate_configuration(), RateConfiguration::from(rate));
    connection
        .execute_batch("DROP TRIGGER reject_rate;")
        .unwrap();
    drop(connection);
    original.shutdown().await.unwrap();
    drop(original);
    let restored = manager(&dir, 1).await;
    assert_eq!(restored.rate_configuration(), RateConfiguration::from(rate));
    restored.shutdown().await.unwrap();
    drop(restored);
    let cleared = Manager::open(Config {
        output_dir: dir.path().into(),
        home_dir: Some(dir.path().join("test-home")),
        limit_rate: Some(RateLimit::UNLIMITED),
        ..Config::default()
    })
    .await
    .unwrap();
    assert_eq!(cleared.rate_configuration().limit_bytes_per_second, None);
    cleared.shutdown().await.unwrap();
    drop(cleared);
    let restored = manager(&dir, 1).await;
    assert_eq!(restored.rate_configuration().limit_bytes_per_second, None);
    restored.shutdown().await.unwrap();
}

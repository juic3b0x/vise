use super::*;
use crate::engine::Config;
use std::{path::Path, task::Poll, time::Duration};

async fn manager() -> (tempfile::TempDir, Manager) {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::open(Config {
        home_dir: Some(root.path().join("home")),
        output_dir: root.path().join("out"),
        concurrent_downloads: 5,
        connections_per_download: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    (root, manager)
}
fn urls() -> Vec<String> {
    vec!["http://127.0.0.1:1/data.bin".into(); 5]
}
fn statuses(manager: &Manager) -> Vec<String> {
    let sql = rusqlite::Connection::open(manager.database_path()).unwrap();
    let mut statement = sql
        .prepare("SELECT status FROM downloads ORDER BY created_at, filename")
        .unwrap();
    statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap()
}
async fn assert_not_started(
    manager: &Manager,
    events: &mut tokio::sync::broadcast::Receiver<crate::events::Event>,
) {
    for job in manager.0.jobs.read().await.values() {
        assert!(
            job.run.lock().await.is_none(),
            "input workers started before Transaction B completed"
        );
    }
    assert!(
        manager.0.scheduled.read().await.is_empty(),
        "input jobs scheduled before Transaction B completed"
    );
    assert!(
        matches!(
            events.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "Added emitted before Transaction B's durability barrier"
    );
}

#[tokio::test]
async fn five_input_files_have_two_atomic_commits_before_events_or_workers() {
    let (_root, manager) = manager().await;
    let permits = manager.0.pool.acquire_many(5).await.unwrap();
    let mut events = manager.subscribe_events();
    let (a_committed, a_release) = manager.0.database.gate_next_commit();
    let worker = manager.clone();
    let mut admission =
        tokio::spawn(async move { worker.add_input(urls(), None, Default::default()).await });
    a_committed.await.unwrap();
    assert_eq!(
        statuses(&manager),
        vec!["paused"; 5],
        "Transaction A must insert the whole input paused"
    );
    assert_not_started(&manager, &mut events).await;
    let (b_committed, b_release) = manager.0.database.gate_next_commit();
    a_release.send(()).unwrap();
    b_committed.await.unwrap();
    assert_eq!(
        statuses(&manager),
        vec!["queued"; 5],
        "Transaction B must queue the whole input atomically"
    );
    assert_not_started(&manager, &mut events).await;
    assert!(
        matches!(futures_util::poll!(&mut admission), Poll::Pending),
        "input admission acknowledged before B's durability barrier"
    );
    let (mut extra_commit, extra_release) = manager.0.database.gate_next_commit();
    b_release.send(()).unwrap();
    admission.await.unwrap().unwrap();
    assert!(
        matches!(
            extra_commit.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ),
        "successful input admission made more than two SQLite commits"
    );
    manager.0.database.clear_commit_gate();
    drop(extra_release);
    let mut names = Vec::new();
    for _ in 0..5 {
        let event = events.try_recv().unwrap();
        assert_eq!(event.kind, EventKind::Added);
        let download = event.download.unwrap();
        assert_eq!(download.status, Status::Queued);
        names.push(download.filename);
    }
    assert_eq!(
        names,
        [
            "data.bin",
            "data (1).bin",
            "data (2).bin",
            "data (3).bin",
            "data (4).bin"
        ]
    );
    assert_eq!(manager.0.scheduled.read().await.len(), 5);
    manager.shutdown().await.unwrap();
    drop(permits);
}

#[tokio::test]
async fn input_preflight_and_transaction_a_failures_leave_no_prefix() {
    let (_root, manager) = manager().await;
    let mut events = manager.subscribe_events();
    assert!(
        manager
            .add_input(
                vec![urls()[0].clone(), "invalid".into()],
                None,
                Default::default()
            )
            .await
            .is_err()
    );
    assert!(manager.list().await.is_empty());
    let sql = rusqlite::Connection::open(manager.database_path()).unwrap();
    sql.execute_batch("CREATE TRIGGER refuse_input BEFORE INSERT ON downloads BEGIN SELECT RAISE(ABORT, 'input A rejected'); END;").unwrap();
    assert!(
        manager
            .add_input(urls(), None, Default::default())
            .await
            .is_err()
    );
    assert!(
        manager.list().await.is_empty(),
        "failed A retained a provisional prefix"
    );
    assert!(statuses(&manager).is_empty());
    assert_not_started(&manager, &mut events).await;
    assert_eq!(
        std::fs::read_dir(manager.0.config.output_dir.join(".vise"))
            .unwrap()
            .filter(|entry| entry
                .as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|e| e == "part"))
            .count(),
        0
    );
    sql.execute_batch("DROP TRIGGER refuse_input").unwrap();
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_transaction_b_retains_every_task_paused_without_events_or_workers() {
    let (_root, manager) = manager().await;
    let mut events = manager.subscribe_events();
    let sql = rusqlite::Connection::open(manager.database_path()).unwrap();
    sql.execute_batch("CREATE TRIGGER refuse_input_start BEFORE UPDATE ON downloads WHEN NEW.status='queued' BEGIN SELECT RAISE(ABORT, 'input B rejected'); END;").unwrap();
    let result = manager.add_input(urls(), None, Default::default()).await;
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("retained paused"));
    assert_eq!(statuses(&manager), vec!["paused"; 5]);
    assert!(
        manager
            .list()
            .await
            .iter()
            .all(|download| download.status == Status::Paused)
    );
    assert_not_started(&manager, &mut events).await;
    sql.execute_batch("DROP TRIGGER refuse_input_start")
        .unwrap();
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn lost_input_acknowledgements_are_reconciled_and_retain_the_whole_batch_paused() {
    for activation in [false, true] {
        let (_root, manager) = manager().await;
        let mut events = manager.subscribe_events();
        let result = if activation {
            let (entered, ready) = tokio::sync::oneshot::channel();
            let (release, wait) = tokio::sync::oneshot::channel();
            *manager.0.input_hook.lock().await = Some((entered, wait));
            let worker = manager.clone();
            let pending =
                tokio::spawn(
                    async move { worker.add_input(urls(), None, Default::default()).await },
                );
            ready.await.unwrap();
            manager.0.database.reject_next_commit_ack();
            release.send(()).unwrap();
            pending.await.unwrap()
        } else {
            manager.0.database.reject_next_commit_ack();
            manager.add_input(urls(), None, Default::default()).await
        };
        assert!(
            result.is_err(),
            "lost acknowledgement was silently accepted"
        );
        assert_eq!(
            statuses(&manager),
            vec!["paused"; 5],
            "an ambiguous input stage left a queued prefix"
        );
        assert_not_started(&manager, &mut events).await;
        manager.checkpoint().await.unwrap();
        assert_eq!(statuses(&manager), vec!["paused"; 5]);
        manager.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn cancelling_input_receiver_does_not_split_the_atomic_stages() {
    let (_root, manager) = manager().await;
    let permits = manager.0.pool.acquire_many(5).await.unwrap();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    *manager.0.input_hook.lock().await = Some((entered, wait));
    let worker = manager.clone();
    let pending =
        tokio::spawn(async move { worker.add_input(urls(), None, Default::default()).await });
    ready.await.unwrap();
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    release.send(()).unwrap();
    // Shutdown's admission lock joins the owned operation before cancelling runs.
    manager.shutdown().await.unwrap();
    assert_eq!(manager.list().await.len(), 5);
    assert_eq!(statuses(&manager), vec!["paused"; 5]);
    drop(permits);
}

const CHILD: &str = "engine::input_batch::tests::input_crash_child";
struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
fn config(root: &Path) -> Config {
    Config {
        home_dir: Some(root.join("home")),
        output_dir: root.join("out"),
        concurrent_downloads: 5,
        connections_per_download: 1,
        ..Default::default()
    }
}
#[tokio::test]
#[ignore = "child entry point for input crash recovery"]
async fn input_crash_child() {
    use tokio::io::AsyncWriteExt;
    let root = std::path::PathBuf::from(std::env::var_os("VISE_TEST_INPUT_ROOT").unwrap());
    let phase = std::env::var("VISE_TEST_INPUT_PHASE").unwrap();
    let manager = Manager::open(config(&root)).await.unwrap();
    let worker = manager.clone();
    let (_pending, _release, _sql_release) = if phase == "after-a" {
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = tokio::sync::oneshot::channel();
        *manager.0.input_hook.lock().await = Some((entered, wait));
        let pending =
            tokio::spawn(async move { worker.add_input(urls(), None, Default::default()).await });
        ready.await.unwrap();
        (pending, Some(release), None)
    } else {
        let (a, a_release) = manager.0.database.gate_next_commit();
        let pending =
            tokio::spawn(async move { worker.add_input(urls(), None, Default::default()).await });
        a.await.unwrap();
        let (b, b_release) = manager.0.database.gate_next_commit();
        a_release.send(()).unwrap();
        b.await.unwrap();
        (pending, None, Some(b_release))
    };
    let mut socket = tokio::net::TcpStream::connect(std::env::var("VISE_TEST_INPUT_ACK").unwrap())
        .await
        .unwrap();
    socket.write_all(phase.as_bytes()).await.unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn input_process_death_recovers_the_entire_list_paused_on_both_sides_of_b() {
    use tokio::io::AsyncReadExt;
    for (phase, durable) in [("after-a", "paused"), ("after-b", "queued")] {
        let root = tempfile::tempdir().unwrap();
        let ack = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut child = Child(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--ignored", "--exact", CHILD])
                .env("HOME", root.path())
                .env("USERPROFILE", root.path())
                .env("VISE_TEST_INPUT_ROOT", root.path())
                .env("VISE_TEST_INPUT_PHASE", phase)
                .env("VISE_TEST_INPUT_ACK", ack.local_addr().unwrap().to_string())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .unwrap(),
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            let (mut socket, _) = ack.accept().await.unwrap();
            let mut data = vec![0; phase.len()];
            socket.read_exact(&mut data).await.unwrap();
            assert_eq!(data, phase.as_bytes());
        })
        .await
        .unwrap();
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let sql = rusqlite::Connection::open(root.path().join("home/.vise/state.sqlite3")).unwrap();
        let count: i64 = sql
            .query_row(
                "SELECT COUNT(*) FROM downloads WHERE status=?1",
                [durable],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 5, "crash left a partial input cohort");
        drop(sql);
        let manager = Manager::open(config(root.path())).await.unwrap();
        assert_eq!(statuses(&manager), vec!["paused"; 5]);
        assert_eq!(manager.list().await.len(), 5);
        assert!(manager.0.scheduled.read().await.is_empty());
        manager.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn input_preserves_private_transports_and_rejects_mixed_credential_origins() {
    let root = tempfile::tempdir().unwrap();
    let cookies = root.path().join("cookies.txt");
    std::fs::write(&cookies, "# Netscape HTTP Cookie File\n127.0.0.1\tFALSE\t/\tFALSE\t2147483647\tsession\timported-private-sentinel\n").unwrap();
    let request = crate::net::credentials::RequestOverrides::from_cli(
        &["X-Input-Secret: header-private-sentinel".into()],
        None,
        None,
    )
    .unwrap();
    let manager = Manager::open(Config {
        cookies_file: Some(cookies.clone()),
        proxy_url: Some(
            "socks5h://user:proxy-private-sentinel@127.0.0.1:9"
                .parse()
                .unwrap(),
        ),
        request,
        ..config(root.path())
    })
    .await
    .unwrap();
    assert!(
        manager
            .add_input(
                vec![urls()[0].clone(), "http://127.0.0.1:2/data.bin".into()],
                None,
                Default::default()
            )
            .await
            .is_err()
    );
    assert!(
        statuses(&manager).is_empty(),
        "mixed credential origins admitted a prefix"
    );
    let sql = rusqlite::Connection::open(manager.database_path()).unwrap();
    sql.execute_batch("CREATE TRIGGER refuse_private_start BEFORE UPDATE ON downloads WHEN NEW.status='queued' BEGIN SELECT RAISE(ABORT, 'private input B rejected'); END;").unwrap();
    assert!(
        manager
            .add_input(urls(), None, Default::default())
            .await
            .is_err()
    );
    let records = manager.0.database.load().await.unwrap();
    assert_eq!(records.len(), 5);
    let canonical_cookies = std::fs::canonicalize(cookies).unwrap();
    for record in &records {
        assert_eq!(record.cookies_file.as_ref(), Some(&canonical_cookies));
        assert!(record.request_credentials.is_some());
        let public = record.snapshot();
        assert!(public.proxy_authenticated);
        assert_eq!(public.request.header_names, ["x-input-secret"]);
    }
    let public = serde_json::to_string(&manager.list().await).unwrap();
    for secret in [
        "header-private-sentinel",
        "proxy-private-sentinel",
        "imported-private-sentinel",
    ] {
        assert!(!public.contains(secret));
    }
    let journal_credentials: String = sql
        .query_row(
            "SELECT group_concat(request_credentials) FROM downloads",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(journal_credentials.contains("header-private-sentinel"));
    assert!(
        !journal_credentials.contains("imported-private-sentinel"),
        "cookie-file values entered private task credentials"
    );
    sql.execute_batch("DROP TRIGGER refuse_private_start")
        .unwrap();
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn atomic_input_keeps_one_probe_and_one_body_request_per_file() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (_root, manager) = manager().await;
    let requests = Arc::new(AtomicUsize::new(0));
    let (url, server) =
        crate::engine::adaptive::tests::crash_server_counted(Some(requests.clone())).await;
    let mut events = manager.subscribe_events();
    manager
        .add_input(vec![url; 5], None, Default::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut completed = 0;
        while completed < 5 {
            let event = events.recv().await.unwrap();
            assert_ne!(event.kind, EventKind::Failed);
            assert_ne!(event.kind, EventKind::Corrupt);
            if event.kind == EventKind::Completed {
                completed += 1;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        requests.load(Ordering::SeqCst),
        10,
        "atomic input changed probe/body request counts"
    );
    manager.shutdown().await.unwrap();
    server.abort();
    let _ = server.await;
}

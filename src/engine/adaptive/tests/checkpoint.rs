//! Abrupt checkpoint death uses the existing local range/checksum fixture.
use super::{crash_data, crash_server, read_split_ack};
use crate::{
    engine::{Config, Job, Manager, layout::validate_record},
    model::{Chunk, ChunkStatus, Record, Status},
};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::io::AsyncWriteExt;
use uuid::Uuid;

const OLD_OFFSET: u64 = 65_536;
const NEW_OFFSET: u64 = 131_072;
const AFTER_SYNC: &str = "checkpoint-after-sync";
const AFTER_COMMIT: &str = "checkpoint-after-commit";
const FRESH: &str = "after-fresh-preparation";
const CHILD: &str = "engine::adaptive::tests::checkpoint::checkpoint_crash_child";

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        // Always kill and reap, including acknowledgement timeout/assertion paths.
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn config(root: &std::path::Path) -> Config {
    Config {
        home_dir: Some(root.join("home")),
        output_dir: root.join("out"),
        connections_per_download: 1,
        min_split_size: 0,
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "child entry point used by checkpoint process-death tests"]
async fn checkpoint_crash_child() {
    let root = PathBuf::from(std::env::var_os("VISE_TEST_CHECKPOINT_ROOT").unwrap());
    let url = std::env::var("VISE_TEST_CHECKPOINT_URL").unwrap();
    let phase = std::env::var("VISE_TEST_CHECKPOINT_PHASE").unwrap();
    let manager = Manager::open(config(&root)).await.unwrap();
    let data = crash_data();
    if phase == FRESH {
        manager
            .add_with_checksums(
                &url,
                None,
                crate::checksum::ExpectedChecksums {
                    sha256: Some(format!("{:x}", Sha256::digest(&data))),
                    md5: None,
                },
            )
            .await
            .unwrap();
        std::future::pending::<()>().await;
        return;
    }
    let id = Uuid::new_v4();
    let record = Record {
        id,
        url,
        filename: "data.bin".into(),
        output_path: manager.0.config.output_dir.join("data.bin"),
        part_path: manager
            .0
            .config
            .output_dir
            .join(".vise")
            .join(format!("{id}.part")),
        proxy_url: None,
        cookies_file: None,
        request_credentials: None,
        status: Status::Downloading,
        total_bytes: Some(data.len() as u64),
        ranged: true,
        validator: Some("\"crash-test\"".into()),
        chunks: vec![Chunk {
            start: 0,
            end: Some(data.len() as u64),
            written: OLD_OFFSET,
            status: ChunkStatus::Downloading,
        }],
        error: None,
        checksums: crate::checksum::ExpectedChecksums {
            sha256: Some(format!("{:x}", Sha256::digest(&data))),
            md5: None,
        }
        .into(),
        speed: 0.0,
        created_at: crate::model::timestamp_millis(),
    };
    validate_record(&record).unwrap();
    let job = Arc::new(Job::new(record, reqwest::Client::new()));
    tokio::fs::write(&job.part_path, &data[..OLD_OFFSET as usize])
        .await
        .unwrap();
    manager.0.jobs.write().await.insert(id, job.clone());
    manager.checkpoint().await.unwrap();
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&job.part_path)
        .await
        .unwrap();
    file.write_all(&data[OLD_OFFSET as usize..NEW_OFFSET as usize])
        .await
        .unwrap();
    file.flush().await.unwrap();
    drop(file);
    job.record.write().await.chunks[0].written = NEW_OFFSET;

    // Arm only this checkpoint, after startup and the baseline commit. Keep its
    // release sender alive until process death; no environment mutation is used.
    let (ready, _async_release, _blocking_release) = match phase.as_str() {
        AFTER_SYNC => {
            let (entered, ready) = tokio::sync::oneshot::channel();
            let (release, wait) = tokio::sync::oneshot::channel();
            *manager.0.checkpoint_hook.lock().await = Some((entered, wait));
            (ready, Some(release), None)
        }
        AFTER_COMMIT => {
            let (ready, release) = manager.0.database.gate_next_commit();
            (ready, None, Some(release))
        }
        _ => panic!("unknown checkpoint crash phase"),
    };
    let worker = manager.clone();
    let mut pending = tokio::spawn(async move { worker.checkpoint().await });
    tokio::time::timeout(Duration::from_secs(10), ready)
        .await
        .unwrap()
        .unwrap();
    let mut socket =
        tokio::net::TcpStream::connect(std::env::var("VISE_TEST_CHECKPOINT_ACK").unwrap())
            .await
            .unwrap();
    socket.write_all(phase.as_bytes()).await.unwrap();
    tokio::select! {
        result = &mut pending => panic!("checkpoint escaped its crash barrier: {result:?}"),
        _ = tokio::time::sleep(Duration::from_secs(15)) => panic!("parent did not kill checkpoint child"),
    }
}

async fn check_death(phase: &str, offset: u64) {
    let root = tempfile::tempdir().unwrap();
    let (url, server) = crash_server().await;
    let ack = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut child = Child(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", CHILD])
            .env("HOME", root.path())
            .env("USERPROFILE", root.path())
            .env("VISE_TEST_CHECKPOINT_ROOT", root.path())
            .env("VISE_TEST_CHECKPOINT_URL", url)
            .env("VISE_TEST_CHECKPOINT_PHASE", phase)
            .env(
                "VISE_TEST_CHECKPOINT_ACK",
                ack.local_addr().unwrap().to_string(),
            )
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut socket, _) = ack.accept().await.unwrap();
        read_split_ack(&mut socket, phase).await.unwrap();
    })
    .await
    .expect("child did not acknowledge the checkpoint boundary");
    child.0.kill().unwrap();
    let exit = child.0.wait().unwrap();
    assert!(!exit.success(), "child must die without orderly shutdown");

    let sql = rusqlite::Connection::open(root.path().join("home/.vise/state.sqlite3")).unwrap();
    let (status, written): (String, i64) = sql.query_row(
        "SELECT d.status,c.downloaded_bytes FROM downloads d JOIN chunks c ON c.download_id=d.id",
        [], |row| Ok((row.get(0)?, row.get(1)?)),
    ).unwrap();
    assert_eq!(status, "downloading", "{phase}: raw durable status");
    assert_eq!(
        written,
        i64::try_from(offset).unwrap(),
        "{phase}: exact durable offset"
    );
    drop(sql);

    let manager = Manager::open(config(root.path())).await.unwrap();
    let records = manager.0.database.load().await.unwrap();
    assert_eq!(records.len(), 1);
    let recovered = &records[0];
    assert_eq!(recovered.status, Status::Paused);
    assert_eq!(recovered.chunks.len(), 1);
    assert_eq!(recovered.chunks[0].status, ChunkStatus::Paused);
    assert_eq!(recovered.chunks[0].written, offset);
    assert_eq!(recovered.downloaded(), offset);
    validate_record(recovered).unwrap();
    assert_eq!(
        manager.get(recovered.id).await.unwrap().downloaded_bytes,
        offset
    );
    assert!(
        !recovered.output_path.exists(),
        "checkpoint must not publish output"
    );

    manager.resume(recovered.id).await.unwrap();
    let done = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let task = manager.get(recovered.id).await.unwrap();
            if !task.status.active() {
                break task;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(done.status, Status::Completed, "{:?}", done.error);
    let data = crash_data();
    let digest = format!("{:x}", Sha256::digest(&data));
    assert_eq!(tokio::fs::read(&done.output_path).await.unwrap(), data);
    assert_eq!(
        done.checksums.expected_sha256.as_deref(),
        Some(digest.as_str())
    );
    assert_eq!(
        done.checksums.actual_sha256.as_deref(),
        Some(digest.as_str())
    );
    assert!(done.checksums.verified_at.is_some());
    manager.shutdown().await.unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn checkpoint_death_after_file_sync_recovers_previous_offsets() {
    check_death(AFTER_SYNC, OLD_OFFSET).await;
}

#[tokio::test]
async fn checkpoint_death_after_sql_commit_recovers_new_offsets() {
    check_death(AFTER_COMMIT, NEW_OFFSET).await;
}

#[tokio::test]
async fn fresh_single_range_file_keeps_all_required_durable_barriers() {
    use crate::engine::journal::barriers::Phase;
    use std::sync::atomic::Ordering;
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::open(config(root.path())).await.unwrap();
    let (url, server) = crash_server().await;
    manager.0.journal_trace.take();
    let baseline = manager.0.database.snapshot_commits.load(Ordering::SeqCst);
    let mut events = manager.subscribe_events();
    manager.add(&url, None).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = events.recv().await.unwrap();
            if event.kind == crate::events::EventKind::Completed {
                break;
            }
            assert_ne!(event.kind, crate::events::EventKind::Failed);
        }
    })
    .await
    .unwrap();
    let phases = manager.0.journal_trace.take();
    let count = |phase| phases.iter().filter(|p| **p == phase).count();
    assert_eq!(
        count(Phase::Capture),
        6,
        "admission, queued, probing, downloading, prepublication and terminal barriers remain"
    );
    assert_eq!(
        count(Phase::Directory),
        6,
        "every barrier includes journal directory sync"
    );
    assert_eq!(
        count(Phase::File),
        2,
        "downloading and prepublication sync staging data; fresh creation needs no reset barrier"
    );
    assert_eq!(count(Phase::StagingDirectory), 2);
    // Both offset-bearing snapshots retain file -> staging directory -> SQL ->
    // journal directory order. Only the offset-free fresh reset was removed.
    for window in phases.split(|phase| *phase == Phase::Capture) {
        if let Some(file) = window.iter().position(|phase| *phase == Phase::File) {
            let staging = window
                .iter()
                .position(|phase| *phase == Phase::StagingDirectory)
                .unwrap();
            let sql = window
                .iter()
                .position(|phase| *phase == Phase::BeforeDirectory)
                .unwrap();
            let directory = window
                .iter()
                .position(|phase| *phase == Phase::Directory)
                .unwrap();
            assert!(file < staging && staging < sql && sql < directory);
        }
    }
    assert_eq!(
        manager.0.database.snapshot_commits.load(Ordering::SeqCst) - baseline,
        6
    );
    manager.shutdown().await.unwrap();
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn death_after_fresh_preparation_recovers_offset_free_and_redownloads() {
    let root = tempfile::tempdir().unwrap();
    let (url, server) = crash_server().await;
    let ack = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut child = Child(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--ignored", "--exact", CHILD])
            .env("HOME", root.path())
            .env("USERPROFILE", root.path())
            .env("VISE_TEST_CHECKPOINT_ROOT", root.path())
            .env("VISE_TEST_CHECKPOINT_URL", url)
            .env("VISE_TEST_CHECKPOINT_PHASE", FRESH)
            .env("VISE_TEST_SPLIT_PHASE", FRESH)
            .env("VISE_TEST_SPLIT_ACK", ack.local_addr().unwrap().to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        let (mut socket, _) = ack.accept().await.unwrap();
        read_split_ack(&mut socket, FRESH).await.unwrap();
    })
    .await
    .unwrap();
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let manager = Manager::open(config(root.path())).await.unwrap();
    let records = manager.0.database.load().await.unwrap();
    assert_eq!(records.len(), 1);
    let record = &records[0];
    assert_eq!(record.status, Status::Paused);
    assert!(record.chunks.is_empty());
    assert_eq!(record.downloaded(), 0);
    assert!(!record.output_path.exists());
    let mut events = manager.subscribe_events();
    manager.resume(record.id).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = events.recv().await.unwrap();
            assert_ne!(event.kind, crate::events::EventKind::Failed);
            assert_ne!(event.kind, crate::events::EventKind::Corrupt);
            if event.kind == crate::events::EventKind::Completed {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(
        tokio::fs::read(&record.output_path).await.unwrap(),
        crash_data()
    );
    assert!(
        manager
            .get(record.id)
            .await
            .unwrap()
            .checksums
            .verified_at
            .is_some()
    );
    manager.shutdown().await.unwrap();
    server.abort();
    let _ = server.await;
}

use super::*;
use crate::{
    engine::{Config, Job},
    model::{Chunk, ChunkStatus},
};
use barriers::Phase;
use std::sync::atomic::Ordering;

async fn staged() -> (tempfile::TempDir, Manager, Arc<Job>) {
    let (root, manager, template) = super::completed_job().await;
    let mut record = template.record.read().await.clone();
    record.status = Status::Downloading;
    record.total_bytes = Some(3);
    record.chunks = vec![Chunk {
        start: 0,
        end: Some(3),
        written: 3,
        status: ChunkStatus::Paused,
    }];
    std::fs::write(&record.part_path, b"abc").unwrap();
    let job = Arc::new(Job::new(record.clone(), reqwest::Client::new()));
    manager.0.jobs.write().await.insert(record.id, job.clone());
    (root, manager, job)
}
fn count(phases: &[Phase], phase: Phase) -> usize {
    phases.iter().filter(|p| **p == phase).count()
}

#[tokio::test]
async fn metadata_and_failed_sql_reuse_data_and_namespace_barriers() {
    let (_root, manager, job) = staged().await;
    manager.checkpoint().await.unwrap();
    manager.0.journal_trace.take();
    job.record.write().await.error = Some("metadata".into());
    let sql = rusqlite::Connection::open(manager.database_path()).unwrap();
    sql.execute_batch("CREATE TRIGGER reject_evidence BEFORE UPDATE ON downloads BEGIN SELECT RAISE(ABORT, 'injected'); END;").unwrap();
    assert!(manager.checkpoint().await.is_err());
    sql.execute_batch("DROP TRIGGER reject_evidence").unwrap();
    manager.checkpoint().await.unwrap();
    let phases = manager.0.journal_trace.take();
    assert_eq!(count(&phases, Phase::File), 0);
    assert_eq!(count(&phases, Phase::StagingDirectory), 0);
    assert_eq!(
        manager.0.database.load().await.unwrap()[0].error.as_deref(),
        Some("metadata")
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_barriers_do_not_create_false_evidence() {
    let (_root, manager, _job) = staged().await;
    manager.0.journal_trace.take();
    manager
        .0
        .journal_trace
        .reject_file
        .store(true, Ordering::SeqCst);
    assert!(manager.checkpoint().await.is_err());
    assert_eq!(count(&manager.0.journal_trace.take(), Phase::File), 0);
    manager
        .0
        .journal_trace
        .reject_staging_directory
        .store(true, Ordering::SeqCst);
    assert!(manager.checkpoint().await.is_err());
    let phases = manager.0.journal_trace.take();
    assert_eq!(count(&phases, Phase::File), 1);
    assert_eq!(count(&phases, Phase::StagingDirectory), 1);
    manager.checkpoint().await.unwrap();
    let phases = manager.0.journal_trace.take();
    assert_eq!(count(&phases, Phase::File), 0);
    assert_eq!(count(&phases, Phase::StagingDirectory), 1);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn writes_layout_truncation_and_replacement_invalidate_evidence() {
    let (_root, manager, job) = staged().await;
    manager.checkpoint().await.unwrap();
    for change in 0..4 {
        manager.0.journal_trace.take();
        match change {
            0 => {
                job.durability.invalidate();
                std::fs::write(&job.part_path, b"xyz").unwrap();
            }
            1 => {
                job.record.write().await.chunks[0].written = 2;
            }
            2 => {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(&job.part_path)
                    .unwrap()
                    .set_len(2)
                    .unwrap();
            }
            _ => {
                std::fs::rename(&job.part_path, job.part_path.with_extension("retired")).unwrap();
                std::fs::write(&job.part_path, b"xy").unwrap();
            }
        }
        job.record.write().await.error = Some(format!("mutation {change}"));
        manager.checkpoint().await.unwrap();
        let phases = manager.0.journal_trace.take();
        assert_eq!(count(&phases, Phase::File), 1, "mutation {change}");
        assert_eq!(
            count(&phases, Phase::StagingDirectory),
            usize::from(change == 3)
        );
    }
    manager.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn cached_evidence_does_not_bypass_substitution_checks() {
    let (root, manager, job) = staged().await;
    manager.checkpoint().await.unwrap();
    std::fs::remove_file(&job.part_path).unwrap();
    let unrelated = root.path().join("unrelated");
    std::fs::write(&unrelated, b"keep").unwrap();
    std::os::unix::fs::symlink(&unrelated, &job.part_path).unwrap();
    job.record.write().await.error = Some("must validate".into());
    assert!(manager.checkpoint().await.is_err());
    assert_eq!(std::fs::read(unrelated).unwrap(), b"keep");
    std::fs::remove_file(&job.part_path).unwrap();
    std::fs::write(&job.part_path, b"abc").unwrap();
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn writes_after_capture_are_not_covered_by_earlier_evidence() {
    let (_root, manager, job) = staged().await;
    manager.checkpoint().await.unwrap();
    job.record.write().await.error = Some("capture".into());
    let snapshots = manager.capture_snapshots(None).await.unwrap();
    job.durability.invalidate();
    std::fs::write(&job.part_path, b"new").unwrap();
    job.record.write().await.chunks[0].written = 2;
    sync_snapshots(snapshots, Some(manager.0.journal_trace.clone()))
        .await
        .unwrap();
    manager.0.journal_trace.take();
    manager.checkpoint().await.unwrap();
    assert_eq!(count(&manager.0.journal_trace.take(), Phase::File), 1);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "child entry point for crash_after_reused_barrier_recovers_durable_bytes"]
async fn reused_barrier_crash_child() {
    let root = PathBuf::from(std::env::var_os("VISE_TEST_JOURNAL_ROOT").unwrap());
    let certified =
        std::env::var("VISE_TEST_SPLIT_PHASE").unwrap() == "after-certified-data-barrier";
    let manager = Manager::open(Config {
        home_dir: Some(root.join("home")),
        output_dir: root.join("output"),
        ..Default::default()
    })
    .await
    .unwrap();
    let id = manager
        .add("http://127.0.0.1:1/unused", None)
        .await
        .unwrap()
        .id;
    manager.pause(id).await.unwrap();
    let job = manager.0.jobs.read().await.get(&id).unwrap().clone();
    std::fs::write(&job.part_path, b"abc").unwrap();
    job.staged.store(true, Ordering::Release);
    {
        let mut r = job.record.write().await;
        r.status = Status::Downloading;
        r.total_bytes = Some(3);
        r.chunks = vec![Chunk {
            start: 0,
            end: Some(3),
            written: if certified { 2 } else { 3 },
            status: ChunkStatus::Paused,
        }];
    }
    manager.checkpoint().await.unwrap();
    if certified {
        job.durability.invalidate();
        job.record.write().await.chunks[0].written = 3;
        let record = job.record.read().await.clone();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&job.part_path)
            .unwrap();
        job.durability
            .certify_data(&file, &record, job.durability.revision(), || {
                file.sync_all()?;
                Ok(())
            })
            .unwrap();
    }
    job.record.write().await.error = Some("uncommitted metadata".into());
    manager.checkpoint().await.unwrap();
}

#[tokio::test]
async fn crash_after_reused_barrier_recovers_durable_bytes() {
    crash_at("after-reused-data-barrier", 3).await;
}

#[tokio::test]
async fn crash_after_certified_barrier_recovers_only_previously_committed_offsets() {
    crash_at("after-certified-data-barrier", 2).await;
}

async fn crash_at(phase: &str, offset: u64) {
    use tokio::io::AsyncReadExt;
    let root = tempfile::tempdir().unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--ignored",
            "--exact",
            "engine::journal::tests::durability::reused_barrier_crash_child",
        ])
        .env("HOME", root.path())
        .env("USERPROFILE", root.path())
        .env("VISE_TEST_JOURNAL_ROOT", root.path())
        .env("VISE_TEST_SPLIT_PHASE", phase)
        .env(
            "VISE_TEST_SPLIT_ACK",
            listener.local_addr().unwrap().to_string(),
        )
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let acknowledgement = tokio::time::timeout(Duration::from_secs(15), async {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = vec![0; phase.len()];
        socket.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes, phase.as_bytes());
    })
    .await;
    let _ = child.kill();
    child.wait().unwrap();
    acknowledgement.unwrap();
    let manager = Manager::open(Config {
        home_dir: Some(root.path().join("home")),
        output_dir: root.path().join("output"),
        ..Default::default()
    })
    .await
    .unwrap();
    let records = manager.0.database.load().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].status, Status::Paused);
    assert_eq!(records[0].downloaded(), offset);
    assert_eq!(std::fs::read(&records[0].part_path).unwrap(), b"abc");
    assert_ne!(records[0].error.as_deref(), Some("uncommitted metadata"));
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn inactive_history_does_not_retain_identity_handles() {
    let (_root, manager, job) = staged().await;
    manager.checkpoint().await.unwrap();
    job.record.write().await.status = Status::Paused;
    manager.checkpoint().await.unwrap();
    manager.0.journal_trace.take();
    job.record.write().await.error = Some("inactive metadata".into());
    manager.checkpoint().await.unwrap();
    // A paused job starts without cached identity/data/namespace evidence.
    let phases = manager.0.journal_trace.take();
    assert_eq!(count(&phases, Phase::File), 1);
    assert_eq!(count(&phases, Phase::StagingDirectory), 1);
    manager.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn replacing_staging_directory_requires_new_namespace_barrier_even_for_same_file() {
    let (_root, manager, job) = staged().await;
    manager.checkpoint().await.unwrap();
    let directory = job.part_path.parent().unwrap();
    let retired = directory.with_file_name("retired-staging");
    std::fs::rename(directory, &retired).unwrap();
    std::fs::create_dir(directory).unwrap();
    std::fs::hard_link(
        retired.join(job.part_path.file_name().unwrap()),
        &job.part_path,
    )
    .unwrap();
    manager.0.journal_trace.take();
    job.record.write().await.error = Some("new namespace".into());
    manager
        .0
        .journal_trace
        .reject_staging_directory
        .store(true, Ordering::SeqCst);
    assert!(manager.checkpoint().await.is_err());
    let phases = manager.0.journal_trace.take();
    assert_eq!(
        count(&phases, Phase::File),
        0,
        "same file data is already durable"
    );
    assert_eq!(count(&phases, Phase::StagingDirectory), 1);
    manager.checkpoint().await.unwrap();
    assert_eq!(
        count(&manager.0.journal_trace.take(), Phase::StagingDirectory),
        1
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_settlement_with_missing_staging_does_not_pin_history_handles() {
    let (_root, manager, job) = staged().await;
    manager.checkpoint().await.unwrap();
    assert!(job.durability.has_evidence());
    std::fs::rename(&job.part_path, job.part_path.with_extension("retired")).unwrap();
    manager.finish(&job, Err(Error::Cancelled)).await;
    assert_eq!(job.record.read().await.status, Status::Paused);
    assert!(
        !job.durability.has_evidence(),
        "unavailable staging retained identity handles"
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn publication_certificate_skips_only_matching_data_and_never_namespace() {
    let (_root, manager, job) = staged().await;
    let record = job.record.read().await.clone();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&job.part_path)
        .unwrap();
    assert!(
        job.durability
            .certify_data(&file, &record, job.durability.revision(), || {
                file.sync_all()?;
                Ok(())
            })
            .unwrap()
    );
    manager.0.journal_trace.take();
    let sql = rusqlite::Connection::open(manager.database_path()).unwrap();
    sql.execute_batch("CREATE TRIGGER reject_certificate BEFORE UPDATE ON downloads BEGIN SELECT RAISE(ABORT, 'injected'); END;").unwrap();
    assert!(manager.checkpoint().await.is_err());
    sql.execute_batch("DROP TRIGGER reject_certificate")
        .unwrap();
    manager.checkpoint().await.unwrap();
    let phases = manager.0.journal_trace.take();
    assert_eq!(count(&phases, Phase::File), 0);
    assert_eq!(
        count(&phases, Phase::StagingDirectory),
        1,
        "certificate did not cover namespace"
    );
    // A new write revision, even at unchanged offsets, cannot use old proof.
    job.durability.invalidate();
    job.record.write().await.error = Some("new write".into());
    manager
        .0
        .journal_trace
        .reject_file
        .store(true, Ordering::SeqCst);
    assert!(manager.checkpoint().await.is_err());
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn certificate_cannot_cover_a_replacement_or_a_failed_barrier() {
    for failed in [false, true] {
        let (_root, manager, job) = staged().await;
        let record = job.record.read().await.clone();
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&job.part_path)
            .unwrap();
        let result = job
            .durability
            .certify_data(&file, &record, job.durability.revision(), || {
                if failed {
                    return Err(std::io::Error::other("injected certificate sync failure").into());
                }
                file.sync_all()?;
                Ok(())
            });
        assert_eq!(result.is_err(), failed);
        if !failed {
            std::fs::rename(&job.part_path, job.part_path.with_extension("retired")).unwrap();
            std::fs::write(&job.part_path, b"xyz").unwrap();
        }
        manager
            .0
            .journal_trace
            .reject_file
            .store(true, Ordering::SeqCst);
        assert!(
            manager.checkpoint().await.is_err(),
            "invalid certificate bypassed real sync"
        );
        manager.shutdown().await.unwrap();
    }
}

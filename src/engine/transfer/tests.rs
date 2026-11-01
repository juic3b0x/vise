use super::*;
use crate::engine::Config;

async fn staged_job() -> (tempfile::TempDir, Manager, Arc<Job>) {
    let dir = tempfile::tempdir().unwrap();
    let manager = Manager::open(Config {
        output_dir: dir.path().into(),
        home_dir: Some(dir.path().join("test-home")),
        ..Config::default()
    })
    .await
    .unwrap();
    let id = Uuid::new_v4();
    let job = Arc::new(Job::new(
        Record {
            id,
            url: "http://127.0.0.1:1/unused".into(),
            filename: "payload.bin".into(),
            output_path: manager.0.config.output_dir.join("payload.bin"),
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
            total_bytes: Some(3),
            ranged: true,
            validator: Some("test".into()),
            chunks: vec![Chunk {
                start: 0,
                end: Some(3),
                written: 0,
                status: ChunkStatus::Queued,
            }],
            error: None,
            checksums: Default::default(),
            speed: 0.0,
            created_at: timestamp_millis(),
        },
        reqwest::Client::new(),
    ));
    (dir, manager, job)
}

#[tokio::test]
async fn fresh_staging_creation_waits_for_the_capture_gate() {
    let (_root, manager, template) = staged_job().await;
    let record = template.record.read().await.clone();
    let job = Arc::new(Job::fresh(record.clone(), reqwest::Client::new()));
    manager.0.jobs.write().await.insert(record.id, job.clone());
    manager.checkpoint().await.unwrap();
    assert!(!job.staged.load(std::sync::atomic::Ordering::Acquire));
    let checkpoint = manager.0.checkpoint.lock().await;
    let mut creation = Box::pin(manager.prepare_partial(
        &job,
        Metadata {
            total: Some(3),
            ranged: true,
            validator: Some("new".into()),
            expected: Default::default(),
        },
    ));
    assert!(futures_util::poll!(creation.as_mut()).is_pending());
    assert!(
        !job.part_path.exists(),
        "creation bypassed the capture gate"
    );
    assert!(!job.staged.load(std::sync::atomic::Ordering::Acquire));
    drop(checkpoint);
    creation.await.unwrap();
    assert!(job.staged.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(std::fs::metadata(&job.part_path).unwrap().len(), 3);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn fresh_preparation_durability_counts() {
    use super::super::journal::barriers::Phase;
    use std::sync::atomic::Ordering;
    let (_root, manager, template) = staged_job().await;
    let mut record = template.record.read().await.clone();
    record.chunks.clear();
    record.total_bytes = None;
    record.validator = None;
    record.ranged = false;
    let job = Arc::new(Job::fresh(record.clone(), reqwest::Client::new()));
    manager.0.jobs.write().await.insert(record.id, job.clone());
    manager.checkpoint().await.unwrap();
    manager.0.journal_trace.take();
    let commits = manager.0.database.snapshot_commits.load(Ordering::SeqCst);
    manager
        .prepare_partial(
            &job,
            Metadata {
                total: Some(3),
                ranged: true,
                validator: Some("new".into()),
                expected: Default::default(),
            },
        )
        .await
        .unwrap();
    let phases = manager.0.journal_trace.take();
    let commits = manager.0.database.snapshot_commits.load(Ordering::SeqCst) - commits;
    let files = phases.iter().filter(|&&phase| phase == Phase::File).count();
    let directories = phases
        .iter()
        .filter(|&&phase| matches!(phase, Phase::StagingDirectory | Phase::Directory))
        .count();
    eprintln!(
        "fresh preparation: commits={commits}, file_syncs={files}, directory_syncs={directories}"
    );
    assert_eq!((commits, files, directories), (0, 0, 0));
    // The next lifecycle commit must still synchronize the created file
    // and its directory before persisting its new layout.
    manager.transition(&job, Status::Downloading).await.unwrap();
    let phases = manager.0.journal_trace.take();
    let file = phases
        .iter()
        .position(|&phase| phase == Phase::File)
        .unwrap();
    let staging = phases
        .iter()
        .position(|&phase| phase == Phase::StagingDirectory)
        .unwrap();
    let journal = phases
        .iter()
        .position(|&phase| phase == Phase::BeforeDirectory)
        .unwrap();
    assert!(file < staging && staging < journal);
    assert_eq!(
        manager.0.database.load().await.unwrap()[0].total_bytes,
        Some(3)
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn preexisting_staging_never_uses_the_fresh_reset_exemption() {
    use std::sync::atomic::Ordering;
    let (_root, manager, template) = staged_job().await;
    let mut record = template.record.read().await.clone();
    record.chunks.clear();
    let job = Arc::new(Job::fresh(record.clone(), reqwest::Client::new()));
    manager.0.jobs.write().await.insert(record.id, job.clone());
    manager.checkpoint().await.unwrap();
    fs::write(&job.part_path, b"old").await.unwrap();
    let commits = manager.0.database.snapshot_commits.load(Ordering::SeqCst);
    manager
        .prepare_partial(
            &job,
            Metadata {
                total: Some(3),
                ranged: true,
                validator: Some("new".into()),
                expected: Default::default(),
            },
        )
        .await
        .unwrap();
    assert_eq!(
        manager.0.database.snapshot_commits.load(Ordering::SeqCst) - commits,
        1
    );
    assert_eq!(
        manager.0.database.load().await.unwrap()[0].chunks[0].written,
        0
    );
    assert_eq!(fs::read(&job.part_path).await.unwrap(), [0; 3]);
    manager.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn first_open_of_fresh_staging_rejects_a_substituted_symlink() {
    let (root, manager, template) = staged_job().await;
    let record = template.record.read().await.clone();
    let job = Arc::new(Job::fresh(record.clone(), reqwest::Client::new()));
    manager.0.jobs.write().await.insert(record.id, job.clone());
    manager.checkpoint().await.unwrap();
    let unrelated = root.path().join("unrelated");
    std::fs::write(&unrelated, b"keep").unwrap();
    std::os::unix::fs::symlink(&unrelated, &job.part_path).unwrap();
    assert!(
        manager
            .prepare_partial(
                &job,
                Metadata {
                    total: Some(3),
                    ranged: true,
                    validator: Some("new".into()),
                    expected: Default::default()
                }
            )
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&unrelated).unwrap(), b"keep");
    assert!(!job.staged.load(std::sync::atomic::Ordering::Acquire));
    std::fs::remove_file(&job.part_path).unwrap();
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn reset_does_not_truncate_before_durable_offsets() {
    let (_dir, manager, job) = staged_job().await;
    job.record.write().await.chunks[0].written = 3;
    manager
        .0
        .jobs
        .write()
        .await
        .insert(job.record.read().await.id, job.clone());
    fs::write(&job.part_path, b"abc").await.unwrap();
    manager.checkpoint().await.unwrap();
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, wait) = tokio::sync::oneshot::channel();
    *manager.0.checkpoint_hook.lock().await = Some((entered, wait));
    let worker = manager.clone();
    let task = job.clone();
    let pending = tokio::spawn(async move {
        worker
            .prepare_partial(
                &task,
                Metadata {
                    total: Some(5),
                    ranged: true,
                    validator: Some("new".into()),
                    expected: Default::default(),
                },
            )
            .await
    });
    ready.await.unwrap();
    assert_eq!(
        manager.0.database.load().await.unwrap()[0].chunks[0].written,
        3
    );
    assert_eq!(
        fs::read(&job.part_path).await.unwrap(),
        b"abc",
        "truncate preceded durable offset invalidation"
    );
    release.send(()).unwrap();
    pending.await.unwrap().unwrap();
    assert_eq!(
        manager.0.database.load().await.unwrap()[0].chunks[0].written,
        0
    );
    assert_eq!(fs::metadata(&job.part_path).await.unwrap().len(), 5);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_chunks_fail_workers_and_do_not_commit_progress() {
    let (_dir, manager, job) = staged_job().await;
    let cancel = CancellationToken::new();
    for index in [1, u64::MAX] {
        assert!(
            matches!(manager.transfer_chunk(job.clone(), index, &cancel).await, Err(Error::Worker(message)) if message.contains("missing chunk"))
        );
        assert!(
            matches!(manager.transfer_once(&job, index, &cancel).await, Err(Error::Worker(message)) if message.contains("missing chunk"))
        );
    }
    assert!(!job.part_path.exists());
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&job.part_path)
        .await
        .unwrap();
    let mut pending = BytesMut::from(b"abc".as_slice());
    assert!(matches!(
        write_pending(&mut file, &job, 1, &mut pending).await,
        Err(Error::Worker(_))
    ));
    assert_eq!(pending.as_ref(), b"abc");
    assert_eq!(chunk_at(&*job.record.read().await, 0).unwrap().written, 0);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn publication_finishes_staging_rename_before_returning() {
    let (_dir, manager, job) = staged_job().await;
    manager
        .0
        .jobs
        .write()
        .await
        .insert(job.record.read().await.id, job.clone());
    chunk_at_mut(&mut *job.record.write().await, 0)
        .unwrap()
        .written = 3;
    let mut file = fs::File::create(&job.part_path).await.unwrap();
    file.write_all(b"abc").await.unwrap();
    manager
        .publish_file(&job, file, job.part_path.clone(), &CancellationToken::new())
        .await
        .unwrap();
    // No polling here: publish_file must await this rename, independently of
    // queue scheduling, SQL completion, or any later cleanup task.
    assert!(!job.part_path.try_exists().unwrap());
    assert_eq!(fs::read(&job.output_path).await.unwrap(), b"abc");
    assert!(
        same_file::is_same_file(
            &job.output_path,
            super::super::journal::publication_path(&job.part_path)
        )
        .unwrap()
    );
    assert_eq!(
        manager
            .0
            .database
            .list()
            .await
            .unwrap()
            .first()
            .unwrap()
            .status,
        Status::Downloading
    );
    manager.shutdown().await.unwrap();
}

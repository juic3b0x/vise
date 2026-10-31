use super::*;
use crate::{
    engine::Job,
    model::{Chunk, ChunkStatus},
};
use barriers::Phase;
use std::sync::atomic::Ordering;

fn another(template: &Record) -> Record {
    let mut record = template.clone();
    record.id = Uuid::new_v4();
    record.filename = format!("{}.bin", record.id);
    record.output_path = template.output_path.with_file_name(&record.filename);
    record.part_path = template
        .part_path
        .with_file_name(format!("{}.part", record.id));
    record
}

fn count(phases: &[Phase], phase: Phase) -> usize {
    phases.iter().filter(|p| **p == phase).count()
}

#[tokio::test]
async fn one_mutation_captures_and_compares_one_record_with_up_to_ten_thousand_history_jobs() {
    let (_root, manager, job) = completed_job().await;
    let template = job.record.read().await.clone();
    let client = reqwest::Client::new();
    let mut added = 0;
    for history in [0, 1_000, 10_000] {
        {
            let mut jobs = manager.0.jobs.write().await;
            while added < history {
                let record = another(&template);
                jobs.insert(record.id, Arc::new(Job::new(record, client.clone())));
                added += 1;
            }
        }
        manager.checkpoint().await.unwrap();
        manager.0.journal_trace.take();
        manager
            .0
            .database
            .records_compared
            .store(0, Ordering::Relaxed);
        job.record.write().await.error = Some(format!("history {history}"));
        manager.checkpoint().await.unwrap();
        let phases = manager.0.journal_trace.take();
        assert_eq!(count(&phases, Phase::RecordCapture), 1, "history {history}");
        assert_eq!(count(&phases, Phase::StagingInspection), 0);
        assert_eq!(
            manager.0.database.records_compared.load(Ordering::Relaxed),
            1
        );
        assert!(!manager.0.changes.has_pending());
    }
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn nine_hundred_ninety_five_fresh_queued_jobs_add_zero_staging_inspections() {
    let (_root, manager, template_job) = completed_job().await;
    let template = template_job.record.read().await.clone();
    let client = reqwest::Client::new();
    let mut active = Vec::new();
    for _ in 0..5 {
        let mut record = another(&template);
        record.status = Status::Downloading;
        record.total_bytes = Some(2);
        record.chunks = vec![Chunk {
            start: 0,
            end: Some(2),
            written: 0,
            status: ChunkStatus::Downloading,
        }];
        std::fs::write(&record.part_path, [0, 0]).unwrap();
        let job = Arc::new(Job::new(record.clone(), client.clone()));
        manager.0.jobs.write().await.insert(record.id, job.clone());
        active.push(job);
    }
    manager.0.journal_trace.take();
    manager.checkpoint().await.unwrap();
    assert_eq!(
        count(&manager.0.journal_trace.take(), Phase::StagingInspection),
        5
    );
    {
        let mut jobs = manager.0.jobs.write().await;
        for _ in 0..995 {
            let mut record = another(&template);
            record.status = Status::Queued;
            record.total_bytes = None;
            jobs.insert(record.id, Arc::new(Job::fresh(record, client.clone())));
        }
    }
    for job in &active {
        job.record.write().await.chunks[0].written = 1;
    }
    manager.checkpoint().await.unwrap();
    let phases = manager.0.journal_trace.take();
    assert_eq!(count(&phases, Phase::StagingInspection), 5);
    assert_eq!(count(&phases, Phase::File), 5);
    for job in &active {
        job.record.write().await.chunks[0].written = 2;
    }
    manager.checkpoint().await.unwrap();
    let phases = manager.0.journal_trace.take();
    assert_eq!(count(&phases, Phase::RecordCapture), 5);
    assert_eq!(count(&phases, Phase::StagingInspection), 5);
    // These synthetic jobs have no workers; settle them before orderly shutdown.
    for job in manager.0.jobs.read().await.values() {
        if job.record.read().await.status.active() {
            job.record.write().await.status = Status::Paused;
        }
    }
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn generation_waits_for_directory_and_newer_mutations_survive_acknowledgement() {
    let (_root, manager, job) = completed_job().await;
    let id = job.record.read().await.id;
    job.record.write().await.error = Some("captured".into());
    let captured_generation = manager.0.changes.generation(id).unwrap();
    let (entered, release) = manager.0.journal_trace.gate(Phase::BeforeDirectory);
    let worker = manager.clone();
    let pending = tokio::spawn(async move { worker.checkpoint().await });
    entered.await.unwrap();
    assert_eq!(manager.0.changes.generation(id), Some(captured_generation));
    job.record.write().await.error = Some("newer".into());
    let newer = manager.0.changes.generation(id).unwrap();
    assert!(newer > captured_generation);
    release.send(()).unwrap();
    pending.await.unwrap().unwrap();
    assert_eq!(manager.0.changes.generation(id), Some(newer));
    assert_eq!(
        manager.0.database.load().await.unwrap()[0].error.as_deref(),
        Some("captured")
    );
    manager.checkpoint().await.unwrap();
    assert_eq!(manager.0.changes.generation(id), None);
    assert_eq!(
        manager.0.database.load().await.unwrap()[0].error.as_deref(),
        Some("newer")
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovered_empty_layout_with_zero_offsets_inspects_staging_on_open_and_resume() {
    let (_root, manager, template) = completed_job().await;
    let mut record = another(&*template.record.read().await);
    let template_id = template.record.read().await.id;
    manager.delete(template_id).await.unwrap();
    record.status = Status::Queued;
    record.total_bytes = None;
    let id = record.id;
    let part = record.part_path.clone();
    let config = manager.0.config.clone();
    manager
        .0
        .jobs
        .write()
        .await
        .insert(id, Arc::new(Job::fresh(record, reqwest::Client::new())));
    manager.shutdown().await.unwrap();
    drop(manager);
    // Models a first staging open whose metadata/layout never committed before
    // process death. The journal alone cannot prove that no staging file exists.
    std::fs::write(&part, b"retained").unwrap();
    let manager = Manager::open(config).await.unwrap();
    let recovered = manager.get(id).await.unwrap();
    assert_eq!(recovered.status, Status::Paused);
    assert_eq!(recovered.downloaded_bytes, 0);
    let phases = manager.0.journal_trace.take();
    assert_eq!(count(&phases, Phase::StagingInspection), 1);
    assert_eq!(count(&phases, Phase::File), 1);
    let permits = manager
        .0
        .pool
        .acquire_many(manager.0.config.concurrent_downloads as u32)
        .await
        .unwrap();
    manager.resume(id).await.unwrap();
    let phases = manager.0.journal_trace.take();
    assert_eq!(count(&phases, Phase::StagingInspection), 1);
    assert_eq!(count(&phases, Phase::File), 1);
    assert_eq!(std::fs::read(part).unwrap(), b"retained");
    manager.shutdown().await.unwrap();
    drop(permits);
}

#[tokio::test]
async fn explicit_deletion_and_reinsertion_ignore_writes_from_the_retired_job() {
    let (_root, manager, old) = completed_job().await;
    let record = old.record.read().await.clone();
    manager.0.jobs.write().await.remove(&record.id);
    manager.checkpoint().await.unwrap();
    assert!(manager.0.database.load().await.unwrap().is_empty());
    let replacement = Arc::new(Job::new(record.clone(), reqwest::Client::new()));
    manager.0.jobs.write().await.insert(record.id, replacement);
    let generation = manager.0.changes.generation(record.id);
    old.record.write().await.error = Some("retired worker".into());
    assert_eq!(manager.0.changes.generation(record.id), generation);
    manager.checkpoint().await.unwrap();
    assert_eq!(manager.0.database.load().await.unwrap(), vec![record]);
    manager.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn monitor_samples_only_downloading_jobs_and_speed_does_not_dirty_the_journal() {
    let (_root, manager, history) = completed_job().await;
    let mut record = another(&*history.record.read().await);
    record.status = Status::Downloading;
    let job = Arc::new(Job::new(record.clone(), reqwest::Client::new()));
    manager.0.jobs.write().await.insert(record.id, job.clone());
    manager.checkpoint().await.unwrap();
    manager.0.journal_trace.take();
    let history_guard = history.record.write().await;
    let stop = CancellationToken::new();
    let worker = manager.clone();
    let cancel = stop.clone();
    let monitor = tokio::spawn(async move { worker.monitor(cancel).await });
    tokio::task::yield_now().await;
    for _ in 0..8 {
        tokio::time::advance(Duration::from_millis(250)).await;
        tokio::task::yield_now().await;
    }
    stop.cancel();
    monitor.await.unwrap().unwrap();
    assert!(!manager.0.changes.has_pending());
    assert!(
        manager.0.journal_trace.take().is_empty(),
        "speed sampling made a checkpoint"
    );
    drop(history_guard);
    job.record.write().await.status = Status::Paused;
    assert!(manager.0.changes.sampling().is_empty());
    manager.shutdown().await.unwrap();
}

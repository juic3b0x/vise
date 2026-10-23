use super::fixture::{Fixture, Mode, manager};

#[tokio::test]
async fn queue_rejects_unbounded_names_before_creating_a_task() {
    let fixture = Fixture::start(Mode::Ranges).await;
    let root = tempfile::tempdir().unwrap();
    let manager = manager(&root, 1).await;
    for name in [
        "../escape",
        "a/b",
        "a\\b",
        "/tmp/escape",
        "C:\\escape",
        "..",
        "",
    ] {
        assert!(manager.add(&fixture.url, Some(name)).await.is_err());
    }
    assert!(manager.list().await.is_empty());
    let database = vise::database::Database::open(manager.database_path())
        .await
        .unwrap();
    assert!(database.list().await.unwrap().is_empty());
    assert!(fixture.state.ranges.lock().unwrap().is_empty());
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn substituted_staging_directory_blocks_admission_resume_and_deletion() {
    use super::fixture::progress;
    use std::os::unix::fs::symlink;
    use vise::model::Status;
    let fixture = Fixture::start(Mode::SlowRanges).await;
    let root = tempfile::tempdir().unwrap();
    let manager = manager(&root, 1).await;
    let task = manager.add(&fixture.url, None).await.unwrap();
    progress(&manager, task.id).await;
    manager.pause(task.id).await.unwrap();
    let staging = task.part_path.parent().unwrap();
    let original = staging.with_extension("saved");
    let outside = tempfile::tempdir().unwrap();
    let unrelated = outside.path().join(task.part_path.file_name().unwrap());
    tokio::fs::write(&unrelated, b"keep").await.unwrap();
    tokio::fs::rename(staging, &original).await.unwrap();
    symlink(outside.path(), staging).unwrap();
    assert!(manager.add(&fixture.url, Some("new.bin")).await.is_err());
    assert!(manager.resume(task.id).await.is_err());
    assert!(manager.delete(task.id).await.is_err());
    assert_eq!(manager.get(task.id).await.unwrap().status, Status::Paused);
    let database = vise::database::Database::open(manager.database_path())
        .await
        .unwrap();
    assert_eq!(database.list().await.unwrap().len(), 1);
    assert_eq!(tokio::fs::read(&unrelated).await.unwrap(), b"keep");
    tokio::fs::remove_file(staging).await.unwrap();
    tokio::fs::rename(original, staging).await.unwrap();
    manager.delete(task.id).await.unwrap();
    manager.shutdown().await.unwrap();
    fixture.close().await;
}

#[cfg(unix)]
#[tokio::test]
async fn substituted_staging_files_do_not_delete_history_or_unrelated_data() {
    use super::fixture::progress;
    use std::os::unix::fs::symlink;
    let fixture = Fixture::start(Mode::SlowRanges).await;
    for extension in ["part", "published"] {
        let root = tempfile::tempdir().unwrap();
        let manager = manager(&root, 1).await;
        let task = manager.add(&fixture.url, None).await.unwrap();
        progress(&manager, task.id).await;
        manager.pause(task.id).await.unwrap();
        let path = task.part_path.with_extension(extension);
        let backup = task.part_path.with_extension("saved");
        if extension == "part" {
            tokio::fs::rename(&path, &backup).await.unwrap();
        }
        let outside = tempfile::tempdir().unwrap();
        let unrelated = outside.path().join("keep");
        tokio::fs::write(&unrelated, b"keep").await.unwrap();
        symlink(&unrelated, &path).unwrap();
        assert!(manager.delete(task.id).await.is_err());
        assert_eq!(manager.list().await.len(), 1);
        let database = vise::database::Database::open(manager.database_path())
            .await
            .unwrap();
        assert_eq!(database.list().await.unwrap().len(), 1);
        assert_eq!(tokio::fs::read(&unrelated).await.unwrap(), b"keep");
        tokio::fs::remove_file(path).await.unwrap();
        if extension == "part" {
            tokio::fs::rename(backup, &task.part_path).await.unwrap();
        }
        manager.delete(task.id).await.unwrap();
        manager.shutdown().await.unwrap();
    }
    fixture.close().await;
}

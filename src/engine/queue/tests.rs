use super::*;

#[tokio::test]
async fn indexed_reservations_cover_collisions_concurrent_admission_and_rollback() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::open(crate::engine::Config {
        home_dir: Some(root.path().join("home")),
        output_dir: root.path().join("out"),
        concurrent_downloads: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    let permit = manager.0.pool.acquire().await.unwrap();
    let original = manager.0.config.output_dir.join("data.bin");
    std::fs::write(&original, b"keep").unwrap();
    let url = "http://127.0.0.1:1/data.bin";
    let first = manager.add(url, None).await.unwrap();
    assert_eq!(first.filename, "data (1).bin");
    assert!(
        !first.output_path.exists(),
        "queued paths must be reserved in memory"
    );
    let (second, third) = tokio::join!(manager.add(url, None), manager.add(url, None));
    let names =
        std::collections::BTreeSet::from([second.unwrap().filename, third.unwrap().filename]);
    assert_eq!(
        names,
        std::collections::BTreeSet::from(["data (2).bin".into(), "data (3).bin".into()])
    );
    let sql = rusqlite::Connection::open(manager.database_path()).unwrap();
    sql.execute_batch("CREATE TRIGGER reject_admission BEFORE INSERT ON downloads WHEN NEW.filename='failed.bin' BEGIN SELECT RAISE(ABORT, 'admission rejected'); END;").unwrap();
    assert!(manager.add(url, Some("failed.bin")).await.is_err());
    assert!(
        !manager
            .0
            .jobs
            .read()
            .await
            .contains_path(&manager.0.config.output_dir.join("failed.bin"))
    );
    sql.execute_batch("DROP TRIGGER reject_admission").unwrap();
    assert_eq!(
        manager.add(url, Some("failed.bin")).await.unwrap().filename,
        "failed.bin"
    );

    sql.execute_batch("CREATE TRIGGER reject_delete BEFORE DELETE ON downloads BEGIN SELECT RAISE(ABORT, 'delete rejected'); END;").unwrap();
    assert!(manager.delete(first.id).await.is_err());
    assert!(
        manager
            .0
            .jobs
            .read()
            .await
            .contains_path(&first.output_path)
    );
    assert_eq!(
        manager.add(url, None).await.unwrap().filename,
        "data (4).bin"
    );
    sql.execute_batch("DROP TRIGGER reject_delete").unwrap();
    manager.delete(first.id).await.unwrap();
    assert_eq!(
        manager.add(url, None).await.unwrap().filename,
        "data (1).bin"
    );
    assert_eq!(std::fs::read(original).unwrap(), b"keep");
    manager.shutdown().await.unwrap();
    drop(permit);
}

#[cfg(unix)]
#[tokio::test]
async fn destination_reservations_do_not_reuse_a_dangling_symlink() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::open(crate::engine::Config {
        home_dir: Some(root.path().join("home")),
        output_dir: root.path().join("out"),
        concurrent_downloads: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    let permit = manager.0.pool.acquire().await.unwrap();
    let output = manager.0.config.output_dir.join("data.bin");
    std::os::unix::fs::symlink(root.path().join("absent"), &output).unwrap();
    assert_eq!(
        manager
            .add("http://127.0.0.1:1/data.bin", None)
            .await
            .unwrap()
            .filename,
        "data (1).bin"
    );
    assert!(
        manager
            .add("http://127.0.0.1:1/data.bin", Some("data.bin"))
            .await
            .is_err()
    );
    assert!(
        std::fs::symlink_metadata(output)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    manager.shutdown().await.unwrap();
    drop(permit);
}

use super::*;
use crate::net::credentials::RequestOverrides;

fn private_record() -> Record {
    let mut record = tests::record();
    record.request_credentials = serde_json::from_value::<RequestOverrides>(serde_json::json!({
        "headers":[{"name":"Authorization","value":"private-journal-sentinel"}]
    }))
    .unwrap()
    .resolve(&RequestOverrides::default())
    .unwrap()
    .map(Arc::new);
    record
}

#[tokio::test]
async fn schema5_upgrade_preserves_old_task_behavior() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join(FILE_NAME);
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            DOWNLOADS_SCHEMA
                .replace("    request_credentials TEXT,\n", "")
                .as_str(),
        )
        .unwrap();
    connection.execute_batch(SCHEMA).unwrap();
    connection
        .pragma_update(None, "application_id", APPLICATION_ID)
        .unwrap();
    connection.pragma_update(None, "user_version", 5).unwrap();
    let record = tests::record();
    connection.execute("INSERT INTO downloads(id,url,filename,output_path,part_path,total_size,status,created_at,updated_at,ranged,validator) VALUES(?1,?2,?3,?4,?5,1024,'paused',1234,1234,1,'\"v1\"')",
        params![record.id.to_string(),record.url,record.filename,crate::paths::text(&record.output_path).unwrap(),crate::paths::text(&record.part_path).unwrap()]).unwrap();
    connection
        .execute(
            "INSERT INTO chunks VALUES(?1,0,0,1024,256,'paused')",
            [record.id.to_string()],
        )
        .unwrap();
    drop(connection);
    let database = Database::open(&path).await.unwrap();
    assert_eq!(database.load().await.unwrap(), vec![record]);
    database
        .call(|state| {
            assert_eq!(
                state
                    .connection
                    .pragma_query_value(None, "user_version", |r| r.get::<_, i32>(0))?,
                6
            );
            assert!(
                state
                    .connection
                    .query_row("SELECT request_credentials FROM downloads", [], |r| r
                        .get::<_, Option<String>>(0))?
                    .is_none()
            );
            Ok(())
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn private_credentials_round_trip_without_public_serialization() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join(FILE_NAME);
    let database = Database::open(&path).await.unwrap();
    let record = private_record();
    database
        .persist_records(vec![record.clone()])
        .await
        .unwrap();
    assert!(
        !serde_json::to_string(&record)
            .unwrap()
            .contains("private-journal-sentinel")
    );
    drop(database);
    let restored = Database::open(&path).await.unwrap();
    assert_eq!(restored.load().await.unwrap(), vec![record]);
}

#[cfg(unix)]
#[tokio::test]
async fn journal_permissions_checked_before_persisting_credentials() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join(FILE_NAME);
    let database = Database::open(&path).await.unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
    let error = database
        .persist_records(vec![private_record()])
        .await
        .unwrap_err();
    assert!(error.to_string().contains("0600"));
    assert!(!format!("{error:?} {error}").contains("private-journal-sentinel"));
    assert!(database.load().await.unwrap().is_empty());
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    database
        .persist_records(vec![private_record()])
        .await
        .unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
    assert!(
        database
            .persist_records(vec![private_record()])
            .await
            .is_err()
    );
}

#[cfg(unix)]
#[tokio::test]
async fn credential_writes_require_private_wal_and_shm_files() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join(FILE_NAME);
    let database = Database::open(&path).await.unwrap();
    for suffix in ["-wal", "-shm"] {
        let sidecar = path.with_file_name(format!("{FILE_NAME}{suffix}"));
        assert!(sidecar.is_file());
        std::fs::set_permissions(&sidecar, std::fs::Permissions::from_mode(0o640)).unwrap();
        assert!(
            database
                .persist_records(vec![private_record()])
                .await
                .is_err()
        );
        assert!(database.load().await.unwrap().is_empty());
        std::fs::set_permissions(sidecar, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    database
        .persist_records(vec![private_record()])
        .await
        .unwrap();
}

use super::fixture::{self, Fixture, Mode};

#[tokio::test]
async fn input_files_join_positional_urls_and_obey_output_jobs_and_connections() {
    let source = Fixture::start(Mode::Ranges).await;
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("urls.txt");
    std::fs::write(
        &file,
        format!(
            "\u{feff}# download list\r\n\n {} \r\n# another\n{}\n",
            source.url, source.url
        ),
    )
    .unwrap();
    let destination = dir.path().join("selected-output");
    let mut command = fixture::command(dir.path());
    command
        .arg(&source.url)
        .arg("--input")
        .arg(file)
        .args(["--batch", "-j", "1", "-c", "1", "-o"])
        .arg(&destination);
    let output = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    for name in ["file.bin", "file (1).bin", "file (2).bin"] {
        assert_eq!(
            std::fs::read(destination.join(name)).unwrap(),
            source.state.data
        );
    }
    assert!(source.state.peak.load(std::sync::atomic::Ordering::SeqCst) <= 1);
    let database = vise::database::Database::open(dir.path().join("test-home/.vise/state.sqlite3"))
        .await
        .unwrap();
    assert_eq!(database.list().await.unwrap().len(), 3);
    source.close().await;
}

#[tokio::test]
async fn invalid_input_does_not_seed_valid_positional_urls_or_create_state() {
    let dir = tempfile::tempdir().unwrap();
    for content in [
        None,
        Some("# empty\n"),
        Some("https://example.com/a\ninvalid-url\n"),
    ] {
        let path = dir.path().join("input.txt");
        if let Some(content) = content {
            std::fs::write(&path, content).unwrap();
        }
        let mut command = fixture::command(dir.path());
        command
            .arg("https://example.com/a")
            .arg("--input")
            .arg(&path)
            .arg("--batch");
        let output = tokio::task::spawn_blocking(move || command.output().unwrap())
            .await
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("input"));
        assert!(!dir.path().join("test-home/.vise").exists());
    }
    std::fs::write(dir.path().join("input.txt"), "https://example.com/b").unwrap();
    let mut command = fixture::command(dir.path());
    command
        .arg("https://example.com/a")
        .arg("--input")
        .arg(dir.path().join("input.txt"))
        .args(["--name", "one.bin", "--batch"]);
    let output = tokio::task::spawn_blocking(move || command.output().unwrap())
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("exactly one URL"));
    assert!(!dir.path().join("test-home/.vise").exists());
}

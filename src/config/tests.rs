use super::*;

#[tokio::test]
async fn creates_private_defaults_and_preserves_the_generated_token() {
    let home = tempfile::tempdir().unwrap();
    let first = load_from_home(home.path().into()).await.unwrap();
    assert_eq!(first.values.daemon.host.to_string(), "127.0.0.1");
    assert_eq!(first.values.daemon.port, 8080);
    assert_eq!(first.values.daemon.api_token.len(), 64);
    assert!(
        first
            .values
            .daemon
            .api_token
            .bytes()
            .all(|b| b.is_ascii_hexdigit())
    );
    assert_eq!(
        first.values.downloads.default_dir,
        PathBuf::from("~/Downloads/Vise")
    );
    assert_eq!(first.values.downloads.max_concurrent, 5);
    assert_eq!(first.values.downloads.global_speed_limit_kbps, 0);
    assert!(first.values.proxy.url.is_empty());
    let original = std::fs::read(&first.path).unwrap();
    let second = load_from_home(home.path().into()).await.unwrap();
    assert_eq!(
        first.values.daemon.api_token,
        second.values.daemon.api_token
    );
    assert_eq!(std::fs::read(&first.path).unwrap(), original);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&first.path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(home.path().join(".vise"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
    }
}

#[tokio::test]
async fn concurrent_first_runs_share_one_complete_configuration() {
    let home = tempfile::tempdir().unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let home = home.path().to_owned();
        tasks.spawn(async move { load_from_home(home).await.unwrap().values.daemon.api_token });
    }
    let expected = tasks.join_next().await.unwrap().unwrap();
    while let Some(token) = tasks.join_next().await {
        assert_eq!(token.unwrap(), expected);
    }
    let files: Vec<_> = std::fs::read_dir(home.path().join(".vise"))
        .unwrap()
        .collect();
    assert_eq!(files.len(), 2); // config.toml and config.lock; no staging files.
}

#[tokio::test]
async fn existing_partial_files_and_legacy_token_names_are_preserved() {
    let original = "# Keep this comment\n[daemon]\nport = 9090\nauth_token = 'legacy-test-token-0123456789abcdef'\n[downloads]\nmax_concurrent = 2\n";
    let (_home, result, saved) = read_existing(original).await;
    let loaded = result.unwrap();
    assert_eq!(loaded.values.daemon.port, 9090);
    assert_eq!(
        loaded.values.daemon.api_token,
        "legacy-test-token-0123456789abcdef"
    );
    assert_eq!(loaded.values.downloads.max_concurrent, 2);
    assert!(loaded.values.proxy.url.is_empty());
    assert_eq!(saved, original);
}

#[tokio::test]
async fn malformed_configs_are_preserved_and_errors_do_not_echo_secrets() {
    for source in [
        "[daemon]\nport = 'private-secret-value'\n",
        "[daemon]\nVISE_API_TOKEN = 'private-secret-value\n",
        "[daemon]\nport = 65536\n",
        "[downloads]\nmax_concurent = 5\n",
    ] {
        let (_home, result, saved) = read_existing(source).await;
        let error = result.err().unwrap().to_string();
        assert!(error.contains("invalid TOML or configuration fields"));
        assert!(!error.contains("private-secret-value"));
        assert_eq!(saved, source);
    }
}

async fn read_existing(source: &str) -> (tempfile::TempDir, anyhow::Result<LoadedConfig>, String) {
    let home = tempfile::tempdir().unwrap();
    let directory = home.path().join(".vise");
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("config.toml");
    std::fs::write(&path, source).unwrap();
    let result = load_from_home(home.path().into()).await;
    let saved = std::fs::read_to_string(path).unwrap();
    (home, result, saved)
}

#[tokio::test]
async fn configuration_io_failures_do_not_replace_existing_paths() {
    let home = tempfile::tempdir().unwrap();
    let path = home.path().join(".vise/config.toml");
    std::fs::create_dir_all(&path).unwrap();
    assert!(load_from_home(home.path().into()).await.is_err());
    assert!(path.is_dir());
    assert!(load_from_home("relative-home".into()).await.is_err());
}

#[test]
fn expands_only_the_home_path_component() {
    let home = tempfile::tempdir().unwrap();
    assert_eq!(
        expand_home(Path::new("~/Downloads/Vise"), home.path()).unwrap(),
        home.path().join("Downloads/Vise")
    );
    assert_eq!(
        expand_home(Path::new("~"), home.path()).unwrap(),
        home.path()
    );
    for path in ["relative/雪", "~other/Downloads"] {
        assert_eq!(
            expand_home(Path::new(path), home.path()).unwrap(),
            PathBuf::from(path)
        );
    }
    assert_eq!(expand_home(home.path(), home.path()).unwrap(), home.path());
    assert!(expand_home(Path::new(""), home.path()).is_err());
}

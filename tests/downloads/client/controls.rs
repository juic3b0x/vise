use super::fixture::{Daemon, decoded, run};
use crate::fixture::{Fixture, Mode, expected_hashes};
use vise::{
    model::{Download, Status},
    rate_limiter::RateConfiguration,
};

#[tokio::test]
async fn existing_daemon_defaults_support_json_controls_without_opening_a_second_engine() {
    let daemon = Daemon::start().await;
    let origin = Fixture::start(Mode::SlowRanges).await;
    let configured: RateConfiguration =
        decoded(&daemon.run(&["limit-rate", "8K", "--json"]).await, 0);
    assert_eq!(configured.limit_bytes_per_second, Some(8192));
    let added: Download = decoded(
        &daemon
            .run(&["add", &origin.url, "--name", "remote.bin", "--json"])
            .await,
        0,
    );
    let prefix = added.id.to_string().chars().take(8).collect::<String>();
    let paused: Download = decoded(&daemon.run(&["pause", &prefix, "--json"]).await, 0);
    assert_eq!(paused.status, Status::Paused);
    let fetched: Download = decoded(&daemon.run(&["get", &prefix, "--json"]).await, 0);
    assert_eq!(fetched.id, added.id);
    let _: Download = decoded(&daemon.run(&["resume", &prefix, "--json"]).await, 0);
    let _: Download = decoded(&daemon.run(&["retry", &prefix, "--json"]).await, 0);
    let jobs: Vec<Download> = decoded(&daemon.run(&["list", "--json"]).await, 0);
    assert_eq!(jobs.len(), 1);
    let removed: serde_json::Value = decoded(&daemon.run(&["delete", &prefix, "--json"]).await, 0);
    assert_eq!(removed.get("deleted"), Some(&serde_json::json!(true)));
    assert!(daemon.manager.list().await.is_empty());
    assert!(!added.part_path.exists());
    assert!(
        !daemon
            .root
            .path()
            .join("test-home/unused-client-default")
            .exists()
    );
    assert!(
        !daemon
            .root
            .path()
            .join("test-home/.vise/config.lock")
            .exists()
    );
    daemon.close().await;
    origin.close().await;
}

#[tokio::test]
async fn wildcard_listener_defaults_map_to_loopback_and_environment_token_takes_precedence() {
    let daemon = Daemon::start().await;
    let path = daemon.root.path().join("test-home/.vise/config.toml");
    let config = std::fs::read_to_string(&path)
        .unwrap()
        .replace("127.0.0.1", "0.0.0.0")
        .replace(
            super::fixture::SECRET,
            "overridden-file-token-0123456789abcdef",
        );
    std::fs::write(&path, &config).unwrap();
    let mut command = daemon.command();
    command
        .args(["list", "--json"])
        .env("VISE_API_TOKEN", super::fixture::SECRET);
    let jobs: Vec<Download> = decoded(&run(command).await, 0);
    assert!(jobs.is_empty());
    assert_eq!(std::fs::read_to_string(&path).unwrap(), config);
    assert!(
        !daemon
            .root
            .path()
            .join("test-home/.vise/config.lock")
            .exists()
    );
    daemon.close().await;
}

#[tokio::test]
async fn wait_returns_only_verified_publication_or_a_terminal_failure() {
    let daemon = Daemon::start().await;
    let origin = Fixture::start(Mode::Ranges).await;
    let hashes = expected_hashes(&origin.state.data);
    let done: Download = decoded(
        &daemon
            .run(&[
                "add",
                &origin.url,
                "--name",
                "verified.bin",
                "--expected-sha256",
                &hashes.sha256.unwrap(),
                "--expected-md5",
                &hashes.md5.unwrap(),
                "--wait",
                "--json",
            ])
            .await,
        0,
    );
    assert_eq!(done.status, Status::Completed);
    assert!(done.checksums.verified_at.is_some());
    assert!(!done.part_path.exists());
    assert_eq!(
        tokio::fs::read(&done.output_path).await.unwrap(),
        origin.state.data
    );
    let corrupt: Download = decoded(
        &daemon
            .run(&[
                "add",
                &origin.url,
                "--name",
                "corrupt.bin",
                "--expected-sha256",
                &"0".repeat(64),
                "--wait",
                "--json",
            ])
            .await,
        2,
    );
    assert_eq!(corrupt.status, Status::Corrupt);
    assert!(corrupt.part_path.exists() && !corrupt.output_path.exists());
    let refused = daemon.run(&["resume", &corrupt.id.to_string()]).await;
    assert_eq!(refused.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&refused.stderr).contains("409"));
    let removed = daemon.run(&["delete", &done.id.to_string()]).await;
    assert!(removed.status.success());
    assert!(done.output_path.exists());
    daemon.close().await;
    origin.close().await;
}

#[tokio::test]
async fn explicit_endpoint_and_environment_authentication_need_no_local_config_or_engine() {
    let daemon = Daemon::start().await;
    let client_root = tempfile::tempdir().unwrap();
    let mut command = crate::fixture::command(client_root.path());
    command
        .args(["remote", "--endpoint", &daemon.endpoint, "list", "--json"])
        .env("VISE_API_TOKEN", super::fixture::SECRET)
        .env("HTTP_PROXY", "http://127.0.0.1:1")
        .env("ALL_PROXY", "http://127.0.0.1:1")
        .env_remove("NO_PROXY")
        .env_remove("no_proxy")
        .env_remove("HOME")
        .env_remove("USERPROFILE")
        .env_remove("HOMEDRIVE")
        .env_remove("HOMEPATH");
    let jobs: Vec<Download> = decoded(&run(command).await, 0);
    assert!(jobs.is_empty());
    assert!(!client_root.path().join("test-home/.vise").exists());
    let mut bad = daemon.command();
    bad.arg("list")
        .env("VISE_API_TOKEN", "wrong-test-token-0123456789abcdef");
    let failed = run(bad).await;
    assert_eq!(failed.status.code(), Some(1));
    let error = String::from_utf8_lossy(&failed.stderr);
    assert!(
        error.contains("401")
            && !error.contains(super::fixture::SECRET)
            && !error.contains("wrong-test-token")
    );
    daemon.close().await;
}

#[tokio::test]
async fn direct_urls_keep_their_original_engine_lock_behavior() {
    let daemon = Daemon::start().await;
    // Local mode still validates its own download defaults; give it valid ones.
    std::fs::write(daemon.root.path().join("test-home/.vise/config.toml"), "").unwrap();
    let mut command = crate::fixture::command(daemon.root.path());
    command.args(["--batch", "https://example.org/direct.bin"]);
    let output = run(command).await;
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("global engine"));
    assert!(daemon.manager.list().await.is_empty());
    daemon.close().await;
}

#[tokio::test]
async fn a_missing_daemon_does_not_create_state_or_fall_back_to_a_local_download() {
    let root = tempfile::tempdir().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    // Keep the socket reserved but unserved so the client must time out.
    let mut command = crate::fixture::command(root.path());
    command
        .args([
            "remote",
            "--endpoint",
            &endpoint,
            "--request-timeout",
            "1",
            "add",
            "https://example.org/file",
        ])
        .env("VISE_API_TOKEN", super::fixture::SECRET);
    let output = run(command).await;
    assert_eq!(output.status.code(), Some(1));
    assert!(!root.path().join("test-home/.vise").exists());
    drop(listener);
}

#[tokio::test]
async fn misplaced_remote_commands_fail_before_creating_local_state() {
    let root = tempfile::tempdir().unwrap();
    let mut command = crate::fixture::command(root.path());
    command.args(["--daemon", "remote", "list"]);
    let output = run(command).await;
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("put remote immediately after vise"));
    assert!(!root.path().join("test-home/.vise").exists());
}

#[tokio::test]
async fn closing_the_stdout_consumer_detaches_successfully_without_stopping_the_daemon() {
    use std::{process::Stdio, time::Duration};
    let daemon = Daemon::start().await;
    let mut process = super::fixture::Process(Some(
        daemon
            .command()
            .args(["watch", "--json"])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    ));
    drop(process.0.as_mut().unwrap().stdout.take());
    let status = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Some(status) = process.0.as_mut().unwrap().try_wait().unwrap() {
                break status;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("client did not detach after its stdout consumer closed");
    assert_eq!(status.code(), Some(0));
    let jobs: Vec<Download> = decoded(&daemon.run(&["list", "--json"]).await, 0);
    assert!(jobs.is_empty());
    daemon.close().await;
}

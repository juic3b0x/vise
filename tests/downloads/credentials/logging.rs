mod fixture;
use fixture::{FILTERS, Origins, Process, Secrets, cookie_file};

#[tokio::test]
async fn global_trace_never_exposes_request_credentials_cli() {
    let mut leaks = Vec::new();
    for filter in FILTERS {
        let root = tempfile::tempdir().unwrap();
        let secrets = Secrets::new();
        let origins = Origins::start(&secrets).await;
        let file = cookie_file(root.path(), &secrets);
        fixture::seed(root.path(), &origins, &secrets, &file).await;
        let mut command = crate::fixture::command(root.path());
        command
            .env("RUST_LOG", filter)
            .env("VISE_API_TOKEN", secrets.get("api_token"))
            .args(["--batch", "--resume-all", "--cookies"])
            .arg(&file);
        let output = Process::spawn(command).finish(false).await;
        assert!(output.status.success());
        origins.verify(&secrets);
        let counts = secrets.matches(&output);
        println!("CLI RUST_LOG={filter}: {counts:?}");
        for (kind, count) in counts {
            if count != 0 {
                leaks.push((filter, kind, count));
            }
        }
        origins.close().await;
    }
    assert!(
        leaks.is_empty(),
        "credential matches in captured stdout/stderr: {leaks:?}"
    );
}

#[tokio::test]
async fn global_trace_never_exposes_request_credentials_daemon() {
    let mut leaks = Vec::new();
    for filter in FILTERS {
        let root = tempfile::tempdir().unwrap();
        let secrets = Secrets::new();
        let origins = Origins::start(&secrets).await;
        let file = cookie_file(root.path(), &secrets);
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let bind = reservation.local_addr().unwrap();
        drop(reservation);
        let base = format!("http://{bind}");
        let mut command = crate::fixture::command(root.path());
        command
            .env("RUST_LOG", filter)
            .env("VISE_API_TOKEN", secrets.get("api_token"))
            .args([
                "--daemon",
                "--bind",
                &bind.to_string(),
                "--connections",
                "1",
                "--proxy",
                &origins.proxy_url,
                "--cookies",
            ])
            .arg(&file)
            .arg("--output")
            .arg(root.path().join("out"));
        let process = Process::spawn(command);
        fixture::ready(&base, secrets.get("api_token")).await;
        let task = fixture::request(
            &base,
            "/api/downloads",
            secrets.get("api_token"),
            Some(secrets.admission(&origins.url)),
        )
        .await;
        fixture::completed(
            &base,
            secrets.get("api_token"),
            task["id"].as_str().unwrap(),
        )
        .await;
        let output = process.finish(true).await;
        origins.verify(&secrets);
        let counts = secrets.matches(&output);
        println!("DAEMON RUST_LOG={filter}: {counts:?}");
        for (kind, count) in counts {
            if count != 0 {
                leaks.push((filter, kind, count));
            }
        }
        origins.close().await;
    }
    assert!(
        leaks.is_empty(),
        "credential matches in captured stdout/stderr: {leaks:?}"
    );
}

mod api;
mod clients;
mod fixture;
mod logging;
mod recovery;
use crate::fixture::{Mode, manager, terminal};
use fixture::{Capture, SECRET, options};
use vise::model::Status;

#[tokio::test]
async fn request_credentials_reach_probe_chunks_and_retries() {
    let server = Capture::start(Mode::Transient).await;
    let root = tempfile::tempdir().unwrap();
    let manager = manager(&root, 4).await;
    let task = manager
        .add_with_options(&server.url, None, options())
        .await
        .unwrap();
    assert_eq!(terminal(&manager, task.id).await.status, Status::Completed);
    let requests = server.requests.lock().unwrap().clone();
    assert!(requests.len() >= 6);
    for (_, headers) in &requests {
        assert_eq!(headers.get("x-secret").unwrap(), SECRET);
        assert_eq!(headers.get("user-agent").unwrap(), SECRET);
        assert_eq!(headers.get("authorization").unwrap(), SECRET);
        assert!(
            headers
                .get("cookie")
                .unwrap()
                .to_str()
                .unwrap()
                .contains(SECRET)
        );
        assert!(
            headers
                .get("referer")
                .unwrap()
                .to_str()
                .unwrap()
                .contains(SECRET)
        );
    }
    assert!(requests.iter().skip(1).any(|(_, h)| {
        h.get("cookie")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("learned=server-session")
    }));
    manager.shutdown().await.unwrap();
    server.close().await;
}

#[tokio::test]
async fn same_origin_redirect_preserves_request_credentials() {
    let server = Capture::start(Mode::Ranges).await;
    *server.redirect.lock().unwrap() = Some(server.url.clone());
    let root = tempfile::tempdir().unwrap();
    let manager = manager(&root, 2).await;
    let url = server.url.replace("/file", "/redirect");
    let task = manager
        .add_with_options(&url, None, options())
        .await
        .unwrap();
    assert_eq!(terminal(&manager, task.id).await.status, Status::Completed);
    for (_, headers) in server.requests.lock().unwrap().iter() {
        assert_eq!(headers.get("x-secret").unwrap(), SECRET);
    }
    manager.shutdown().await.unwrap();
    server.close().await;
}

#[tokio::test]
async fn cross_origin_redirect_strips_credentials_but_sends_origin_referer() {
    for different_host in [false, true] {
        let source = Capture::start(Mode::Ranges).await;
        let target = Capture::start(Mode::Ranges).await;
        *source.redirect.lock().unwrap() = Some(if different_host {
            target.url.replace("127.0.0.1", "localhost")
        } else {
            target.url.clone()
        });
        let root = tempfile::tempdir().unwrap();
        let manager = manager(&root, 2).await;
        let task = manager
            .add_with_options(&source.url.replace("/file", "/redirect"), None, options())
            .await
            .unwrap();
        assert_eq!(terminal(&manager, task.id).await.status, Status::Completed);
        for (_, headers) in target.requests.lock().unwrap().iter() {
            assert!(
                headers.get("x-secret").is_none()
                    && headers.get("authorization").is_none()
                    && headers.get("cookie").is_none()
            );
            assert_ne!(headers.get("user-agent").unwrap(), SECRET);
            assert_eq!(headers.get("referer").unwrap(), "http://source.example/");
        }
        manager.shutdown().await.unwrap();
        source.close().await;
        target.close().await;
    }
}

#[tokio::test]
async fn redirect_hops_use_task_proxy_route() {
    use crate::proxy_support::{Kind, TestProxy};
    let source = Capture::start(Mode::Ranges).await;
    let target = Capture::start(Mode::Ranges).await;
    *source.redirect.lock().unwrap() = Some(target.url.clone());
    let proxy = TestProxy::start(Kind::Http, true).await;
    let root = tempfile::tempdir().unwrap();
    let manager = manager(&root, 2).await;
    let mut opts = options();
    opts.proxy_url = Some(proxy.url.parse().unwrap());
    let task = manager
        .add_with_options(&source.url.replace("/file", "/redirect"), None, opts)
        .await
        .unwrap();
    assert_eq!(terminal(&manager, task.id).await.status, Status::Completed);
    let requests = proxy.requests.lock().unwrap().clone();
    for url in [&source.url, &target.url] {
        let host = url::Url::parse(url).unwrap().authority().to_owned();
        assert!(requests.iter().any(|r| r.authority == host));
    }
    manager.shutdown().await.unwrap();
    proxy.close().await;
    source.close().await;
    target.close().await;
}

#[tokio::test]
async fn redirect_back_does_not_restore_credentials() {
    let source = Capture::start(Mode::Ranges).await;
    let target = Capture::start(Mode::Ranges).await;
    *source.redirect.lock().unwrap() = Some(target.url.replace("/file", "/redirect"));
    *target.redirect.lock().unwrap() = Some(source.url.clone());
    let root = tempfile::tempdir().unwrap();
    let manager = manager(&root, 2).await;
    let task = manager
        .add_with_options(&source.url.replace("/file", "/redirect"), None, options())
        .await
        .unwrap();
    assert_eq!(terminal(&manager, task.id).await.status, Status::Completed);
    for (path, headers) in source.requests.lock().unwrap().iter() {
        if path == "/file" {
            assert!(headers.get("x-secret").is_none() && headers.get("cookie").is_none());
            assert_eq!(headers.get("referer").unwrap(), "http://source.example/");
        }
    }
    manager.shutdown().await.unwrap();
    source.close().await;
    target.close().await;
}

#[tokio::test]
async fn legacy_tasks_use_unchanged_redirect_path() {
    let source = Capture::start(Mode::Ranges).await;
    let target = Capture::start(Mode::Ranges).await;
    *source.redirect.lock().unwrap() = Some(target.url.clone());
    let root = tempfile::tempdir().unwrap();
    let manager = manager(&root, 2).await;
    let cookie_file = root.path().join("cookies.txt");
    std::fs::write(
        &cookie_file,
        "127.0.0.1\tFALSE\t/\tFALSE\t0\tlegacy\tlegacy-session\n",
    )
    .unwrap();
    let url = source.url.replace("/file", "/redirect");
    let task = manager
        .add_with_options(
            &url,
            None,
            vise::engine::TransferOptions {
                cookies_file: Some(cookie_file),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(terminal(&manager, task.id).await.status, Status::Completed);
    for (_, headers) in target.requests.lock().unwrap().iter() {
        assert_eq!(headers.get("referer").unwrap(), url.as_str());
        assert!(
            headers
                .get("cookie")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("legacy=legacy-session")
        );
    }
    let connection = rusqlite::Connection::open(manager.database_path()).unwrap();
    assert!(
        connection
            .query_row("SELECT request_credentials FROM downloads", [], |r| r
                .get::<_, Option<String>>(0))
            .unwrap()
            .is_none()
    );
    drop(connection);
    manager.shutdown().await.unwrap();
    source.close().await;
    target.close().await;
}

#[tokio::test]
async fn same_origin_set_cookie_reaches_later_chunks() {
    let server = Capture::start(Mode::Ranges).await;
    let root = tempfile::tempdir().unwrap();
    let manager = manager(&root, 2).await;
    let task = manager
        .add_with_options(&server.url, None, options())
        .await
        .unwrap();
    assert_eq!(terminal(&manager, task.id).await.status, Status::Completed);
    for (_, headers) in server.requests.lock().unwrap().iter().skip(1) {
        assert!(
            headers
                .get("cookie")
                .unwrap()
                .to_str()
                .unwrap()
                .contains("learned=server-session")
        );
    }
    let connection = rusqlite::Connection::open(manager.database_path()).unwrap();
    let saved: String = connection
        .query_row("SELECT request_credentials FROM downloads", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert!(!saved.contains("server-session"));
    drop(connection);
    manager.shutdown().await.unwrap();
    server.close().await;
}

#[tokio::test]
async fn supplied_cookie_overrides_matching_file_cookie() {
    let server = Capture::start(Mode::Ranges).await;
    let root = tempfile::tempdir().unwrap();
    let manager = manager(&root, 2).await;
    let file = root.path().join("cookies.txt");
    std::fs::write(
        &file,
        "127.0.0.1\tFALSE\t/\tFALSE\t0\tsession\told-file-cookie\n",
    )
    .unwrap();
    let mut opts = options();
    opts.cookies_file = Some(file);
    let task = manager
        .add_with_options(&server.url, None, opts)
        .await
        .unwrap();
    assert_eq!(terminal(&manager, task.id).await.status, Status::Completed);
    for (_, headers) in server.requests.lock().unwrap().iter() {
        let cookie = headers.get("cookie").unwrap().to_str().unwrap();
        assert!(cookie.contains(SECRET) && !cookie.contains("old-file-cookie"));
    }
    manager.shutdown().await.unwrap();
    server.close().await;
}

#[tokio::test]
async fn redirect_limit_and_invalid_locations_fail_without_echoing_values() {
    let source = Capture::start(Mode::Ranges).await;
    let root = tempfile::tempdir().unwrap();
    let manager = manager(&root, 2).await;
    let url = source.url.replace("/file", "/redirect");
    for location in [
        url.clone(),
        format!("http://user:{SECRET}@127.0.0.1/file"),
        format!("file:///{SECRET}"),
    ] {
        *source.redirect.lock().unwrap() = Some(location);
        let before = source.requests.lock().unwrap().len();
        let task = manager
            .add_with_options(&url, None, options())
            .await
            .unwrap();
        let done = terminal(&manager, task.id).await;
        assert_eq!(done.status, Status::Failed);
        assert!(!done.error.unwrap().contains(SECRET));
        assert!(source.requests.lock().unwrap().len() - before <= 11);
    }
    manager.shutdown().await.unwrap();
    source.close().await;
}

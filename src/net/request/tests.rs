use super::*;

#[test]
fn http_to_https_redirect_same_host_strips_credentials() {
    // Equal explicit ports isolate the scheme change from the port comparison.
    let initial = Url::parse("http://example.com:8443/file").unwrap();
    let next = Url::parse("https://example.com:8443/file").unwrap();
    assert!(!redirect_credentials_allowed(&initial, &next, false));
    assert!(redirect_credentials_allowed(&initial, &initial, false));
}

#[test]
fn redirect_back_decision_never_restores_stripped_credentials() {
    let initial = Url::parse("http://example.com/file").unwrap();
    let next = Url::parse("http://other.example.com/file").unwrap();
    let allowed = redirect_credentials_allowed(&initial, &next, false);
    assert!(!allowed);
    assert!(!redirect_credentials_allowed(&initial, &initial, !allowed));
}

#[tokio::test]
async fn only_absent_task_settings_can_initialize_shared_transport() {
    use super::super::credentials::RequestOverrides;
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("cookies.txt");
    std::fs::write(&file, "127.0.0.1\tFALSE\t/\tFALSE\t0\tsession\tprivate\n").unwrap();
    let proxy: ProxyUrl = "http://user:password@127.0.0.1:1".parse().unwrap();
    let context = RequestOverrides::from_cli(&["X-Secret: private".into()], None, None)
        .unwrap()
        .resolve(&Default::default())
        .unwrap()
        .unwrap();
    for mask in 1..8 {
        let shared = tokio::sync::OnceCell::new();
        let proxy = (mask & 1 != 0).then_some(&proxy);
        let file = (mask & 2 != 0).then_some(file.as_path());
        let context = (mask & 4 != 0).then_some(&context);
        Transport::for_task(&shared, Duration::from_secs(1), proxy, file, context)
            .await
            .unwrap();
        assert!(
            shared.get().is_none(),
            "task-specific transport entered shared cell"
        );
        let (plain, path) = Transport::for_task(&shared, Duration::from_secs(1), None, None, None)
            .await
            .unwrap();
        assert!(shared.get().is_some());
        assert!(plain.private.is_none() && path.is_none());
        let request = plain
            .base("http://127.0.0.1/file", None, None)
            .build()
            .unwrap();
        for name in [
            header::COOKIE,
            header::AUTHORIZATION,
            header::PROXY_AUTHORIZATION,
            header::REFERER,
            header::HeaderName::from_static("x-secret"),
        ] {
            assert!(request.headers().get(name).is_none());
        }
    }
}

use super::*;
use serde_json::json;

fn input(value: serde_json::Value) -> RequestOverrides {
    serde_json::from_value(value).unwrap()
}

#[test]
fn credentials_absent_from_debug_serialize_and_error_chains() {
    let secret = "unique-private-credential-sentinel";
    let options = input(json!({"headers":[{"name":"Authorization","value":secret}],
        "referer":format!("https://example.com/{secret}"),"user_agent":secret,
        "cookies":[{"name":"session","value":secret,"domain":"example.com",
            "path":"/","secure":true,"expires":null,"httpOnly":true}]}));
    let context = options
        .resolve(&RequestOverrides::default())
        .unwrap()
        .unwrap();
    for public in [
        format!("{options:?}"),
        format!("{context:?}"),
        format!("{context}"),
        serde_json::to_string(&options).unwrap(),
        serde_json::to_string(&context).unwrap(),
    ] {
        assert!(!public.contains(secret));
    }
    let encoded = context.journal_json().unwrap();
    assert!(encoded.contains(secret));
    assert_eq!(TaskRequestContext::from_journal(&encoded).unwrap(), context);
    let bad = format!("{{\"cookies\":[{{\"secure\":\"{secret}\"}}]}}");
    assert!(
        !serde_json::from_str::<RequestOverrides>(&bad)
            .unwrap_err()
            .to_string()
            .contains(secret)
    );
}

#[test]
fn credential_validation_rejects_controls_and_injection_without_echo() {
    for value in [
        "private\r\nInjected: true",
        "private\0value",
        "private\tvalue",
        "private\u{7f}",
    ] {
        let options = input(json!({"headers":[{"name":"X-Test","value":value}]}));
        let error = options.resolve(&RequestOverrides::default()).unwrap_err();
        assert!(!format!("{error:?} {error}").contains("private"));
    }
    for name in ["bad name", "bad:name", "", "x\nsecret"] {
        assert!(
            input(json!({"headers":[{"name":name,"value":"private"}]}))
                .resolve(&RequestOverrides::default())
                .is_err()
        );
    }
}

#[test]
fn engine_owned_headers_are_rejected_without_echo() {
    for name in [
        "Host",
        "Range",
        "If-Range",
        "Content-Length",
        "Transfer-Encoding",
        "Connection",
        "Accept-Encoding",
        "Content-Encoding",
        "Cookie",
        "Set-Cookie",
        "Proxy-Authorization",
        "If-Match",
        "Want-Repr-Digest",
    ] {
        let error = input(json!({"headers":[{"name":name,"value":"private"}]}))
            .resolve(&RequestOverrides::default())
            .unwrap_err()
            .to_string();
        assert!(!error.contains("private"));
        if matches!(name, "Cookie" | "Set-Cookie") {
            assert!(error.contains("--cookies FILE") && error.contains("structured cookies"));
        }
    }
}

#[test]
fn credential_limits_and_duplicate_user_agent_are_rejected() {
    for value in [
        json!({"headers":[{"name":"X-Test","value":"s".repeat(8193)}]}),
        json!({"headers":(0..33).map(|_|json!({"name":"X-Test","value":"s"})).collect::<Vec<_>>()}),
        json!({"headers":[{"name":"User-Agent","value":"first"}],"user_agent":"second"}),
        json!({"headers":[{"name":"Referer","value":"https://example.com"}],"referer":"https://example.com"}),
    ] {
        assert!(input(value).resolve(&RequestOverrides::default()).is_err());
    }
}

#[test]
fn api_explicit_overrides_and_clears_are_respected() {
    let defaults = input(json!({"headers":[{"name":"X-One","value":"old"},
        {"name":"X-Two","value":"retained"}],"referer":"https://example.com/private","user_agent":"configured"}));
    for value in [
        json!({}),
        json!({"headers":null,"referer":null,"user_agent":null}),
    ] {
        assert_eq!(
            input(value).resolve(&defaults).unwrap(),
            defaults.resolve(&RequestOverrides::default()).unwrap()
        );
    }
    let options = input(json!({"headers":[],"referer":"","user_agent":""}));
    let summary = options.resolve(&defaults).unwrap().unwrap().summary();
    assert!(!summary.has_custom_headers && !summary.has_referer && !summary.has_user_agent);
    let context = input(json!({"headers":[{"name":"x-one","value":"new"}]}))
        .resolve(&defaults)
        .unwrap()
        .unwrap();
    assert_eq!(context.summary().header_count, 2);
    assert!(context.journal_json().unwrap().contains("retained"));
}

#[test]
fn multi_origin_batch_with_credentials_is_rejected() {
    let urls = vec![
        "https://example.com/a".into(),
        "https://example.com:443/b".into(),
    ];
    let headers = input(json!({"headers":[{"name":"X-Auth","value":"private"}]}));
    let context = headers
        .resolve(&RequestOverrides::default())
        .unwrap()
        .unwrap();
    assert!(context.validate_urls(&urls).is_ok());
    for url in [
        "http://example.com/b",
        "https://example.com:444/b",
        "https://other.example.com/b",
    ] {
        let urls = vec![urls.first().unwrap().clone(), url.into()];
        assert!(
            context
                .validate_urls(&urls)
                .unwrap_err()
                .to_string()
                .contains("split")
        );
        assert!(
            input(json!({"user_agent":"private"}))
                .resolve(&RequestOverrides::default())
                .unwrap()
                .unwrap()
                .validate_urls(&urls)
                .is_ok()
        );
    }
}

#[test]
fn cookie_scope_prefixes_and_expired_override_are_enforced() {
    use reqwest::cookie::CookieStore;
    let valid = json!({"name":"session","value":"private","domain":"example.com","hostOnly":true,
        "path":"/private","secure":true,"expires":null,"httpOnly":true});
    let context = input(json!({"cookies":[valid.clone()]}))
        .resolve(&RequestOverrides::default())
        .unwrap()
        .unwrap();
    assert!(
        context
            .validate_urls(&["https://example.com/private/file".into()])
            .is_ok()
    );
    for url in [
        "http://example.com/private/file",
        "https://sub.example.com/private/file",
        "https://example.com/private2/file",
    ] {
        assert!(context.validate_urls(&[url.into()]).is_err());
    }
    let jar = reqwest::cookie::Jar::default();
    let url = Url::parse("https://example.com/private/file").unwrap();
    jar.add_cookie_str("session=old; Path=/private", &url);
    let mut expired = valid;
    expired["expires"] = serde_json::json!(1);
    input(json!({"cookies":[expired]}))
        .resolve(&RequestOverrides::default())
        .unwrap()
        .unwrap()
        .populate(&jar)
        .unwrap();
    assert!(jar.cookies(&url).is_none());
    let bad = json!({"name":"__Host-session","value":"private","domain":"example.com","hostOnly":false,
        "path":"/","secure":true,"expires":null,"httpOnly":true});
    assert!(
        input(json!({"cookies":[bad]}))
            .resolve(&RequestOverrides::default())
            .is_err()
    );
}

#[test]
fn aggregate_limits_and_single_origin_rules_cover_every_credential_kind() {
    let header = |n: usize| json!({"name":"X-Secret","value":"v".repeat(n)});
    let cookie = json!({"name":"session","value":"v".repeat(1000),"domain":"example.com","path":"/",
        "secure":true,"expires":null,"httpOnly":true});
    for value in [
        json!({"headers":[header(8192),header(8192),header(8192),header(8192)]}),
        json!({"user_agent":"v".repeat(513)}),
        json!({"referer":format!("https://example.com/{}","v".repeat(8192))}),
        json!({"cookies":vec![cookie.clone();129]}),
        json!({"cookies":vec![cookie.clone();32]}),
    ] {
        assert!(input(value).resolve(&RequestOverrides::default()).is_err());
    }
    for value in [
        json!({"referer":"https://example.com/private"}),
        json!({"cookies":[cookie]}),
    ] {
        let context = input(value)
            .resolve(&RequestOverrides::default())
            .unwrap()
            .unwrap();
        assert!(
            context
                .validate_urls(&[
                    "https://example.com/a".into(),
                    "https://example.com:444/b".into()
                ])
                .is_err()
        );
    }
    let cli = RequestOverrides::from_cli(&["User-Agent: explicit".into()], None, None)
        .unwrap()
        .with_user_agent_default(Some("configured"))
        .unwrap();
    let mut wire = json!({});
    cli.encode_admission(&mut wire).unwrap();
    assert_eq!(wire["user_agent"], "explicit");
}

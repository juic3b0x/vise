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

//! The dashboard fixture is generated at the real Record -> Download -> JSON boundary.
use super::*;
use crate::net::credentials::RequestOverrides;
use serde_json::json;
use std::sync::Arc;

const SENTINELS: [&str; 5] = [
    "GOLDEN_HEADER_SENTINEL_83172",
    "GOLDEN_AUTH_SENTINEL_59384",
    "GOLDEN_REFERER_SENTINEL_24961",
    "GOLDEN_AGENT_SENTINEL_48273",
    "GOLDEN_COOKIE_SENTINEL_73018",
];

fn credentialed_record() -> Record {
    let [header, authorization, referer, agent, cookie] = SENTINELS;
    let request: RequestOverrides = serde_json::from_value(json!({
        "headers": [
            {"name":"X-Session", "value": header},
            {"name":"Authorization", "value": authorization}
        ],
        "referer": format!("https://downloads.example.com/{referer}"),
        "user_agent": agent,
        "cookies": [{"name":"session", "value":cookie,
            "domain":"downloads.example.com", "hostOnly":true, "path":"/",
            "secure":true, "expires":null, "httpOnly":true}]
    }))
    .unwrap();
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    Record {
        id: Uuid::parse_str("11111111-2222-4333-8444-555555555555").unwrap(),
        url: "https://downloads.example.com/archive.zip".into(),
        filename: "archive.zip".into(),
        output_path: root.join("downloads/archive.zip"),
        part_path: root.join("downloads/.vise/11111111-2222-4333-8444-555555555555.part"),
        proxy_url: None,
        cookies_file: None,
        request_credentials: Some(Arc::new(
            request.resolve(&Default::default()).unwrap().unwrap(),
        )),
        status: Status::Paused,
        total_bytes: Some(1024),
        ranged: true,
        validator: Some("\"golden\"".into()),
        chunks: vec![Chunk {
            start: 0,
            end: Some(1024),
            written: 256,
            status: ChunkStatus::Paused,
        }],
        error: None,
        checksums: Default::default(),
        created_at: 1_700_000_000_000,
        speed: 0.0,
    }
}

#[test]
fn public_download_snapshot_matches_dashboard_golden_fixture() {
    let record = credentialed_record();
    record
        .request_credentials
        .as_ref()
        .unwrap()
        .validate_urls(std::slice::from_ref(&record.url))
        .unwrap();
    let mut download = record.snapshot();
    // Normalize only volatile path VALUES before serialization. Never patch JSON
    // keys: removals/renames/type/nesting changes must fail the golden check too.
    download.output_path = "downloads/archive.zip".into();
    download.part_path = "downloads/.vise/11111111-2222-4333-8444-555555555555.part".into();
    let actual = serde_json::to_value(download).unwrap();
    let serialized = serde_json::to_string_pretty(&actual).unwrap();
    for sentinel in SENTINELS {
        // Check before printing any equality diagnostics or writing a fixture.
        assert!(
            !serialized.contains(sentinel),
            "public download snapshot leaked a request credential"
        );
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/public-download.json");
    // Explicit regeneration: VISE_UPDATE_PUBLIC_DOWNLOAD_FIXTURE=1 cargo test
    // --lib public_download_snapshot_matches_dashboard_golden_fixture --locked
    if std::env::var("VISE_UPDATE_PUBLIC_DOWNLOAD_FIXTURE").as_deref() == Ok("1") {
        std::fs::write(&path, format!("{serialized}\n")).unwrap();
    }
    let golden: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(
        actual, golden,
        "public Download JSON changed; review and regenerate the shared dashboard fixture"
    );
}

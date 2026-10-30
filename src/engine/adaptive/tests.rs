use super::*;

fn candidate(end: u64, written: u64) -> Candidate {
    Candidate {
        start: 0,
        end,
        written,
        frontier: written,
        speed: None,
    }
}

fn check(c: Candidate, minimum: u64, decision: Option<Decision>) {
    if let Some(d) = decision {
        assert_eq!(d.start, c.start);
        assert_eq!(d.old_end, c.end);
        assert!(d.boundary >= c.start + c.written + minimum);
        assert!(d.boundary >= c.frontier + MARGIN);
        assert!(d.boundary <= c.end - minimum);
        assert!(d.boundary.is_multiple_of(ALIGNMENT));
    }
}

#[test]
fn split_decision_exhaustive_small_layouts() {
    for end in 0..48 {
        for written in 0..=end {
            for reserved in written..=end {
                let c = Candidate {
                    frontier: reserved * ALIGNMENT,
                    ..candidate(end * ALIGNMENT, written * ALIGNMENT)
                };
                for minimum in [0, ALIGNMENT, 2 * ALIGNMENT, MARGIN] {
                    let result = choose_split(&[c], &[None], minimum, 1);
                    let low = (c.written + minimum + MARGIN)
                        .max(c.frontier + MARGIN)
                        .div_ceil(ALIGNMENT)
                        * ALIGNMENT;
                    let eligible = minimum > 0
                        && c.end >= minimum
                        && low <= (c.end - minimum) / ALIGNMENT * ALIGNMENT;
                    assert_eq!(result.is_some(), eligible);
                    check(c, minimum, result);
                    assert_eq!(result, choose_split(&[c], &[None], minimum, 1));
                }
            }
        }
    }
}

#[test]
fn split_decision_seeded_random_layouts() {
    let mut seed = 0x816a_57e1_4163_937du64;
    let mut next = || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    for _ in 0..20_000 {
        let end = next() % (i64::MAX as u64);
        let written = next() % end.max(1);
        let frontier = written + next() % (end - written).max(1);
        let c = Candidate {
            frontier,
            speed: Some((next() % 1_000_000 + 1) as f64),
            ..candidate(end, written)
        };
        check(
            c,
            1 << 20,
            choose_split(&[c], &[Some(8_000_000.0)], 1 << 20, 1),
        );
    }
    for end in [u64::MAX, i64::MAX as u64] {
        let c = Candidate {
            frontier: end - 1,
            ..candidate(end, end - 1)
        };
        assert_eq!(choose_split(&[c], &[None], 1 << 20, 1), None);
    }
}

#[test]
fn weighted_split_accounts_for_all_idle_workers() {
    let c = Candidate {
        speed: Some(0.8),
        ..candidate(128 << 20, 0)
    };
    let d = choose_split(&[c], &[Some(8.0); 3], 1 << 20, 4).unwrap();
    let child = (d.old_end - d.boundary) as f64;
    assert!((child / (128 << 20) as f64 - 8.0 / 24.8).abs() < 0.001);
    assert_eq!(choose_split(&[c], &[], 1 << 20, 4), None);
    assert_eq!(choose_split(&[c], &[None], 0, 4), None);
    assert_eq!(choose_split(&[c], &[None], 1, MAX_CHUNKS), None);
}

#[test]
fn largest_remaining_wins_and_bad_rates_fall_back() {
    let a = candidate(16 << 20, 0);
    let b = Candidate {
        start: 16 << 20,
        end: 48 << 20,
        frontier: 16 << 20,
        ..a
    };
    assert_eq!(
        choose_split(&[a, b], &[None], 1 << 20, 2).unwrap().start,
        b.start
    );
    for speed in [f64::NAN, f64::INFINITY, -1.0, 0.0] {
        assert_eq!(
            choose_split(
                &[Candidate {
                    speed: Some(speed),
                    ..a
                }],
                &[Some(8.0)],
                1 << 20,
                1
            ),
            choose_split(&[a], &[None], 1 << 20, 1)
        );
    }
}

#[test]
fn every_split_preserves_exact_coverage() {
    let mut ranges = vec![(0, 128 << 20)];
    for _ in 0..MAX_CHUNKS {
        let candidates: Vec<_> = ranges
            .iter()
            .map(|&(start, end)| Candidate {
                start,
                end,
                written: 0,
                frontier: start,
                speed: None,
            })
            .collect();
        let Some(d) = choose_split(&candidates, &[None], 1 << 20, ranges.len()) else {
            break;
        };
        let i = ranges
            .iter()
            .position(|&(start, _)| start == d.start)
            .unwrap();
        ranges[i].1 = d.boundary;
        ranges.insert(i + 1, (d.boundary, d.old_end));
        assert_eq!(ranges[0].0, 0);
        assert_eq!(ranges.last().unwrap().1, 128 << 20);
        assert!(ranges.windows(2).all(|pair| pair[0].1 == pair[1].0));
        assert!(ranges.iter().all(|&(start, end)| start < end));
    }
}

pub(crate) fn hook(phase: &str) {
    crate::database::split_test_hook(phase);
}

async fn staged() -> (tempfile::TempDir, Manager, Arc<Job>) {
    use crate::model::{Status, timestamp_millis};
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::open(super::super::Config {
        home_dir: Some(root.path().join("home")),
        output_dir: root.path().join("out"),
        connections_per_download: 2,
        ..Default::default()
    })
    .await
    .unwrap();
    let id = uuid::Uuid::new_v4();
    let record = Record {
        id,
        url: "http://127.0.0.1:1/unused".into(),
        filename: "data.bin".into(),
        output_path: manager.0.config.output_dir.join("data.bin"),
        part_path: manager
            .0
            .config
            .output_dir
            .join(".vise")
            .join(format!("{id}.part")),
        proxy_url: None,
        cookies_file: None,
        request_credentials: None,
        status: Status::Downloading,
        total_bytes: Some(16 << 20),
        ranged: true,
        validator: Some("\"test\"".into()),
        chunks: super::super::layout::make_chunks(Some(16 << 20), true, 2),
        error: None,
        checksums: Default::default(),
        speed: 0.0,
        created_at: timestamp_millis(),
    };
    let job = Arc::new(Job::new(record, reqwest::Client::new()));
    tokio::fs::write(&job.part_path, vec![0; 16 << 20])
        .await
        .unwrap();
    job.record.write().await.chunks[0].status = ChunkStatus::Downloading;
    job.adaptive.begin_body(0, 0);
    manager.0.jobs.write().await.insert(id, job.clone());
    manager.checkpoint().await.unwrap();
    (root, manager, job)
}

#[tokio::test]
async fn writer_never_crosses_revised_boundary() {
    use tokio::io::AsyncSeekExt;
    let (_root, manager, job) = staged().await;
    job.adaptive.lock().chunks.get_mut(&0).unwrap().end = Some(ALIGNMENT);
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .open(&job.part_path)
        .await
        .unwrap();
    file.seek(std::io::SeekFrom::Start(0)).await.unwrap();
    let mut pending = bytes::BytesMut::from(vec![9; 2 * ALIGNMENT as usize].as_slice());
    super::super::transfer::write_pending(&mut file, &job, 0, &mut pending)
        .await
        .unwrap();
    assert_eq!(job.record.read().await.chunks[0].written, ALIGNMENT);
    let bytes = tokio::fs::read(&job.part_path).await.unwrap();
    assert!(bytes[..ALIGNMENT as usize].iter().all(|&b| b == 9));
    assert!(bytes[ALIGNMENT as usize..].iter().all(|&b| b == 0));
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn checkpoint_and_split_commit_atomically() {
    let (_root, manager, job) = staged().await;
    let external = rusqlite::Connection::open(manager.database_path()).unwrap();
    external.execute_batch("BEGIN IMMEDIATE").unwrap();
    let m = manager.clone();
    let j = job.clone();
    let split =
        tokio::spawn(async move { m.split_tail(&j, &[None], &CancellationToken::new()).await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while job.adaptive.lock().pending.is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(job.record.read().await.chunks.len(), 2);
    let m = manager.clone();
    let checkpoint = tokio::spawn(async move { m.checkpoint().await });
    assert!(!checkpoint.is_finished());
    external.execute_batch("ROLLBACK").unwrap();
    let child = split.await.unwrap().unwrap().unwrap();
    checkpoint.await.unwrap().unwrap();
    let record = manager.0.database.load().await.unwrap().remove(0);
    super::super::layout::validate_record(&record).unwrap();
    assert_eq!(record.chunks.len(), 3);
    assert_eq!(record.chunks[1].start, child);
    assert_eq!(record.chunks[2].start, 8 << 20);
    // A worker for the old second chunk still addresses its original bytes.
    assert_eq!(job.adaptive.reserve(8 << 20, 12).unwrap(), 12);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn split_transaction_failure_preserves_old_layout() {
    let (_root, manager, job) = staged().await;
    let external = rusqlite::Connection::open(manager.database_path()).unwrap();
    external.execute_batch("CREATE TRIGGER refuse_child BEFORE INSERT ON chunks WHEN NEW.chunk_index=2 BEGIN SELECT RAISE(ABORT, 'test split failure'); END;").unwrap();
    assert!(
        manager
            .split_tail(&job, &[None], &CancellationToken::new())
            .await
            .is_err()
    );
    let record = manager.0.database.load().await.unwrap().remove(0);
    assert_eq!(record.chunks.len(), 2);
    assert_eq!(record.chunks[0].end, Some(8 << 20));
    assert_eq!(job.record.read().await.chunks.len(), 2);
    super::super::layout::validate_record(&record).unwrap();
    external.execute_batch("DROP TRIGGER refuse_child").unwrap();
    manager.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn throughput_requires_observation_and_rejects_limiter_waits() {
    let mut speed = Speed::default();
    speed.observe(0, Duration::ZERO);
    for _ in 0..7 {
        tokio::time::advance(Duration::from_millis(250)).await;
        speed.observe(1 << 20, Duration::ZERO);
        assert!(speed.rate().is_none());
    }
    tokio::time::advance(Duration::from_millis(250)).await;
    speed.observe(1 << 20, Duration::ZERO);
    assert!((speed.rate().unwrap() - (4 << 20) as f64).abs() < 1.0);
    speed.observe(1, Duration::from_secs(1));
    assert!(speed.rate().is_none());
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(speed.rate().is_none());
}

#[tokio::test(start_paused = true)]
async fn split_cooldown_on_429_503_and_refusal() {
    let adaptive = Adaptive::default();
    for status in [429, 503] {
        adaptive.refused(&Error::Remote {
            status: reqwest::StatusCode::from_u16(status).unwrap(),
            retry_after: Some(45),
        });
        assert_eq!(
            adaptive.lock().cooldown.unwrap() - Instant::now(),
            Duration::from_secs(45)
        );
        tokio::time::advance(Duration::from_secs(46)).await;
        assert!(adaptive.lock().cooldown.unwrap() < Instant::now());
    }
    adaptive.refused(&Error::PrivateHttp {
        kind: "connection failed",
        retryable: true,
    });
    assert_eq!(
        adaptive.lock().cooldown.unwrap() - Instant::now(),
        Duration::from_secs(30)
    );
}

#[tokio::test(start_paused = true)]
async fn split_discarded_bytes_share_global_rate_budget() {
    check_prefetched_suffix(true).await;
}

#[tokio::test]
async fn split_discards_and_accounts_prefetched_suffix() {
    check_prefetched_suffix(false).await;
}

async fn check_prefetched_suffix(capped: bool) {
    let (_root, manager, job) = staged().await;
    if capped {
        manager.set_rate_limit("1M".parse().unwrap()).await.unwrap();
    }
    let size = 1024 * 1024;
    job.adaptive.lock().chunks.get_mut(&0).unwrap().end = Some(size);
    // A single raw frame guarantees a token-waiting prefetched suffix exists.
    let response: reqwest::Response = axum::http::Response::builder()
        .body(bytes::Bytes::from(vec![7; 3 * size as usize]))
        .unwrap()
        .into();
    let start = Instant::now();
    manager
        .stream_chunk(
            &job,
            0,
            response,
            0,
            Some(3 * size),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(job.record.read().await.chunks[0].written, size);
    let view = job.snapshot().await;
    assert_eq!(view.split_discarded_bytes, 2 * size);
    if capped {
        assert!(start.elapsed() >= Duration::from_secs(3));
    }
    let bytes = tokio::fs::read(&job.part_path).await.unwrap();
    assert!(bytes[..size as usize].iter().all(|&b| b == 7));
    assert!(bytes[size as usize..].iter().all(|&b| b == 0));
    let mut older = serde_json::to_value(&view).unwrap();
    older
        .as_object_mut()
        .unwrap()
        .remove("split_discarded_bytes");
    older.as_object_mut().unwrap().remove("tail_splits");
    let decoded: Download = serde_json::from_value(older).unwrap();
    assert_eq!((decoded.tail_splits, decoded.split_discarded_bytes), (0, 0));
    manager.shutdown().await.unwrap();
}

// Spawn this *unit-test executable*, not a production binary. Its hook parks at
// a named boundary and acknowledges over a private local socket before the
// parent kills it. This exercises abrupt death without running shutdown code.
#[tokio::test]
#[ignore = "child entry point used by split_process_death_at_every_phase"]
async fn split_crash_child() {
    use sha2::Digest;
    let root = std::path::PathBuf::from(std::env::var_os("VISE_TEST_SPLIT_ROOT").unwrap());
    let url = std::env::var("VISE_TEST_SPLIT_URL").unwrap();
    let data = crash_data();
    let expected = crate::checksum::ExpectedChecksums::new(
        Some(&format!("{:x}", sha2::Sha256::digest(&data))),
        None,
    )
    .unwrap();
    let manager = Manager::open(super::super::Config {
        home_dir: Some(root.join("home")),
        output_dir: root.join("out"),
        connections_per_download: 2,
        min_split_size: 64 * 1024,
        ..Default::default()
    })
    .await
    .unwrap();
    manager
        .add_with_checksums(&url, None, expected)
        .await
        .unwrap();
    std::future::pending::<()>().await;
}
fn crash_data() -> Vec<u8> {
    (0..4 * 1024 * 1024).map(|i| (i % 251) as u8).collect()
}

async fn crash_server() -> (String, tokio::task::JoinHandle<()>) {
    use axum::{
        body::Body,
        extract::State,
        http::{HeaderMap, StatusCode, header},
        response::Response,
        routing::get,
    };
    async fn serve(State(data): State<Arc<Vec<u8>>>, headers: HeaderMap) -> Response {
        let text = headers
            .get(header::RANGE)
            .unwrap()
            .to_str()
            .unwrap()
            .strip_prefix("bytes=")
            .unwrap();
        let (start, end) = text.split_once('-').unwrap();
        let start: usize = start.parse().unwrap();
        let end: usize = end.parse().unwrap();
        let slow = start == data.len() / 2;
        let stream = futures_util::stream::unfold((start, data), move |(at, data)| async move {
            if at > end {
                return None;
            }
            tokio::time::sleep(Duration::from_millis(if slow { 50 } else { 5 })).await;
            let next = (at + 65536).min(end + 1);
            Some((
                Ok::<_, std::io::Error>(bytes::Bytes::copy_from_slice(&data[at..next])),
                (next, data),
            ))
        });
        Response::builder()
            .status(StatusCode::PARTIAL_CONTENT)
            .header(header::ETAG, "\"crash-test\"")
            .header(
                header::CONTENT_RANGE,
                format!("bytes {start}-{end}/{}", 4 * 1024 * 1024),
            )
            .header(header::CONTENT_LENGTH, end - start + 1)
            .body(Body::from_stream(stream))
            .unwrap()
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/data.bin", listener.local_addr().unwrap());
    let router = axum::Router::new()
        .route("/data.bin", get(serve))
        .with_state(Arc::new(crash_data()));
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (url, handle)
}

async fn read_split_ack(
    socket: &mut (impl tokio::io::AsyncRead + Unpin),
    phase: &str,
) -> std::io::Result<()> {
    use tokio::io::AsyncReadExt;
    let mut message = vec![0; phase.len()];
    socket.read_exact(&mut message).await?;
    if message != phase.as_bytes() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "unexpected split phase acknowledgment",
        ));
    }
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn split_ack_does_not_wait_for_child_exit() {
    use tokio::io::AsyncWriteExt;
    let phase = "after-commit";
    let (mut child, mut parent) = tokio::io::duplex(64);
    child.write_all(phase.as_bytes()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), read_split_ack(&mut parent, phase))
        .await
        .expect("acknowledgment must be readable while the child is still alive")
        .unwrap();
    drop(child);
}

#[tokio::test]
async fn split_ack_rejects_incomplete_or_wrong_phase() {
    for (message, expected) in [
        (b"after-comm".as_slice(), std::io::ErrorKind::UnexpectedEof),
        (b"before-child".as_slice(), std::io::ErrorKind::InvalidData),
    ] {
        let error = read_split_ack(&mut &message[..], "after-commit")
            .await
            .unwrap_err();
        assert_eq!(error.kind(), expected);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn split_process_death_at_every_phase() {
    use crate::model::Status;
    let (url, server) = crash_server().await;
    for phase in [
        "before-boundary",
        "after-boundary",
        "after-snapshot",
        "after-sync",
        "after-sql-donor",
        "after-sql-layout",
        "before-sql-commit",
        "after-commit",
        "after-install",
        "before-child",
        "after-child-write",
    ] {
        let root = tempfile::tempdir().unwrap();
        let ack = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "engine::adaptive::tests::split_crash_child",
                "--ignored",
            ])
            .env("HOME", root.path())
            .env("USERPROFILE", root.path())
            .env("VISE_TEST_SPLIT_ROOT", root.path())
            .env("VISE_TEST_SPLIT_URL", &url)
            .env("VISE_TEST_SPLIT_PHASE", phase)
            .env("VISE_TEST_SPLIT_ACK", ack.local_addr().unwrap().to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        // The child parks with this socket open. Consume its complete message
        // before killing it; abrupt process exit can reset the socket on Windows.
        let reached = tokio::time::timeout(Duration::from_secs(10), async {
            let (mut socket, _) = ack.accept().await?;
            read_split_ack(&mut socket, phase).await
        })
        .await;
        child.kill().unwrap();
        child.wait().unwrap();
        reached
            .unwrap_or_else(|_| panic!("child did not reach {phase}"))
            .unwrap();
        let manager = Manager::open(super::super::Config {
            home_dir: Some(root.path().join("home")),
            output_dir: root.path().join("out"),
            connections_per_download: 2,
            min_split_size: 0,
            ..Default::default()
        })
        .await
        .unwrap();
        let saved = manager.0.database.load().await.unwrap().remove(0);
        assert_eq!(saved.status, Status::Paused, "{phase}");
        super::super::layout::validate_record(&saved).unwrap();
        manager.resume(saved.id).await.unwrap();
        let done = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let task = manager.get(saved.id).await.unwrap();
                if !task.status.active() {
                    break task;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(done.status, Status::Completed, "{phase}: {:?}", done.error);
        assert!(done.checksums.verified_at.is_some(), "{phase}");
        assert_eq!(
            tokio::fs::read(&done.output_path).await.unwrap(),
            crash_data(),
            "{phase}"
        );
        manager.shutdown().await.unwrap();
    }
    server.abort();
    let _ = server.await;
}

#[tokio::test]
async fn simultaneous_idle_workers_reserve_slots_once() {
    let (_root, manager, job) = staged().await;
    let record = job.record.read().await;
    let decision = job
        .adaptive
        .propose(&record, &[None, None], 1 << 20)
        .unwrap();
    assert!(
        job.adaptive
            .propose(&record, &[None, None], 1 << 20)
            .is_none()
    );
    assert!(decision.boundary > decision.start);
    drop(record);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn every_sql_split_preserves_exact_coverage() {
    let (_root, manager, job) = staged().await;
    let mut count = 0;
    loop {
        {
            let mut record = job.record.write().await;
            for chunk in &mut record.chunks {
                chunk.status = ChunkStatus::Downloading;
                job.adaptive
                    .begin_body(chunk.start, chunk.start + chunk.written);
            }
        }
        let Some(_) = manager
            .split_tail(&job, &[None], &CancellationToken::new())
            .await
            .unwrap()
        else {
            break;
        };
        count += 1;
        let saved = manager.0.database.load().await.unwrap().remove(0);
        super::super::layout::validate_record(&saved).unwrap();
        assert_eq!(saved.chunks.len(), count + 2);
        assert!(saved.chunks.len() <= MAX_CHUNKS);
    }
    assert!(count > 2);
    manager.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn idle_estimates_expire_and_cap_changes_invalidate_them() {
    let rate = "1M".parse().unwrap();
    let estimate = Estimate::new(Some(1_000_000.0), RateLimit::UNLIMITED);
    assert!(estimate.trusted(RateLimit::UNLIMITED).is_some());
    assert!(estimate.trusted(rate).is_none());
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(estimate.trusted(RateLimit::UNLIMITED).is_none());
}

#[tokio::test]
async fn resume_resets_counters_before_probe() {
    let (_root, manager, job) = staged().await;
    let id = job.record.read().await.id;
    job.adaptive.lock().splits = 7;
    job.adaptive.discard(1234);
    manager.pause(id).await.unwrap();
    assert_eq!(manager.get(id).await.unwrap().tail_splits, 7);
    manager.resume(id).await.unwrap();
    let view = manager.get(id).await.unwrap();
    assert_eq!((view.tail_splits, view.split_discarded_bytes), (0, 0));
    manager.pause(id).await.unwrap();
    manager.shutdown().await.unwrap();
}

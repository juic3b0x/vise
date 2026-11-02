use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Raw framing lets us test EOF separately from receiving the expected byte.
async fn response(
    wire: &'static [u8],
) -> (Response, CancellationToken, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let stop = CancellationToken::new();
    let token = stop.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(socket.read_u8().await.unwrap());
            assert!(request.len() < 8192);
        }
        socket.write_all(wire).await.unwrap();
        token.cancelled().await;
    });
    let response = reqwest::Client::new()
        .get(format!("http://{address}/probe"))
        .send()
        .await
        .unwrap();
    (response, stop, server)
}

#[tokio::test]
async fn probe_requires_exactly_one_byte_and_body_completion() {
    for (wire, valid) in [
        (&b"HTTP/1.1 206 Partial Content\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n\r\n"[..], true),
        (&b"HTTP/1.1 206 Partial Content\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n"[..], false),
        (&b"HTTP/1.1 206 Partial Content\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nxx\r\n0\r\n\r\n"[..], false),
        (&b"HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\n\r\nxx"[..], false),
    ] {
        let (response, stop, server) = response(wire).await;
        let result = consume_probe(response, &CancellationToken::new(), Duration::from_secs(1)).await;
        stop.cancel();
        server.await.unwrap();
        assert_eq!(result.is_ok(), valid, "{result:?}");
    }
}

#[tokio::test]
async fn probe_completion_is_cancellable_and_has_an_overall_deadline() {
    for cancelled in [false, true] {
        // The byte arrives but chunked EOF never does. Receiving one byte alone
        // must not hand an unread response back to the connection pool.
        let (response, stop, server) = response(
            b"HTTP/1.1 206 Partial Content\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n",
        )
        .await;
        let cancel = CancellationToken::new();
        if cancelled {
            cancel.cancel();
        }
        let result = consume_probe(response, &cancel, Duration::from_millis(20)).await;
        stop.cancel();
        server.await.unwrap();
        if cancelled {
            assert!(matches!(result, Err(Error::Cancelled)));
        } else {
            assert!(matches!(
                result,
                Err(Error::PrivateHttp {
                    kind: "probe body timeout",
                    retryable: true
                })
            ));
        }
    }
}

#[tokio::test]
async fn fresh_prefix_requires_exact_bounds() {
    for (wire, valid) in [
        (&b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-2/3\r\nContent-Length: 3\r\n\r\nabc"[..], true),
        (&b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 1-2/3\r\nContent-Length: 2\r\n\r\nbc"[..], false),
        (&b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-1/3\r\nContent-Length: 2\r\n\r\nab"[..], false),
        (&b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-1048576/2097152\r\nContent-Length: 0\r\n\r\n"[..], false),
    ] {
        let (response, stop, server) = response(wire).await;
        assert_eq!(initial_metadata(&response, InitialRange::FreshPrefix).is_ok(), valid);
        stop.cancel(); server.await.unwrap();
    }
}

#[tokio::test]
async fn transport_body_classification_survives_redaction() {
    for wire in [
        &b"HTTP/1.1 206 Partial Content\r\nTransfer-Encoding: chunked\r\n\r\nx\r\n"[..],
        &b"HTTP/1.1 206 Partial Content\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\nx\r\n"[..],
    ] {
        let (mut response, stop, server) = response(wire).await;
        let error = loop {
            match response.chunk().await {
                Ok(Some(_)) => {}
                Err(error) => break Error::Http(error),
                Ok(None) => panic!("expected broken framing"),
            }
        };
        assert!(error.retryable() && error.body_interrupted());
        let private = error.without_request_details();
        assert!(private.retryable() && private.body_interrupted());
        assert!(!format!("{private:?} {private}").contains("http://"));
        stop.cancel();
        server.await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn delayed_retry_schedule_and_retry_after_remain_cancellable() {
    let root = tempfile::tempdir().unwrap();
    let manager = Manager::open(super::super::Config {
        home_dir: Some(root.path().join("home")),
        output_dir: root.path().join("out"),
        ..Default::default()
    })
    .await
    .unwrap();
    for (attempt, error, seconds) in [
        (
            0,
            Error::PrivateHttp {
                kind: "timeout",
                retryable: true,
            },
            1,
        ),
        (
            1,
            Error::PrivateHttp {
                kind: "connection failed",
                retryable: true,
            },
            2,
        ),
        (
            2,
            Error::Protocol("unexpected end of response body".into()),
            4,
        ),
        (
            5,
            Error::PrivateHttp {
                kind: "body interrupted",
                retryable: true,
            },
            30,
        ),
        (
            0,
            Error::Remote {
                status: StatusCode::TOO_MANY_REQUESTS,
                retry_after: Some(7),
            },
            7,
        ),
        (
            1,
            Error::Remote {
                status: StatusCode::SERVICE_UNAVAILABLE,
                retry_after: Some(120),
            },
            60,
        ),
        (
            0,
            Error::Remote {
                status: StatusCode::SERVICE_UNAVAILABLE,
                retry_after: Some(0),
            },
            1,
        ),
    ] {
        let cancel = CancellationToken::new();
        let before = tokio::time::Instant::now();
        manager.backoff(attempt, &error, &cancel).await.unwrap();
        assert_eq!(before.elapsed(), Duration::from_secs(seconds));
        let mut wait = Box::pin(manager.backoff(attempt, &error, &cancel));
        assert!(futures_util::poll!(&mut wait).is_pending());
        cancel.cancel();
        assert!(matches!(wait.await, Err(Error::Cancelled)));
    }
    manager.shutdown().await.unwrap();
}

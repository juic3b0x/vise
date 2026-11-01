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

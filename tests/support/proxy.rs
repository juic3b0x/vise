//! Isolated local proxies. Credentials and TLS keys here are public test fixtures.
use base64::{Engine, engine::general_purpose::STANDARD};
use std::{
    io,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    },
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy)]
pub enum Kind {
    Http,
    Socks5,
    TlsHttp,
}
#[derive(Debug, Clone)]
pub struct Request {
    pub authority: String,
    pub method: String,
    pub range: Option<String>,
    pub domain: bool,
}
pub struct TestProxy {
    pub url: String,
    pub requests: Arc<Mutex<Vec<Request>>>,
    stop: CancellationToken,
    task: JoinHandle<()>,
}
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
type Socket = Box<dyn Io>;

pub const CERTIFICATE: &[u8] = include_bytes!("../certs/proxy-cert.der");
fn tls_acceptor() -> TlsAcceptor {
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(
        vec![CertificateDer::from(CERTIFICATE.to_vec())],
        PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            include_bytes!("../certs/proxy-key.der").to_vec(),
        )),
    )
    .unwrap();
    TlsAcceptor::from(Arc::new(config))
}
impl TestProxy {
    pub async fn start(kind: Kind, authenticated: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let scheme = match kind {
            Kind::Http => "http",
            Kind::Socks5 => "socks5",
            Kind::TlsHttp => "https",
        };
        let credentials = if authenticated {
            "alice:p%40ssword@"
        } else {
            ""
        };
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let stop = CancellationToken::new();
        let token = stop.clone();
        let task = tokio::spawn(async move {
            let mut children = JoinSet::new();
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else { break; };
                        let captured = captured.clone();
                        children.spawn(async move {
                            let socket: Socket = if matches!(kind, Kind::TlsHttp) {
                                let Ok(socket) = tls_acceptor().accept(socket).await else { return; }; Box::new(socket)
                            } else { Box::new(socket) };
                            let _ = if matches!(kind, Kind::Socks5) { socks(socket, authenticated, captured).await }
                                else { http(socket, authenticated, captured).await };
                        });
                    }
                    _ = children.join_next(), if !children.is_empty() => (),
                }
            }
            children.abort_all();
            while children.join_next().await.is_some() {}
        });
        Self {
            url: format!("{scheme}://{credentials}{address}"),
            requests,
            stop,
            task,
        }
    }
    pub async fn close(self) {
        self.stop.cancel();
        self.task.await.unwrap();
    }
}
async fn header(socket: &mut Socket) -> io::Result<String> {
    let mut data = Vec::new();
    while !data.ends_with(b"\r\n\r\n") {
        if data.len() >= 16384 {
            return Err(io::Error::other("oversized test request"));
        }
        data.push(socket.read_u8().await?);
    }
    String::from_utf8(data).map_err(io::Error::other)
}
async fn upstream(host: &str, port: u16) -> io::Result<TcpStream> {
    TcpStream::connect((
        if host == "origin.invalid" {
            "127.0.0.1"
        } else {
            host
        },
        port,
    ))
    .await
}
async fn http(
    mut socket: Socket,
    auth: bool,
    requests: Arc<Mutex<Vec<Request>>>,
) -> io::Result<()> {
    let head = header(&mut socket).await?;
    let mut lines = head.split("\r\n");
    let mut first = lines.next().unwrap_or("").split_whitespace();
    let method = first.next().unwrap_or("");
    let destination = first.next().unwrap_or("");
    let fields: Vec<_> = lines.filter_map(|line| line.split_once(':')).collect();
    if auth
        && !fields.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("proxy-authorization")
                && value.trim() == format!("Basic {}", STANDARD.encode("alice:p@ssword"))
        })
    {
        socket.write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await?;
        return Ok(());
    }
    if method == "CONNECT" {
        let target =
            url::Url::parse(&format!("https://{destination}")).map_err(io::Error::other)?;
        requests.lock().unwrap().push(Request {
            authority: destination.into(),
            method: method.into(),
            range: None,
            domain: true,
        });
        let mut remote = upstream(
            target.host_str().unwrap(),
            target.port_or_known_default().unwrap(),
        )
        .await?;
        socket
            .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            .await?;
        tokio::io::copy_bidirectional(&mut socket, &mut remote).await?;
        return Ok(());
    }
    let target = url::Url::parse(destination).map_err(io::Error::other)?;
    requests.lock().unwrap().push(Request {
        authority: target.authority().into(),
        method: method.into(),
        range: fields
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("range"))
            .map(|(_, value)| value.trim().into()),
        domain: true,
    });
    let mut remote = upstream(
        target.host_str().unwrap(),
        target.port_or_known_default().unwrap(),
    )
    .await?;
    let path = format!(
        "{}{}",
        target.path(),
        target
            .query()
            .map(|query| format!("?{query}"))
            .unwrap_or_default()
    );
    let mut forwarded = format!("{method} {path} HTTP/1.1\r\n");
    for (name, value) in fields {
        if !["proxy-authorization", "proxy-connection", "connection"]
            .iter()
            .any(|excluded| name.eq_ignore_ascii_case(excluded))
        {
            forwarded.push_str(&format!("{name}:{value}\r\n"));
        }
    }
    forwarded.push_str("Connection: close\r\n\r\n");
    remote.write_all(forwarded.as_bytes()).await?;
    tokio::io::copy_bidirectional(&mut socket, &mut remote).await?;
    Ok(())
}
async fn socks(
    mut socket: Socket,
    authenticated: bool,
    requests: Arc<Mutex<Vec<Request>>>,
) -> io::Result<()> {
    if socket.read_u8().await? != 5 {
        return Err(io::Error::other("SOCKS version"));
    }
    let count = socket.read_u8().await?;
    let mut methods = vec![0; count as usize];
    socket.read_exact(&mut methods).await?;
    let method = if authenticated { 2 } else { 0 };
    if !methods.contains(&method) {
        socket.write_all(&[5, 255]).await?;
        return Ok(());
    }
    socket.write_all(&[5, method]).await?;
    if authenticated {
        if socket.read_u8().await? != 1 {
            return Err(io::Error::other("auth version"));
        }
        let length = socket.read_u8().await?;
        let mut username = vec![0; length as usize];
        socket.read_exact(&mut username).await?;
        let length = socket.read_u8().await?;
        let mut password = vec![0; length as usize];
        socket.read_exact(&mut password).await?;
        let valid = username == b"alice" && password == b"p@ssword";
        socket.write_all(&[1, if valid { 0 } else { 1 }]).await?;
        if !valid {
            return Ok(());
        }
    }
    let mut prefix = [0; 4];
    socket.read_exact(&mut prefix).await?;
    if prefix[..3] != [5, 1, 0] {
        return Err(io::Error::other("SOCKS command"));
    }
    let (host, domain) = match prefix[3] {
        1 => {
            let mut address = [0; 4];
            socket.read_exact(&mut address).await?;
            (std::net::Ipv4Addr::from(address).to_string(), false)
        }
        4 => {
            let mut address = [0; 16];
            socket.read_exact(&mut address).await?;
            (std::net::Ipv6Addr::from(address).to_string(), false)
        }
        3 => {
            let length = socket.read_u8().await?;
            let mut name = vec![0; length as usize];
            socket.read_exact(&mut name).await?;
            (String::from_utf8(name).map_err(io::Error::other)?, true)
        }
        _ => return Err(io::Error::other("SOCKS address")),
    };
    let port = socket.read_u16().await?;
    requests.lock().unwrap().push(Request {
        authority: format!("{host}:{port}"),
        method: "SOCKS CONNECT".into(),
        range: None,
        domain,
    });
    let mut remote = upstream(&host, port).await?;
    socket.write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 0]).await?;
    tokio::io::copy_bidirectional(&mut socket, &mut remote).await?;
    Ok(())
}

pub struct HttpsOrigin {
    pub url: String,
    stop: CancellationToken,
    task: JoinHandle<()>,
}
impl HttpsOrigin {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "https://127.0.0.1:{}/file",
            listener.local_addr().unwrap().port()
        );
        let stop = CancellationToken::new();
        let token = stop.clone();
        let task = tokio::spawn(async move {
            let mut children = JoinSet::new();
            loop {
                tokio::select! {
                    _ = token.cancelled() => break,
                    accepted = listener.accept() => {
                        let Ok((socket,_)) = accepted else { break; };
                        children.spawn(async move {
                            let Ok(socket) = tls_acceptor().accept(socket).await else { return; };
                            let mut socket: Socket = Box::new(socket);
                            if header(&mut socket).await.is_ok() {
                                let _ = socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nConnection: close\r\n\r\nabc").await;
                                let _ = socket.shutdown().await;
                            }
                        });
                    }
                    _ = children.join_next(), if !children.is_empty() => (),
                }
            }
            children.abort_all();
            while children.join_next().await.is_some() {}
        });
        Self { url, stop, task }
    }
    pub async fn close(self) {
        self.stop.cancel();
        self.task.await.unwrap();
    }
}

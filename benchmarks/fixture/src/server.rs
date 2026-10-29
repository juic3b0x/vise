use crate::{
    content, http,
    stats::{RequestLog, Stats},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, io, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Mutex, watch},
    task::JoinSet,
    time::Instant,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub seed: u64,
    pub sizes: Vec<u64>,
    pub rate: u64,
    pub latency_ms: u64,
    pub slow_every: u64,
    pub slow_divisor: u64,
    pub faults_per_file: u64,
}

impl Config {
    fn validate(&self) -> io::Result<()> {
        let valid = !self.sizes.is_empty()
            && self.sizes.len() <= 10_000
            && self
                .sizes
                .iter()
                .all(|size| (1..=16 * 1024 * 1024 * 1024).contains(size))
            && self.rate <= 16 * 1024 * 1024 * 1024
            && self.latency_ms <= 60_000
            && (1..=1000).contains(&self.slow_divisor)
            && (self.rate == 0 || self.slow_every == 0 || self.rate >= self.slow_divisor)
            && self.faults_per_file <= 10;
        if !valid {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid benchmark bounds",
            ));
        }
        Ok(())
    }
}

struct Shared {
    config: Config,
    stats: Mutex<Stats>,
    used_faults: Mutex<BTreeSet<(usize, u64)>>,
    epoch: Instant,
}

#[derive(Serialize)]
struct Ready {
    port: u16,
    hashes: Vec<String>,
}

pub async fn run() -> io::Result<()> {
    let path = std::env::args_os()
        .nth(1)
        .ok_or_else(|| io::Error::other("missing configuration"))?;
    let file = std::fs::File::open(path)?;
    if file.metadata()?.len() > 1024 * 1024 {
        return Err(io::Error::other("configuration too large"));
    }
    let config: Config = serde_json::from_reader(file)?;
    config.validate()?;
    let hashes = config
        .sizes
        .iter()
        .enumerate()
        .map(|(file, size)| content::digest(config.seed, file, *size))
        .collect();
    // Numeric loopback only; this is never a public server.
    let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).await?;
    let ready = Ready {
        port: listener.local_addr()?.port(),
        hashes,
    };
    let shared = Arc::new(Shared {
        config,
        stats: Mutex::new(Stats::default()),
        used_faults: Mutex::new(BTreeSet::new()),
        epoch: Instant::now(),
    });
    let mut stdout = tokio::io::stdout();
    stdout
        .write_all(serde_json::to_string(&ready)?.as_bytes())
        .await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await?;
    let mut stdin = BufReader::new(tokio::io::stdin());
    let mut line = String::new();
    let (stop, stopped) = watch::channel(false);
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            _ = stdin.read_line(&mut line) => break,
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
            accepted = listener.accept(), if tasks.len() < 2048 => {
                let (stream, _) = accepted?;
                let mut stats = shared.stats.lock().await;
                stats.connections += 1;
                stats.open_connections += 1;
                stats.peak_open_connections = stats.peak_open_connections.max(stats.open_connections);
                let id = stats.connections;
                drop(stats);
                let shared = shared.clone();
                let stopped = stopped.clone();
                tasks.spawn(async move {
                    let _ = connection(stream, id, shared.clone(), stopped).await;
                    shared.stats.lock().await.open_connections -= 1;
                });
            }
        }
    }
    stop.send_replace(true);
    drop(listener);
    let joined = tokio::time::timeout(Duration::from_secs(5), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    if joined.is_err() {
        return Err(io::Error::other("client still serving at shutdown"));
    }
    let stats = shared.stats.lock().await;
    stdout
        .write_all(serde_json::to_string(&*stats)?.as_bytes())
        .await?;
    stdout.write_all(b"\n").await?;
    stdout.flush().await
}

async fn connection(
    mut stream: TcpStream,
    id: u64,
    shared: Arc<Shared>,
    mut stopped: watch::Receiver<bool>,
) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let slow = shared.config.slow_every != 0 && id.is_multiple_of(shared.config.slow_every);
    let rate = if slow {
        shared.config.rate / shared.config.slow_divisor
    } else {
        shared.config.rate
    };
    let mut pace = Pacer::new(rate);
    let mut buffer = Vec::with_capacity(16384);
    loop {
        if *stopped.borrow() {
            return Ok(());
        }
        if shared.stats.lock().await.requests.len() >= 100_000 {
            return Ok(());
        }
        buffer.clear();
        // Read only to the bounded header terminator; no unbounded lines or bodies.
        while !buffer.ends_with(b"\r\n\r\n") {
            if buffer.len() == 16384 {
                return Ok(());
            }
            let byte = tokio::select! {
                _ = stopped.changed() => return Ok(()),
                value = tokio::time::timeout(Duration::from_secs(35), stream.read_u8()) => match value {
                    Ok(Ok(byte)) => byte,
                    _ => return Ok(()),
                },
            };
            buffer.push(byte);
        }
        let request = match http::parse(&buffer, shared.config.seed, &shared.config.sizes) {
            Ok(request) => request,
            Err(status) => {
                shared.stats.lock().await.requests.push(RequestLog {
                    connection: id,
                    file: None,
                    start: 0,
                    end: 0,
                    status,
                    head: false,
                    slow,
                    body_bytes: 0,
                    started_s: shared.epoch.elapsed().as_secs_f64(),
                    first_byte_s: None,
                    last_byte_s: None,
                    reset: false,
                });
                stream.write_all(format!("HTTP/1.1 {status} Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await?;
                return Ok(());
            }
        };
        tokio::time::sleep(Duration::from_millis(shared.config.latency_ms)).await;
        let close = request.close;
        if !serve(&mut stream, id, slow, &shared, &request, &mut pace).await? || close {
            return Ok(());
        }
    }
}

async fn fault(shared: &Shared, request: &http::Request) -> Option<u64> {
    if request.head || request.end == request.start {
        return None;
    }
    let size = *shared.config.sizes.get(request.file)?;
    let mut used = shared.used_faults.lock().await;
    let mut candidates = Vec::new();
    for index in 0..shared.config.faults_per_file {
        let offset =
            1 + content::mix(shared.config.seed ^ (request.file as u64) ^ index.wrapping_mul(997))
                % (size - 1).max(1);
        if offset > request.start
            && offset <= request.end
            && !used.contains(&(request.file, offset))
        {
            candidates.push(offset);
        }
    }
    let offset = candidates.into_iter().min()?;
    used.insert((request.file, offset));
    Some(offset)
}

async fn serve(
    stream: &mut TcpStream,
    id: u64,
    slow: bool,
    shared: &Shared,
    request: &http::Request,
    pace: &mut Pacer,
) -> io::Result<bool> {
    let Some(size) = shared.config.sizes.get(request.file) else {
        return Ok(false);
    };
    let count = request.end - request.start + 1;
    let range = if request.status == 206 {
        format!(
            "Content-Range: bytes {}-{}/{size}\r\n",
            request.start, request.end
        )
    } else {
        String::new()
    };
    let header = format!(
        "HTTP/1.1 {} OK\r\nContent-Length: {count}\r\nAccept-Ranges: bytes\r\nETag: {}\r\n{range}Connection: {}\r\n\r\n",
        request.status,
        http::etag(shared.config.seed, request.file, *size),
        if request.close { "close" } else { "keep-alive" }
    );
    stream.write_all(header.as_bytes()).await?;
    let fault = fault(shared, request).await;
    let end = fault.unwrap_or(request.end + 1);
    let mut log = RequestLog {
        connection: id,
        file: Some(request.file),
        start: request.start,
        end: request.end,
        status: request.status,
        head: request.head,
        slow,
        body_bytes: 0,
        started_s: shared.epoch.elapsed().as_secs_f64(),
        first_byte_s: None,
        last_byte_s: None,
        reset: false,
    };
    let mut buffer = [0; 65536];
    let mut alive = true;
    while !request.head && request.start + log.body_bytes < end {
        let offset = request.start + log.body_bytes;
        let count = (end - offset).min(buffer.len() as u64) as usize;
        let Some(bytes) = buffer.get_mut(..count) else {
            return Err(io::Error::other("invalid buffer bounds"));
        };
        content::fill(shared.config.seed, request.file, offset, bytes);
        pace.wait(count as u64).await;
        // Count actual accepted write prefixes, including failed partial writes.
        let mut rest: &[u8] = bytes;
        while !rest.is_empty() {
            match stream.write(rest).await {
                Ok(0) | Err(_) => {
                    alive = false;
                    break;
                }
                Ok(written) => {
                    let now = shared.epoch.elapsed().as_secs_f64();
                    log.first_byte_s.get_or_insert(now);
                    log.last_byte_s = Some(now);
                    log.body_bytes += written as u64;
                    let Some(remaining) = rest.get(written..) else {
                        return Err(io::Error::other("invalid write count"));
                    };
                    rest = remaining;
                }
            }
        }
        if !alive {
            break;
        }
    }
    if fault.is_some() && alive && !request.head {
        // Safe socket2 wrapper: zero linger makes the injected close a TCP reset.
        socket2::SockRef::from(&*stream).set_linger(Some(Duration::ZERO))?;
        log.reset = true;
        alive = false;
    }
    shared.stats.lock().await.requests.push(log);
    Ok(alive)
}

struct Pacer {
    rate: u64,
    next: Instant,
}
impl Pacer {
    fn new(rate: u64) -> Self {
        Self {
            rate,
            next: Instant::now(),
        }
    }
    async fn wait(&mut self, bytes: u64) {
        if self.rate == 0 {
            return;
        }
        self.next = self.next.max(Instant::now())
            + Duration::from_secs_f64(bytes as f64 / self.rate as f64);
        tokio::time::sleep_until(self.next).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn cap_is_shared_by_requests_on_one_connection() {
        let mut pacer = Pacer::new(1024);
        let start = Instant::now();
        pacer.wait(512).await;
        pacer.wait(512).await;
        assert!(start.elapsed() >= Duration::from_secs(1));
    }

    #[test]
    fn configuration_bounds_are_enforced() {
        let config = Config {
            seed: 1,
            sizes: vec![0],
            rate: 0,
            latency_ms: 0,
            slow_every: 4,
            slow_divisor: 10,
            faults_per_file: 0,
        };
        assert!(config.validate().is_err());
    }
}

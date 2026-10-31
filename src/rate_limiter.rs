//! One shared, dynamically configurable token bucket for response body bytes.
use std::{
    fmt,
    num::NonZeroU64,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[cfg(test)]
use bytes::Bytes;
#[cfg(test)]
use futures_util::{Stream, StreamExt, stream};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex as AsyncMutex, watch},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};

const BYTE_CREDIT: u128 = 1_000_000_000;
const MAX_GRANT: u64 = 64 * 1024;
const MAX_BURST: u64 = 8 * 1024 * 1024;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct RateLimit(Option<NonZeroU64>);

impl RateLimit {
    pub const UNLIMITED: Self = Self(None);
    pub fn from_bytes_per_second(bytes: Option<u64>) -> Self {
        Self(bytes.and_then(NonZeroU64::new))
    }
    pub fn bytes_per_second(self) -> Option<u64> {
        self.0.map(NonZeroU64::get)
    }
}

impl fmt::Display for RateLimit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Some(rate) = self.bytes_per_second() else {
            return formatter.write_str("unlimited");
        };
        for (factor, unit) in [
            (1u64 << 40, "TiB"),
            (1u64 << 30, "GiB"),
            (1u64 << 20, "MiB"),
            (1u64 << 10, "KiB"),
            (1_000_000_000_000, "TB"),
            (1_000_000_000, "GB"),
            (1_000_000, "MB"),
            (1_000, "KB"),
        ] {
            if rate % factor == 0 {
                return write!(formatter, "{} {unit}/s", rate / factor);
            }
        }
        write!(formatter, "{rate} B/s")
    }
}

impl FromStr for RateLimit {
    type Err = Error;
    fn from_str(input: &str) -> Result<Self> {
        let input = input.trim();
        if input.eq_ignore_ascii_case("off") || input.eq_ignore_ascii_case("unlimited") {
            return Ok(Self::UNLIMITED);
        }
        let split = input
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(input.len());
        let (number, unit) = input
            .split_at_checked(split)
            .ok_or_else(|| Error::Invalid("invalid rate boundary".into()))?;
        let unit = unit.trim().to_ascii_lowercase();
        let factor = unit_factor(unit.strip_suffix("/s").unwrap_or(&unit).trim())?;
        let bytes = parse_byte_rate(number, factor)?;
        Ok(Self::from_bytes_per_second(Some(bytes)))
    }
}

fn unit_factor(unit: &str) -> Result<u128> {
    match unit {
        "" | "b" => Ok(1),
        "k" | "ki" | "kib" => Ok(1 << 10),
        "m" | "mi" | "mib" => Ok(1 << 20),
        "g" | "gi" | "gib" => Ok(1 << 30),
        "t" | "ti" | "tib" => Ok(1 << 40),
        "kb" => Ok(1_000),
        "mb" => Ok(1_000_000),
        "gb" => Ok(1_000_000_000),
        "tb" => Ok(1_000_000_000_000),
        _ => Err(Error::Invalid(
            "unknown rate unit; use B/s, K, M, G, KB/s, MB/s, or KiB/s".into(),
        )),
    }
}

fn parse_byte_rate(number: &str, factor: u128) -> Result<u64> {
    let invalid = || {
        Error::Invalid("invalid rate; use a positive byte rate such as 500K or 1.5M, or off".into())
    };
    let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
    if number.is_empty()
        || (whole.is_empty() && fraction.is_empty())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > 18
    {
        return Err(invalid());
    }
    let whole = if whole.is_empty() {
        0
    } else {
        whole.parse::<u128>().map_err(|_| invalid())?
    };
    let scale = 10u128.pow(fraction.len() as u32);
    let fractional = if fraction.is_empty() {
        0
    } else {
        fraction.parse::<u128>().map_err(|_| invalid())?
    };
    let numerator = whole
        .checked_mul(scale)
        .and_then(|v| v.checked_add(fractional))
        .and_then(|v| v.checked_mul(factor))
        .ok_or_else(invalid)?;
    if numerator != 0 && numerator / scale == 0 {
        return Err(Error::Invalid(
            "rate must be at least one byte per second".into(),
        ));
    }
    let bytes = u64::try_from(numerator / scale).map_err(|_| invalid())?;
    Ok(bytes)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RateConfiguration {
    pub limit_rate: String,
    pub limit_bytes_per_second: Option<u64>,
}
impl From<RateLimit> for RateConfiguration {
    fn from(limit: RateLimit) -> Self {
        Self {
            limit_rate: limit.to_string(),
            limit_bytes_per_second: limit.bytes_per_second(),
        }
    }
}

struct Bucket {
    rate: RateLimit,
    credit: u128,
    updated: Instant,
}
impl Bucket {
    fn refill(&mut self, now: Instant) {
        if let Some(rate) = self.rate.bytes_per_second() {
            let capacity = rate.div_ceil(10).clamp(1, MAX_BURST) as u128 * BYTE_CREDIT;
            self.credit = self
                .credit
                .saturating_add(
                    now.saturating_duration_since(self.updated)
                        .as_nanos()
                        .saturating_mul(rate as u128),
                )
                .min(capacity);
        }
        self.updated = now;
    }
}

struct Inner {
    // Zero is unlimited. A grant linearizes at this load, like the old bucket
    // read; an already granted frame may finish across a live limit change.
    rate: AtomicU64,
    bucket: Mutex<Bucket>,
    // FIFO admission prevents fast segments from starving other chunk readers.
    turn: AsyncMutex<()>,
    changed: watch::Sender<u64>,
}

#[derive(Clone)]
pub(crate) struct RateLimiter(Arc<Inner>);

impl RateLimiter {
    pub(crate) fn new(rate: RateLimit) -> Self {
        let (changed, _) = watch::channel(0);
        Self(Arc::new(Inner {
            rate: AtomicU64::new(rate.bytes_per_second().unwrap_or(0)),
            bucket: Mutex::new(Bucket {
                rate,
                credit: 0,
                updated: Instant::now(),
            }),
            turn: AsyncMutex::new(()),
            changed,
        }))
    }

    pub(crate) fn limit(&self) -> RateLimit {
        RateLimit::from_bytes_per_second(Some(self.0.rate.load(Ordering::Acquire)))
    }

    /// Changing a limit discards old credits and wakes the current token waiter.
    /// No response streams, connection handles, or worker tasks are replaced.
    pub(crate) fn set_limit(&self, rate: RateLimit) {
        {
            let mut bucket = self
                .0
                .bucket
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if bucket.rate == rate {
                return;
            }
            bucket.rate = rate;
            bucket.credit = 0;
            bucket.updated = Instant::now();
            self.0
                .rate
                .store(rate.bytes_per_second().unwrap_or(0), Ordering::Release);
        }
        self.0
            .changed
            .send_modify(|version| *version = version.wrapping_add(1));
    }

    /// Grant at most 64 KiB, shared across every clone. Credit uses integer
    /// byte-nanoseconds, so fractional refill never rounds away at low rates.
    pub(crate) async fn acquire(
        &self,
        maximum: usize,
        cancel: &CancellationToken,
    ) -> Result<usize> {
        if maximum == 0 {
            return Ok(0);
        }
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        if self.0.rate.load(Ordering::Acquire) == 0 {
            return Ok(maximum.min(MAX_GRANT as usize));
        }
        let _turn = tokio::select! { biased;
            _ = cancel.cancelled() => return Err(Error::Cancelled),
            turn = self.0.turn.lock() => turn,
        };
        let mut changes = self.0.changed.subscribe();
        loop {
            if cancel.is_cancelled() {
                return Err(Error::Cancelled);
            }
            let delay = {
                let mut bucket = self
                    .0
                    .bucket
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                bucket.refill(Instant::now());
                match bucket.rate.bytes_per_second() {
                    None => return Ok(maximum.min(MAX_GRANT as usize)),
                    Some(rate) => {
                        let wanted = (maximum as u64).min(rate.div_ceil(50).clamp(1, MAX_GRANT));
                        let needed = wanted as u128 * BYTE_CREDIT;
                        if bucket.credit >= needed {
                            bucket.credit -= needed;
                            return Ok(wanted as usize);
                        }
                        Duration::from_nanos(
                            (needed - bucket.credit)
                                .div_ceil(rate as u128)
                                .min(u64::MAX as u128) as u64,
                        )
                    }
                }
            };
            tokio::select! { biased;
                _ = cancel.cancelled() => return Err(Error::Cancelled),
                _ = changes.changed() => (),
                _ = tokio::time::sleep(delay) => (),
            }
        }
    }

    /// Poll one upstream frame at a time, then deliver token-charged slices.
    /// Upstream is not polled while tokens are exhausted, providing backpressure.
    /// Reqwest's idle read timer resets after each frame, so these intentional
    /// waits do not turn into connection timeouts.
    // This helper exists for tests only.
    #[cfg(test)]
    pub(crate) fn throttle<S, E>(
        &self,
        source: S,
        cancel: CancellationToken,
    ) -> impl Stream<Item = Result<Bytes>> + Send
    where
        S: Stream<Item = std::result::Result<Bytes, E>> + Send + 'static,
        E: Into<Error> + Send + 'static,
    {
        let limiter = self.clone();
        stream::unfold(
            (Box::pin(source), Bytes::new(), cancel, limiter, false),
            |(mut source, mut pending, cancel, limiter, done)| async move {
                if done {
                    return None;
                }
                while pending.is_empty() {
                    let next = tokio::select! { biased;
                        _ = cancel.cancelled() => return Some((Err(Error::Cancelled), (source, pending, cancel, limiter, true))),
                        next = source.next() => next,
                    };
                    match next {
                        Some(Ok(bytes)) => {
                            pending = bytes;
                            if pending.is_empty() {
                                tokio::task::yield_now().await;
                            }
                        }
                        Some(Err(error)) => {
                            return Some((
                                Err(error.into()),
                                (source, pending, cancel, limiter, true),
                            ));
                        }
                        None => return None,
                    }
                }
                match limiter.acquire(pending.len(), &cancel).await {
                    Ok(count) => Some((
                        Ok(pending.split_to(count)),
                        (source, pending, cancel, limiter, false),
                    )),
                    Err(error) => Some((Err(error), (source, pending, cancel, limiter, true))),
                }
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn unlimited_grants_bypass_both_locks_and_respect_cancellation() {
        let limiter = RateLimiter::new(RateLimit::UNLIMITED);
        let _turn = limiter.0.turn.lock().await;
        let _bucket = limiter.0.bucket.lock().unwrap();
        let waker = futures_util::task::noop_waker();
        let mut context = std::task::Context::from_waker(&waker);
        let cancel = CancellationToken::new();
        let mut grant = Box::pin(limiter.acquire(usize::MAX, &cancel));
        assert!(matches!(
            std::future::Future::poll(grant.as_mut(), &mut context),
            std::task::Poll::Ready(Ok(65_536))
        ));
        cancel.cancel();
        let mut grant = Box::pin(limiter.acquire(1, &cancel));
        assert!(matches!(
            std::future::Future::poll(grant.as_mut(), &mut context),
            std::task::Poll::Ready(Err(Error::Cancelled))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn enabling_a_limit_after_unlimited_grants_uses_one_shared_budget() {
        let limiter = RateLimiter::new(RateLimit::UNLIMITED);
        consume(&limiter, 1_000_000, &CancellationToken::new()).await;
        limiter.set_limit("1000".parse().unwrap());
        let start = Instant::now();
        let first = limiter.clone();
        let second = limiter.clone();
        let a = tokio::spawn(async move { consume(&first, 500, &CancellationToken::new()).await });
        let b = tokio::spawn(async move { consume(&second, 500, &CancellationToken::new()).await });
        a.await.unwrap();
        b.await.unwrap();
        assert!(start.elapsed() >= Duration::from_secs(1));
        limiter.set_limit(RateLimit::UNLIMITED);
        assert_eq!(
            limiter
                .acquire(1000, &CancellationToken::new())
                .await
                .unwrap(),
            1000
        );
    }

    #[test]
    fn parses_byte_rates_without_float_rounding_or_overflow() {
        for (input, expected) in [
            ("5M", 5 * 1024 * 1024),
            ("1.5MiB/s", 1_572_864),
            ("1.1M", 1_153_433),
            ("5 MB/s", 5_000_000),
            ("500KB/s", 500_000),
            (".5K", 512),
            ("32", 32),
            ("1B/s", 1),
            ("\u{2003}1.5 MiB/s\u{2003}", 1_572_864),
            ("18446744073709551615", u64::MAX),
        ] {
            let rate: RateLimit = input.parse().unwrap();
            assert_eq!(rate.bytes_per_second(), Some(expected), "{input}");
            assert_eq!(rate.to_string().parse::<RateLimit>().unwrap(), rate);
        }
        for input in ["off", "unlimited", "0", "0M"] {
            assert_eq!(input.parse::<RateLimit>().unwrap(), RateLimit::UNLIMITED);
        }
        for input in [
            "",
            ".",
            "1..5M",
            "-1M",
            "NaN",
            "1Mbps",
            "1e3",
            "0.1B/s",
            "18446744073709551616",
            "999999999999999999999999999999999999999M",
            "1\u{1f680}M",
            "\u{ff11}M",
            "1M\u{00e9}",
        ] {
            assert!(input.parse::<RateLimit>().is_err(), "{input}");
        }
    }

    async fn consume(limiter: &RateLimiter, mut bytes: usize, cancel: &CancellationToken) {
        while bytes > 0 {
            bytes -= limiter.acquire(bytes, cancel).await.unwrap();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_readers_share_a_single_byte_budget() {
        let limiter = RateLimiter::new("1000".parse().unwrap());
        let started = Instant::now();
        let mut readers = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let limiter = limiter.clone();
            readers.spawn(async move {
                consume(&limiter, 500, &CancellationToken::new()).await;
            });
        }
        while let Some(reader) = readers.join_next().await {
            reader.unwrap();
        }
        assert!(started.elapsed() >= Duration::from_secs(2));
        assert!(started.elapsed() < Duration::from_millis(2200));
    }

    #[tokio::test(start_paused = true)]
    async fn idle_credit_is_bounded_and_does_not_grow_forever() {
        let limiter = RateLimiter::new("1000".parse().unwrap());
        tokio::time::advance(Duration::from_secs(100)).await;
        let started = Instant::now();
        consume(&limiter, 100, &CancellationToken::new()).await;
        assert_eq!(started.elapsed(), Duration::ZERO);
        consume(&limiter, 20, &CancellationToken::new()).await;
        assert!(started.elapsed() >= Duration::from_millis(20));
    }

    #[tokio::test(start_paused = true)]
    async fn changing_to_unlimited_wakes_sleepers_without_waiting_for_old_deadline() {
        let limiter = RateLimiter::new("1".parse().unwrap());
        let worker = limiter.clone();
        let pending =
            tokio::spawn(async move { worker.acquire(100, &CancellationToken::new()).await });
        tokio::task::yield_now().await;
        let changed = Instant::now();
        limiter.set_limit(RateLimit::UNLIMITED);
        assert_eq!(pending.await.unwrap().unwrap(), 100);
        assert_eq!(changed.elapsed(), Duration::ZERO);
    }

    #[tokio::test(start_paused = true)]
    async fn lowering_the_rate_recalculates_the_pending_grant() {
        let limiter = RateLimiter::new("10000".parse().unwrap());
        let worker = limiter.clone();
        let pending =
            tokio::spawn(async move { worker.acquire(1000, &CancellationToken::new()).await });
        tokio::task::yield_now().await;
        limiter.set_limit("1".parse().unwrap());
        let changed = Instant::now();
        assert_eq!(pending.await.unwrap().unwrap(), 1);
        assert!(changed.elapsed() >= Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_releases_fifo_admission_for_other_readers() {
        let limiter = RateLimiter::new("1".parse().unwrap());
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let worker = limiter.clone();
        let first = tokio::spawn(async move { worker.acquire(1, &token).await });
        tokio::task::yield_now().await;
        let worker = limiter.clone();
        let second =
            tokio::spawn(async move { worker.acquire(1, &CancellationToken::new()).await });
        tokio::task::yield_now().await;
        cancel.cancel();
        assert!(matches!(first.await.unwrap(), Err(Error::Cancelled)));
        limiter.set_limit(RateLimit::UNLIMITED);
        assert_eq!(second.await.unwrap().unwrap(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn stream_wrapper_accounts_for_every_byte_and_preserves_content() {
        let limiter = RateLimiter::new("10".parse().unwrap());
        let source = stream::iter([
            Ok::<_, Error>(Bytes::new()),
            Ok(Bytes::from_static(b"abc")),
            Ok(Bytes::from_static(b"defghij")),
        ]);
        let started = Instant::now();
        let mut stream = limiter.throttle(source, CancellationToken::new()).boxed();
        let mut output = Vec::new();
        while let Some(bytes) = stream.next().await {
            output.extend_from_slice(&bytes.unwrap());
        }
        assert_eq!(output, b"abcdefghij");
        assert!(started.elapsed() >= Duration::from_secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn stream_configuration_changes_preserve_the_pending_upstream_frame() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let reads = Arc::new(AtomicUsize::new(0));
        let count = reads.clone();
        let source = stream::unfold(false, move |done| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                if done {
                    None
                } else {
                    Some((Ok::<_, Error>(Bytes::from_static(b"pending frame")), true))
                }
            }
        });
        let limiter = RateLimiter::new("1".parse().unwrap());
        let worker = limiter.clone();
        let pending = tokio::spawn(async move {
            let mut stream = worker.throttle(source, CancellationToken::new()).boxed();
            let mut output = Vec::new();
            while let Some(bytes) = stream.next().await {
                output.extend_from_slice(&bytes.unwrap());
            }
            output
        });
        tokio::task::yield_now().await;
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        limiter.set_limit(RateLimit::UNLIMITED);
        assert_eq!(pending.await.unwrap(), b"pending frame");
        assert_eq!(reads.load(Ordering::SeqCst), 2); // One frame and EOF; no replacement source.
    }
}

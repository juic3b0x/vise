//! Tail ownership decisions and runtime write reservations. No controller types.
const MARGIN: u64 = 256 * 1024;
const ALIGNMENT: u64 = 64 * 1024;
pub(super) const MAX_CHUNKS: usize = 1024;

#[derive(Clone, Copy)]
struct Candidate {
    start: u64,
    end: u64,
    written: u64,
    frontier: u64,
    speed: Option<f64>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Decision {
    start: u64,
    old_end: u64,
    boundary: u64,
}
fn choose_split(
    chunks: &[Candidate],
    idle: &[Option<f64>],
    minimum: u64,
    count: usize,
) -> Option<Decision> {
    if minimum == 0 || idle.is_empty() || count >= MAX_CHUNKS {
        return None;
    }
    let bounds = |c: &Candidate| {
        let offset = c.start.checked_add(c.written)?;
        let low = offset
            .checked_add(minimum)?
            .checked_add(MARGIN)?
            .max(c.frontier.checked_add(MARGIN)?);
        let low = low.checked_add(ALIGNMENT - 1)? / ALIGNMENT * ALIGNMENT;
        let high = c.end.checked_sub(minimum)? / ALIGNMENT * ALIGNMENT;
        (offset <= c.frontier && low <= high).then_some((offset, low, high))
    };
    let c = chunks
        .iter()
        .filter(|c| bounds(c).is_some())
        .max_by_key(|c| (c.end - c.start - c.written, std::cmp::Reverse(c.start)))?;
    let (offset, low, high) = bounds(c)?;
    let valid = |v: f64| v.is_finite() && v > 0.0;
    let fraction = match (
        c.speed.filter(|&v| valid(v)),
        idle.iter().copied().collect::<Option<Vec<_>>>(),
    ) {
        (Some(v), Some(rates)) if rates.iter().all(|&u| valid(u)) => {
            // Normalize first: even caller-provided finite rates must not overflow their sum.
            let scale = rates.iter().copied().fold(v, f64::max);
            1.0 - (rates.first().copied()? / scale)
                / (v / scale + rates.iter().map(|u| u / scale).sum::<f64>())
        }
        _ => 0.5,
    };
    let desired = offset.saturating_add(((c.end - offset) as f64 * fraction) as u64);
    let boundary = desired.saturating_add(ALIGNMENT - 1) / ALIGNMENT * ALIGNMENT;
    Some(Decision {
        start: c.start,
        old_end: c.end,
        boundary: boundary.clamp(low, high),
    })
}

#[cfg(test)]
pub(crate) mod tests;

use super::{Job, Manager};
use crate::{
    error::{Error, Result},
    model::{Chunk, ChunkStatus, Download, Record},
    rate_limiter::RateLimit,
};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, MutexGuard, atomic::Ordering},
    time::Duration,
};
use tokio::{sync::Notify, task::JoinSet, time::Instant};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Speed {
    samples: VecDeque<(Instant, u64)>,
    bytes: u64,
    waits: Duration,
    smoothed: f64,
}
impl Speed {
    fn observe(&mut self, bytes: u64, wait: Duration) {
        let now = Instant::now();
        self.bytes = self.bytes.saturating_add(bytes);
        self.waits += wait;
        if self
            .samples
            .back()
            .is_none_or(|(when, _)| now.duration_since(*when) >= Duration::from_millis(250))
        {
            self.samples.push_back((now, self.bytes));
            while self.samples.len() > 2
                && self
                    .samples
                    .get(1)
                    .is_some_and(|(when, _)| now.duration_since(*when) > Duration::from_secs(2))
            {
                self.samples.pop_front();
            }
            if let Some(&(when, old)) = self.samples.front() {
                let seconds = now.duration_since(when).as_secs_f64();
                if seconds > 0.0 {
                    let rate = (self.bytes - old) as f64 / seconds;
                    self.smoothed = if self.smoothed == 0.0 {
                        rate
                    } else {
                        self.smoothed * 0.75 + rate * 0.25
                    };
                }
            }
        }
    }
    fn rate(&self) -> Option<f64> {
        let &(first, _) = self.samples.front()?;
        let &(last, _) = self.samples.back()?;
        (self.samples.len() >= 4
            && last.duration_since(first) >= Duration::from_secs(2)
            && last.elapsed() <= Duration::from_secs(1)
            && self.bytes >= 256 * 1024
            && self.waits.as_secs_f64() < first.elapsed().as_secs_f64() * 0.2
            && self.smoothed.is_finite()
            && self.smoothed > 0.0)
            .then_some(self.smoothed)
    }
}
struct Control {
    end: Option<u64>,
    written_to: u64,
    frontier: u64,
    reserved: u64,
    serving: bool,
    #[cfg(test)]
    child: bool,
    speed: Speed,
}
#[derive(Default)]
struct State {
    chunks: BTreeMap<u64, Control>,
    pending: Option<u64>,
    cooldown: Option<Instant>,
    rate: RateLimit,
    splits: u64,
    discarded: u64,
}
#[derive(Default)]
pub(super) struct Adaptive {
    state: Mutex<State>,
    changed: Notify,
}
impl Adaptive {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn has_tail(&self, minimum: u64) -> bool {
        if minimum == 0 {
            return false;
        }
        minimum
            .checked_mul(2)
            .and_then(|n| n.checked_add(MARGIN))
            .is_some_and(|size| {
                self.lock().chunks.values().any(|c| {
                    c.serving
                        && c.end
                            .is_some_and(|end| end.saturating_sub(c.written_to) >= size)
                })
            })
    }
    fn propose(&self, record: &Record, idle: &[Option<f64>], minimum: u64) -> Option<Decision> {
        if !record.ranged || record.validator.is_none() || record.total_bytes.is_none() {
            return None;
        }

        let mut s = self.lock();
        if s.pending.is_some() || s.cooldown.is_some_and(|until| until > Instant::now()) {
            return None;
        }
        let candidates: Vec<_> = record
            .chunks
            .iter()
            .filter_map(|chunk| {
                let c = s.chunks.get(&chunk.start)?;
                (c.serving && chunk.status == ChunkStatus::Downloading).then_some(Candidate {
                    start: chunk.start,
                    end: chunk.end?,
                    written: chunk.written,
                    frontier: c.frontier.max(c.reserved),
                    speed: c.speed.rate(),
                })
            })
            .collect();
        let d = choose_split(&candidates, idle, minimum, record.chunks.len())?;
        #[cfg(test)]
        tests::hook("before-boundary");
        s.chunks.get_mut(&d.start)?.end = Some(d.boundary);
        s.pending = Some(d.start);
        Some(d)
    }

    pub(super) fn reset(&self, record: &Record, rate: RateLimit) {
        *self.lock() = State {
            chunks: record
                .chunks
                .iter()
                .map(|c| {
                    (
                        c.start,
                        Control {
                            end: c.end,
                            written_to: c.start + c.written,
                            frontier: c.start + c.written,
                            reserved: c.start + c.written,
                            serving: false,
                            #[cfg(test)]
                            child: false,
                            speed: Speed::default(),
                        },
                    )
                })
                .collect(),
            rate,
            ..State::default()
        };
    }
    pub(super) fn decorate(&self, mut snapshot: Download) -> Download {
        let state = self.lock();
        snapshot.tail_splits = state.splits;
        snapshot.split_discarded_bytes = state.discarded;
        snapshot
    }
    pub(super) fn reset_speeds(&self, rate: RateLimit) {
        let mut s = self.lock();
        if s.rate != rate {
            s.rate = rate;
            for c in s.chunks.values_mut() {
                c.speed = Speed::default();
            }
        }
    }
    pub(super) fn begin_body(&self, key: u64, offset: u64) {
        if let Some(c) = self.lock().chunks.get_mut(&key) {
            c.serving = true;
            c.frontier = offset;
            c.speed = Speed::default();
            c.speed.observe(0, Duration::ZERO);
        }
    }
    pub(super) fn received(&self, key: u64, end: u64) {
        if let Some(c) = self.lock().chunks.get_mut(&key) {
            c.frontier = c.frontier.max(end);
        }
    }
    pub(super) fn observe(&self, key: u64, bytes: u64, wait: Duration) {
        if let Some(c) = self.lock().chunks.get_mut(&key) {
            c.speed.observe(bytes, wait);
        }
    }
    pub(super) fn stopped(&self, key: u64) -> Option<f64> {
        let mut s = self.lock();
        let c = s.chunks.get_mut(&key)?;
        c.serving = false;
        c.speed.rate()
    }
    #[cfg(test)]
    pub(super) fn is_child(&self, key: u64) -> bool {
        self.lock().chunks.get(&key).is_some_and(|c| c.child)
    }
    pub(super) fn end(&self, key: u64) -> Option<u64> {
        self.lock().chunks.get(&key).and_then(|c| c.end)
    }
    pub(super) fn reserve(&self, key: u64, bytes: usize) -> Result<usize> {
        let mut s = self.lock();
        let c = s
            .chunks
            .get_mut(&key)
            .ok_or_else(|| Error::Worker("missing chunk write control".into()))?;
        let size = c
            .end
            .map_or(bytes as u64, |end| (end - c.written_to).min(bytes as u64))
            as usize;
        c.reserved = c.written_to + size as u64;
        Ok(size)
    }
    pub(super) fn written(&self, key: u64, size: usize) {
        if let Some(c) = self.lock().chunks.get_mut(&key) {
            c.written_to += size as u64;
            c.reserved = c.written_to;
        }
    }
    pub(super) fn discard(&self, bytes: u64) {
        let mut s = self.lock();
        s.discarded = s.discarded.saturating_add(bytes);
    }
    pub(super) async fn resolved(&self, key: u64, cancel: &CancellationToken) -> Result<()> {
        loop {
            // Register before checking to avoid a lost wakeup on commit.
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.lock().pending != Some(key) {
                return Ok(());
            }
            tokio::select! { biased; _ = cancel.cancelled() => return Err(Error::Cancelled), _ = notified => {} }
        }
    }
    pub(super) fn refused(&self, error: &Error) {
        let seconds = match error {
            Error::Remote {
                status,
                retry_after,
            } if status.as_u16() == 429 || status.as_u16() == 503 => {
                30.max(retry_after.unwrap_or(0).min(60))
            }
            Error::Http(e)
                if e.is_connect()
                    || e.is_timeout()
                    || e.is_body()
                    || e.is_request()
                    || e.is_decode() =>
            {
                30
            }
            Error::PrivateHttp {
                retryable: true, ..
            } => 30,
            _ => return,
        };
        self.lock().cooldown = Some(Instant::now() + Duration::from_secs(seconds));
    }
}

#[derive(Clone, Copy)]
struct Estimate {
    speed: Option<f64>,
    when: Instant,
    limit: RateLimit,
}
impl Estimate {
    fn new(speed: Option<f64>, limit: RateLimit) -> Self {
        Self {
            speed,
            when: Instant::now(),
            limit,
        }
    }
    fn trusted(self, limit: RateLimit) -> Option<f64> {
        (self.limit == limit && self.when.elapsed() <= Duration::from_secs(1))
            .then_some(self.speed)
            .flatten()
    }
}
type WorkerResult = (Estimate, Result<()>);

impl Manager {
    pub(super) async fn run_chunks(
        &self,
        job: &Arc<Job>,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let mut queue: VecDeque<_> = job
            .record
            .read()
            .await
            .chunks
            .iter()
            .filter(|c| c.end.is_none_or(|end| c.start + c.written < end))
            .map(|c| c.start)
            .collect();
        let mut idle = vec![
            Estimate::new(None, self.0.rate_limiter.limit());
            self.0.config.connections_per_download
        ];
        let mut workers = JoinSet::new();
        let worker_cancel = cancel.child_token();
        let mut failure = None;
        loop {
            self.fill_slots(
                job,
                &mut queue,
                &mut idle,
                &mut workers,
                &mut failure,
                &worker_cancel,
            )
            .await;
            let Some(result) = workers.join_next().await else {
                break;
            };
            consume_result(result, &mut idle, &mut failure, &worker_cancel);
            while let Some(result) = workers.try_join_next() {
                consume_result(result, &mut idle, &mut failure, &worker_cancel);
            }
            // Coalesce only a genuine tail. Queued recovery work and completed
            // small files must not pay an extra delay per chunk or per task.
            let can_split = job.adaptive.has_tail(self.0.config.min_split_size);
            if failure.is_none()
                && !worker_cancel.is_cancelled()
                && queue.is_empty()
                && !workers.is_empty()
                && can_split
            {
                tokio::select! { biased; _ = worker_cancel.cancelled() => {}, _ = tokio::time::sleep(Duration::from_millis(10)) => {} }
                while let Some(result) = workers.try_join_next() {
                    consume_result(result, &mut idle, &mut failure, &worker_cancel);
                }
            }
        }
        // Never abort: a file write and its accounting must finish together.
        while let Some(result) = workers.join_next().await {
            consume_result(result, &mut idle, &mut failure, &worker_cancel);
        }
        if let Some(error) = failure {
            return Err(error);
        }
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        Ok(())
    }

    async fn fill_slots(
        &self,
        job: &Arc<Job>,
        queue: &mut VecDeque<u64>,
        idle: &mut Vec<Estimate>,
        workers: &mut JoinSet<WorkerResult>,
        failure: &mut Option<Error>,
        worker_cancel: &CancellationToken,
    ) {
        while failure.is_none() && !worker_cancel.is_cancelled() && !idle.is_empty() {
            let key = if let Some(key) = queue.pop_front() {
                Some(key)
            } else {
                match self
                    .split_tail(
                        job,
                        &idle
                            .iter()
                            .map(|e| e.trusted(self.0.rate_limiter.limit()))
                            .collect::<Vec<_>>(),
                        worker_cancel,
                    )
                    .await
                {
                    Ok(key) => key,
                    Err(error) => {
                        *failure = Some(error);
                        worker_cancel.cancel();
                        None
                    }
                }
            };
            let Some(key) = key else { break };
            idle.remove(0);
            let manager = self.clone();
            let job = job.clone();
            let cancel = worker_cancel.clone();
            workers.spawn(async move {
                let result = manager.transfer_chunk(job.clone(), key, &cancel).await;
                let speed = job.adaptive.stopped(key);
                (Estimate::new(speed, manager.0.rate_limiter.limit()), result)
            });
        }
    }

    async fn split_tail(
        &self,
        job: &Job,
        idle: &[Option<f64>],
        cancel: &CancellationToken,
    ) -> Result<Option<u64>> {
        if cancel.is_cancelled() || self.0.stopping.load(Ordering::Acquire) {
            return Ok(None);
        }
        let _checkpoint = self.0.checkpoint.lock().await;
        if cancel.is_cancelled() {
            return Ok(None);
        }
        let record = job.record.read().await;
        let Some(decision) = job
            .adaptive
            .propose(&record, idle, self.0.config.min_split_size)
        else {
            return Ok(None);
        };
        drop(record);
        #[cfg(test)]
        tests::hook("after-boundary");
        let mut candidate = job.record.read().await.clone();
        insert_split(&mut candidate, decision)?;
        super::layout::validate_record(&candidate)?;
        let id = candidate.id;
        let snapshots = self.capture_snapshots(Some(candidate)).await?;
        #[cfg(test)]
        tests::hook("after-snapshot");
        let snapshots = super::journal::sync_snapshots(snapshots).await?;
        #[cfg(test)]
        tests::hook("after-sync");
        if let Err(error) = self.0.database.persist(snapshots).await {
            // A failed/ambiguous commit must be reconciled before another snapshot can overwrite it.
            match self.0.database.load().await {
                Ok(records) => {
                    if records.iter().any(|r| {
                        r.id == id && r.chunks.iter().any(|c| c.start == decision.boundary)
                    }) {
                        install_split(job, decision).await?;
                    }
                }
                Err(_) => {
                    job.journal_blocked.store(true, Ordering::Release);
                }
            }
            return Err(error);
        }
        #[cfg(test)]
        tests::hook("after-commit");
        install_split(job, decision).await?;
        #[cfg(test)]
        tests::hook("after-install");
        super::journal::sync_directory(
            super::storage::parent(self.0.database.path())?.to_path_buf(),
        )
        .await?;
        if cancel.is_cancelled() {
            return Err(Error::Cancelled);
        }
        #[cfg(test)]
        tests::hook("before-child");
        Ok(Some(decision.boundary))
    }
}
fn consume_result(
    result: std::result::Result<WorkerResult, tokio::task::JoinError>,
    idle: &mut Vec<Estimate>,
    failure: &mut Option<Error>,
    cancel: &CancellationToken,
) {
    let (speed, result) = result.unwrap_or_else(|e| {
        (
            Estimate::new(None, RateLimit::UNLIMITED),
            Err(Error::Worker(e.to_string())),
        )
    });
    idle.push(speed);
    if let Err(error) = result
        && failure.is_none()
    {
        *failure = Some(error);
        cancel.cancel();
    }
}
fn insert_split(record: &mut Record, d: Decision) -> Result<()> {
    let i = record
        .chunks
        .binary_search_by_key(&d.start, |c| c.start)
        .map_err(|_| Error::Worker("missing split donor".into()))?;
    let donor = record
        .chunks
        .get_mut(i)
        .ok_or_else(|| Error::Worker("missing split donor".into()))?;
    if donor.end != Some(d.old_end) || donor.written > d.boundary - d.start {
        return Err(Error::Worker("split donor changed".into()));
    }
    donor.end = Some(d.boundary);
    record.chunks.insert(
        i + 1,
        Chunk {
            start: d.boundary,
            end: Some(d.old_end),
            written: 0,
            status: ChunkStatus::Queued,
        },
    );
    Ok(())
}
async fn install_split(job: &Job, d: Decision) -> Result<()> {
    let mut record = job.record.write().await;
    insert_split(&mut record, d)?;
    let mut s = job.adaptive.lock();
    s.chunks.insert(
        d.boundary,
        Control {
            end: Some(d.old_end),
            written_to: d.boundary,
            frontier: d.boundary,
            reserved: d.boundary,
            serving: false,
            #[cfg(test)]
            child: true,
            speed: Speed::default(),
        },
    );
    s.pending = None;
    s.splits += 1;
    job.adaptive.changed.notify_waiters();
    Ok(())
}

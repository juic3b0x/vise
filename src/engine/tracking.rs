//! Indexed journal mutations. Record guards register changes before unlocking;
//! captures acknowledge only their own generation after the durability barrier.
use super::Job;
use crate::model::{Record, Status};
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use uuid::Uuid;

#[derive(Default)]
pub(super) struct Changes {
    state: Mutex<State>,
    pub blocked: AtomicBool,
}

#[derive(Default)]
struct State {
    next: u64,
    pending: BTreeMap<Uuid, u64>,
    entries: BTreeMap<Uuid, Arc<Entry>>,
    sampling: BTreeSet<Uuid>,
}

struct Entry {
    id: Uuid,
    owner: OnceLock<Weak<Changes>>,
    downloading: AtomicBool,
}

impl State {
    fn dirty(&mut self, id: Uuid) {
        // A generation cannot wrap and accidentally acknowledge newer work.
        assert_ne!(self.next, u64::MAX, "journal generation exhausted");
        self.next += 1;
        self.pending.insert(id, self.next);
    }
}

impl Changes {
    pub fn has_pending(&self) -> bool {
        !self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .is_empty()
    }
    pub fn pending(&self) -> Vec<(Uuid, u64)> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .iter()
            .map(|(id, generation)| (*id, *generation))
            .collect()
    }

    pub fn generation(&self, id: Uuid) -> Option<u64> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending
            .get(&id)
            .copied()
    }

    pub fn acknowledge(&self, captured: &[(Uuid, u64)]) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (id, generation) in captured {
            if state.pending.get(id) == Some(generation) {
                state.pending.remove(id);
            }
        }
    }

    pub fn sampling(&self) -> Vec<Uuid> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .sampling
            .iter()
            .copied()
            .collect()
    }

    fn register(self: &Arc<Self>, entry: &Arc<Entry>) {
        if entry.owner.get().is_none() {
            let _ = entry.owner.set(Arc::downgrade(self));
        }
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.entries.insert(entry.id, entry.clone());
        if entry.downloading.load(Ordering::Acquire) {
            state.sampling.insert(entry.id);
        }
        state.dirty(entry.id);
    }

    fn remove(&self, id: Uuid) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.entries.remove(&id);
        state.sampling.remove(&id);
        state.dirty(id);
    }
}

/// No mutable map dereference: insertion/removal must maintain both indexes.
pub(super) struct Jobs {
    entries: BTreeMap<Uuid, Arc<Job>>,
    reserved: BTreeSet<PathBuf>,
    changes: Arc<Changes>,
}

impl Jobs {
    pub fn new(entries: BTreeMap<Uuid, Arc<Job>>, changes: Arc<Changes>) -> Self {
        let mut jobs = Self {
            entries: BTreeMap::new(),
            reserved: BTreeSet::new(),
            changes,
        };
        for (id, job) in entries {
            jobs.insert(id, job);
        }
        jobs
    }

    pub fn contains_path(&self, path: &Path) -> bool {
        self.reserved.contains(path)
    }

    pub fn insert(&mut self, id: Uuid, job: Arc<Job>) -> Option<Arc<Job>> {
        let old = self.remove(&id);
        self.reserved.insert(job.output_path.clone());
        self.changes.register(&job.record.entry);
        self.entries.insert(id, job);
        old
    }

    pub fn remove(&mut self, id: &Uuid) -> Option<Arc<Job>> {
        let old = self.entries.remove(id)?;
        self.reserved.remove(&old.output_path);
        self.changes.remove(*id);
        Some(old)
    }
}

impl Deref for Jobs {
    type Target = BTreeMap<Uuid, Arc<Job>>;
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

pub(super) struct TrackedRecord {
    value: RwLock<Record>,
    entry: Arc<Entry>,
}

impl TrackedRecord {
    pub fn block_journal(&self) {
        if let Some(owner) = self.entry.owner.get().and_then(Weak::upgrade) {
            owner.blocked.store(true, Ordering::Release);
        }
    }
    pub fn new(record: Record) -> Self {
        Self {
            entry: Arc::new(Entry {
                id: record.id,
                owner: OnceLock::new(),
                downloading: AtomicBool::new(record.status == Status::Downloading),
            }),
            value: RwLock::new(record),
        }
    }

    pub async fn read(&self) -> RwLockReadGuard<'_, Record> {
        self.value.read().await
    }

    pub async fn write(&self) -> Write<'_> {
        Write {
            record: self.value.write().await,
            entry: &self.entry,
            mutated: false,
        }
    }

    /// Only telemetry may bypass mutation tracking; callers cannot modify fields.
    pub async fn sample_speed(&self, sample: Option<f64>) {
        let mut record = self.value.write().await;
        if let Some(speed) = sample {
            record.speed = if record.status == Status::Downloading {
                record.speed * 0.6 + speed * 0.4
            } else {
                0.0
            };
        }
    }
}

pub(super) struct Write<'a> {
    record: RwLockWriteGuard<'a, Record>,
    entry: &'a Arc<Entry>,
    mutated: bool,
}

impl Deref for Write<'_> {
    type Target = Record;
    fn deref(&self) -> &Record {
        &self.record
    }
}

impl DerefMut for Write<'_> {
    fn deref_mut(&mut self) -> &mut Record {
        self.mutated = true;
        &mut self.record
    }
}

impl Drop for Write<'_> {
    fn drop(&mut self) {
        if !self.mutated {
            return;
        }
        let downloading = self.record.status == Status::Downloading;
        self.entry.downloading.store(downloading, Ordering::Release);
        let Some(owner) = self.entry.owner.get().and_then(Weak::upgrade) else {
            return;
        };
        let mut state = owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // An old worker cannot dirty a removed job or its replacement.
        if !state
            .entries
            .get(&self.entry.id)
            .is_some_and(|entry| Arc::ptr_eq(entry, self.entry))
        {
            return;
        }
        if downloading {
            state.sampling.insert(self.entry.id);
        } else {
            state.sampling.remove(&self.entry.id);
        }
        state.dirty(self.entry.id);
    }
}

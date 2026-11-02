//! Volatile durability evidence. Never restored from SQL: recovery starts cold.
use crate::{
    error::Result,
    model::{Record, Status},
};
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

type Layout = (Option<u64>, Vec<(u64, Option<u64>, u64)>);
fn layout(record: &Record) -> Layout {
    (
        record.total_bytes,
        record
            .chunks
            .iter()
            .map(|c| (c.start, c.end, c.written))
            .collect(),
    )
}
struct Certificate {
    identity: same_file::Handle,
    revision: u64,
    length: u64,
    layout: Layout,
}
impl Certificate {
    fn covers(
        &self,
        identity: &same_file::Handle,
        revision: u64,
        length: u64,
        layout: &Layout,
    ) -> bool {
        self.identity == *identity
            && self.revision == revision
            && self.length == length
            && self.layout == *layout
    }
}
struct Evidence {
    identity: same_file::Handle,
    namespace: Namespace,
    revision: u64,
    length: u64,
    layout: Layout,
    directory: bool,
}
// Windows has no directory fsync guarantee in the existing policy. Unix
// namespace evidence also pins the parent inode, so replacing a directory with
// another containing a hard link to the same file cannot reuse its barrier.
#[cfg(unix)]
type Namespace = same_file::Handle;
#[cfg(not(unix))]
#[derive(PartialEq, Eq)]
struct Namespace;
fn namespace(path: &std::path::Path) -> Result<Namespace> {
    #[cfg(unix)]
    {
        Ok(same_file::Handle::from_path(path)?)
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(Namespace)
    }
}

#[derive(Default)]
pub(super) struct Durability {
    revision: AtomicU64,
    evidence: Mutex<Option<Evidence>>,
    // Data-only proof produced outside the checkpoint gate after writers join.
    // It must never mutate namespace evidence or acknowledge a directory sync.
    certified: Mutex<Option<Certificate>>,
}
impl Durability {
    // Call BEFORE any write or truncation. A failed/cancelled write is also dirty.
    pub fn invalidate(&self) {
        self.revision.fetch_add(1, Ordering::AcqRel);
    }
    pub fn forget(&self) {
        *self
            .evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
        *self
            .certified
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
    }
    #[cfg(test)]
    pub fn has_evidence(&self) -> bool {
        self.evidence.lock().unwrap().is_some() || self.certified.lock().unwrap().is_some()
    }
    pub fn revision(&self) -> u64 {
        self.revision.load(Ordering::Acquire)
    }

    /// All writers have joined. Capture offsets/revision before this barrier,
    /// then certify only this handle's data. The checkpoint still validates and
    /// reopens the path and supplies any required namespace barrier before SQL.
    pub fn certify_data(
        &self,
        file: &std::fs::File,
        record: &Record,
        revision: u64,
        sync: impl FnOnce() -> Result<()>,
    ) -> Result<bool> {
        let certificate = Certificate {
            identity: same_file::Handle::from_file(file.try_clone()?)?,
            revision,
            length: file.metadata()?.len(),
            layout: layout(record),
        };
        let covered = self
            .evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|e| {
                e.identity == certificate.identity
                    && e.revision == revision
                    && e.length == certificate.length
                    && e.layout == certificate.layout
            });
        if !covered {
            sync()?;
        }
        *self
            .certified
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(certificate);
        Ok(!covered)
    }

    /// Called only under the checkpoint gate, after capture and path validation.
    /// Exact layout/offset equality makes in-flight writes conservative: their
    /// later progress cannot reuse evidence captured before progress was updated.
    pub fn synchronize(
        &self,
        file: &std::fs::File,
        directory_path: &std::path::Path,
        record: &Record,
        revision: u64,
        sync: impl FnOnce() -> Result<()>,
    ) -> Result<(bool, bool)> {
        let identity = same_file::Handle::from_file(file.try_clone()?)?;
        let namespace = namespace(directory_path)?;
        let length = file.metadata()?.len();
        let layout = layout(record);
        let mut saved = self
            .evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let same_identity = saved.as_ref().is_some_and(|e| e.identity == identity);
        let directory = same_identity
            && saved
                .as_ref()
                .is_some_and(|e| e.directory && e.namespace == namespace);
        let covered = saved.as_ref().is_some_and(|e| {
            same_identity && e.revision == revision && e.length == length && e.layout == layout
        });
        let certified = self
            .certified
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|c| c.covers(&identity, revision, length, &layout));
        if !covered && !certified {
            sync()?;
        }
        #[cfg(test)]
        if certified && !covered {
            crate::database::split_test_hook("after-certified-data-barrier");
        }
        // Keep identity handles only for executing jobs, bounded by the worker
        // pool. Paused/queued history must not retain one descriptor per file.
        *saved = matches!(
            record.status,
            Status::Probing | Status::Downloading | Status::Verifying
        )
        .then_some(Evidence {
            identity,
            namespace,
            revision,
            length,
            layout,
            directory,
        });
        Ok((!covered && !certified, !directory))
    }
    // Only after successful directory fsync. Failure keeps namespace work pending.
    pub fn directory_synced(&self) {
        if let Some(e) = self
            .evidence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_mut()
        {
            e.directory = true;
        }
    }
}

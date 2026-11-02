//! Compile-time optional, credential-free process timings for local profiling.
//! Durations overlap across workers and phases; they are not wall-time shares.
#[derive(Clone, Copy)]
#[repr(usize)]
pub(crate) enum Phase {
    CheckpointWait,
    CheckpointHold,
    SqlCommit,
    SqlCommitted,
    FileSync,
    DirectorySync,
    Admission,
    Preparation,
    Transfer,
    Publication,
    Settlement,
}

pub(crate) struct Timer {
    #[cfg(feature = "perf-diagnostics")]
    phase: Phase,
    #[cfg(feature = "perf-diagnostics")]
    start: std::time::Instant,
}
impl Timer {
    #[inline]
    pub(crate) fn finish(self) {}

    #[inline]
    pub(crate) fn new(_phase: Phase) -> Self {
        Self {
            #[cfg(feature = "perf-diagnostics")]
            phase: _phase,
            #[cfg(feature = "perf-diagnostics")]
            start: std::time::Instant::now(),
        }
    }
}
#[cfg(feature = "perf-diagnostics")]
mod enabled {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    struct Counter {
        count: AtomicU64,
        ns: AtomicU64,
        max: AtomicU64,
    }
    static COUNTERS: [Counter; 11] = [const {
        Counter {
            count: AtomicU64::new(0),
            ns: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }; 11];
    impl Drop for Timer {
        fn drop(&mut self) {
            let ns = self.start.elapsed().as_nanos().min(u64::MAX as u128) as u64;
            let c = &COUNTERS[self.phase as usize];
            c.count.fetch_add(1, Ordering::Relaxed);
            c.ns.fetch_add(ns, Ordering::Relaxed);
            c.max.fetch_max(ns, Ordering::Relaxed);
        }
    }
    pub(super) fn report() {
        let names = [
            "checkpoint_wait",
            "checkpoint_hold",
            "sql_commit",
            "sql_committed",
            "file_sync",
            "directory_sync",
            "admission",
            "preparation",
            "transfer",
            "publication",
            "settlement",
        ];
        let values: serde_json::Map<String, serde_json::Value> = names
            .into_iter()
            .zip(&COUNTERS)
            .map(|(name, c)| {
                (
                    name.into(),
                    serde_json::json!({
                        "count": c.count.load(Ordering::Relaxed),
                        "total_ns": c.ns.load(Ordering::Relaxed),
                        "max_ns": c.max.load(Ordering::Relaxed),
                    }),
                )
            })
            .collect();
        // Only fixed names and numbers. Never include paths, URLs or errors.
        eprintln!("VISE_PERF {}", serde_json::Value::Object(values));
    }
}

/// Emits one aggregate line on stderr when built with `perf-diagnostics`.
/// Normal builds have no counters, clock reads or reporting.
pub struct Report;
impl Drop for Report {
    fn drop(&mut self) {
        #[cfg(feature = "perf-diagnostics")]
        enabled::report();
    }
}

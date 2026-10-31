//! Unit-test-only post-commit gate; absent from shipped binaries.
use super::{CommitHook, Database};
use crate::error::{Error, Result};
use std::{sync::Mutex, time::Duration};

impl Database {
    pub(crate) fn clear_commit_gate(&self) {
        self.after_commit.lock().unwrap().take();
    }

    pub(crate) fn gate_next_commit(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let mut hook = self.after_commit.lock().unwrap();
        assert!(hook.is_none(), "post-commit test gate is already armed");
        *hook = Some((entered, wait));
        (ready, release)
    }
}

pub(super) fn wait_after_commit(hook: &Mutex<Option<CommitHook>>) -> Result<()> {
    // One-shot and per database. A broken parent must not leave the blocking
    // pool parked indefinitely during a crash experiment.
    let hook = hook.lock().unwrap().take();
    if let Some((entered, release)) = hook {
        let _ = entered.send(());
        release
            .recv_timeout(Duration::from_secs(15))
            .map_err(|error| Error::Worker(format!("checkpoint test release failed: {error}")))?;
    }
    Ok(())
}

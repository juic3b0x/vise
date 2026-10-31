//! Immediate group commit. The checkpoint mutex remains the filesystem/reset barrier.
use super::super::Manager;
use crate::error::{Error, Result};
use std::sync::{Arc, Mutex};
use tokio::sync::{Notify, oneshot};

type Receipt = oneshot::Sender<std::result::Result<(), Arc<Error>>>;

#[derive(Default)]
struct Pending {
    running: bool,
    receipts: Vec<Receipt>,
}

#[derive(Default)]
pub(in crate::engine) struct Group {
    pending: Mutex<Pending>,
    idle: Notify,
    #[cfg(test)]
    pub(in crate::engine) before_idle: tokio::sync::Mutex<Option<super::super::CheckpointHook>>,
}

impl Group {
    pub(in crate::engine) async fn submit(self: &Arc<Self>, manager: Manager) -> Result<()> {
        let (receipt, wait) = oneshot::channel();
        {
            // Enrollment and the active-to-idle handoff share this short lock.
            // No application lock or I/O is awaited while it is held.
            let mut pending = self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            pending.receipts.push(receipt);
            if !pending.running {
                pending.running = true;
                let group = self.clone();
                tokio::spawn(async move { group.run(manager).await });
            }
        }
        wait.await
            .map_err(|_| Error::Worker("checkpoint owner stopped before acknowledgement".into()))?
            .map_err(Error::Checkpoint)
    }

    async fn run(self: Arc<Self>, manager: Manager) {
        loop {
            // Exclusive reset/settlement/delete/split paths retain their old gate.
            let checkpoint = manager.0.checkpoint.lock().await;
            let receipts = {
                let mut pending = self
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                // Membership freezes before capture; later arrivals belong to another batch.
                std::mem::take(&mut pending.receipts)
            };
            let result = manager.checkpoint_locked().await.map_err(Arc::new);
            #[cfg(test)]
            if let Some((entered, release)) = self.before_idle.lock().await.take() {
                let _ = entered.send(());
                let _ = release.await;
            }
            let idle = {
                let mut pending = self
                    .pending
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let idle = pending.receipts.is_empty();
                if idle {
                    pending.running = false;
                }
                idle
            };
            drop(checkpoint);
            if idle {
                // The last receipt must not keep the engine's process lock alive
                // in a detached owner after its caller has dropped the Manager.
                drop(manager);
                self.idle.notify_waiters();
                for receipt in receipts {
                    let _ = receipt.send(result.clone());
                }
                return;
            }
            for receipt in receipts {
                let _ = receipt.send(result.clone());
            }
        }
    }

    pub(in crate::engine) async fn drain(&self) {
        loop {
            let changed = self.idle.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if !self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .running
            {
                return;
            }
            changed.await;
        }
    }

    #[cfg(test)]
    pub(in crate::engine) fn assert_owned(&self) {
        let pending = self.pending.lock().unwrap();
        assert!(
            pending.receipts.is_empty() || pending.running,
            "enrolled receipts lost their checkpoint owner"
        );
    }
}

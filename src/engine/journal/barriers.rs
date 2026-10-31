//! Per-manager durability trace and gates. This module never exists in production.
use std::sync::Mutex;
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::engine) enum Phase {
    Capture,
    File,
    StagingDirectory,
    BeforeDirectory,
    Directory,
}

#[derive(Default)]
pub(in crate::engine) struct Trace {
    phases: Mutex<Vec<Phase>>,
    gate: Mutex<Option<(Phase, super::super::CheckpointHook)>>,
}
impl Trace {
    pub(in crate::engine) fn record(&self, phase: Phase) {
        self.phases.lock().unwrap().push(phase);
    }
    pub(in crate::engine) async fn at(&self, phase: Phase) {
        self.record(phase);
        let gate = {
            let mut gate = self.gate.lock().unwrap();
            if gate.as_ref().is_some_and(|(target, _)| *target == phase) {
                gate.take()
            } else {
                None
            }
        };
        if let Some((_, (entered, release))) = gate {
            let _ = entered.send(());
            let _ = release.await;
        }
    }
    pub(in crate::engine) fn gate(
        &self,
        phase: Phase,
    ) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (entered, ready) = oneshot::channel();
        let (release, wait) = oneshot::channel();
        assert!(
            self.gate
                .lock()
                .unwrap()
                .replace((phase, (entered, wait)))
                .is_none()
        );
        (ready, release)
    }
    pub(in crate::engine) fn take(&self) -> Vec<Phase> {
        std::mem::take(&mut *self.phases.lock().unwrap())
    }
}

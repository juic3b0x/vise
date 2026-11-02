//! One prompt recovery per execution, shared across all range workers.
use super::{
    Job, Manager,
    transfer::{chunk_at, missing_chunk},
};
use crate::{
    error::{Error, Result},
    events::EventKind,
};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(super) struct PromptRecovery(AtomicBool);

impl PromptRecovery {
    pub fn reset(&self) {
        self.0.store(false, Ordering::Release);
    }

    pub fn allow(&self, error: &Error, validated: bool, before: u64, after: u64) -> bool {
        // Request-amplification policy: at most ONE retry can bypass its wait,
        // regardless of chunk count or repeated trickles of progress. It still
        // consumes the configured retry budget; later delays retain their index.
        validated
            && after > before
            && error.body_interrupted()
            && self
                .0
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    }
}

impl Manager {
    pub(super) async fn retry_chunk(
        &self,
        job: &Job,
        index: u64,
        attempt: u32,
        written_before: u64,
        error: &Error,
        cancel: &CancellationToken,
    ) -> Result<()> {
        let position = job
            .record
            .read()
            .await
            .chunks
            .binary_search_by_key(&index, |c| c.start)
            .map_err(|_| missing_chunk(index))?;
        tracing::warn!(chunk = position, retry = attempt + 1, %error, "retrying transfer");
        self.publish_job(
            EventKind::Retrying {
                chunk: position,
                attempt: attempt + 1,
                error: error.to_string(),
            },
            job,
        )
        .await;
        let prompt = {
            let record = job.record.read().await;
            job.prompt_recovery.allow(
                error,
                record.ranged && record.validator.is_some(),
                written_before,
                chunk_at(&record, index)?.written,
            )
        };
        if !prompt {
            self.backoff(attempt, error, cancel).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn interrupted() -> Error {
        Error::Protocol("unexpected end of response body".into())
    }

    #[test]
    fn progressing_faults_cannot_replenish_the_allowance() {
        let policy = PromptRecovery::default();
        for fault in 0..10 {
            assert_eq!(
                policy.allow(&interrupted(), true, fault, fault + 1),
                fault == 0
            );
        }
        policy.reset();
        assert!(policy.allow(&interrupted(), true, 0, 1));
    }

    #[test]
    fn ineligible_failures_do_not_spend_the_allowance() {
        let policy = PromptRecovery::default();
        assert!(!policy.allow(&interrupted(), false, 0, 1));
        assert!(!policy.allow(&interrupted(), true, 1, 1));
        for error in [
            Error::PrivateHttp {
                kind: "timeout",
                retryable: true,
            },
            Error::PrivateHttp {
                kind: "connection failed",
                retryable: true,
            },
            Error::PrivateHttp {
                kind: "request or body failed",
                retryable: true,
            },
            Error::Protocol("unexpected end of response probe body".into()),
            Error::Protocol("remote file validator changed".into()),
            Error::Remote {
                status: reqwest::StatusCode::TOO_MANY_REQUESTS,
                retry_after: Some(7),
            },
            Error::Remote {
                status: reqwest::StatusCode::SERVICE_UNAVAILABLE,
                retry_after: None,
            },
            Error::Cancelled,
        ] {
            assert!(!policy.allow(&error, true, 0, 1));
        }
        assert!(policy.allow(&interrupted(), true, 0, 1));
    }

    #[test]
    fn all_workers_share_one_allowance() {
        let policy = PromptRecovery::default();
        let count = std::thread::scope(|scope| {
            let tasks: Vec<_> = (0..64)
                .map(|_| scope.spawn(|| policy.allow(&interrupted(), true, 0, 1)))
                .collect();
            tasks
                .into_iter()
                .map(|task| task.join().unwrap())
                .filter(|allowed| *allowed)
                .count()
        });
        assert_eq!(count, 1);
    }
}

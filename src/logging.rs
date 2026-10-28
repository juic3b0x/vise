//! Binary logging policy. RUST_LOG selects verbosity; it cannot enable targets
//! known to format credentials. Keep the narrow deny-list here when auditing
//! dependency upgrades. Do not format event fields while deciding whether to drop.
use tracing::{
    Event, Metadata, Subscriber,
    field::{Field, Visit},
};
use tracing_subscriber::{
    EnvFilter,
    layer::{Context, Filter},
    prelude::*,
};

#[cfg(test)]
mod tests;

pub(super) fn init() {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| "vise=warn".into()))
        .with(
            tracing_subscriber::fmt::layer()
                .with_writer(std::io::stderr)
                .with_filter(CredentialGuard),
        )
        .init();
}

// cookie_store 0.22's store_response_cookies Debug-formats raw Set-Cookie
// values. Other HTTP/proxy targets have been audited and remain available.
const CREDENTIAL_TARGETS: &[&str] = &["cookie_store::cookie_store"];
fn credential_target(target: &str) -> bool {
    CREDENTIAL_TARGETS.iter().any(|prefix| {
        target == *prefix
            || target
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with("::"))
    })
}

struct CredentialGuard;
impl<S: Subscriber> Filter<S> for CredentialGuard {
    fn enabled(&self, metadata: &Metadata<'_>, _: &Context<'_, S>) -> bool {
        !credential_target(metadata.target())
    }
    fn event_enabled(&self, event: &Event<'_>, _: &Context<'_, S>) -> bool {
        let mut target = BridgeTarget {
            blocked: credential_target(event.metadata().target()),
        };
        // tracing-log emits metadata.target() == "log"; its normalized target
        // is in the string field "log.target". Checking metadata alone is not
        // sufficient for bridged records. This visitor never renders messages.
        event.record(&mut target);
        !target.blocked
    }
}
struct BridgeTarget {
    blocked: bool,
}
impl Visit for BridgeTarget {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "log.target" {
            self.blocked |= credential_target(value);
        }
    }
    fn record_debug(&mut self, field: &Field, _: &dyn std::fmt::Debug) {
        // The bridge uses record_str. If that contract changes, fail closed
        // rather than formatting an unknown target/value and risking a leak.
        if field.name() == "log.target" {
            self.blocked = true;
        }
    }
}

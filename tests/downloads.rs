//! Integration coverage grouped by transfer responsibility; all tests share the
//! same local HTTP/proxy fixtures and run in one integration-test binary.

// Compile the shared logging entry points from a separate crate without
// installing a process-global subscriber or changing the runtime test inventory.
const _: fn() = vise::logging::init;
const _: vise::logging::CredentialGuard = vise::logging::CredentialGuard;

#[path = "downloads/fixture.rs"]
mod fixture;
#[path = "support/proxy.rs"]
mod proxy_support;

#[path = "downloads/bulk.rs"]
mod bulk;
#[path = "downloads/checksums.rs"]
mod checksums;
#[path = "downloads/cli.rs"]
mod cli;
#[path = "downloads/client/mod.rs"]
mod client;
#[path = "downloads/cookies.rs"]
mod cookies;
#[path = "downloads/daemon.rs"]
mod daemon;
#[path = "downloads/global_state.rs"]
mod global_state;
#[path = "downloads/path_security.rs"]
mod path_security;
#[path = "downloads/proxies.rs"]
mod proxies;
#[path = "downloads/rate_limits.rs"]
mod rate_limits;
#[path = "downloads/recovery.rs"]
mod recovery;
#[path = "downloads/transfers.rs"]
mod transfers;
#[path = "downloads/web.rs"]
mod web;

#[path = "downloads/credentials/mod.rs"]
mod credentials;

#[path = "downloads/adaptive.rs"]
mod adaptive;

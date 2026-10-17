//! Integration coverage grouped by transfer responsibility; all tests share the
//! same local HTTP/proxy fixtures and run in one integration-test binary.
#[path = "downloads/fixture.rs"]
mod fixture;
#[path = "support/proxy.rs"]
mod proxy_support;

#[path = "downloads/checksums.rs"]
mod checksums;
#[path = "downloads/cli.rs"]
mod cli;
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

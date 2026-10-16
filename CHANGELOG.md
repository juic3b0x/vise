# Changelog

All notable changes to Vise are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

The repository currently declares package version `0.1.0` and has no release tags. The development history below is collected under `Unreleased`; it does not imply a published release or release date. Future releases should have versioned sections with `YYYY-MM-DD` dates, newest first.

## [Unreleased]

### Added

- Concurrent HTTP(S) downloads using Tokio and Reqwest, with configurable connections per file and simultaneous downloads. The CLI provides Indicatif progress, speed, ETA, batch downloads, an interactive queue, pause/resume, and bounded retries.
- An embedded SQLite state journal with download records, individual chunk offsets, and persisted lifecycle transitions. WAL mode, full synchronization, and transactional checkpoints support recovery from durable offsets after interruption. Unfinished tasks return as paused on startup and can be resumed through either interface or `--resume-all`.
- A shared token-bucket bandwidth limiter across all downloads and chunks. `--limit-rate` accepts human-readable units; interactive commands and WebUI controls change the cap during active transfers without reconnecting. The journal preserves the configured limit across restarts.
- An industrial dark WebUI served by Axum exclusively through `--webui`, with embedded HTML, CSS, and JavaScript. Live WebSocket telemetry, polling fallback, throughput graphs, chunk-progress grids, queue filtering, and immediate transfer controls share the CLI's download manager.
- A WebUI terminal monitor with timestamped, colorized action and lifecycle events, plus one-second progress, speed, and ETA summaries. Default CLI behavior remains available independently.
- Automated SHA-256 and MD5 verification through CLI flags, API fields, WebUI inputs, and supported HTTP digest metadata. Completed staging files enter verification before final publication. Expected and actual digests are journaled; mismatches become `corrupt`, retain staging data, and require an explicit full re-download to retry.
- Per-transfer HTTP, HTTPS, SOCKS5, and SOCKS5h proxy routing through `--proxy` and API `proxy_url`, including authenticated routes. SOCKS5 uses local destination DNS; SOCKS5h delegates destination DNS to the proxy. Saved transfers retain their effective route across restarts.

### Changed

- Replaced flat-file progress tracking with SQLite-backed state. Existing `.vise/state.json` journals are imported once and preserved as `state.json.migrated`.
- Extended journal migrations to preserve checksum expectations, verification results, and proxy configuration alongside existing chunk progress and settings.
- Separated the transfer engine, CLI controller, WebUI bridge, journal, checksum verifier, and bandwidth limiter into dedicated modules.

### Security

- Explicit proxy routes cover probes, chunks, redirects, retries, and resumed transfers without falling back to a direct connection. These routes override environment proxy and `NO_PROXY` settings; failure of the configured proxy fails or retries the transfer through that route.
- Proxy credentials are removed from public transfer snapshots and proxy display/debug output, and invalid proxy diagnostics avoid echoing credential-bearing input. Complete authenticated routes remain in the local journal for recovery.
- TLS certificate verification remains enabled for HTTPS destinations and HTTPS proxies.
- The optional WebUI binds only to loopback and validates request hosts and browser origins. JSON-only mutations and a Content Security Policy constrain browser access.
- Final output publication refuses to overwrite an existing destination and occurs only after any configured checksum verification succeeds. MD5 remains a compatibility option; SHA-256 from a trusted source is preferred for security-sensitive verification.

[Unreleased]: https://github.com/juic3b0x/vise/commits/main/

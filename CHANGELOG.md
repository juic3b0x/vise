# Changelog

All notable changes to Vise are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.1] - 2026-10-16

Maintenance release focused on reliability, code organization, and leaner dependencies.

### Added

- Regression coverage for missing chunk indices, telemetry unit boundaries, Unicode rate inputs, and error-output write failures.

### Changed

- Split the download engine into focused modules for queue control, transfers, HTTP handling, recovery, checkpoints, state, and chunk layout. Extracted application supervision and SQLite initialization into dedicated modules.
- Organized integration tests by behavior and simplified CLI dispatch, journal decoding, recovery, rate parsing, and proxy test helpers while preserving existing interfaces and persistence ordering.
- Reduced checksum hex-formatting allocations, shared journal paths as `Arc<Path>`, and removed unused tracing macro support. Aligned the MD5 dependency name with its Rust imports.

### Fixed

- Enabled integer overflow checks in optimized release builds.
- Replaced unchecked chunk indexing and byte/string slicing with checked access and explicit errors or safe fallbacks, including UTF-8 boundaries in rate parsing and download ID displays.
- Validated fixed WebUI response headers at compile time instead of parsing them with runtime `expect` calls. Existing cache, content-type, and Content Security Policy safeguards remain intact.
- Routed top-level CLI error reporting through a caller-provided writer, preserving error context and failure exit codes when output cannot be written.

## [0.1.0] - 2026-10-16

Initial release.

### Added

- **Concurrent downloads:** Tokio and Reqwest power parallel HTTP(S) transfers with configurable connection and queue limits, pause/resume, and bounded retries. The CLI includes Indicatif progress, speed, ETA, batch downloads, and an interactive queue.
- **SQLite-backed journaling:** WAL mode, full synchronization, and atomic transactions persist download metadata, chunk offsets, and lifecycle transitions. Unfinished tasks recover as paused and resume from durable checkpoints through either interface or `--resume-all`.
- **Journal compatibility:** Automatic schema migrations preserve chunk progress, settings, checksums, and proxy configuration. Legacy `.vise/state.json` journals are imported once and preserved as `state.json.migrated`.
- **Token-bucket rate limiting:** All transfers share one bandwidth budget. `--limit-rate` accepts human-readable units; CLI commands and WebUI controls adjust the cap live without reconnecting. Settings persist across restarts.
- **Industrial WebUI:** An embedded Axum dashboard, launched only with `--webui`, provides live WebSocket telemetry, polling fallback, throughput graphs, chunk-progress grids, queue filters, and transfer controls without page reloads.
- **Terminal telemetry:** WebUI mode emits timestamped, colorized action and lifecycle events alongside one-second progress, speed, and ETA summaries. Default CLI mode remains independent.
- **SHA-256 and MD5 verification:** Expected hashes come from CLI flags, API fields, WebUI inputs, or supported HTTP metadata. Complete staging files are verified before final publication. Mismatches are journaled as `corrupt`, with expected/actual digests and retained staging data for inspection or explicit full re-download.
- **Multi-protocol proxies:** Per-transfer HTTP, HTTPS, SOCKS5, and SOCKS5h routing through `--proxy` and API `proxy_url`, including authentication and saved routes for resume. SOCKS5 resolves destination names locally; SOCKS5h delegates destination DNS to the proxy.
- **Modular architecture:** Dedicated engine, CLI, WebUI, journal, checksum, and rate-limiter modules keep both interfaces connected to the same worker pool.
- **Release automation:** Tag-triggered GitHub Actions builds optimized Linux x86_64, macOS Intel/Apple Silicon, and Windows x86_64 binaries. Archives bundle the README, both licenses, and docs; releases include SHA-256 checksums and publish after all build and quality checks pass.

### Security

- **No proxy fallback:** Explicit routes cover probes, chunks, redirects, retries, and resume, overriding environment proxy and `NO_PROXY` settings. Proxy failure never triggers a direct connection.
- **Credential redaction:** Public transfer snapshots and proxy display/debug output omit proxy credentials; invalid proxy diagnostics avoid echoing sensitive input. Full authenticated routes remain in the unencrypted local journal for recovery.
- **TLS validation:** Certificate verification stays enabled for HTTPS destinations and HTTPS proxies.
- **Local WebUI safeguards:** Loopback-only binding, host and browser-origin validation, JSON-only mutations, and a Content Security Policy constrain browser access.
- **Safe publication:** Existing output files are never overwritten, and configured checksum verification must succeed before final publication. SHA-256 from a trusted source is preferred for security-sensitive verification; MD5 supports legacy compatibility and accidental-corruption detection.

[0.1.1]: https://github.com/juic3b0x/vise/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/juic3b0x/vise/releases/tag/v0.1.0

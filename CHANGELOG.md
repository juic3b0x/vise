# Changelog

All notable changes to Vise are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.4.0] - 2026-10-22

### Added

- **Global state journal:** All modes share `~/.vise/state.sqlite3`, alongside configuration. New Unix state directories use `0700`; database and lock files use `0600`. WAL, full synchronization, serialized blocking-pool transactions, and global engine ownership protect concurrent workers and API readers.
- **Cross-directory recovery:** Schema 5 stores absolute destination and staging paths with each task. History, chunk offsets, authentication metadata, and rate limits survive changes to the default output directory. Identical filenames in separate destinations no longer conflict.
- **Legacy journal imports:** Selecting an old output directory imports its SQLite or JSON history once. SQLite backup snapshots include committed WAL data without modifying the source. Task rows, initial rate, and source markers commit atomically; conflicts abort without replacing global history. JSON backups remain available.

### Changed

- Raised the minimum supported Rust version from 1.85 to 1.88 and aligned installation and contribution requirements.
- Refreshed the Cargo lockfile against Rust 1.88, including compatible cookie-store, time, ICU, and Tokio updates.
- Adopted Rust 1.88 let-chain conditions where required to keep current stable Clippy warning-free.
- `--output` sets the default destination for new tasks; `--resume-all`, interactive controls, daemon, and WebUI operate on the global queue at saved paths. Partial files remain beside their destinations for atomic publication on the same filesystem.
- Saved rate limits survive the initial zero configuration default; explicit CLI limits and nonzero configuration limits retain precedence.
- CLI batch success reflects tasks scheduled in that invocation, so unrelated paused or failed history does not cause false failures.

## [0.3.0] - 2026-10-20

### Added

- **Cookie-authenticated transfers:** `--cookies FILE` and optional `downloads.cookies_file` configuration import Netscape cookie files into isolated Reqwest jars. Preserve host-only/domain scope, paths, secure flags, session/expiring cookies, and `#HttpOnly_` records. Probes, parallel chunks, redirects, and retries share the task's jar, including cookies set by responses.
- **Authenticated resume:** SQLite schema 4 journals each task's absolute cookie-file path with its chunk offsets. Resume reloads that file without copying cookie values into SQLite or public snapshots. Missing or malformed saved files fail the affected task before network requests while allowing other tasks to recover.
- Parser, CLI/configuration precedence, authenticated range/retry, cookie isolation, session refresh, and migration regression tests. File parsing runs on the blocking pool with bounded input size and diagnostics that omit cookie contents.

### Changed

- Moved common HTTP client construction into `src/net/`, preserving proxy routing and TLS verification. Existing schema 1–3 journals migrate transactionally without losing transfer state.
- Enabled Reqwest's optional `cookies` feature; added no direct parser dependency.

## [0.2.0] - 2026-10-20

### Added

- **Persistent configuration:** Automatically create and load `~/.vise/config.toml` using typed Serde/TOML settings. Defaults select `~/Downloads/Vise`, five concurrent downloads, unlimited bandwidth, and no proxy. `--bind`, `--host`, `--port`, `--output`, `--jobs`, `--limit-rate`, and `--proxy` override file settings.
- **Persistent daemon credentials:** Generate a random token on first run under `daemon.VISE_API_TOKEN`; the environment variable overrides the file, and `auth_token` remains an accepted alias. Serialize concurrent first-run creation, publish complete files without overwriting existing configuration, and use private Unix directory/file permissions.
- **Daemon mode:** `-d` / `--daemon` runs the shared download manager, REST API, WebSockets, and embedded dashboard quietly in the foreground for systemd or container supervision. No stdin reader, progress bars, or terminal telemetry loop is started.
- **Remote control:** Daemon mode accepts configured IPv4/IPv6 interfaces through configuration or CLI overrides. Non-loopback listeners require a valid file or environment token; API clients use Bearer authentication, and browsers use HTTP Basic authentication with username `vise` and the token as password. The same authentication protects WebSocket upgrades and dashboard assets.
- **Service deployment:** A systemd user-service example includes restrictive file permissions, graceful SIGTERM shutdown, and optional resume of saved transfers. Daemon sessions reuse SQLite WAL initialization, atomic checkpoints, and startup recovery.

### Changed

- Moved requirements, Cargo installation, verification, and systemd setup into `INSTALL.md`, keeping the README focused on features and usage. Release archives include the installation guide alongside the README.

### Security

- Kept unauthenticated `--webui` restricted to loopback. Daemon requests retain Host validation, same-origin/cross-site guards, JSON-only mutations, and the existing response security headers. Daemon credentials are not journaled or included in API snapshots.

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

[0.4.0]: https://github.com/juic3b0x/vise/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/juic3b0x/vise/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/juic3b0x/vise/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/juic3b0x/vise/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/juic3b0x/vise/releases/tag/v0.1.0

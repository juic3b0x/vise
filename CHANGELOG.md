# Changelog

All notable changes to Vise are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- Use one metadata-and-payload request for fresh single-connection downloads, retaining the initial open-ended response through staging preparation and preserving bounded retries, validator-based resume, checksums, credentials and proxy isolation. Consume validated one-byte range probes through EOF with cancellation and a deadline to preserve connection pooling.
- Skip the destructive-reset checkpoint only for atomically created, offset-free fresh staging files. Keep lifecycle commits, FULL/WAL durability, checksum-before-publication and completion-after-commit ordering.
- Bump the benchmark fixture to 0.1.1, expose its executable version and startup version, and query the selected binary rather than the checkout manifest in reports. Remove zero-latency benchmark timers; reject RAM-disk declarations contradicted by the detected filesystem, label unknown storage unverified, and record Linux fixture CPU around the timed client interval separately from hash preparation.

- Checkpoint only dirty record generations and explicit deletions, acknowledging captured generations after file synchronization, the atomic SQLite commit and journal-directory synchronization. Avoid staging inspection for fresh unstarted tasks; validate their staging paths before first access and retain conservative recovery/resume handling. Keep atomic input admission, grouped receipts, lifecycle barriers and publication ordering without a schema change.
- Skip progress-bar construction and rendering for batch runs without terminal drawing; use settlement-aware session completion and sample speeds only for downloading tasks. Keep exit codes, failure formatting and redirected-output behavior. Skip idle monitor checkpoints when no persistent changes remain.
- Index destination reservations across admission, rollback and deletion, and bypass grant locks for unlimited transfers while preserving live shared rate limits and cancellation. No dependency or version changes.

- Admit startup lists supplied with `--input` in two atomic durable stages: all new tasks paused, then all queued. Emit `Added` in input order after the second barrier and before workers start. Known first-stage rollbacks admit none; activation failures and reconciled lost acknowledgements retain the whole admitted list paused. Preserve duplicates, collision-safe names, transports and credential-origin validation. Positional-only and daemon batch contracts stay unchanged. No version bump is made.

- Group ready concurrent checkpoint requests into one capture/file-sync/FULL-WAL-transaction/directory-sync barrier without timers or delayed lifecycle commits. Receipts cover frozen batches; late requests wait for another capture. Enrolled work survives receiver cancellation and drains on shutdown. Reconcile uncertain SQL outcomes before further captures; retain exclusive reset, terminal, deletion and split paths. No schema, request-count or version changes.

- Cache CLI progress styles and unchanged formatted prefixes/messages while preserving progress-bar setter, redraw, finish/reset and redirected-output behavior.
- Avoid acquiring the global checkpoint lock when an adaptive split cannot fit a serving tail; retain authoritative eligibility and cancellation checks under the lock.
- Reuse a stateless HTTP client across tasks without explicit proxies, cookie files or request contexts; retain isolated transports for every other task, including recovery. Preserve probe/body requests and private routing/cookie behavior.

### Fixed

- Replace the small benchmark's interactive workaround with `--batch --input`; add `--small-files N` and bound both it and `small_count` by the production 1,000-URL limit. Preserve seed, sizes and connection/job budgets; earlier interactive results need a new baseline.
- Reduce the full small-file benchmark from 2,000 URLs to 1,000 URLs to avoid Vise failures with the larger workload; keep the smoke workload unchanged.

## [0.8.0] - 2026-10-30

### Added

- Adaptive HTTP range tail splitting: idle workers steal throughput-weighted portions of unfinished ranges, with conservative halving when observations are unreliable. Preserve validators, proxy/credential transport, shared rate limiting, bounded retries, checksum publication, and atomic SQLite range ownership without a schema migration.
- `--min-split-size BYTES` and `downloads.min_split_size` (default 1048576; zero disables splitting), a 1,024-chunk task cap, 30–60 second refusal cooldown, and additive per-attempt `tail_splits` / `split_discarded_bytes` telemetry in snapshots, dashboard, and WebUI terminal output.
- Exhaustive/seeded split tests, process-death recovery barriers, heterogeneous fixture and credential/proxy regressions, and a deterministic developer simulation comparing fixed quarters, halving, weighted tails, and initial 1 MiB pieces.

- Embed the build commit and committer date in `vise --version` and `-V`, with a dirty marker scoped to build inputs. Support compile-time packager overrides, worktrees, detached HEAD and shallow clones using only std and Git; retain a package-version-only fallback when Git metadata is unavailable. Version output requires no runtime Git access and preserves the `vise X.Y.Z` prefix.
- Developer-only reproducible benchmark harness with an independent, locked Rust HTTP fixture and Python 3.11+ standard-library runner. Compare Vise with matched/default aria2 profiles and optional single-connection curl across large-file, connection-capped, heterogeneous, small-file, delayed-response and seeded-reset scenarios without changing Vise's runtime or dependencies.
- Mandatory output hashes, corruption self-tests, isolated homes, alternating repeated runs, JSON/Markdown reports, CPU/memory/request/connection/wasted-byte metrics, serving-tail measurements and heterogeneous connection accounting. Label smoke output non-publishable and exclude flaky recovery from throughput comparisons. Document methodology, platform limits and benchmark source isolation.

### Changed

- Tighten the internal Rust library surface used by the binary and tests; retain public items required by existing binary/integration callers and keep `Download.url` and `Download.split_discarded_bytes` public. Runtime behavior, serialized snapshots, dependencies, and adaptive tail splitting are unchanged. The entire Rust surface remains internal and unstable throughout 0.x, without a supported embedding API.
- Compile the existing credential logging policy once in the library and use its shared initializer from the binary at the same startup point. Expose only `logging::init` and `logging::CredentialGuard`; preserve native/log-bridge filtering and existing logging tests. The initializer protects its own output, without extending that guarantee to independently configured host sinks.
- Keep cookie import implementation private to `net`, with a crate-only loader re-export for engine recovery. Compile the unchanged stream-throttling helper only for its existing unit tests; production transfer throttling continues through the shared token bucket.

### Fixed

- Read the complete split-phase acknowledgment before killing the crash-test child, avoiding socket-reset races; cover incomplete and incorrect acknowledgments.
- Synchronize the donor-retry fixture with the split child’s actual range request instead of relying on frame timing, with a virtual-clock regression.
- Include each available benchmark tool's resolved absolute executable path alongside its version and SHA-256 in report environment metadata; retain explicit skips for missing optional tools.

## [0.7.0] - 2026-10-28

### Added

- Private per-task custom headers, referer, user agent, and structured API cookies, persisted in SQLite schema 6 for authenticated retries and restart-safe resume. Add repeatable local and remote `--header`, `--referer`, and `--user-agent`, plus optional `[downloads].user_agent` configuration. Explicit CLI values, including an empty user agent selecting the built-in value, override TOML.
- Shared request settings for positional, input-file, remote, and API batches. Validate all credentialed URLs against one origin before admitting tasks or starting download requests. Public snapshots expose validated header names/counts and credential-presence flags only.
- Write-only WebUI advanced request controls for headers, referer, user agent, and structured cookies. Clear inputs after submission attempts and navigation, show redacted task summaries, and explain mixed-origin batch rejection without exposing values in browser storage, activity, or console output.
- Browser extension 0.3.0: independent cookie/referer forwarding switches, both off by default; per-site consent and optional runtime permissions; explicit tab-store/frame-partition checks; preserved hostOnly/domain/path/secure/expiry/httpOnly cookie metadata; and a separate **Send URL only to Vise** fallback. Refuse cookie capture without a trustworthy partition lookup, including on current Firefox; retain URL-only and independent referer forwarding.

### Changed

- Newly configured tasks use an initial-origin-aware transport for probes, chunks, retries, and resume. Same-origin response cookies update the task's in-memory jar; supplied cookies override matching imported-file cookies, including removal by expired records. Legacy NULL-context tasks retain their existing Reqwest redirects and automatic referer behavior.
- Expand single-download admission and remote-client JSON bodies to 128 KiB for bounded credentials; batch admission remains 2 MiB, and unrelated routes retain 16 KiB. Return sanitized HTTP 400/413 admission errors.
- Refresh compatible dependencies without changing requirements or Rust 1.88 support: `unicode-width` 0.2.2 → 0.2.3. Retain `uuid` 1.26.1 because newer available compatible-range releases require Rust 1.89.
- Align API, architecture, security, and installation guidance with the shipped credential controls and extension permissions.

### Fixed

- Box remote add arguments to avoid Windows Clippy's `large_enum_variant` warning without changing command-line behavior.
- Remove WAL-layout-dependent comparisons from the rejected-batch journal regression. Join seeding connection cleanup and compare complete logical contents while preserving zero-request and no-new-output assertions, eliminating a macOS-sensitive false failure.
- Render WebUI request summaries from the API's flattened public fields instead of an absent nested object. Add a Rust-verified golden public snapshot and a controller regression proving that summaries remain visible without exposing values or refilling write-only inputs.

### Security

- Fix pre-existing plaintext cookie exposure in CLI and daemon dependency debug/trace logs, affecting Netscape cookie-file imports and response cookies in earlier versions. Protect newly supplied task cookies too. Always suppress the audited `cookie_store::cookie_store` target, including `log`-bridged events, regardless of global or targeted `RUST_LOG` directives; retain other networking diagnostics. The legacy file-cookie exposure was reproduced on the pre-fix main branch.
- Centrally redact private request values in formatting and ordinary serialization, and sanitize admission JSON and transport errors. Reject injection, engine-owned headers, raw Cookie/Set-Cookie headers, duplicate user-agent/referer sources, and oversized credentials.
- Strip custom headers, custom user agents, and cookies after a cross-origin redirect; reduce a supplied referer to its origin and never restore credentials later in the chain. Reject HTTPS downgrades and invalid or credential-bearing redirect targets; preserve the task's proxy route without direct fallback.
- Reject process-wide header/referer defaults in daemon/WebUI modes and mixed-origin credentialed batches. Keep daemon API tokens out of task credentials. Require Unix journal and existing WAL/SHM files to be exactly `0600` before credential persistence; warn once on Windows to protect home-directory ACLs. Supplied credentials persist as plaintext; file-imported cookie values and response-cookie updates do not.
- Keep extension endpoints loopback-only and captured credentials exclusively in admission JSON, separate from daemon authentication. Store preferences/site consent rather than captured cookies or referers; use generic capture-failure notifications and an explicit URL-only fallback. Add local-fixture and mocked-browser regressions for redirect, recovery, proxy, permission, and credential-leak boundaries.

## [0.6.0] - 2026-10-26

### Added

- **Bulk URL ingestion:** `--input FILE` loads UTF-8 URL lists before engine startup. Trim whitespace, skip blank lines and `#` comments, accept a leading BOM and CRLF, and preserve ordering and duplicate URLs. Positional URLs precede file entries; existing job, destination, proxy, cookie, and resume rules apply.
- **Native daemon batches:** Authenticated `POST /api/downloads/batch` accepts either a URL array or text content, with an optional shared proxy. Validate the entire list before admission; return accepted task snapshots and indexed operational errors without replaying submissions. Single-task admission remains compatible.
- **WebUI batch inputs:** Paste multiple URLs, choose a `.txt` file, or drop one into the dashboard. Files are parsed locally and submitted as JSON; show validation, queueing, and partial-admission feedback without reloading.
- **Remote file queueing:** `vise remote add --input FILE` and multiple URL arguments submit to the daemon; `--wait` waits for every accepted task, and JSON output includes per-entry results. Filename/checksum overrides require one resolved URL.
- Shared bounds of 1 MiB of text, 1,000 URLs, and 8,192 bytes per URL; the batch route accepts JSON bodies up to 2 MiB while existing routes retain their 16 KiB limit. Add Rust and dependency-free frontend regressions for parsing, authentication, limits, concurrency, and durable queue admission.

### Changed

- Refresh compatible dependencies for this minor release: `rustix` 1.1.5 → 1.1.6. Keep existing dependency requirements and Rust 1.88 support; add no production dependencies.

### Fixed

- Keep terminal task snapshots private until their SQLite commit and publication cleanup settle. If completion persistence fails, CLI/API/WebSocket readers see the resolved failed state instead of a transient `completed` result. Serialize terminal mutations with checkpoints and preserve publication evidence for recovery.
- Add a deterministic SQLite-lock regression for both successful and rejected completion commits, alongside the end-to-end publication recovery test, to remove the scheduling-dependent Ubuntu failure.

## [0.5.1] - 2026-10-25

### Changed

- Refine the embedded WebUI with a calm dark palette, fine structural borders, readable typography, and a queue-first layout. Remove decorative identifiers, neon glows, and nested card surfaces.
- Present throughput and bandwidth in a compact monitoring column. Use restrained state indicators, neutral metadata, and monospace numeric telemetry; expand a transfer's chunk details on demand.
- Keep existing REST/WebSocket contracts, checksum controls, bandwidth presets, search, filters, and connection fallback. Retain keyboard navigation and reduced-motion support, with descriptive task-action labels and responsive layouts.

## [0.5.0] - 2026-10-25

### Added

- **Daemon-aware CLI:** `vise remote` controls an existing authenticated daemon with add/list/get/pause/resume/retry/delete commands, live global bandwidth changes, JSON results, and terminal or newline-delimited JSON telemetry monitoring. Full UUIDs and unique prefixes are supported.
- **Scriptable completion:** `remote add --wait` succeeds only after configured verification and final publication; failed, corrupt, or paused tasks return exit code 2. Interrupting a client leaves daemon transfers running.
- **Read-only client startup:** Existing daemon configuration supplies address/token defaults; `--endpoint` and `VISE_API_TOKEN` override them. Fully specified clients need no local home, configuration, journal, staging data, or engine locks. Wildcard local listener addresses resolve to loopback.
- Remote CLI documentation and regressions for daemon ownership, authenticated controls, credential redaction, mutation replay prevention, bounded responses, checksum success/failure, machine output, and signal-driven detachment.
- A Linux CI check for the declared Rust 1.88 MSRV, alongside the existing stable OS matrix.

### Changed

- Share the snapshot-only Indicatif renderer between local and remote controllers, including cleanup for removed tasks and escaped terminal filenames. Direct `vise URL` downloads retain their original behavior.
- Refresh compatible dependencies for this minor release: `cc` 1.10.0, `smallvec` 1.16.4, and `webpki-roots` 1.0.11, retaining Rust 1.88 support. Require the already-resolved Reqwest 0.12.28 API to explicitly disable control-request retries.
- Document compatible `cargo update` and MSRV validation before every major and minor release; skip automatic refreshes for patch releases.

### Security

- Isolate daemon credentials from transfer proxies/cookies, reject redirects and automatic request replay, preserve TLS verification, require HTTPS for non-loopback origins, bound API response sizes, and escape human-readable snapshot text. Failed client connections never fall back to a local engine.

## [0.4.4] - 2026-10-23

### Added

- Material for MkDocs documentation site with a landing page, installation and CLI guides, daemon/API reference, WebUI and extension guides, and engine architecture. Strict link checks run on pull requests, with GitHub Pages deployment from `main`.

### Fixed

- Register shutdown signal listeners before queue seeding or controller startup, removing the scheduling window in which an active interactive session could terminate directly on SIGTERM. Propagate listener registration errors before starting background tasks.
- Make the interactive SIGTERM regression wait for a durably committed runtime command instead of database-file creation and a fixed sleep. Keep stdin open during shutdown and always reap child processes on failure.

## [0.4.3] - 2026-10-22

### Security

- Compare fixed-size daemon token SHA-256 fingerprints with `subtle::ConstantTimeEq` for both Bearer and browser Basic authentication, replacing the hand-written byte comparison. Reuse the existing `subtle` version already resolved by TLS dependencies.
- Document that `VISE_API_TOKEN` is a randomly generated bearer credential rather than a user password. Custom tokens must come from a cryptographically secure generator; format validation alone does not establish entropy.

### Fixed

- Add authentication regressions for every digest-byte mismatch, minimum and maximum token lengths, prefixes, appended bytes, empty values, and case differences.

## [0.4.2] - 2026-10-22

### Security

- Validate absolute canonical directory identities, existence, and directory type before journal synchronization. Reject traversal and substituted symlinks before filesystem operations.
- Bound queue filenames and UUID-derived staging paths to their expected directories. Validate stored layouts and existing staging files before checkpoints or deletion; reject symlinks without removing task history or unrelated files.

### Fixed

- Preserve raw parent-directory components in traversal regression fixtures on Windows, where joining onto canonical verbatim paths otherwise removes `..` before validation. Share the pre-I/O lexical guard across directory, child-path, and journal-record validation; cover drive, UNC, and extended-length inputs.
- Treat dangling output symlinks as occupied filenames and preserve actionable I/O errors during collision checks. Keep filesystem validation on the blocking pool and tolerate staging paths disappearing during awaited publication.
- Add regression coverage for traversal, absolute-path injection, directory/file type mismatches, symlink substitution, missing destinations, and queue/journal preservation after rejected operations.

## [0.4.1] - 2026-10-22

### Fixed

- Await final-file publication and the staging `.part` rename before marking a download completed in SQLite or emitting its completion event. Publication failures retain recoverable data; failed state commits no longer emit successful completion events.
- Preserve a temporary same-file recovery link across the staging rename and SQLite commit. Startup recovers interrupted publication without network requests, rechecks configured checksums, and cleans up recovery links after persisting recovered state.
- Bound staging-cleanup assertions with asynchronous filesystem polling to tolerate OS visibility delays. Add regression coverage for publication ordering, completion events, SQLite state, and crashes before and after the completion commit.

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

[0.8.0]: https://github.com/juic3b0x/vise/compare/v0.7.0...v0.8.0
[0.7.0]: https://github.com/juic3b0x/vise/compare/v0.6.0...v0.7.0
[0.6.0]: https://github.com/juic3b0x/vise/compare/v0.5.1...v0.6.0
[0.5.1]: https://github.com/juic3b0x/vise/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/juic3b0x/vise/compare/v0.4.4...v0.5.0
[0.4.4]: https://github.com/juic3b0x/vise/compare/v0.4.3...v0.4.4
[0.4.3]: https://github.com/juic3b0x/vise/compare/v0.4.2...v0.4.3
[0.4.2]: https://github.com/juic3b0x/vise/compare/v0.4.1...v0.4.2
[0.4.1]: https://github.com/juic3b0x/vise/compare/v0.4.0...v0.4.1
[0.4.0]: https://github.com/juic3b0x/vise/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/juic3b0x/vise/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/juic3b0x/vise/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/juic3b0x/vise/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/juic3b0x/vise/releases/tag/v0.1.0

[Unreleased]: https://github.com/juic3b0x/vise/compare/v0.8.0...HEAD

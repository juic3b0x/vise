# AGENTS.md — Vise

## Project
Vise is a Rust (stable) async HTTP(S) download manager: multi-threaded chunking, global SQLite state, quiet daemon, embedded Axum WebUI, proxy/SOCKS5 routing, browser extension. Current stage: pre-1.0, many shipped features. Preserve existing behavior unless the task says otherwise.

## Working rules
- Read this file, then locate and read the relevant code, tests, and docs before editing. Never guess file or function names.
- For tasks touching more than ~3 files or any persisted state/API, give a short plan first and then implement.
- Keep diffs focused: no unrelated refactors, renames, or formatting churn.
- Never run `git push`, create tags, or publish. Never touch files outside the repo.
- Final message must list: files changed, behavior summary, tests added, commands run with results, and anything unverified.

## Tech stack (fixed)
Tokio (async runtime), Reqwest (streaming + range requests), Axum (API/WebSocket/WebUI), rusqlite with bundled SQLite (WAL), Indicatif, Clap. Do not add `sqlx` or other ORMs/runtimes.

## Dependencies
- Add a crate only when strictly necessary; state the reason, size/feature impact, and alternatives considered in the final message.
- Disable default features where possible. Never widen version requirements without being asked.
- Respect the MSRV declared in Cargo.toml (`rust-version`); do not use newer language/library features than it allows.

## Architecture
- Keep the core download engine decoupled from the CLI loop, the daemon, and the Axum/WebUI telemetry bridge. The engine must not depend on UI or HTTP-server types.
- Repository map:
  - `src/lib.rs` -> public library module exports (`pub mod` declarations).
  - `src/logging.rs`, `src/logging/tests.rs` -> library-owned logging policy shared with the binary, immutable credential-target filter, log-bridge handling and regressions (`init`, `CredentialGuard`).
  - `src/main.rs` -> Clap arguments, early remote dispatch and local startup (`Args`, `Command`, `run`).
  - `src/startup.rs` -> CLI-over-TOML/environment precedence and controller configuration (`Startup`, `load`, `resolve`).
  - `src/app.rs` -> queue seeding, controller supervision, signals and ordered shutdown (`Controller`, `Background`, `run_session`).
  - `src/engine.rs` -> shared manager, job locks, worker semaphore and transfer options (`Manager`, `Job`, `Config`, `TransferOptions`).
  - `src/engine/queue.rs` -> admission, collision-safe destinations and task lifecycle controls (`add_with_options`, `start`, `pause`, `delete`, `shutdown`).
  - `src/engine/batch.rs` -> prevalidated, disconnect-safe bulk admission and indexed results (`add_batch`, `BatchResult`).
  - `src/engine/state.rs` -> owned snapshots, event subscriptions, transitions and persisted rate changes (`list`, `get`, `subscribe_events`, `set_rate_limit`).
  - `src/engine/http.rs` -> range probes, validators, response checks and retry backoff (`probe`, `request`, `content_range`, `backoff`).
  - `src/engine/layout.rs` -> chunk partitioning, filename/record validation and status normalization (`make_chunks`, `validate_record`).
  - `src/engine/transfer.rs` -> throttled chunk I/O, resume offsets, staged verification and safe publication (`execute`, `transfer_chunk`, `publish_file`).
  - `src/engine/completion.rs` -> terminal-state commit/visibility ordering and recovery-link cleanup (`finish`, `remove_publication_evidence`).
  - `src/engine/journal.rs` -> speed sampling, durable progress checkpoints and directory synchronization (`monitor`, `checkpoint_locked`, `sync_directory`).
  - `src/engine/{recovery,storage,migration}.rs` -> SQL/publication recovery, home/staging locks and legacy imports (`open`, `prepare`, `lock_outputs`, `import_selected`).
  - `src/database.rs`, `src/database/{setup,migration}.rs` -> blocking-pool SQLite WAL transactions, schema upgrades and backup-based imports (`Database`, `persist`, `upgrade_paths`, `legacy_snapshot`).
  - `src/config.rs`, `src/config/{storage,tests}.rs` -> typed TOML defaults, secure atomic creation, read-only client loading and regressions (`FileConfig`, `LoadedConfig`, `load`, `read_existing`).
  - `src/paths.rs`, `src/paths/{checked,tests}.rs` -> private directories, absolute/bounded path and symlink checks, legacy path conversion and regressions (`validate`, `directory`, `staging_files`).
  - `src/model.rs` -> persisted records, public redacted transfer/chunk snapshots and statuses (`Record`, `Download`, `ChunkProgress`, `Status`).
  - `src/events.rs`, `src/error.rs` -> timestamped lifecycle envelopes and retry-classified failures (`Event`, `EventKind`, `Error`, `Result`).
  - `src/rate_limiter.rs` -> rate-unit parsing and shared live token-bucket stream throttling (`RateLimit`, `RateLimiter`, `acquire`, `throttle`).
  - `src/checksum.rs` -> expected/header digests and cancellable SHA-256/MD5 file hashing (`ExpectedChecksums`, `Checksums`, `from_response`, `hash_file`).
  - `src/input.rs` -> bounded UTF-8 URL-list/file parsing and validation (`parse`, `read_file`, `load`, `validate_urls`).
  - `src/cli.rs`, `src/cli/progress.rs` -> interactive commands, batch completion and shared Indicatif rendering (`run`, `command`, `ProgressDisplay`).
  - `src/client.rs`, `src/client/{settings,api,commands,batch,output}.rs` -> authenticated remote commands, monitoring/waits and bounded stdout (`Settings`, `Api`, `execute`, `submit`, `Output`).
  - `src/net/{mod,cookies,request,credentials}.rs`, `src/net/credentials/` -> per-task clients, legacy/origin-aware transports, bounded cookie import, private request settings and explicit codecs (`client`, `Transport`, `RequestOverrides`, `TaskRequestContext`).
  - `src/proxy.rs` -> validated HTTP(S)/SOCKS5/SOCKS5h routing, credential redaction and explicit no-fallback configuration (`ProxyUrl`, `apply`).
  - `src/telemetry.rs` -> WebUI-only stdout event/progress monitor with injectable async writer (`Options`, `run_with_writer`, `format_event`).
  - `src/web.rs`, `src/web/{access,batch}.rs` -> daemon/local Axum REST, WebSocket snapshots, asset serving and authentication/origin guards (`DaemonConfig`, `router`, `stream`, `Access`, `Token`).
  - `src/dashboard.{html,css,js}`, `src/dashboard-input.mjs` -> embedded dashboard shell/styles, graphs, controls, WS/polling and local URL-file parsing (`render`, `connect`, `poll`, `parseUrls`, `readUrlFile`).
  - `extension/` -> MV3 manifest/icons, context-menu queueing, validated local API/token settings, options UI and mocked-browser tests (`registerMenu`, `ViseExtension`, `request`).
  - `tests/`; `docs/`; `.github/` -> HTTP/proxy/TLS and frontend regressions; MkDocs guides/assets/systemd unit; CI, releases, Pages, audits and weekly Cargo updates (`Fixture`, `TestProxy`, `vise.service`, workflows).

  **Boundaries**
  - Engine -> database/model/events/config/paths/input/net/proxy/checksum/rate_limiter/error; never Axum, web, CLI, telemetry or remote-client types.
  - Database/model/events/error/paths/config/input/net/proxy/checksum/rate_limiter -> shared helpers and external crates; never engine or controllers.
  - main/startup/app compose controllers; CLI/web/telemetry may call Manager and shared helpers; core modules must not import those controllers.
  - Remote client -> read-only config, input/checksum/model/rate types, CLI ProgressDisplay, engine BatchResult DTO and app::shutdown_signal; no Manager/Database operations.
  - Dashboard/extension -> browser APIs and daemon REST/WebSocket contracts, never direct journal or in-process engine access.

## Invariants (never violate)
**State**
- All progress, chunk offsets, settings, and live rate limit persist atomically in the global SQLite journal (`~/.vise/state.sqlite3`, WAL). Lifecycle transitions commit immediately; chunk checkpoints run about once per second. No flat-file progress tracking.
- Unfinished tasks recover as paused. One engine per home directory (process lock); `vise remote` must never open the journal or start an engine.

**File safety**
- Never overwrite an existing output file. Publish the final filename only after all expected checksums match.
- Checksum mismatch => status `corrupt`; keep the staging file at `<out>/.vise/<id>.part`; exclude from `--resume-all`.
- Parallel range downloads require range support plus a strong ETag or Last-Modified; otherwise use a single stream.

**Network & secrets**
- An explicit proxy never falls back to a direct connection. Routes are saved per task and reused for probe, chunks, retries, and resume.
- SQLite stores cookie-file paths, never cookie values imported from those files. Explicitly supplied request headers, referer, custom user agent, and structured cookies may be persisted as private per-task credentials for restart-safe resume; response cookies remain memory-only. Credential values must never appear in logs, errors, Debug/Display, ordinary serialization, public API/WebSocket snapshots, WebUI transfer views, or terminal telemetry. Only explicit private journal and outbound-request codecs may access credential bytes. Public request metadata is limited to validated header names, counts, and presence flags; public proxy metadata remains a credential-free proxy_url plus proxy_authenticated. New request credentials are confined to the task’s initial origin across redirects; legacy file-only cookie behavior is preserved. Daemon API tokens must never enter task credentials or SQLite. Credentialed requests (custom headers, referer, supplied cookies) apply only to a single origin per admission; batches spanning multiple origins are rejected.
- Daemon token lives in config/env only, never SQLite. Non-loopback binds require a valid token. `--webui` stays loopback-only with origin checks.
- The dashboard is embedded in the binary: no Node, build step, CDN, or external fonts.

**Behavior**
- CLI flags override `config.toml` even when equal to a default.
- The global rate limit is one shared token bucket on streamed body bytes; live changes must not reconnect transfers.
- Config creation is atomic; new Unix dirs are `0700`, files `0600`.

## Testing
- Use the existing local HTTP fixtures; no tests may hit the public internet.
- Tests must isolate `HOME`/`USERPROFILE` to a temp dir. Never read or write the real `~/.vise`.
- Prefer Tokio's virtual clock over sleeps for timing tests.
- Every bug fix gets a regression test; every new behavior gets a test.

## Definition of done
All must pass with zero warnings before a task is complete:
```sh
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
```
If `docs/` or `mkdocs.yml` changed, also run `python -m mkdocs build --strict`. Do not silence lints with `#[allow]` unless justified in a code comment.

## Docs & changelog
Any change to CLI flags, config keys, API, or user-visible behavior updates `README.md`, the relevant `docs/` page, and `CHANGELOG.md` in the same change.

## Commits & branches
Format: `type(scope): description` with types `feat`, `fix`, `refactor`, `test`, `docs`, `perf`, `build`, `ci`, `chore`. Scope is the area (e.g. `engine`, `journal`, `webui`, `daemon`, `proxy`). For commit-worthy changes, provide the commit message and pull-request commands. Branch names have no `codex/` prefix.

## Release preparation
- Follow Semantic Versioning 2.0.0.
- Before every major or minor release, run a compatible `cargo update` without widening dependency requirements. Preserve the declared MSRV, review the lockfile diff, and validate with the minimum Rust toolchain and current stable: full tests, Clippy, builds, and installation checks.
- Skip automatic dependency refreshes for patch releases.
- Prepare the version bump, changelog, and release notes only. Do not tag or push.

## Rust Doctor audits
Always pass the duplicate-major-versions rule override. From the repository root:
```sh
rust-doctor . --yes --rule rust_doctor::cargo::duplicate_major_versions=off
```
Fix findings or explain each one left open. If `rust-doctor` is unavailable in the environment, say so instead of skipping it silently.

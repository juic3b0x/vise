# Architecture and API

Vise uses one shared download manager for its CLI, WebUI, and daemon.

## Execution modes

Default CLI sessions use Indicatif and optional interactive input. `--webui` runs a loopback-only Axum server with a terminal telemetry bridge. `-d` / `--daemon` runs the same Axum routes and shared manager quietly in the foreground for a service manager, without the stdin thread, progress bars, or terminal telemetry task. All modes use the existing Tokio runtime, SQLite WAL initialization, ordered shutdown, and crash recovery; WAL permits external readers while Vise serializes its own transactional writes. A global home-directory lock admits one engine cache/worker pool per user; staging locks also exclude older per-directory engines.

Daemon mode uses configured host/port values or `--bind` / `--host` / `--port` overrides for IPv4/IPv6 listeners. Non-loopback binds require a valid token from configuration or the environment. Bearer authentication and HTTP Basic authentication (`vise` / token) protect REST, assets, and WebSocket upgrades. Basic authentication lets the embedded dashboard use the browser's normal HTTP authentication cache. Invalid authentication returns HTTP 401. Browser origins must match the request Host; only authenticated daemon requests may use arbitrary valid hostnames, including names preserved by an HTTPS reverse proxy. The local unauthenticated WebUI retains its exact loopback Host check.

The optional Manifest V3 browser extension uses its own loopback host permissions to POST chosen links to `/api/downloads`. A serialized Chrome-extension ID or Firefox-extension UUID origin receives an exception to same-origin/cross-site checks only when an explicit Bearer token authenticates it. Cached Basic authentication and unauthenticated servers do not qualify, and no website CORS headers are added. The extension revalidates local endpoint settings per request, refuses redirects, omits cookies/referrers, and does not retry ambiguous queue submissions. See [extension/README.md](../extension/README.md).

## User configuration

`src/config.rs` deserializes `~/.vise/config.toml` into typed daemon, downloads, and proxy settings using Serde and TOML. Home resolution uses `HOME` on Unix and `USERPROFILE` (or `HOMEDRIVE` plus `HOMEPATH`) on Windows. Blocking I/O, a cross-process creation lock, staging-file synchronization, and atomic publication run on Tokio's blocking pool. First startup creates missing directories and a complete file with a random persistent token. Existing files are never rewritten. Newly created Unix directories/files use `0700`/`0600`. Malformed or unknown fields fail without echoing source text; missing fields use defaults.

`src/startup.rs` resolves CLI options over file settings before opening the manager. Optional CLI fields distinguish absence from an explicit value equal to an older default. `--bind` overrides the whole socket address; `--host` and `--port` independently override file fields. `VISE_API_TOKEN` overrides `daemon.VISE_API_TOKEN` (`auth_token` is an alias). Output paths beginning with a standalone `~` component expand to the resolved home. `downloads.global_speed_limit_kbps` uses 1024 bytes per unit and checked multiplication; the initial zero default restores the saved global cap (or unlimited for a new journal). An explicit CLI limit or nonzero file limit overrides the global cap. With the initial zero file default, startup restores the saved global cap instead. Live controls still persist in SQLite and do not rewrite user configuration.

## HTTP and WebSocket API


| Method | Endpoint | Result |
| --- | --- | --- |
| GET | `/api/downloads` | Array of all saved and active downloads |
| GET | `/api/telemetry` | Timestamped download snapshots and global configuration for HTTP fallback |
| GET | `/api/config` | Global bandwidth configuration |
| POST | `/api/config` | Apply and persist `{"limit_rate":"5M"}`; use `off` to clear |
| POST | `/api/downloads` | Add URL, optional filename, `proxy_url`, `expected_sha256`, and/or `expected_md5`; HTTP 201 |
| GET | `/api/downloads/{uuid}` | One download |
| DELETE | `/api/downloads/{uuid}` | Stop the task and remove its queue entry/partial data; HTTP 204; completed outputs are kept |
| POST | `/api/downloads/{uuid}/pause` | Pause and wait for workers to stop |
| POST | `/api/downloads/{uuid}/resume` | Queue a paused/failed download |
| POST | `/api/downloads/{uuid}/retry` | Resume a failed/paused task; reset every chunk when corrupt |
| WS | `/ws` | Full JSON download snapshots on change and at least every 500 ms |
| WS | `/ws/telemetry` | `{timestamp, downloads, configuration}` on change and at least every 500 ms |
| GET | `/assets/dashboard.css` | Embedded stylesheet, `text/css` |
| GET | `/assets/dashboard.js` | Embedded controller, `text/javascript` |

Mutation requests require `Content-Type: application/json`; send `{}` for pause/resume/retry/delete. URLs must be HTTP(S), without embedded credentials. Explicit filenames must be plain filenames, never paths. Requests are limited to 16 KiB.

```sh
curl -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/file.zip"}' \
  http://127.0.0.1:8080/api/downloads
curl http://127.0.0.1:8080/api/downloads
curl -X POST -H 'Content-Type: application/json' -d '{}' \
  http://127.0.0.1:8080/api/downloads/REPLACE_WITH_RETURNED_UUID/pause
```

Download snapshots contain `id`, `url`, `filename`, `status`, `total_bytes` (nullable), `downloaded_bytes`, `bytes_per_second`, `eta_seconds` (nullable), `error` (nullable), `created_at` (UTC Unix epoch milliseconds), and `chunks`. Each chunk exposes `index` (zero-based), `start_byte`, `end_byte` (exclusive, nullable for unknown-length streams), `downloaded_bytes`, and `status`. These come directly from the worker record under its read lock; the frontend never estimates individual segment progress from the aggregate percentage. Download states are `queued`, `probing`, `downloading`, `verifying`, `paused`, `completed`, `failed`, and `corrupt`; chunk states omit `probing`, `verifying`, and `corrupt`. Older serialized download snapshots without `chunks` deserialize to an empty array. Engine errors use an `error` JSON field and HTTP 400, 404, 409, or 500.

Both configuration endpoints return `{"limit_rate":"5 MiB/s","limit_bytes_per_second":5242880}` for a `5M` cap. Unlimited is represented by `{"limit_rate":"unlimited","limit_bytes_per_second":null}`. The POST body requires a string and rejects unknown fields. Invalid rates return HTTP 400 without changing the previous setting; failed journal commits return HTTP 500 and preserve the live setting. The dashboard uses `/ws/telemetry` to receive downloads and configuration together. `/ws` retains its download-array format for existing clients; its snapshots gain the additive `chunks` field.

```sh
curl -H 'Content-Type: application/json' -d '{"limit_rate":"1.5M"}' \
  http://127.0.0.1:8080/api/config
curl http://127.0.0.1:8080/api/config
```

## Transfers and recovery


- A single shared manager serves both controllers. Tokio's multithreaded runtime runs segment workers, and a semaphore bounds simultaneous downloads. The maximum number of segment requests is `jobs × connections`. Segments are at least approximately 1 MiB, with up to `connections` segments per file. Buffers hold at most 256 KiB per segment, in addition to HTTP buffers.
- Vise probes actual range support with `GET Range: bytes=0-0`; it does not trust `Accept-Ranges` alone. It checks status codes, every `Content-Range`, exact lengths, and response validators. TLS verification stays enabled, redirects are bounded, and the client requests identity encoding so compressed representations cannot invalidate byte offsets.
- Parallel/resumed ranges require a strong ETag or Last-Modified validator. `If-Range` protects requests and retries. If the resource changes, resuming reprobes it and resets the partial layout. When ranges or validators are unavailable, Vise safely uses a single streaming request and restarts that stream after a pause or dropped connection. Unknown lengths are supported.
- Connection/read failures, HTTP 408/429/5xx, and truncated responses retry with bounded exponential backoff. Numeric `Retry-After` is honored up to 60 seconds. Other HTTP errors, invalid ranges, and disk errors fail with a readable error. A stalled read has a configurable timeout; large downloads have no total-duration limit.
- `~/.vise/state.sqlite3` stores all destinations and their chunks in normalized SQLite tables. WAL mode, `synchronous=FULL`, foreign keys, and atomic transactions protect durable metadata. Progress snapshots are captured before synchronizing partial files, then committed once a second; start, state transition, pause, completion, reset, and deletion also checkpoint. A crash can lose work since the last checkpoint; bytes beyond the saved offsets are downloaded again. On Unix, journal directory entries are synchronized as well. See [SQLite WAL durability](https://www.sqlite.org/wal.html).
- Downloads publish only after every segment finishes and the file is synchronized. A hard link creates the final filename atomically without overwriting existing files. The engine awaits renaming `.part` to a private `.published` recovery link and synchronizing its directory before marking the task completed. After SQLite commits, it removes the recovery link before emitting the completion event. An interrupted publish is recovered by checking whether the final and staging/recovery paths identify the same file. This requires an output filesystem that supports hard links and atomic renames; unsupported operations report an error. Windows syncs individual files but lacks the Unix directory-sync durability guarantee.
- Automatically derived names receive numeric suffixes on collision. Explicit name collisions fail. A global process lock prevents independent Vise caches from replacing one another's durable state, and per-output staging locks exclude older Vise processes. Corrupt or unsupported journals are rejected rather than silently discarded.

Local journal and partial files are trusted application data; do not modify them during a transfer. HTTP validators establish representation identity. Optional expected checksums verify file integrity before publication; Vise does not provide segmented downloads from multiple mirrors.

## Project layout


```text
Cargo.toml / Cargo.lock    Dependencies and reproducible resolution
src/main.rs               Arguments, configuration, mode selection
src/app.rs                Controller supervision, signals, ordered shutdown
src/startup.rs            CLI/config precedence and validated startup settings
src/lib.rs                Public library modules
src/config.rs             Typed persistent user preferences and home resolution
src/config/storage.rs     Blocking, private, atomic first-run configuration creation
src/engine.rs             Shared manager types and configuration
src/engine/recovery.rs    Global manager initialization and saved-path recovery
src/engine/storage.rs     Home resolution, private directories, engine/staging locks
src/engine/migration.rs   Per-output legacy import orchestration
src/engine/queue.rs       Admission, pause/resume/retry, deletion, shutdown
src/engine/completion.rs  Worker terminal states, durable completion, recovery-link cleanup
src/engine/state.rs       Snapshots, events, bandwidth configuration
src/engine/journal.rs     Progress sampling and durable checkpoints
src/engine/http.rs        Probes, requests, response validation, retry backoff
src/engine/transfer.rs    Chunk workers, throttled I/O, verified publication
src/engine/layout.rs      Chunk layout, filenames, persisted-record validation
src/database.rs           SQLite schema, migration, transactions, async DB boundary
src/database/setup.rs     Blocking global journal validation and WAL initialization
src/database/migration.rs Atomic path upgrades, source markers, read-only WAL snapshots
src/paths.rs              Shared absolute-path and secure-directory invariants
src/checksum.rs           Expected digests, representation headers, cancellable file hashing
src/proxy.rs              Validated proxy URLs, credential redaction, required routes
src/net/mod.rs            Common per-task HTTP client and cookie-provider setup
src/net/cookies.rs        Bounded Netscape import, scope/expiry checks, async I/O boundary
src/rate_limiter.rs       Rate parsing, shared token bucket, response-stream adapter
src/model.rs              Internal records and public snapshots
src/events.rs             Timestamped lifecycle and server events
src/telemetry.rs          WebUI-only stdout event log and periodic summaries
src/error.rs              Typed errors and retry classification
src/cli.rs                Interactive commands and progress rendering
src/web.rs                REST, WebSocket, and local request guards
src/web/access.rs         Local Host/origin guards and daemon authentication
src/dashboard.html        Embedded dashboard markup and accessible card template
src/dashboard.css         Responsive industrial dark theme, no external fonts
src/dashboard.js          Live telemetry, SVG graphs, chunk grids, runtime controls
tests/downloads.rs        Integration-test module declarations
tests/downloads/          Shared HTTP fixture and tests grouped by behavior
tests/support/            Local proxy and TLS fixtures
```


## WebUI terminal telemetry

The executable starts `telemetry::run` only when `--webui` is present, subscribing before initial URLs or saved tasks are queued. CLI progress bars and interactive commands retain their existing behavior.

The manager has two independent channels: the existing `watch` channel coalesces frequent progress notifications for controllers, while a bounded Tokio `broadcast` channel preserves discrete lifecycle events for telemetry subscribers. Events carry their creation timestamp and an owned download snapshot. Download actions and worker completions publish through the same manager; handlers do not print from arbitrary tasks.

One asynchronous writer owns monitor stdout. It flushes each timestamped event immediately and reports queued, probing, and downloading tasks once per second, including state, percentage, bytes, smoothed speed, and ETA. UTC timestamps include milliseconds. ANSI colors are enabled for terminal stdout unless `NO_COLOR` is set or `TERM=dumb`; files and pipes receive plain text. User-controlled text is escaped to prevent terminal control sequences or forged log lines.

No application locks are held during stdout writes. The event channel holds 2,048 events; a slow subscriber receives an explicit missed-event warning and a fresh active-status snapshot. This bounds memory without applying terminal backpressure to downloads. The logger remains alive while workers pause and checkpoint during shutdown, then drains final events before exiting. Logger or persistence-monitor failures stop the controller and trigger transfer shutdown.

Deletion serializes with task controls and persistence: workers stop before the queue entry is durably removed. A removed-job flag prevents a concurrent resume from resurrecting a deleted task. Partial-file cleanup failures emit a warning; completed output files are always preserved.

## SQLite journal

The database module uses bundled SQLite through `rusqlite`. An `Arc<Mutex<Connection>>` equivalent shares a single connection; an asynchronous gate is acquired before scheduling `spawn_blocking`, so lock waits and SQL operations never occupy Tokio's async workers. The gate travels into the blocking operation, preserving commit order even if its async caller is cancelled. SQLite connection teardown also runs on the blocking pool.

| Table | Stored fields |
| --- | --- |
| `downloads` | UUID `id`, `url`, `filename`, absolute `output_path` and `part_path`, nullable `proxy_url`, nullable `cookies_file`, nullable `total_size`, `status`, `created_at`, `updated_at`, `ranged`, `validator`, nullable `error`, expected/actual SHA-256 and MD5, nullable `verified_at` |
| `chunks` | `download_id`, zero-based `chunk_index`, `start_byte`, nullable `end_byte`, `downloaded_bytes`, `status` |
| `metadata` | Per-source legacy-import completion markers and global `limit_rate` setting |

`filename` is a plain display name. `output_path` and `part_path` are absolute UTF-8 paths; staging is validated as `<destination-dir>/.vise/<uuid>.part`. Paths are validated before filesystem access and snapshots expose both absolute paths. Changing `Config.output_dir` affects new admissions only. End offsets are exclusive; a null end denotes an unknown-length stream. Download states are queued, probing, downloading, paused, completed, or failed; chunk states are queued, downloading, paused, completed, or failed. Timestamps are UTC Unix epoch milliseconds. Byte counts use nonnegative SQLite signed 64-bit integers, and larger representations are rejected explicitly.

Transactions update each changed parent and its chunks together; replacing a segment layout removes obsolete rows, and deleting a parent cascades to its children. Constraints reject invalid counts/statuses and duplicate output paths. A cache of durable snapshots avoids rewriting unchanged rows or issuing idle WAL transactions. Runtime speed samples are never stored in SQL. Live CLI, REST, WebSocket, and terminal-telemetry views use the manager's owned in-memory snapshots; startup always reconstructs jobs from SQL. `Database::list()` provides an asynchronous, transaction-consistent durable view.

On startup, an application ID and schema version identify the database. Unknown/foreign schemas and integrity failures are rejected. Pending jobs become paused without losing saved segment offsets. Representation validators are reprobed before ranges resume. A `.published` recovery link restores the staging name for same-file identity checks and configured checksum verification; this also supports older interrupted publications that still have `.part`. Recovery commits its SQL state before removing redundant links and synchronizing their directories. Corrupt data retains its `.part` file and the exposed final link is retracted.

Legacy `state.json` version 1 imports into both tables in one transaction, along with a completion marker. The source is then renamed to `state.json.migrated` when that backup name is free. If import fails, the JSON is kept unchanged and no partial import commits. If a crash interrupts backup renaming, the marker prevents importing stale JSON again, including after all tasks have been deleted. Older records without creation timestamps receive their import time.

WAL mode is intended for a local filesystem, not network shares. Do not copy only the main database file while Vise is running; SQLite may have committed data in its `-wal` file. Keep the complete `.vise` directory together, or stop Vise cleanly before copying it. The global engine lock and per-destination staging locks prevent independent managers from writing the same partial files. No external database service or SQLite runtime installation is needed.

## Bandwidth control

Every manager owns one `RateLimiter`, shared by all files, segments, and retries through an `Arc`. It wraps each Reqwest response body stream, reads one frame, and yields token-charged slices of at most 64 KiB. It retains the unconsumed frame until enough tokens arrive, applying backpressure without replacing the response or its connection. Range probes and headers are outside this body-byte budget; transport buffers can read ahead of the application. Intentional token waits happen between upstream polls and do not consume Reqwest's idle-read timeout.

The bucket refills using integer byte-nanosecond credit and Tokio's monotonic clock. It starts empty and holds up to approximately 100 ms of credit, rounded up to at least one byte and capped at 8 MiB. This bounds idle bursts while accommodating fast connections and timer granularity. Grants target about 20 ms of traffic, up to 64 KiB. An asynchronous FIFO admission mutex prevents one segment from starving others; a short synchronous mutex protects only the arithmetic. All waits use Tokio timers and are cancellation-aware, including while waiting for admission. No synchronous lock spans an await.

CLI `--limit-rate`, interactive `limit-rate RATE`, and POST `/api/config` all use the same parser and manager setting. `K/M/G/T` and `KiB/MiB/GiB/TiB` mean powers of 1,024; `KB/MB/GB/TB` mean powers of 1,000. Units are case-insensitive with an optional `/s` suffix. Decimal fractions use checked integer arithmetic and truncate fractional bytes; positive values below 1 B/s, negative values, malformed units, and overflow are rejected. Zero, `off`, and `unlimited` disable throttling.

Settings changes serialize under their own asynchronous gate, commit the exact bytes-per-second value to SQLite's `metadata.limit_rate` key (zero for unlimited), and then update the live bucket. An owned task finishes this operation even if an HTTP caller disconnects. Changing a limit discards old credits and uses a watch notification to wake token waiters immediately; it never cancels or restarts chunk connections. WebUI telemetry receives a `RateLimitChanged` event. Library callers inherit this setting when `Config::limit_rate` is `None`; the CLI supplies a positive file cap or an explicit `--limit-rate` override; a zero file default leaves the journal setting intact. The existing metadata table supports the setting without a schema migration.

Low-rate transfers flush accumulated file bytes at least every 250 ms when token slices arrive, in addition to the normal 256 KiB threshold, so progress and journal checkpoints remain useful at slow speeds. At rates below four bytes per second, the interval is bounded by the next whole-byte grant. Reported speeds measure file progress and can differ briefly from the token budget because of buffering, retries, and sampling.

## Embedded dashboard

Axum embeds the HTML, CSS, and JavaScript independently with `include_str!`, so installation still produces one executable. The combined frontend source is approximately 55 KiB, with no new crate or browser dependencies. Asset routes use explicit MIME types and `nosniff`. The content security policy allows scripts only from the same origin, with no inline scripts or dynamic code evaluation; inline style attributes support progress indicators. Host/Origin guards and configured daemon authentication apply to assets and both WebSocket routes as well as REST endpoints.

The frontend patches reusable card nodes and chunk tiles instead of replacing the queue on each message. A single animation-frame callback coalesces incoming updates. Each SVG graph holds at most 65 samples across a rolling 60-second window, sampled once per second; per-transfer histories are removed when their queue entries disappear. Plot rendering pauses while the document is hidden. The activity feed holds at most 40 entries derived from observed snapshots and successful controls; it is a browser-session feed, not a durable audit log, and can miss transitions between snapshots.

The WebSocket sends current state immediately, then on manager notifications and every 500 ms. Slow clients have a five-second send timeout. Disconnections trigger reconnects with bounded exponential backoff and jitter. HTTP telemetry polling runs every two seconds while the socket is unavailable; a generation counter prevents a slow HTTP response from replacing a newer WebSocket snapshot. Readings older than 3.5 seconds are marked stale, speeds become unavailable, and disconnected graph segments are left blank. Browser navigation restores connections when returning from the back/forward cache.

Pause/resume/cancel buttons show a pending state and suppress duplicate commands per task. Server acknowledgments and fresh snapshots establish the resulting state; a failed action displays an error without inventing success. The rate slider is logarithmic from 16 KiB/s through 1 GiB/s, plus unlimited. Text input supports the full parser range. Slider releases and presets apply immediately; requests serialize and retain the latest queued setting so rapid adjustments cannot finish out of order. Controls remain usable with keyboard navigation, progress/chunk labels are available to assistive technology, and reduced-motion preferences disable transitions.

## Checksum verification and recovery

POST `/api/downloads` accepts optional `expected_sha256` and `expected_md5` strings (or null). Input is trimmed, validated for exact hexadecimal length, and normalized to lowercase before the task is created. Invalid values return HTTP 400 without adding a task. `Manager::add_with_checksums` provides the equivalent library interface; the original `add` API remains available for metadata-only expectations.

Every public snapshot adds nullable `expected_sha256`, `expected_md5`, `actual_sha256`, `actual_md5`, and `verified_at` fields at the top level. `verified_at` is a successful verification timestamp in UTC Unix milliseconds. Download states also include `verifying` (active) and `corrupt` (terminal); chunk states remain unchanged. A corrupt file can have all chunks marked complete because these describe received bytes, not independent segment verification. Both accepted digests must match. No expected digest means the existing byte-length/range validation and publication path remains in use, with no extra hashing pass.

After chunk workers join, Vise flushes and synchronizes the staging file, checkpoints its offsets under `verifying`, and hashes only the requested algorithms in one sequential pass. RustCrypto `sha2` and `md-5` share the Digest interface. The blocking pool performs reads and hashing with a 1 MiB buffer, checking cancellation between reads. Transfer controls join the worker before resetting or deleting staging data. The manager commits matching results before atomically linking the staging file to its final filename; checksum failure never creates that link. Verification and corruption events flow through the existing terminal telemetry channel, and CLI batch mode returns a nonzero status for corrupt downloads.

SQLite schema 5 adds an explicit display filename and absolute destination/staging paths. Schema 1–4 sources are upgraded transactionally in a temporary in-memory snapshot before import; missing checksum/proxy/cookie fields default to null. Relative legacy filenames are resolved against their source output directory, while offsets, statuses, created times, checksums, proxy routes, and cookie paths are retained. Foreign-key checks run before commit; failed imports leave the global history and source unchanged.

On mismatch, actual digests and the expected-versus-actual error are committed with `status='corrupt'`. The staging file stays available for inspection. Resume returns HTTP 409 for corrupt tasks, and `--resume-all` excludes them. Explicit retry invalidates all durable offsets before truncating the file and retains the expected digests. SHA-256/MD5 whole-file checksums cannot locate a damaged segment, so automatic partial-chunk guessing is avoided. Retrying with an incorrect pinned expectation will fail again; create a new task with a corrected expectation when necessary.

If a crash occurs after creating the final hard link but before committing completion, startup rehashes the shared staging/final inode before accepting it as completed. A mismatch is durably recorded as corrupt before removing that uncommitted final link; the staging link is preserved. Already-completed files are not rehashed on every launch. A crash during verification restores a paused task, which rechecks the complete file on resume.

HTTP requests advertise `Want-Repr-Digest: sha-256=10, md5=1`. Supported `Repr-Digest` dictionary entries are base64 byte sequences for `sha-256` and `md5`; full HTTP 200 responses can also supply `Content-Digest` or legacy `Content-MD5`. Both probes and actual transfer headers are inspected, since a server may omit integrity metadata from a range probe. Content digests on HTTP 206 fragments are never interpreted as whole-file hashes. Repr-Digest bound by Content-Location to a different URL is ignored. Malformed supported digests or conflicting header values fail explicitly. Trailer-only fields, obsolete Digest headers, and other algorithms are not consumed. Caller-supplied expectations take precedence per algorithm; learned expectations persist across resumes rather than silently following later server changes. See [RFC 9530](https://www.rfc-editor.org/rfc/rfc9530.html).

## Per-transfer proxy routing

`proxy::ProxyUrl` accepts absolute `http://`, `https://`, `socks5://`, and `socks5h://` URLs with an optional username/password. It rejects missing hosts, port zero, non-root paths, query strings, fragments, whitespace, oversized input, and unsupported schemes. Scheme default ports are accepted (HTTP 80, HTTPS 443, SOCKS 1080). Parsing errors never reproduce the credential-bearing input. Username/password URL decoding and the protocol handshake are delegated to Reqwest.

`Config::proxy_url` is the process default for new tasks. `TransferOptions::proxy_url` supplies a per-transfer override via `Manager::add_with_options`; the original `add` and `add_with_checksums` interfaces remain supported and inherit the process default. The add API accepts an optional `proxy_url` string; missing/null values inherit the process default. Invalid routes return HTTP 400 before queue admission or persistence. A route becomes immutable for that task's lifetime; existing tasks always recover their saved route rather than inheriting a new command-line default.

Each job owns one pooled Reqwest client built from its effective saved route. Probing and every segment request use that client, including redirects and retry attempts. Clients are reused across pause/resume and isolated across transfers, so authentication and route configuration cannot leak into other downloads. The worker semaphore, cancellation tokens, global bandwidth limiter, checksum verification, and SQLite checkpoints continue to use the shared engine. No synchronous lock is held during network operations.

An explicit route uses `ClientBuilder::no_proxy()` followed by `Proxy::all()`, selecting the configured proxy for both HTTP and HTTPS targets and preventing environment exclusions from bypassing it. Unconfigured transfers keep the previous Reqwest default environment behavior. Unreachable proxies or rejected authentication fail through the existing bounded retry/error path; there is no direct-route fallback. HTTP targets use forward-proxy requests; HTTPS targets use CONNECT through HTTP/HTTPS proxies. HTTPS proxy URLs add TLS to the proxy hop. SOCKS5 uses local destination resolution and SOCKS5h sends the hostname to the proxy. Proxy and destination certificate validation stay enabled.

SQLite version 3 adds nullable `downloads.proxy_url`, persisted atomically alongside transfer state and chunks. Null means no explicitly selected route. The complete normalized URL is local credential-bearing metadata and survives restart; it is never copied into the public snapshot unchanged. Snapshot `proxy_url` contains only scheme/host/port, and `proxy_authenticated` indicates whether authentication was configured. Display and Debug implementations also remove user information. JSON serialization of the internal route deliberately preserves it for journal/import recovery; do not expose internal records as API payloads. Credentials are not encrypted in SQLite, so the journal's local access controls matter.

The test suite uses local authenticated HTTP and SOCKS proxies, remote-DNS fixtures with an otherwise unresolvable origin, and TLS fixtures for CONNECT and encrypted HTTPS proxy hops. The self-signed test certificate is trusted only in explicitly configured test clients; untrusted-certificate requests are checked to fail. `tokio-rustls` is a development dependency already present transitively, and enabling Reqwest's `socks` feature adds no new production dependency crate.

## Cookie sessions

`src/net/cookies.rs` imports seven-field, tab-separated Netscape records into `reqwest::cookie::Jar`. It supports `#HttpOnly_`, host-only and subdomain scope, paths, secure flags, Unix expiration, and session cookies. Strict field/header validation prevents attribute injection, and import rejection is surfaced instead of silently dropping authentication. File reads and parsing run on the blocking pool, limited to 16 MiB per file and 64 KiB per record. Diagnostics contain paths and line numbers, never cookie contents.

`--cookies FILE` overrides optional `downloads.cookies_file` in the TOML configuration. `Manager::open` validates and canonicalizes this default before creating the journal. Queue admission imports a separate jar and saves the effective absolute UTF-8 path in the task record. REST/interactive additions inherit the process default, while library callers can supply `TransferOptions.cookies_file`. Cookie paths and values are not part of public download snapshots.

At the start of each run, the engine reloads the task's saved file and rebuilds its client with the saved proxy route. All requests in that run share the jar, including server cookie updates. Recovered tasks retain their saved authentication source independently of new CLI defaults. Missing or malformed saved files fail only their task before HTTP requests, preserving offsets. No cookie values or response cookie changes are written to SQLite or back to the source file; refresh that file when authentication expires. The engine client lock is held only to swap or clone the client, never across HTTP I/O.

## Global journal ownership and imports

Startup resolves the OS home (the binary reuses the home already resolved for configuration), securely creates `~/.vise/`, and takes `engine.lock` before opening `state.sqlite3`. Embedded callers can inject `Config.home_dir` for isolated profiles/tests; the CLI has no alternate journal path. The manager canonicalizes the default output directory and acquires its `.vise/lock`, then imports any local legacy source. Additional saved destinations are locked before recovery. File preparation and lock acquisition run on the blocking pool. Only the global database receives runtime state writes.

Legacy SQLite files are opened read-only and copied through SQLite's backup API, including WAL commits, into an in-memory database. Path-schema conversion occurs there. One global transaction inserts task/chunk rows, initializes the rate only when unset, and records `legacy-output:<canonical-directory>`. A source marker prevents replay after deletion or an interrupted post-import JSON rename. UUID and destination conflicts abort the import without overwriting existing tasks. SQLite sources are preserved as backups; older binaries must remain stopped. Imports discover the selected output directory only, so visit each old directory once with `-o`. JSON import follows the same per-source marker policy.

Every recovered job uses immutable destination/staging paths copied from its SQL record. Checkpoints capture offsets, synchronize partial files and staging directories, and then commit global rows. Publication validates checksums, hard-links the file within its destination filesystem, and synchronizes the destination directory. Deletion and recovery also use saved paths, never the current process's default output directory. Missing destination directories retain saved offsets; no background startup scan relocates a task.

The API and interactive queue show the entire global history. The CLI separately tracks tasks scheduled by the current process for batch exit status, so old paused history does not make an unrelated successful batch fail. A running daemon owns the global engine; other processes can inspect SQLite via WAL readers or control the daemon through its authenticated API instead of starting a second engine.

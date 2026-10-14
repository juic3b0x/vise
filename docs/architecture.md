# Architecture and API

Vise uses one shared download manager for its CLI and WebUI.

## HTTP and WebSocket API


| Method | Endpoint | Result |
| --- | --- | --- |
| GET | `/api/downloads` | Array of all saved and active downloads |
| POST | `/api/downloads` | Add `{"url":"https://…","filename":"optional.zip"}`; HTTP 201 |
| GET | `/api/downloads/{uuid}` | One download |
| DELETE | `/api/downloads/{uuid}` | Stop the task and remove its queue entry/partial data; HTTP 204; completed outputs are kept |
| POST | `/api/downloads/{uuid}/pause` | Pause and wait for workers to stop |
| POST | `/api/downloads/{uuid}/resume` | Queue a paused/failed download |
| POST | `/api/downloads/{uuid}/retry` | Alias for resume |
| WS | `/ws` | Full JSON download snapshots on change and at least every 500 ms |

Mutation requests require `Content-Type: application/json`; send `{}` for pause/resume/retry/delete. URLs must be HTTP(S), without embedded credentials. Explicit filenames must be plain filenames, never paths. Requests are limited to 16 KiB.

```sh
curl -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/file.zip"}' \
  http://127.0.0.1:8080/api/downloads
curl http://127.0.0.1:8080/api/downloads
curl -X POST -H 'Content-Type: application/json' -d '{}' \
  http://127.0.0.1:8080/api/downloads/REPLACE_WITH_RETURNED_UUID/pause
```

Download snapshots contain `id`, `url`, `filename`, `status`, `total_bytes` (nullable), `downloaded_bytes`, `bytes_per_second`, `eta_seconds` (nullable), and `error` (nullable). States are `queued`, `probing`, `downloading`, `paused`, `completed`, and `failed`. Engine errors use an `error` JSON field and HTTP 400, 404, 409, or 500.

## Transfers and recovery


- A single shared manager serves both controllers. Tokio's multithreaded runtime runs segment workers, and a semaphore bounds simultaneous downloads. The maximum number of segment requests is `jobs × connections`. Segments are at least approximately 1 MiB, with up to `connections` segments per file. Buffers hold at most 256 KiB per segment, in addition to HTTP buffers.
- Vise probes actual range support with `GET Range: bytes=0-0`; it does not trust `Accept-Ranges` alone. It checks status codes, every `Content-Range`, exact lengths, and response validators. TLS verification stays enabled, redirects are bounded, and the client requests identity encoding so compressed representations cannot invalidate byte offsets.
- Parallel/resumed ranges require a strong ETag or Last-Modified validator. `If-Range` protects requests and retries. If the resource changes, resuming reprobes it and resets the partial layout. When ranges or validators are unavailable, Vise safely uses a single streaming request and restarts that stream after a pause or dropped connection. Unknown lengths are supported.
- Connection/read failures, HTTP 408/429/5xx, and truncated responses retry with bounded exponential backoff. Numeric `Retry-After` is honored up to 60 seconds. Other HTTP errors, invalid ranges, and disk errors fail with a readable error. A stalled read has a configurable timeout; large downloads have no total-duration limit.
- `<output>/.vise/state.json` records the queue, resource validators, chunk boundaries, and byte offsets. Partial files live in the same `.vise` directory. Once a second, offsets are captured, their underlying files are synchronized, and the journal is atomically replaced. Pause and shutdown also checkpoint. On Unix, directory entries are synchronized. A crash can lose work since the last checkpoint; unrecorded bytes are downloaded again.
- Downloads publish only after every segment finishes and the file is synchronized. A hard link creates the final filename atomically without overwriting existing files. An interrupted publish is recovered by checking whether the final and partial paths identify the same file. This requires an output filesystem that supports hard links and atomic renames; unsupported operations report an error. Windows syncs individual files but lacks the Unix directory-sync durability guarantee.
- Automatically derived names receive numeric suffixes on collision. Explicit name collisions fail. A process-wide output-directory lock prevents two Vise instances from corrupting the same journal. Corrupt or unsupported journals are rejected rather than silently discarded.

Local journal and partial files are trusted application data; do not modify them during a transfer. HTTP validators establish representation identity, not cryptographic integrity. Vise checks byte counts but does not provide checksum verification or segmented downloads from multiple mirrors.

## Project layout


```text
Cargo.toml / Cargo.lock    Dependencies and reproducible resolution
src/main.rs               Arguments, lifecycle, signals, mode selection
src/lib.rs                Public library modules
src/engine.rs             Worker pool, transfers, retries, persistence
src/model.rs              Internal records and public snapshots
src/events.rs             Timestamped lifecycle and server events
src/telemetry.rs          WebUI-only stdout event log and periodic summaries
src/error.rs              Typed errors and retry classification
src/cli.rs                Interactive commands and progress rendering
src/web.rs                REST, WebSocket, and local request guards
src/dashboard.html        Embedded dashboard
tests/downloads.rs        Local HTTP fixtures and integration tests
```


## WebUI terminal telemetry

The executable starts `telemetry::run` only when `--webui` is present, subscribing before initial URLs or saved tasks are queued. CLI progress bars and interactive commands retain their existing behavior.

The manager has two independent channels: the existing `watch` channel coalesces frequent progress notifications for controllers, while a bounded Tokio `broadcast` channel preserves discrete lifecycle events for telemetry subscribers. Events carry their creation timestamp and an owned download snapshot. Download actions and worker completions publish through the same manager; handlers do not print from arbitrary tasks.

One asynchronous writer owns monitor stdout. It flushes each timestamped event immediately and reports queued, probing, and downloading tasks once per second, including state, percentage, bytes, smoothed speed, and ETA. UTC timestamps include milliseconds. ANSI colors are enabled for terminal stdout unless `NO_COLOR` is set or `TERM=dumb`; files and pipes receive plain text. User-controlled text is escaped to prevent terminal control sequences or forged log lines.

No application locks are held during stdout writes. The event channel holds 2,048 events; a slow subscriber receives an explicit missed-event warning and a fresh active-status snapshot. This bounds memory without applying terminal backpressure to downloads. The logger remains alive while workers pause and checkpoint during shutdown, then drains final events before exiting. Logger or persistence-monitor failures stop the controller and trigger transfer shutdown.

Deletion serializes with task controls and persistence: workers stop before the queue entry is durably removed. A removed-job flag prevents a concurrent resume from resurrecting a deleted task. Partial-file cleanup failures emit a warning; completed output files are always preserved.

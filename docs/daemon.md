# Daemon and API

The daemon runs Vise's engine, REST API, WebSocket streams, and embedded dashboard without an interactive terminal. It stays in the foreground so systemd, Docker, or another service manager can supervise it.

```sh
vise --daemon --resume-all
```

The initial listener is `127.0.0.1:8080`. Use `-d` as the short form, or override the address explicitly:

```sh
vise -d --bind 127.0.0.1:9090 --resume-all
```

Stop any other Vise engine before starting the daemon. An empty queue does not stop the service. Graceful shutdown waits for workers and durable checkpoints.

## Native terminal control

Once the daemon is running, use the [remote CLI](remote.md) from another terminal:

```sh
vise remote list
vise remote add --wait https://www.rust-lang.org/static/images/rust-logo-blk.svg
vise remote watch
```

This client reads the existing daemon configuration, authenticates through the API, and never opens a second engine or SQLite connection. `--endpoint` overrides the address; `VISE_API_TOKEN` overrides the token. A remote origin must use HTTPS, or be reached through a loopback tunnel. Closing the client leaves daemon transfers running.

## Configuration and precedence

First startup creates `~/.vise/config.toml`, including a random persistent 64-character `VISE_API_TOKEN`. Edit that generated file instead of replacing it with a sample token. Existing files are preserved; malformed or unknown fields fail with an error.

| TOML key | Initial value / behavior |
| --- | --- |
| `daemon.host` | `"127.0.0.1"`; numeric IPv4 or IPv6 address |
| `daemon.port` | `8080` |
| `daemon.VISE_API_TOKEN` | Random token created once; keep it private |
| `downloads.default_dir` | `"~/Downloads/Vise"` |
| `downloads.max_concurrent` | `5`; range 1–64 |
| `downloads.global_speed_limit_kbps` | `0` restores the journal's rate; positive values use 1024 bytes per unit |
| `downloads.cookies_file` | Unset; optional Netscape cookie-file path for new tasks |
| `proxy.url` | `""`; no explicit default proxy |

CLI options override file settings. `--bind` replaces the full socket address; alternatively, `--host` and `--port` override individual fields. `--bind` cannot be combined with those two options. `VISE_API_TOKEN` in the environment overrides the configured token. The older `auth_token` key is an alias; configure only one token key.

For example, this starts the daemon with explicit queue and destination overrides while retaining the generated token:

```sh
vise --daemon --host 127.0.0.1 --port 8080 -j 5 -o ./downloads
```

Live rate changes persist in SQLite and do not rewrite `config.toml`. With the initial zero file limit, startup restores that saved cap; `--limit-rate off` explicitly clears it. New output, cookie, and proxy defaults apply to new tasks; saved tasks retain their destinations and authentication sources.

## Authentication and remote access

Daemon requests use either:

- `Authorization: Bearer` with the token, for native clients and the extension.
- HTTP Basic with username `vise` and the token as password, for the dashboard's browser login.

Authentication protects dashboard assets, REST routes, and WebSocket upgrades. If a custom token is required, generate it cryptographically rather than choosing a memorable password. The accepted format is 32–256 ASCII letters, digits, hyphens, or underscores. An empty environment override is invalid. The [security policy](https://github.com/juic3b0x/vise/blob/main/SECURITY.md) explains token handling and credential redaction.

!!! warning "Use encrypted transport for remote control"
    The built-in server speaks HTTP. A bearer token does not encrypt traffic. Keep the listener on loopback and use SSH forwarding, or place an authenticated non-loopback listener behind a correctly configured HTTPS reverse proxy. Preserve the public Host, same-origin policy, and WebSocket upgrades. Never disable certificate validation on clients.

Non-loopback daemon binds require a valid token. A loopback daemon with no configured token can run unauthenticated, but that configuration is unsuitable for the browser extension. `--webui` is a separate, loopback-only, unauthenticated local-controller mode; see the [WebUI guide](webui.md).

The server validates Host and browser Origin headers. Ordinary websites do not receive permissive CORS access. Authenticated extension origins are accepted only with an explicit bearer header. WebSocket clients must authenticate during the upgrade; the embedded dashboard uses the browser's Basic authentication cache. Do not put tokens in query strings.

## Systemd user service

The supplied [vise.service](vise.service) runs `~/.cargo/bin/vise --daemon --resume-all`, restarts on failure, and sends SIGTERM for shutdown. It uses a private umask and loads an optional environment file from `~/.config/vise/daemon.env`.

After [installing with Cargo](installation.md#install-with-cargo), run these commands from the repository checkout:

```sh
install -D -m 644 docs/vise.service ~/.config/systemd/user/vise.service
systemctl --user daemon-reload
systemctl --user enable --now vise.service
systemctl --user status vise.service
```

If Cargo installed Vise elsewhere, edit the unit's `ExecStart` to use that absolute executable path. The service uses the account's home directory for configuration and global state; it does not create a separate journal in its working directory.

Inspect logs or apply a configuration change:

```sh
journalctl --user -u vise.service -f
systemctl --user restart vise.service
```

Quiet daemon mode omits the WebUI terminal event stream, so an idle healthy service may produce no log lines. Inspect its status and queue through the API/dashboard. A system administrator must enable user lingering if the user service must continue after logout. Protect an optional `daemon.env` file with mode `0600`; its `VISE_API_TOKEN` value overrides the TOML token. Avoid keeping divergent tokens in multiple locations.

To hand control back to a terminal session:

```sh
systemctl --user stop vise.service
vise --interactive
```

## REST API

All URLs below use the initial loopback listener. API task controls require the full download UUID returned by queue admission or listing. Mutation requests require `Content-Type: application/json`, including DELETE. JSON request bodies are limited to 16 KiB.

| Method | Path | Result |
| --- | --- | --- |
| GET | `/api/downloads` | All saved and active download snapshots |
| POST | `/api/downloads` | Queue a task; HTTP 201 and its snapshot |
| GET | `/api/downloads/{id}` | One task |
| DELETE | `/api/downloads/{id}` | Stop/remove task and staging data; HTTP 204; completed output is kept |
| POST | `/api/downloads/{id}/pause` | Stop workers and durably pause |
| POST | `/api/downloads/{id}/resume` | Queue a paused or failed task |
| POST | `/api/downloads/{id}/retry` | Retry; reset every chunk for corrupt tasks |
| GET | `/api/config` | Current global bandwidth configuration |
| POST | `/api/config` | Set and persist a new global cap |
| GET | `/api/telemetry` | Timestamped downloads plus configuration |

### Queue and control a task

The following POSIX-shell session uses curl's interactive Basic-auth password prompt to avoid placing the token in a command argument. Enter the generated token at each prompt. Python 3 extracts the actual returned ID; no hard-coded task ID is required.

```sh
VISE_API_URL=http://127.0.0.1:8080
VISE_TASK_ID=$(
  curl --fail-with-body --silent --show-error --user vise \
    -H 'Content-Type: application/json' \
    -d '{"url":"https://www.rust-lang.org/static/images/rust-logo-blk.svg","filename":"rust-logo.svg"}' \
    "$VISE_API_URL/api/downloads" |
    python3 -c 'import json, sys; print(json.load(sys.stdin)["id"])'
)
curl --fail-with-body --user vise "$VISE_API_URL/api/downloads/$VISE_TASK_ID"
```

For a task that is still active, pause it and then resume it:

```sh
curl --fail-with-body --user vise -H 'Content-Type: application/json' \
  -d '{}' "$VISE_API_URL/api/downloads/$VISE_TASK_ID/pause"
curl --fail-with-body --user vise -H 'Content-Type: application/json' \
  -d '{}' "$VISE_API_URL/api/downloads/$VISE_TASK_ID/resume"
```

A small file may finish before pause reaches the server; a conflicting state returns an explicit error. Remove its queue entry when no longer needed:

```sh
curl --fail-with-body --user vise -X DELETE \
  -H 'Content-Type: application/json' -d '{}' \
  "$VISE_API_URL/api/downloads/$VISE_TASK_ID"
```

Deletion preserves an already published final file. It removes unfinished staging data, so use pause when you intend to resume later.

### Admission fields and snapshots

| JSON field | Requirement |
| --- | --- |
| `url` | Required HTTP(S) URL without embedded credentials |
| `filename` | Optional plain filename; paths are rejected |
| `expected_sha256` | Optional 64-digit hexadecimal digest |
| `expected_md5` | Optional 32-digit hexadecimal digest |
| `proxy_url` | Optional supported proxy URL; omitted/null inherits the process default |

Unknown fields are rejected. The API has no cookie-file upload field: new tasks inherit the daemon's configured `downloads.cookies_file` or `--cookies` default. See [cookie sessions](architecture.md#cookie-sessions).

Snapshots include `id`, `url`, `filename`, absolute `output_path` and `part_path`, `status`, `downloaded_bytes`, nullable `total_bytes`, `bytes_per_second`, nullable `eta_seconds`, nullable `error`, checksum fields, and `chunks`. Each chunk has an index, start offset, exclusive end offset (nullable for unknown length), downloaded bytes, and state. Proxy credentials and cookie paths/values are excluded from public snapshots; exposed proxy routes are redacted. Timestamps use UTC Unix epoch milliseconds.

Download states are `queued`, `probing`, `downloading`, `verifying`, `paused`, `completed`, `failed`, and `corrupt`. A complete byte count is not proof of successful verification/publication. Treat `completed` as the final success state.

### Bandwidth configuration

```sh
curl --fail-with-body --user vise -H 'Content-Type: application/json' \
  -d '{"limit_rate":"5M"}' http://127.0.0.1:8080/api/config
curl --fail-with-body --user vise http://127.0.0.1:8080/api/config
```

The response for that cap is:

```json
{"limit_rate":"5 MiB/s","limit_bytes_per_second":5242880}
```

Send `{"limit_rate":"off"}` to clear it. Unlimited is returned as `{"limit_rate":"unlimited","limit_bytes_per_second":null}`. Invalid rates or failed journal commits preserve the previous live setting. See [rate units](usage.md#bandwidth-control).

### Errors

Engine errors return an `error` JSON field with HTTP 400 for invalid input, 404 for a missing task, 409 for a state conflict, or 500 for an internal failure. Authentication/Origin guards can return 401/403; invalid mutation media types return 415, and oversized bodies return 413. Malformed JSON and invalid UUID path extraction use Axum's rejection responses. Clients must check HTTP status before decoding a successful snapshot.

## WebSocket telemetry

| Route | Message format |
| --- | --- |
| `/ws` | JSON array of download snapshots |
| `/ws/telemetry` | Object with `timestamp`, `downloads`, and `configuration` |

Connections receive an initial snapshot, then updates on state changes and at least every 500 ms. The dashboard uses `/ws/telemetry` and falls back to HTTP telemetry polling when needed. Streams carry snapshots rather than a durable event history; use REST mutations for pause/resume/retry/delete.

Native WebSocket clients can supply the bearer header during upgrade. Browser WebSocket APIs cannot set arbitrary authorization headers, which is why the bundled dashboard uses the authenticated same-origin browser session. Preserve upgrades and avoid proxy buffering when operating behind HTTPS.

Continue with [WebUI operation](webui.md), [extension setup](extension.md), or the [engine architecture](architecture.md).

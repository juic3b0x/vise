<p align="center">
  <img src="docs/logo.png" alt="Vise logo: a download arrow held by vise jaws" width="180">
</p>

# Vise

**A firm grip on every download.**

Vise is a high-performance HTTP(S) download manager built in Rust. Multi-threaded chunked downloads run on Tokio's async runtime, while Indicatif keeps the CLI clear with live progress, speed, and ETA. Prefer a dashboard? Launch the lightweight, embedded Axum WebUI with `--webui`.

## Features

- **Parallel transfers:** Split eligible files into chunks and download them concurrently, with configurable connection and queue limits.
- **Clear progress:** Track downloaded bytes, percentage, transfer speed, and ETA in the terminal.
- **SQLite crash recovery:** Control individual tasks and restore saved chunk offsets from an embedded transactional journal after restarting.
- **Resilient downloads:** Retry dropped connections and transient HTTP failures with bounded backoff.
- **Authenticated downloads:** Import Netscape `cookies.txt` files into isolated per-task cookie jars, with saved file paths for resume.
- **Live bandwidth control:** Share one token-bucket speed limit across all downloads and change it from the interactive CLI or WebUI without reconnecting.
- **CLI first:** Download from arguments or manage an interactive queue. The web server starts only with `--webui` or `--daemon`.
- **Quiet daemon:** Run the same engine and API under a service manager, with authenticated remote control and no terminal loop.
- **Daemon-aware CLI:** Use `vise remote` to queue and control tasks, monitor live progress, emit JSON, or wait for verified publication without starting a second engine.
- **Embedded WebUI:** A clean dark dashboard with live speed graphs, actual chunk-progress grids, queue filters, and controls that update without reloading.
- **Live terminal monitor:** In WebUI mode, follow timestamped events and one-second transfer summaries on stdout while the browser controls downloads.
- **Careful file handling:** Validate range responses, save durable checkpoints, and refuse to overwrite existing output files.
- **Checksum verification:** Verify SHA-256 and MD5 before publishing a file; preserve corrupt downloads for inspection or an explicit retry.
- **Per-transfer proxies:** HTTP/HTTPS proxies and SOCKS5 tunnels, including authentication and saved routes for resumed downloads.
- **Browser integration:** A lightweight Chrome/Firefox extension adds **Send to Vise** to HTTP(S) link menus. See [extension setup](INSTALL.md#browser-extension).

## Installation

See [INSTALL.md](INSTALL.md) for system requirements, Cargo installation, verification, and systemd service setup.

## Documentation

Browse the [documentation site](https://juic3b0x.github.io/vise/) for installation, CLI usage, daemon/API configuration, WebUI controls, browser extension setup, and engine architecture. The [source guides](docs/index.md) are also readable in this repository.

## Control a running daemon

Keep `vise --daemon --resume-all` running in a service or another terminal, then use:

```sh
vise remote add https://www.rust-lang.org/static/images/rust-logo-blk.svg
vise remote list --json
vise remote limit-rate 5M
vise remote watch
```

Add `--wait` to `remote add` to return success only after verification and publication. The client reads the existing daemon address/token from configuration; `--endpoint` and `VISE_API_TOKEN` supply overrides. It never opens the local journal or silently starts an engine. Direct `vise URL` usage stays intact. See the [remote CLI guide](docs/remote.md) for authentication, task controls, and exit codes.

## Persistent configuration

On the first normal startup, Vise creates `~/.vise/config.toml`. On Unix it resolves `~` from `HOME`; on Windows it uses `USERPROFILE`, falling back to `HOMEDRIVE` plus `HOMEPATH`. `--help` and `--version` exit without creating files. Edit the file and restart Vise to apply preferences.

| Setting | Initial value / meaning |
| --- | --- |
| `daemon.host` | `"127.0.0.1"`; numeric IPv4 or IPv6 |
| `daemon.port` | `8080` |
| `daemon.VISE_API_TOKEN` | Random 64-character token generated once and preserved |
| `downloads.default_dir` | `"~/Downloads/Vise"`; `~` expands to the resolved home |
| `downloads.max_concurrent` | `5`; accepted range 1–64 |
| `downloads.global_speed_limit_kbps` | `0` restores the saved cap (initially unlimited); positive values are KiB/s |
| `proxy.url` | `""` for no explicit proxy |
| `downloads.cookies_file` | Unset; optional Netscape cookie-file path |
| `downloads.user_agent` | Unset; optional user agent for new tasks |

Explicit CLI options take precedence, even if their values match a previous default. `--bind` overrides the complete address; alternatively use `--host` and/or `--port` to override individual file fields. These address options require `--daemon` or `--webui`; `--bind` is exclusive with `--host`/`--port`.

```sh
vise --daemon --host 127.0.0.1 --port 9090
vise -o ./downloads -j 3 --limit-rate off --proxy "" https://example.com/archive.zip
```

The environment variable `VISE_API_TOKEN` overrides `daemon.VISE_API_TOKEN`. The original `auth_token` spelling is accepted as an alias; define only one spelling in a file. A missing/empty token disables authentication only on loopback; non-loopback binds still require a valid token. New files contain a generated token, so daemon authentication is enabled by default. The local `--webui` mode retains its existing loopback-only, unauthenticated behavior.

Creation runs on the blocking pool, serializes concurrent startup, and publishes a complete file atomically. Existing files and comments are preserved. Newly created Unix directories use mode `0700` and files use `0600`; on Windows, protect the home directory with its account ACLs. Malformed TOML, unknown keys, or incorrect field types produce an error without echoing credential values. Missing sections/fields use defaults; an omitted token in an existing file is left empty rather than generating a new credential on every restart.

These are startup preferences. Transfer state, chunk offsets, and live settings still belong to SQLite. On CLI startup, an explicit `--limit-rate` or a positive file bandwidth cap overrides the saved global cap; the initial zero file default restores it. Runtime controls persist in the journal without rewriting `config.toml`; edit the file to choose the cap for future starts.

## Quick start

### Download one file

```sh
vise https://example.com/archive.zip
```

### Download a batch

```sh
vise --batch https://example.com/a.zip https://example.com/b.zip
vise --input urls.txt -j 3 -o ./downloads
vise remote add --input urls.txt --wait --json
```

URL lists are UTF-8 text, one HTTP(S) URL per line. Whitespace, blank lines, and `#` comments are handled automatically. Lists may contain up to 1,000 URLs and 1 MiB of text; positional URLs are queued before file entries. Names, hashes, proxies, cookies, and destinations follow the normal [CLI rules](docs/usage.md#input-files).

URL arguments run until the queue settles. `--batch` explicitly disables terminal input.

### Set the maximum connections

```sh
vise -c 8 https://example.com/large.iso
```

`-c` sets the maximum parallel connections **per download**. Use `-j` to limit simultaneous downloads:

```sh
vise -j 3 -c 8 https://example.com/a.zip https://example.com/b.zip
```

### Choose the output directory and filename

```sh
vise -o ./downloads https://example.com/archive.zip
vise -o ./downloads -n release.zip https://example.com/archive.zip
```

`-o` overrides the configured output directory, initially `~/Downloads/Vise`. `-n` sets a filename for exactly one URL.

### Limit bandwidth

```sh
vise --limit-rate 5M https://example.com/large.iso
vise --interactive --limit-rate 500K
vise --webui --limit-rate 5M
```

The limit is **global**: every active file and chunk shares the same byte budget. `K`, `M`, `G`, and `T` use powers of 1,024; `KB/s`, `MB/s`, `GB/s`, and `TB/s` use powers of 1,000. Explicit binary units such as `KiB/s` and `MiB/s` also work. Fractions are accepted (`1.5M`); fractional bytes are rounded down. Use `off`, `unlimited`, or `0` to remove the cap.

Live limits are saved in `~/.vise/state.sqlite3` and restored across output directories and restarts. An explicit `--limit-rate` or a nonzero `downloads.global_speed_limit_kbps` overrides the saved cap. The initial zero configuration default leaves a saved cap intact; use `--limit-rate off` to clear it. A new journal starts unlimited.

The cap applies to streamed response body bytes, including retries. HTTP headers and range probes are excluded. A bounded token bucket permits short bursts, and network/client buffering can briefly receive data ahead of the stream; the limit controls sustained application throughput.

## Proxy routing

Route downloads through an HTTP, HTTPS, or SOCKS5 proxy:

```sh
vise --proxy socks5://127.0.0.1:1080 https://example.com/archive.zip
vise --proxy socks5h://127.0.0.1:1080 https://example.com/archive.zip
vise --proxy http://proxy.example:8080 --batch https://example.com/a.zip https://example.com/b.zip
vise --proxy https://proxy.example:8443 https://example.com/archive.zip
vise --webui --proxy socks5h://127.0.0.1:1080
```

`socks5://` resolves destination hostnames locally; `socks5h://` delegates them to the proxy. HTTP proxies forward HTTP requests and use CONNECT tunnels for HTTPS destinations. An `https://` proxy URL also encrypts the connection to the proxy. TLS certificate verification remains enabled for both proxy and destination.

`--proxy` sets the default for **new** downloads in that process, including interactive additions and dashboard/API submissions. API requests can override it with a per-transfer `proxy_url`:

```sh
curl -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/archive.zip","proxy_url":"socks5h://127.0.0.1:1080"}' \
  http://127.0.0.1:8080/api/downloads
```

Omitting `proxy_url` or passing null inherits the CLI default. Each transfer saves its effective route in SQLite and reuses it for probes, chunks, retries, and resume. A later `--proxy` value does not replace routes for saved tasks. Failed explicit proxies never fall back to direct connections. With no explicit route, Reqwest's existing environment-proxy behavior is preserved; an explicit route overrides environment proxy and `NO_PROXY` settings. See the [Reqwest proxy guide](https://docs.rs/reqwest/0.12.28/reqwest/#proxies).

Authentication can be included as `scheme://username:password@host:port`; percent-encode reserved characters in credentials. API/telemetry responses expose a credential-free `proxy_url` and `proxy_authenticated` flag. The complete route, including credentials, is stored in the local journal so authenticated downloads can resume. Protect the output directory and journal as local credential-bearing data.

### Cookie-authenticated downloads

```sh
vise --cookies ./cookies.txt https://example.com/private/archive.zip
vise --daemon --cookies ./cookies.txt --resume-all
```

`--cookies FILE` imports a Netscape-format cookie file for new tasks, including additions through the interactive queue, WebUI, and daemon API. Alternatively, set `cookies_file = "~/cookies.txt"` under `[downloads]` in `~/.vise/config.toml`; the CLI flag overrides it. Domain/host-only scope, paths, secure flags, expiration, and `#HttpOnly_` records are preserved. Empty lines and comments are ignored; expired cookies are skipped. Invalid records report the file and line number without echoing their contents.

Each task has its own Reqwest cookie jar, shared by that task's probe, chunks, retries, and redirects. Response cookies update that jar during the run. SQLite stores the absolute cookie-file path, **not cookies imported from that file**. Explicitly supplied API cookies are private journal credentials, as described below. Resume reloads the saved file, so keep it available and replace its contents when the login session needs refreshing. A new process default does not replace a saved task's file. Missing or malformed saved files fail only their task, without sending an unauthenticated request or discarding saved progress. Cookies learned from responses are kept in memory and are not written back to the file; after a restart the file remains the source of authentication.

Cookie files are plaintext credentials: keep them private and outside source control. Cookie paths and values are omitted from public download snapshots. The browser extension still forwards only the selected URL; it does not export browser cookies.

## Per-task request credentials

```sh
vise --header "Authorization: Bearer $DOWNLOAD_TOKEN" \
  --referer https://downloads.example.com/releases \
  --user-agent 'MyDownloader/1.0' https://downloads.example.com/archive.zip

vise remote add --header "Authorization: Bearer $DOWNLOAD_TOKEN" \
  --input urls.txt --wait
```

Repeat `--header 'Name: value'` for additional headers. Local CLI/interactive additions inherit startup settings; remote flags apply to every URL in that invocation. `--daemon` and `--webui` reject process-wide `--header` and `--referer`: send these per task through the API instead. `--user-agent` and optional `[downloads].user_agent` remain allowed in server modes; explicit CLI settings take precedence.

Credentialed batches must share one origin (scheme, normalized host, effective port); split mixed-origin lists before submission. User agent alone is exempt. Raw `Cookie`/`Set-Cookie` headers are rejected: use `--cookies FILE` or the API's structured `cookies` records.

New request settings survive restart in the **plaintext private SQLite journal**; Unix credential writes require mode `0600`. Protect the home directory with account ACLs on Windows. Recovered tasks never inherit later defaults. Public snapshots expose header names/counts and presence flags only. Cross-origin redirects strip credentials and custom user agents, reduce a supplied referer to its origin, and never restore credentials later in the chain. Old tasks without these settings retain their original redirect behavior.

The dashboard and extension are unchanged in this backend update. See [usage](docs/usage.md#request-headers-and-referer), [API details](docs/daemon.md#request-credentials), and the [security policy](SECURITY.md#per-task-request-credentials).

## Checksum verification

Supply a publisher's expected digest for one URL. SHA-256 requires 64 hexadecimal digits; MD5 requires 32. Uppercase and lowercase are accepted. Set `SHA256` and `MD5` to the publisher's values before running these examples:

```sh
vise --expected-sha256 "$SHA256" https://example.com/archive.zip
vise --expected-md5 "$MD5" https://example.com/archive.zip
vise --expected-sha256 "$SHA256" --expected-md5 "$MD5" https://example.com/archive.zip
```

Both must match when both are supplied. The flags require exactly one URL and also work with `--webui` or `--interactive` when seeding a download. In the dashboard, expand **Checksum verification** under **New transfer** and enter either or both expected hashes.

At 100% received, Vise enters `verifying`, synchronizes the staging file, and hashes it in one bounded-memory pass on the blocking worker pool. It publishes the final filename only after all expected checksums match. Expected/actual digests and the verification timestamp are saved in SQLite and shown in the dashboard.

A mismatch becomes `corrupt`: the final filename is not published, the staging file is retained under `<output>/.vise/<task-id>.part`, and the error includes expected and actual values. Corrupt tasks remain corrupt across restarts and are excluded from `--resume-all`. Use interactive `retry ID`, the WebUI's **Re-download** button, or POST `/api/downloads/{id}/retry` to reset and download every chunk again. A whole-file checksum cannot identify which segment is damaged. If the expected digest itself was wrong, add a new task with the corrected value.

Vise can also learn checksums from HTTP metadata: `Repr-Digest` for SHA-256/MD5 on representation responses, and `Content-Digest` or `Content-MD5` on complete HTTP 200 bodies. Message-content digests on range responses are ignored because they cover only that fragment. Explicit values take precedence per algorithm; learned values are saved and pinned for later resumes. Trailer-only digests and the obsolete `Digest` field are not supported. See [HTTP digest semantics](https://www.rfc-editor.org/rfc/rfc9530.html).

Use SHA-256 from a trusted publisher for security-sensitive verification. MD5 is supported for legacy compatibility and accidental corruption detection; it is not collision-resistant. A digest supplied by the download server does not independently authenticate that server. See the [RustCrypto MD5 documentation](https://docs.rs/md-5/0.10.6/md5/).

## Interactive mode

Run `vise` without arguments in a terminal to open the interactive queue:

```sh
vise
```

To queue initial URLs and keep the command loop open:

```sh
vise --interactive https://example.com/large.iso
```

Inside the queue, paste a URL or use these commands:

```text
add https://example.com/archive.zip
add https://example.com/archive.zip custom-name.zip
list
pause 9ac2
resume 9ac2
retry 9ac2
limit-rate 2M
limit-rate
limit-rate off
help
quit
```

Task IDs accept a unique prefix of at least four characters. Use `list` to find them. `retry` resumes a failed task after checking the remote resource again, or downloads all chunks again for a corrupt task.

`limit-rate RATE` changes the shared cap immediately while transfers remain connected. `limit-rate` displays the current cap; `limit-rate off` removes it.

`quit`, Ctrl+C, and SIGTERM stop active transfers and save partial progress. Resume saved paused or failed tasks from every destination:

```sh
vise --resume-all
```

Download state is stored globally in `~/.vise/state.sqlite3`, alongside `config.toml`. SQLite WAL transactions preserve task history, individual chunk offsets, status transitions, and the shared rate limit. Progress checkpoints run once a second and lifecycle transitions commit immediately. Unfinished tasks recover as paused; a crash can lose work since the last checkpoint, which is downloaded again.

Every task stores absolute final and staging paths. `-o` selects the destination for **new** tasks; it does not relocate existing downloads. Staging stays at `<destination-dir>/.vise/<task-id>.part` on the same filesystem as the final file. The CLI, interactive queue, daemon, WebUI, and `--resume-all` see the same history, regardless of the current output directory. Batch exit status reflects tasks scheduled by that invocation rather than unrelated paused history.

One Vise engine may run per home directory. Stop a running daemon before starting a separate CLI engine, or control it through its API/WebUI. The global process lock protects the engine's live cache and workers; WAL still allows concurrent SQLite readers. New Unix state directories use `0700`, and new database/lock files use `0600`.

On first use of an old output directory, Vise imports its `<output>/.vise/state.sqlite3` automatically. Select each old directory once with `vise -o /path/to/old/downloads --interactive`; then resume its tasks from any mode. Imports preserve absolute file placement, offsets, timestamps, checksums, proxies, and cookie-file paths. SQLite sources and their WAL files are read without modification; JSON journals are backed up as `state.json.migrated`. Source markers commit with the imported tasks to prevent duplicate imports or resurrection after deletion. Conflicting task IDs or destinations fail without replacing existing history. The first imported rate fills an unset global rate; subsequent imports do not overwrite it.

Keep `~/.vise/` and destination staging files available for recovery. Stop Vise cleanly before copying its SQLite database, or use a SQLite-aware backup that includes committed WAL data. Old output journals remain backups; older Vise versions do not share the new global state.

Parallel range downloads require server range support and a strong ETag or Last-Modified validator. If either is unavailable, Vise uses a single streaming request and restarts that transfer when resumed. Unknown-length streams are supported.

## Daemon / service mode

Run the engine, REST/WebSocket API, and embedded dashboard without terminal input or progress output:

```sh
vise -d
vise --daemon --bind 127.0.0.1:8080 -o ./downloads --resume-all
```

Daemon mode stays in the foreground for systemd, Docker, or another process supervisor; it does not fork or detach itself. It keeps serving when the queue is empty. SIGTERM or Ctrl+C stops workers and commits recoverable progress. It is exclusive with `--webui`, `--interactive`, and `--batch`. Startup errors and warnings still go to stderr.

For a LAN or wildcard bind, use the generated `daemon.VISE_API_TOKEN` from your configuration file or override it through the environment. Tokens must contain 32–256 ASCII letters, digits, `-`, or `_`:

```sh
export VISE_API_TOKEN="$(openssl rand -hex 32)"
vise --daemon --bind 0.0.0.0:8080 -o ./downloads --resume-all
```

`--bind` accepts numeric IPv4 and IPv6 socket addresses, including `[::]:8080`, and overrides the configured host/port. The initial default is `127.0.0.1:8080`. Non-loopback binds fail before opening the journal unless a valid file or environment token is present. Protect remote connections with an HTTPS reverse proxy or an authenticated SSH tunnel: the embedded listener itself uses HTTP, so authentication alone does not encrypt credentials or metadata.

API clients send `Authorization: Bearer`; all REST endpoints and both WebSocket endpoints use the same token:

```sh
curl -H "Authorization: Bearer $VISE_API_TOKEN" \
  http://127.0.0.1:8080/api/downloads
curl -H "Authorization: Bearer $VISE_API_TOKEN" -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/archive.zip"}' \
  http://127.0.0.1:8080/api/downloads
```

Open the daemon's address in a browser for the dashboard. In the browser's HTTP authentication prompt, enter **`vise`** as the username and your token as the password. Same-origin browser requests and WebSocket upgrades use those credentials. Native WebSocket clients may supply either Bearer authentication or HTTP Basic authentication. Tokens live in the protected TOML file or environment, and are not stored in SQLite.

For service setup, see [Systemd user service](INSTALL.md#systemd-user-service).

## WebUI

Launch the embedded dashboard:

```sh
vise --webui
```

Or set the loopback address explicitly:

```sh
vise --webui --bind 127.0.0.1:8080
```

Open **[http://127.0.0.1:8080](http://127.0.0.1:8080)** in your browser. Use the exact numeric loopback address printed by Vise.

The dashboard provides a rolling 60-second throughput graph, per-transfer sparklines, and live chunk grids showing active, completed, pending, paused, and failed segments. Filter the queue by state or search filenames and URLs. Paste one or more URLs or choose/drop a `.txt` list, then select **Add transfers**. Pause, resume, retry, or cancel tasks without reloading. Cancel discards partial data; Remove clears a completed queue entry while keeping its output file. The dashboard is served directly from the executable: no Node.js, frontend build, external fonts, CDN, or separate server is required.

Use the **Bandwidth** slider, presets, or **Global bandwidth limit** input to change the cap during a transfer. Enter values such as `500K`, `5M`, or `off`, then select **Apply** or press Enter. The logarithmic slider spans 16 KiB/s–1 GiB/s, with unlimited at its left edge, and applies on release. The text input accepts the full rate range, including values outside the slider's range. The current setting streams to every open dashboard and is saved in the journal. Applied changes also appear in the WebUI terminal monitor.

The WebSocket reconnects automatically; HTTP polling keeps the dashboard usable if the socket drops. Stale readings are marked, and the graphs leave gaps when telemetry is unavailable. Graph history and the recent activity feed belong to the current browser session; download state remains persistent.

You can also seed the queue or restore saved tasks at startup:

```sh
vise --webui https://example.com/large.iso
vise --webui -o ./downloads --resume-all
```

WebUI mode uses the same download engine and worker pool as the CLI, and replaces the interactive terminal loop with a live stdout monitor. It immediately logs actions, state changes, retries, failures, and completions with UTC timestamps, then reports active progress, speed, and ETA every second. Terminal output is colorized when stdout is a terminal; redirected output is plain text, and `NO_COLOR` disables color. The server accepts loopback addresses only and checks browser origins. Remote access can use an authenticated SSH tunnel.

## Showcase

### CLI

![Vise CLI Output](docs/cli-demo.png)

### WebUI

![Vise WebUI: live speed graphs, chunk progress, and bandwidth controls](docs/webui-industrial.jpg)


## Options at a glance

| Option | Purpose | Default |
| --- | --- | --- |
| `-c, --connections` | Maximum connections per download, 1–64 | `8` |
| `-j, --jobs` | Simultaneous downloads, 1–64 | Configured; initially `5` |
| `-o, --output` | Default destination for new tasks; history stays global | Configured; initially `~/Downloads/Vise` |
| `-n, --name` | Filename for one URL | Derived from URL |
| `--retries` | Retries per segment, 0–20 | `5` |
| `--timeout` | Idle read timeout in seconds | `30` |
| `--limit-rate` | Shared body-byte cap, e.g. `5M`, `500KB/s`, or `off` | Configured; initially unlimited |
| `--expected-sha256` | Expected SHA-256 for one URL, 64 hex digits | Learn from metadata if available |
| `--expected-md5` | Expected MD5 for one URL, 32 hex digits | Learn from metadata if available |
| `--proxy` | Default HTTP/HTTPS/SOCKS5/SOCKS5h route for new downloads; `""` clears it | Configured; initially no explicit route |
| `--input FILE` | Read a UTF-8 URL list before queue startup | Off |
| `--cookies FILE` | Netscape cookie file for new tasks; its path is journaled for resume | Configured; initially none |
| `--header NAME:VALUE` | Repeatable custom header; forbidden as a server-wide default | None |
| `--referer URL` | Referer; forbidden as a server-wide default | None |
| `--user-agent STRING` | User agent for new tasks | Configured; initially Vise/version |
| `-i, --interactive` | Keep the terminal queue open | Off with URL arguments |
| `--batch` | Never read terminal input | Off |
| `--resume-all` | Resume global paused/failed tasks at their saved paths | Off |
| `--webui` | Start the embedded dashboard | Off |
| `-d, --daemon` | Run the API/dashboard quietly for a service manager | Off |
| `--bind` | Override the complete server address | Configured; initially `127.0.0.1:8080` |
| `--host`, `--port` | Override individual server address fields | Configured |

Run `vise --help` for the full command reference. Set `RUST_LOG=vise=info` for diagnostic logs.

## Tech stack

| Crate | Role |
| --- | --- |
| [Tokio](https://docs.rs/tokio/) | Async execution on a multithreaded runtime |
| [Reqwest](https://docs.rs/reqwest/) | HTTP(S), streaming responses, and range requests |
| [Axum](https://docs.rs/axum/) | Optional HTTP server, REST API, and WebSockets |
| [Indicatif](https://docs.rs/indicatif/) | Terminal progress bars and transfer monitoring |
| [Clap](https://docs.rs/clap/) | Argument parsing and CLI help |
| [rusqlite](https://docs.rs/rusqlite/) | Embedded SQLite state journal with bundled SQLite |

See [architecture, recovery behavior, and API documentation](docs/architecture.md) for implementation details.

## Development

```sh
cargo build --release --locked
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
```

The test suite exercises parallel transfers, connection limits, shared bandwidth budgets, live rate changes without reconnecting, settings persistence, retries, pause/resume, abrupt process recovery, safe output handling, CLI behavior, and REST/WebSocket controls using local HTTP fixtures. Token-bucket timing is also checked with Tokio's virtual clock.

The documentation site uses MkDocs with the Material theme. To preview it locally, install the pinned tools in a virtual environment:

```sh
python3 -m venv /tmp/vise-docs-venv
/tmp/vise-docs-venv/bin/python -m pip install -r requirements-docs.txt
/tmp/vise-docs-venv/bin/python -m mkdocs build --strict
/tmp/vise-docs-venv/bin/python -m mkdocs serve
```

Open `http://127.0.0.1:8000/vise/`. Run the strict build before submitting documentation changes. See [documentation contribution and publishing instructions](CONTRIBUTING.md#documentation-site) for Windows commands and GitHub Pages setup.

## License

Vise is open source, dual-licensed under **MIT / Apache-2.0**. You may use it under either the [MIT license](LICENSE) or the [Apache License 2.0](LICENSE-APACHE), at your option.

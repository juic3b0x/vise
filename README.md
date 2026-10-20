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
- **Live bandwidth control:** Share one token-bucket speed limit across all downloads and change it from the interactive CLI or WebUI without reconnecting.
- **CLI first:** Download from arguments or manage an interactive queue. The web server starts only with `--webui` or `--daemon`.
- **Quiet daemon:** Run the same engine and API under a service manager, with authenticated remote control and no terminal loop.
- **Embedded WebUI:** An industrial dark console with live speed graphs, actual chunk-progress grids, queue filters, and controls that update without reloading.
- **Live terminal monitor:** In WebUI mode, follow timestamped events and one-second transfer summaries on stdout while the browser controls downloads.
- **Careful file handling:** Validate range responses, save durable checkpoints, and refuse to overwrite existing output files.
- **Checksum verification:** Verify SHA-256 and MD5 before publishing a file; preserve corrupt downloads for inspection or an explicit retry.
- **Per-transfer proxies:** HTTP/HTTPS proxies and SOCKS5 tunnels, including authentication and saved routes for resumed downloads.

## Installation

### Requirements

Install a Rust toolchain with Cargo. Vise declares Rust **1.85 or newer** as its minimum version. Git is also required to clone the repository. A working C build toolchain is needed for bundled SQLite; no separate SQLite installation or database server is required.

### From a cloned repository

```sh
git clone https://github.com/juic3b0x/vise.git
cd vise
cargo install --path .
```

### Directly from GitHub

```sh
cargo install --git https://github.com/juic3b0x/vise.git
```

For a reproducible build using the repository's `Cargo.lock`, append `--locked` to either installation command:

```sh
cargo install --path . --locked
cargo install --git https://github.com/juic3b0x/vise.git --locked
```

By default, Cargo places the executable in `~/.cargo/bin/` (`%USERPROFILE%\.cargo\bin\` on Windows). Ensure that directory is on your `PATH` to run `vise` instantly from any terminal. Custom Cargo installation roots change this location; see the [Cargo installation reference](https://doc.rust-lang.org/cargo/commands/cargo-install.html).

Confirm the installation:

```sh
vise --version
vise --help
```

Local source installation and Git-source installation have been verified in isolated installation directories. The Git check used a local repository; installation from the live GitHub repository has not yet been verified.

## Quick start

### Download one file

```sh
vise https://example.com/archive.zip
```

### Download a batch

```sh
vise --batch https://example.com/a.zip https://example.com/b.zip
```

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

`-o` selects an output directory, which defaults to `downloads`. `-n` sets a filename for exactly one URL.

### Limit bandwidth

```sh
vise --limit-rate 5M https://example.com/large.iso
vise --interactive --limit-rate 500K
vise --webui --limit-rate 5M
```

The limit is **global**: every active file and chunk shares the same byte budget. `K`, `M`, `G`, and `T` use powers of 1,024; `KB/s`, `MB/s`, `GB/s`, and `TB/s` use powers of 1,000. Explicit binary units such as `KiB/s` and `MiB/s` also work. Fractions are accepted (`1.5M`); fractional bytes are rounded down. Use `off`, `unlimited`, or `0` to remove the cap.

Limits are saved in the output directory's SQLite journal and restored on the next launch. An explicit `--limit-rate` overrides the saved setting; use `--limit-rate off` to clear it. New journals start unlimited.

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

`quit`, Ctrl+C, and SIGTERM stop active transfers and save partial progress. Resume saved paused or failed tasks using the same output directory:

```sh
vise -o ./downloads --resume-all
```

Download state is stored in `<output>/.vise/state.sqlite3`. Vise saves download metadata and individual chunk offsets in SQLite transactions, flushing progress at one-second intervals and saving lifecycle transitions immediately. Unfinished tasks are restored as paused on startup; use `--resume-all` or the interactive/WebUI resume controls to continue from durable offsets. A crash can lose work since the last checkpoint, which is downloaded again.

Existing `.vise/state.json` journals are imported automatically once and preserved as `state.json.migrated`. Keep the entire `.vise` directory with your downloads, including any SQLite `-wal` and `-shm` files, and use the same output directory when resuming.

Parallel range downloads require server range support and a strong ETag or Last-Modified validator. If either is unavailable, Vise uses a single streaming request and restarts that transfer when resumed. Unknown-length streams are supported.

## Daemon / service mode

Run the engine, REST/WebSocket API, and embedded dashboard without terminal input or progress output:

```sh
vise -d
vise --daemon --bind 127.0.0.1:8080 -o ./downloads --resume-all
```

Daemon mode stays in the foreground for systemd, Docker, or another process supervisor; it does not fork or detach itself. It keeps serving when the queue is empty. SIGTERM or Ctrl+C stops workers and commits recoverable progress. It is exclusive with `--webui`, `--interactive`, and `--batch`. Startup errors and warnings still go to stderr.

For a LAN or wildcard bind, set a secret token first. Tokens must contain 32–256 ASCII letters, digits, `-`, or `_`; generate a random value rather than a memorable password:

```sh
export VISE_API_TOKEN="$(openssl rand -hex 32)"
vise --daemon --bind 0.0.0.0:8080 -o ./downloads --resume-all
```

`--bind` accepts numeric IPv4 and IPv6 socket addresses, including `[::]:8080`. The default is `127.0.0.1:8080`. Non-loopback binds fail before opening the journal unless a valid `VISE_API_TOKEN` is present. Setting a token on a loopback daemon enables authentication there too. Protect remote connections with an HTTPS reverse proxy or an authenticated SSH tunnel: the embedded listener itself uses HTTP, so authentication alone does not encrypt credentials or metadata.

API clients send `Authorization: Bearer`; all REST endpoints and both WebSocket endpoints use the same token:

```sh
curl -H "Authorization: Bearer $VISE_API_TOKEN" \
  http://127.0.0.1:8080/api/downloads
curl -H "Authorization: Bearer $VISE_API_TOKEN" -H 'Content-Type: application/json' \
  -d '{"url":"https://example.com/archive.zip"}' \
  http://127.0.0.1:8080/api/downloads
```

Open the daemon's address in a browser for the dashboard. In the browser's HTTP authentication prompt, enter **`vise`** as the username and your token as the password. Same-origin browser requests and WebSocket upgrades use those credentials. Native WebSocket clients may supply either Bearer authentication or HTTP Basic authentication. Tokens are supplied through the environment and are not stored in SQLite; use the same protected environment configuration when restarting the service.

For a systemd user service, install Vise with Cargo, create a private environment file, then install the provided unit:

```sh
install -d -m 700 ~/.config/vise
(umask 077; printf 'VISE_API_TOKEN=%s\n' "$(openssl rand -hex 32)" > ~/.config/vise/daemon.env)
install -D -m 644 docs/vise.service ~/.config/systemd/user/vise.service
systemctl --user daemon-reload
systemctl --user enable --now vise.service
```

The example binds all IPv4 interfaces on port 8080 and saves downloads to `~/Downloads/vise`. Adjust its bind/output paths to your deployment. User services that must survive logout also need user lingering enabled by the system administrator. See [docs/vise.service](docs/vise.service).

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

The dashboard provides a rolling 60-second throughput graph, per-transfer sparklines, and live chunk grids showing active, completed, pending, paused, and failed segments. Filter the queue by state or search filenames and URLs. Add transfers and pause, resume, retry, or cancel them without reloading. Cancel discards partial data; Remove clears a completed queue entry while keeping its output file. The dashboard is served directly from the executable: no Node.js, frontend build, external fonts, CDN, or separate server is required.

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
| `-j, --jobs` | Simultaneous downloads, 1–64 | `3` |
| `-o, --output` | Output directory | `downloads` |
| `-n, --name` | Filename for one URL | Derived from URL |
| `--retries` | Retries per segment, 0–20 | `5` |
| `--timeout` | Idle read timeout in seconds | `30` |
| `--limit-rate` | Shared body-byte cap, e.g. `5M`, `500KB/s`, or `off` | Saved setting; otherwise unlimited |
| `--expected-sha256` | Expected SHA-256 for one URL, 64 hex digits | Learn from metadata if available |
| `--expected-md5` | Expected MD5 for one URL, 32 hex digits | Learn from metadata if available |
| `--proxy` | Default HTTP/HTTPS/SOCKS5/SOCKS5h route for new downloads | No explicit route |
| `-i, --interactive` | Keep the terminal queue open | Off with URL arguments |
| `--batch` | Never read terminal input | Off |
| `--resume-all` | Resume saved paused and failed tasks | Off |
| `--webui` | Start the embedded dashboard | Off |
| `-d, --daemon` | Run the API/dashboard quietly for a service manager | Off |
| `--bind` | Server address; remote binds require daemon mode and a token | `127.0.0.1:8080` |

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

## License

Vise is open source, dual-licensed under **MIT / Apache-2.0**. You may use it under either the [MIT license](LICENSE) or the [Apache License 2.0](LICENSE-APACHE), at your option.

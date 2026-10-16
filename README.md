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
- **CLI first:** Download from arguments or manage an interactive queue. The web server starts only with `--webui`.
- **Embedded WebUI:** An industrial dark console with live speed graphs, actual chunk-progress grids, queue filters, and controls that update without reloading.
- **Live terminal monitor:** In WebUI mode, follow timestamped events and one-second transfer summaries on stdout while the browser controls downloads.
- **Careful file handling:** Validate range responses, save durable checkpoints, and refuse to overwrite existing output files.

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

Task IDs accept a unique prefix of at least four characters. Use `list` to find them. `retry` resumes a failed task after checking the remote resource again.

`limit-rate RATE` changes the shared cap immediately while transfers remain connected. `limit-rate` displays the current cap; `limit-rate off` removes it.

`quit`, Ctrl+C, and SIGTERM stop active transfers and save partial progress. Resume saved paused or failed tasks using the same output directory:

```sh
vise -o ./downloads --resume-all
```

Download state is stored in `<output>/.vise/state.sqlite3`. Vise saves download metadata and individual chunk offsets in SQLite transactions, flushing progress at one-second intervals and saving lifecycle transitions immediately. Unfinished tasks are restored as paused on startup; use `--resume-all` or the interactive/WebUI resume controls to continue from durable offsets. A crash can lose work since the last checkpoint, which is downloaded again.

Existing `.vise/state.json` journals are imported automatically once and preserved as `state.json.migrated`. Keep the entire `.vise` directory with your downloads, including any SQLite `-wal` and `-shm` files, and use the same output directory when resuming.

Parallel range downloads require server range support and a strong ETag or Last-Modified validator. If either is unavailable, Vise uses a single streaming request and restarts that transfer when resumed. Unknown-length streams are supported.

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
| `-i, --interactive` | Keep the terminal queue open | Off with URL arguments |
| `--batch` | Never read terminal input | Off |
| `--resume-all` | Resume saved paused and failed tasks | Off |
| `--webui` | Start the embedded dashboard | Off |
| `--bind` | WebUI loopback address | `127.0.0.1:8080` |

Run `vise --help` for the full command reference. Set `RUST_LOG=vise=info` for diagnostic logs.

## Tech stack

| Crate | Role |
| --- | --- |
| [Tokio](https://docs.rs/tokio/) | Async execution on a multithreaded runtime |
| [Reqwest](https://docs.rs/reqwest/) | HTTP(S), streaming responses, and range requests |
| [Axum](https://docs.rs/axum/) | Optional local HTTP server, REST API, and WebSockets |
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

# Usage

Vise accepts HTTP(S) URLs as arguments or through an interactive queue. All modes use the same global journal and download engine. [Install Vise](installation.md) first; use `vise --help` to inspect the exact options in your installed version.

## Quick start

Download one file to the configured default destination:

```sh
vise https://www.rust-lang.org/static/images/rust-logo-blk.svg
```

Download a batch, with two concurrent files and up to eight connections per file:

```sh
vise --batch -j 2 -c 8 \
  https://www.rust-lang.org/static/images/rust-logo-blk.svg \
  https://raw.githubusercontent.com/rust-lang/rust/master/LICENSE-MIT
```

Choose a destination directory and an explicit filename for one URL:

```sh
vise -o ./downloads -n rust-logo.svg \
  https://www.rust-lang.org/static/images/rust-logo-blk.svg
```

`-o` selects a **directory**; `-n` selects a plain filename. Vise refuses explicit filename collisions. Automatically derived names receive numeric suffixes when necessary. URL arguments normally run until the newly scheduled queue settles; `--interactive` keeps it open.

## Core modes

| Mode | Command | Controller |
| --- | --- | --- |
| Terminal batch | `vise --batch URL` | Indicatif progress; no terminal input |
| Interactive queue | `vise --interactive` | Terminal commands and progress bars |
| Local WebUI | `vise --webui` | Browser dashboard with terminal telemetry |
| Foreground daemon | `vise --daemon` | Authenticated API/dashboard; suitable for systemd |
| Daemon client | `vise remote list` | Control a running daemon without opening another engine |
| Resume saved work | `vise --resume-all` | Queue unfinished global tasks at their saved destinations |

`URL` in the mode table denotes a URL argument; the runnable examples above use public Rust project resources. With no arguments and terminal stdin, `vise` opens the interactive queue. Use `--interactive` explicitly when starting from another program. Server modes are mutually exclusive with each other, `--interactive`, and `--batch`.

Stop a session with Ctrl+C; a service manager can send SIGTERM. Vise stops workers and commits their progress before exiting. One engine owns the journal at a time: use the running daemon's API rather than launching another Vise process against the same home.

## CLI reference

| Option | Behavior / initial default |
| --- | --- |
| `[URLS]...` | HTTP(S) URLs to queue; credentials embedded in URLs are rejected |
| `-o, --output <DIR>` | Destination for new tasks; initially `~/Downloads/Vise` |
| `-n, --name <NAME>` | Plain output filename; requires exactly one URL |
| `-j, --jobs <COUNT>` | Simultaneous downloads, 1–64; initially 5 from configuration |
| `-c, --connections <COUNT>` | Maximum chunks per file, 1–64; default 8 |
| `--retries <COUNT>` | Retries after the initial segment attempt, 0–20; default 5 |
| `--timeout <SECONDS>` | Idle read timeout, at least 1 second; default 30 |
| `--limit-rate <RATE>` | Global response-body bandwidth cap; `off` clears it |
| `--expected-sha256 <HEX>` | Expected 64-digit SHA-256 for exactly one URL |
| `--expected-md5 <HEX>` | Expected 32-digit MD5 for exactly one URL |
| `--proxy <URL>` | Default HTTP/HTTPS/SOCKS5/SOCKS5h route for new tasks |
| `--cookies <FILE>` | Netscape cookie file for new tasks |
| `--resume-all` | Resume eligible unfinished tasks from the global journal |
| `-i, --interactive` | Keep the command queue open, including with URL arguments |
| `--batch` | Disable interactive input |
| `--webui` | Start the loopback dashboard and terminal telemetry |
| `-d, --daemon` | Start the API/dashboard quietly in the foreground |
| `--bind <IP:PORT>` | Override the whole server address; conflicts with host/port |
| `--host <IP>` / `--port <PORT>` | Override individual server configuration fields |
| `-h, --help` / `-V, --version` | Print help/version without creating local state |

Bind, host, and port options require `--webui` or `--daemon`. A WebUI bind must be loopback; a non-loopback daemon bind requires authentication. The timeout limits inactivity, so a progressing large download can run longer than 30 seconds.

Parallelism is conditional on verified range support and a representation validator. Servers without them use one streaming request; after interruption that stream restarts safely. See [transfer and recovery details](architecture.md#transfers-and-recovery).

## Interactive queue

```sh
vise --interactive
```

At the prompt, paste an HTTP(S) URL or enter a command:

| Command | Result |
| --- | --- |
| `add URL [FILENAME]` | Queue a URL, optionally under a plain filename |
| `list` | Show saved and active tasks, including their IDs |
| `pause ID` | Stop workers and checkpoint the task |
| `resume ID` | Queue a paused or failed task |
| `retry ID` | Retry; corrupt tasks discard all chunk offsets and download again |
| `limit-rate` | Show the current global cap |
| `limit-rate RATE` | Apply a live cap, such as `5M` or `off` |
| `help` | Print the command summary |
| `quit` / `exit` | Checkpoint the queue and exit |

IDs accept a unique prefix of at least four characters; ambiguous prefixes fail explicitly. API routes require the full UUID. Interactive `add` takes the URL as its first word and treats the rest of the line as the filename, including spaces; shell-style quotes are unnecessary inside that prompt.

Example commands to enter inside the queue:

```text
add https://www.rust-lang.org/static/images/rust-logo-blk.svg rust-logo.svg
list
limit-rate 1.5M
limit-rate off
quit
```

## Bandwidth control

```sh
vise --limit-rate 5M --interactive
```

The cap is shared across all active files, segments, and retries. It does not reserve that amount for each connection. Live changes preserve existing HTTP connections and persist in SQLite.

| Rate | Bytes per second |
| --- | --- |
| `500K` | 512,000 |
| `5M` / `5MiB/s` | 5,242,880 |
| `5MB/s` | 5,000,000 |
| `1.5M` | 1,572,864 |
| `off` / `unlimited` / `0` | No cap |

Short forms K/M/G/T use powers of 1024; KB/MB/GB/TB use powers of 1000. This limits consumed response-body bytes, not protocol headers or range probes. HTTP buffers and the token bucket allow short bursts; judge the cap over sustained transfers.

The initial configuration value `downloads.global_speed_limit_kbps = 0` restores the journal's saved cap. An explicit CLI limit or a positive configuration value overrides it. Use `--limit-rate off` to clear an older saved cap. See [WebUI controls](webui.md#global-bandwidth-controls) and the [configuration API](daemon.md#bandwidth-configuration).

## Checksum verification

Pass a trusted expected SHA-256 or MD5 with exactly one URL. This example uses the known SHA-256 of an empty file to verify the public HTTP testing service's empty response:

```sh
vise --expected-sha256 e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855 \
  https://httpbin.org/bytes/0
```

For release artifacts, obtain the expected digest from a trusted release publisher and pass it to `--expected-sha256` (or `--expected-md5` for compatibility). Both checks must match if supplied together. Supported HTTP digest metadata can supply expectations automatically; explicit values take precedence per algorithm.

At 100% transferred, a task can still be **verifying**. Vise checks the staging file before publishing the final path. A mismatch records **corrupt**, includes expected/actual values in the error, and preserves staging data. `resume` and `--resume-all` do not bypass corruption; explicit `retry` downloads the whole file again. MD5 is for legacy integrity compatibility; use SHA-256 from a trusted source for security-sensitive files.

## Cookies and proxies

Pass a Netscape-format cookie file for an authenticated session, or set `downloads.cookies_file` in the [configuration](daemon.md#configuration-and-precedence). The file must already exist and be readable. Each task saves its absolute cookie-file path and reloads it when resumed; the cookie values themselves are not written to SQLite.

For a file exported to `~/cookies.txt`, start a session that applies it to new tasks:

```sh
vise --cookies "$HOME/cookies.txt" --interactive
```

Route new transfers through an existing local SOCKS proxy:

```sh
vise --proxy socks5h://127.0.0.1:1080 --interactive
```

`http://`, `https://`, `socks5://`, and `socks5h://` are supported. SOCKS5 resolves destinations locally; SOCKS5h resolves them through the proxy. Explicit routes override environment proxy exclusions and never fall back to direct connections. Saved tasks retain their original route even if the process default changes.

Cookie files and proxy credentials are sensitive local data. Protect their permissions and avoid putting credentials in shared shell history. See the [security policy](https://github.com/juic3b0x/vise/blob/main/SECURITY.md).

## Resume and inspect history

```sh
vise --resume-all
```

The journal is always `~/.vise/state.sqlite3`; `-o` does not create a separate task history. Previously saved absolute destinations and staging paths remain unchanged. A crash may require downloading bytes since the last durable checkpoint again. Missing disks, changed remote content, or expired cookie files fail explicitly rather than silently relocating or authenticating a task incorrectly.

Use the [native remote CLI](remote.md), [daemon API clients](daemon.md#rest-api), or the [dashboard](webui.md) to inspect the same history while the daemon owns the engine.

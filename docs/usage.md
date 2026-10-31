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

## Adaptive range tails

When a worker finishes and no queued ranges remain, Vise can assign it part of the largest remaining active range. With reliable speed samples, shares reflect the donor and idle workers' throughputs; otherwise Vise halves the remainder. The configured connection count stays fixed. Known-length range support and an existing non-weak ETag or Last-Modified validator are required; unsupported or unknown-length streams stay single-stream.

`--min-split-size BYTES` overrides `downloads.min_split_size` in `config.toml`, including explicit default and zero values. The default is **1048576 bytes (1 MiB)** per unfinished portion, plus a 256 KiB donor safety margin; boundaries use 64 KiB alignment. Zero disables tail splitting. This is startup tuning, not a live API setting; it does not rewrite saved ownership or credentials. Tasks have at most 1,024 chunks from adaptive splitting, and resumed layouts use the current connection budget even when they contain more chunks.

```sh
vise --connections 4 --min-split-size 1048576 --input urls.txt
vise --min-split-size 0 --input urls.txt
```

HTTP 429/503 or a refused/reset connection suppresses further splits for 30 seconds, extended by numeric Retry-After up to 60 seconds. Already-assigned ranges retain their ownership and bounded retry policy. Existing requests are not reconnected when the global rate changes. Speed observations reset on retry, resume, and cap changes; short, stale, bursty, or limiter-dominated measurements fall back to halving.

`tail_splits` and `split_discarded_bytes` are numeric per-execution counters, omitted at zero and reset on resume/restart. Discarded bytes are response-body bytes observed by Vise beyond a shortened range end, including prefetched frame remainders. They exclude bytes still hidden in HTTP/socket buffers and differ from the benchmark server's wasted-byte count. The abandoned response closes without draining its remaining range; already-fetched discarded bytes share the global token bucket.

## Input files

```sh
vise --input urls.txt -j 3 -c 8 -o ./downloads
vise --input urls.txt --interactive
vise --webui --input urls.txt
```

Use a UTF-8 text file with one HTTP(S) URL per line:

```text
# Release assets
https://www.rust-lang.org/static/images/rust-logo-blk.svg

https://raw.githubusercontent.com/rust-lang/rust/master/LICENSE-MIT
```

Vise trims each line, ignores empty lines and lines beginning with `#`, and accepts CRLF and a leading UTF-8 BOM. Fragments and query parameters stay part of the URL; inline comments are not stripped. Repeated URLs are preserved and receive collision-safe filenames.

Limits are 1 MiB of input text, 1,000 URLs per invocation, and 8,192 bytes per URL. Missing files, directories, invalid UTF-8, malformed URLs, and empty lists fail before configuration or journal creation and before any positional URLs are queued. A malformed line reports its line number without echoing the URL. Positional arguments precede file entries. An explicitly provided empty file is an error even with positional URLs.

When `--input` is supplied, the complete resolved list (positional URLs first, followed by file entries) is preflighted and admitted in two atomic durable stages. The first transaction stores every new task paused; the second commits every queued transition. Vise emits `Added` events in input order after the second durability barrier, before starting any of these workers. Duplicates keep their normal collision-safe names and each task keeps its proxy, cookies and origin-bound request credentials.

Preflight errors and a known rollback of the first transaction admit none of the new list. If activation fails, the command fails and the entire admitted list stays paused, with no new `Added` events or workers; resume it explicitly after fixing the error. Lost commit acknowledgements are read back: a committed first stage is retained paused, and a committed second stage is repaired durably to paused before reporting failure. If the outcome cannot be reconciled, Vise requires reopening the engine. After a crash, every unfinished admitted task recovers paused. Admission and activation are separate transactions; this does not promise rollback to an empty queue after admission has committed.

This whole-list contract applies only to startup with `--input`. Positional-only startup keeps sequential admission and its existing failure behavior; daemon batches keep their indexed per-entry operational results. No version bump accompanies this change.

The combined queue uses the normal `-j`, `-c`, `-o`, proxy, cookie, and rate settings. Without `--interactive` or a server flag, Vise exits after the session's tasks settle and reports failures through its exit code. `--name` and expected-checksum flags require exactly one URL after combining arguments and the file. `--resume-all` retains its normal behavior for saved tasks.

With a running daemon, use [remote file ingestion](remote.md#bulk-ingestion) instead of starting another engine.

## Core modes

| Mode | Command | Controller |
| --- | --- | --- |
| Terminal batch | `vise --batch URL` | Indicatif progress when drawing is available; no terminal input |
| Interactive queue | `vise --interactive` | Terminal commands and progress bars |
| Local WebUI | `vise --webui` | Browser dashboard with terminal telemetry |
| Foreground daemon | `vise --daemon` | Authenticated API/dashboard; suitable for systemd |
| Daemon client | `vise remote list` | Control a running daemon without opening another engine |
| Resume saved work | `vise --resume-all` | Queue unfinished global tasks at their saved destinations |

`URL` in the mode table denotes a URL argument; the runnable examples above use public Rust project resources. With no arguments and terminal stdin, `vise` opens the interactive queue. Use `--interactive` explicitly when starting from another program. Server modes are mutually exclusive with each other, `--interactive`, and `--batch`.

Stop a session with Ctrl+C; a service manager can send SIGTERM. Vise stops workers and commits their progress before exiting. One engine owns the journal at a time: use the running daemon's API rather than launching another Vise process against the same home.

## CLI reference

Batch mode skips progress-bar construction and rendering when terminal drawing is unavailable. It waits for scheduled tasks to finish terminal persistence and publication cleanup, including tasks that finish during startup. Exit codes and the redirected-output policy are unchanged. Completed history does not participate in repeated batch-completion checks.

| Option | Behavior / initial default |
| --- | --- |
| `[URLS]...` | HTTP(S) URLs to queue; credentials embedded in URLs are rejected |
| `--input <FILE>` | Read a bounded UTF-8 URL list; positional URLs come first |
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
| `--header <NAME:VALUE>` | Repeatable custom request header; local CLI defaults only |
| `--referer <URL>` | HTTP(S) referer; local CLI defaults only |
| `--user-agent <STRING>` | User agent for new tasks; overrides TOML |
| `--resume-all` | Resume eligible unfinished tasks from the global journal |
| `-i, --interactive` | Keep the command queue open, including with URL arguments |
| `--batch` | Disable interactive input |
| `--webui` | Start the loopback dashboard and terminal telemetry |
| `-d, --daemon` | Start the API/dashboard quietly in the foreground |
| `--bind <IP:PORT>` | Override the whole server address; conflicts with host/port |
| `--host <IP>` / `--port <PORT>` | Override individual server configuration fields |
| `-h, --help` / `-V, --version` | Print help/version without creating local state; version includes build commit/date when available |

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

## Request headers and referer

```sh
vise --header "Authorization: Bearer $DOWNLOAD_TOKEN" \
  --header 'X-Client: vise' --referer https://downloads.example.com/releases \
  --user-agent 'MyDownloader/1.0' https://downloads.example.com/file.zip
vise --header "Authorization: Bearer $DOWNLOAD_TOKEN" --input urls.txt
```

These startup flags configure new tasks, including later interactive additions. Existing `add URL [FILENAME]` and bare-URL commands retain their syntax and inherit these defaults; there is no inline interactive header syntax. Saved tasks use their journaled values after pause, retry, or restart, independently of new process defaults. An optional `[downloads].user_agent` sets the file default. Explicit `--user-agent`, including the built-in text or an empty string, overrides it. An empty user-agent flag selects the built-in Vise agent. Default custom headers are intentionally not accepted in TOML.

In `--daemon` and `--webui`, process-wide `--header` and `--referer` are rejected without echoing their values. Use [per-task API admission](daemon.md#request-credentials). User-agent defaults remain permitted. Headers, a referer, or supplied cookies require every URL in one positional/input-file batch to share an origin: scheme, normalized host, and effective port. A mixed-origin batch fails before any task is added. User agent alone is exempt.

Header names must be HTTP tokens of at most 128 bytes. Values cannot contain control characters. Limits are 32 entries, 8 KiB per value, and 32 KiB combined custom headers. Referers are absolute HTTP(S) URLs without embedded credentials, at most 8 KiB; fragments are removed. Custom user agents are limited to 512 bytes. Supplying User-Agent or Referer through both a header and its dedicated flag is an error; a User-Agent header alone overrides the TOML agent.

Vise rejects Host, range/conditional headers, framing and hop-by-hop headers, content/accept encoding, proxy authentication, and its digest negotiation header. Raw Cookie and Set-Cookie headers are always rejected: use `--cookies FILE` or structured API cookies so domain/path/secure scope is retained.

On the new transport, same-origin redirects retain settings. A host, scheme, or effective-port change strips custom headers, custom user agent, and all jar cookies, and sends only the origin of a supplied referer. Credentials remain disabled for the rest of that redirect chain, even if it returns to the starting origin. HTTPS-to-HTTP redirects are refused. New settings combined with a cookie file pin the combined jar to the initial origin. Tasks without new settings keep the existing Reqwest redirect and automatic referer behavior.

Credentials are saved in plaintext private task state, not returned by inspection commands. Unix journal mode must be exactly `0600`; Windows users must protect the home directory with account ACLs. The binary logger suppresses credential-bearing cookie-store events even with global trace/debug logging or explicit cookie-store directives. This does not protect shell history or process listings, where CLI credentials can still be exposed. See [security](https://github.com/juic3b0x/vise/blob/main/SECURITY.md#per-task-request-credentials) and [remote commands](remote.md#request-credentials).

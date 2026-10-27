# Remote CLI

`vise remote` controls an already running authenticated Vise daemon. It never starts a download engine, opens SQLite, creates configuration, or acquires the global engine lock. This lets you keep a daemon under systemd and manage the same queue from terminals, scripts, the WebUI, and the browser extension.

Direct downloads keep their existing behavior:

```sh
vise https://www.rust-lang.org/static/images/rust-logo-blk.svg
```

That command runs the local engine. If a daemon already owns the home directory's engine lock, it fails clearly. Use `remote add` to explicitly submit to the daemon instead.

## Connect to your daemon

Start the daemon in another terminal, or use the [systemd service](daemon.md#systemd-user-service):

```sh
vise --daemon --resume-all
```

From a second terminal:

```sh
vise remote list
vise remote add https://www.rust-lang.org/static/images/rust-logo-blk.svg
vise remote watch
```

By default, the client reads the existing `daemon.host`, `daemon.port`, and `daemon.VISE_API_TOKEN` from `~/.vise/config.toml`. The environment variable `VISE_API_TOKEN` overrides the file token. Wildcard listener addresses (`0.0.0.0` and `::`) map to their loopback equivalents for local connections. Use a known listener port; an automatically assigned daemon port cannot be discovered from a configuration value of zero.

Override the endpoint explicitly when needed:

```sh
vise remote --endpoint http://127.0.0.1:9090 list --json
```

An endpoint is an HTTP(S) origin, without credentials, a path, query parameters, or fragments. Non-loopback endpoints require HTTPS; for a remote HTTP-only daemon, connect through a local SSH tunnel. Public endpoints such as `https://downloads.example.org` require your own correctly configured HTTPS reverse proxy and the token of that daemon.

With both an explicit endpoint and `VISE_API_TOKEN`, the client needs no local configuration or home directory. Keep tokens in protected configuration or environment setup instead of shell arguments; there is deliberately no `--token` option. The client does not initialize a missing configuration file.

## Commands

| Command | Behavior |
| --- | --- |
| `remote add URL` | Admit a new task and return its snapshot; success means queued, not completed |
| `remote add --input FILE` | Read a client-local URL list and queue it in the daemon |
| `remote list` | List every saved and active task |
| `remote get ID` | Inspect one task |
| `remote pause ID` | Stop its workers and checkpoint its offsets |
| `remote resume ID` | Queue a paused or failed task |
| `remote retry ID` | Retry; a corrupt task restarts all chunks |
| `remote delete ID` | Remove the queue entry and partial data; completed files are preserved |
| `remote limit-rate` | Show the current shared bandwidth cap |
| `remote limit-rate RATE` | Change and persist it, using units such as `5M` or `off` |
| `remote watch` | Monitor snapshots until interrupted |

Task controls accept a full UUID or a unique hexadecimal prefix of at least four characters. Missing and ambiguous prefixes fail before sending a mutation. A prefix requires a listing request; a full UUID goes directly to the task endpoint.

These options are accepted before or after the remote subcommand:

| Option | Meaning |
| --- | --- |
| `--endpoint URL` | Override the daemon origin |
| `--json` | Write compact JSON to stdout; watch writes newline-delimited telemetry objects |
| `--request-timeout SECONDS` | Bound each request, 1–300 seconds; default 15 |

Put `remote` immediately after `vise`. Local-engine options such as `--output`, `--cookies`, `--jobs`, and `--webui` cannot be mixed with remote commands. Misplaced `remote` commands are rejected before engine startup. The request timeout is independent of the total time spent waiting for a download.

## Add and wait for publication

```sh
vise remote add --wait --name rust-logo.svg \
  https://www.rust-lang.org/static/images/rust-logo-blk.svg
```

`--wait` waits for the returned task to reach a terminal state. It succeeds only for `completed`, after configured checksum verification and final publication. A failed, corrupt, or paused task returns exit code 2. A removed task or lost API connection returns an error; the client does not silently resume or retry the task.

The add command also accepts `--expected-sha256`, `--expected-md5`, and `--proxy`, with the same digest formats and proxy schemes as [direct downloads](usage.md#checksum-verification). `--name` selects a plain filename, not a destination path. The output directory and cookie file belong to the **daemon's filesystem and configuration**, not the client machine. Proxy settings on an add request route the download, never the API connection.

For scripting:

```sh
vise remote list --json > vise-queue.json
vise remote add --wait --json \
  https://www.rust-lang.org/static/images/rust-logo-blk.svg
```

`add --wait --json` writes one final download snapshot. Other one-shot commands write one result: arrays for lists, download objects for task operations, a configuration object for rate controls, and a deletion receipt containing `id` and `deleted: true`. Errors go to stderr without contaminating JSON stdout. JSON contains the same task metadata as the API, including source URLs and absolute daemon paths; protect captured output when those are sensitive.

## Bulk ingestion

```sh
vise remote add --input urls.txt --wait --json
vise remote --request-timeout 120 add --input urls.txt
vise remote add https://www.rust-lang.org/static/images/rust-logo-blk.svg --input urls.txt
```

The file is read on the **client machine**, validated using the [input-file rules](usage.md#input-files), and sent as a URL array; the daemon never opens a client-supplied file path. The client does not create a journal or change daemon concurrency/output settings. Positional URLs come first; repeated URLs are retained. `--name` and checksum overrides require exactly one combined URL. `--proxy` may apply to an entire batch and still affects only transfer traffic.

Multiple resolved URLs use the [batch endpoint](daemon.md#batch-ingestion). JSON output is `{accepted, rejected}` with one-based indices and task snapshots or errors. A single resolved URL retains the existing single-task endpoint and snapshot output. Any rejected entry produces exit code 2; inspect daemon history before retrying, because an operational failure can leave a paused task.

For batches, `--wait` waits until all accepted tasks settle and returns success only if every entry was admitted and published. Failed, paused, or corrupt tasks produce exit code 2. A removed task or lost connection is an error. Interrupting the client detaches without stopping daemon work. For a large list, raise `--request-timeout` if admission exceeds the default 15 seconds; it remains a per-request timeout.

## Live monitoring

```sh
vise remote watch
vise remote watch --json
```

The client polls `/api/telemetry` once per second after each successful response. Interactive terminals use the shared Indicatif renderer for progress, speed, ETA, and states. Redirected human output prints timestamped summaries; `--json` emits one complete `{timestamp, downloads, configuration}` object per line. The first snapshot is requested immediately, including for an empty queue.

Ctrl+C or SIGTERM on Unix exits with 130; Windows supports Ctrl+C. Interrupting watch or a waiting client detaches only the client. The daemon and its downloads continue. A blocked stdout pipe does not hold client shutdown open. A broken output pipe ends the client successfully, which supports ordinary shell pipelines.

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Command acknowledged, or a waited task completed; also a closed stdout consumer |
| `1` | Configuration, authentication, transport, API, protocol, or output failure |
| `2` | A batch entry was rejected, or a waited task failed, was corrupt, or was paused; Clap also uses 2 for invalid CLI arguments |
| `130` | Client interrupted; daemon work is left running |

## Failure and credential handling

The API client bypasses environment proxies, has no download cookie jar, preserves TLS certificate validation, and refuses redirects. Tokens are marked sensitive in request headers and never copied into diagnostic output. Responses are bounded to 32 MiB; ordinary request bodies retain the API's 16 KiB limit, and batch bodies allow up to 2 MiB. Human-readable server text is escaped before terminal display.

Requests are not automatically retried. If a submission loses its response, it might already have been admitted: inspect `vise remote list` before repeating it. The client never falls back to starting a local engine after authentication or connection failure.

Use [daemon configuration and API details](daemon.md) for server setup, [WebUI controls](webui.md) for browser management, and [architecture](architecture.md) for durable worker behavior.

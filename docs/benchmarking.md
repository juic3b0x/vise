# Reproducible download benchmarks

Vise includes a developer-only harness comparing complete downloads with aria2. It measures correctness first, then wall time, useful throughput, client CPU time, peak resident memory, HTTP requests, TCP connections, accepted body bytes, and completion tails. Optional curl measurements form a separate single-connection cohort.

**Loopback is not the internet.** These experiments isolate specific server policies; they cannot establish general performance across CDNs, WANs, TLS, HTTP/2, proxies, storage devices, or authenticated services. Smoke output is explicitly non-publishable.

## Requirements and build

Clone the [source repository](https://github.com/juic3b0x/vise). Release archives include this guide but deliberately exclude `benchmarks/` and benchmark binaries. The harness never runs in CI and requires no Python packages or system changes.

- Python **3.11 or later**, standard library only.
- Rust compatible with the declared **1.88** MSRV, Cargo, and the normal [Vise build requirements](installation.md).
- `aria2c` on PATH, preferably 1.37.0 for comparable results. If absent, the runner reports skipped aria2 profiles and still runs Vise.
- Optional `curl` with parallel transfers and `--retry-all-errors` support (curl 7.71+); the exercised local version is 8.22.0.

From the repository root:

```sh
cargo build --release --locked
cargo build --manifest-path benchmarks/fixture/Cargo.toml --release --locked
python -m unittest discover -s benchmarks/tests -v
python benchmarks/run.py --smoke --curl --storage real-disk --output target/benchmarks/smoke
```

Use a **new output directory** for every invocation. The runner confines reports and temporary data to a subdirectory of `target/benchmarks/`. Use `--storage ramdisk` only when that is true of the target filesystem. `--storage auto` detects Linux tmpfs/ramfs; other cases remain explicitly unknown instead of guessing. There is no disk-cache flushing, privileged tuning, package installation, or automatic ramdisk setup.

For measured work, run on an otherwise idle machine:

```sh
python benchmarks/run.py --curl --runs 6 --connections 4 --jobs 5 --storage real-disk --output target/benchmarks/full
```

The full profile discards one warm-up per tool/scenario/cohort and defaults to six measured runs; fewer than five are rejected. Smoke substitutes an 8 MiB large file and twelve 64–128 KiB small files, retains all six scenarios, discards warm-ups, and measures exactly three runs. Tool order rotates and reverses between rounds; seeds and file sizes match across tools in each round. Six full rounds balance positions for the three main profiles. Warm-ups are recorded but excluded from aggregates. Failed attempts are never replaced with extra successes.

Build the optimized binaries before timing. `--vise PATH` and `--fixture PATH` select existing binaries. The environment block's `tools` entries record `path` (the resolved absolute executable actually used), `version`, and `sha256` for Vise, the fixture, and each available aria2/curl tool. Missing optional tools are explicitly reported as skipped and omitted from `tools`, never represented by an empty path. The Vise commit and dirty flag, CPU/OS, filesystem, storage declaration and POSIX file descriptor limits also appear in `results.json`. Keep that metadata with any report. A dirty checkout or unknown storage deserves investigation before publication.

## Controlled scenarios

All parameters are in the tracked [`benchmarks/scenarios.json`](https://github.com/juic3b0x/vise/blob/main/benchmarks/scenarios.json). Copy it inside the repository and pass `--config PATH` to change sizes, file count, byte-rate cap, latency, faults, seed, timeout, connections, jobs, or selected scenarios. `--connections`, `--jobs`, `--seed`, and `--scenarios large,capped` also override the corresponding fields. Integer bounds are validated. Large files must allow at least 2 MiB per requested connection, matching aria2's minimum split restrictions.

| Scenario | Full default | What it exercises |
| --- | --- | --- |
| `large` | One 512 MiB file, unthrottled | Whole-tool loopback overhead and storage cost |
| `capped` | Same file; 8 MiB/s per TCP connection | Parallel connection utilization |
| `heterogeneous` | Same cap; every fourth accepted TCP connection gets one tenth of it | Work distribution and completion tails |
| `small` | 2,000 seeded files, 64 KiB–1 MiB, five concurrent jobs | Startup, probes, admissions, per-file publication and journaling |
| `latency` | 80 ms before **each** response | Request/probe overhead; this is response delay, not simulated network RTT |
| `flaky` | Two seeded absolute fault offsets per file | Reset recovery, success rate, correct-completion time and observed retries |

The small-file scenario uses C=1 for every client. Vise receives URLs over `--interactive` stdin and then EOF, which waits for all queued downloads. This bypasses the 1,000-URL batch input bound without changing runtime code. aria2 receives its stdin input list; curl uses a generated config file. Timing includes process startup, stdin feeding, queue admission, and shutdown. **Do not send `quit`**, which cancels Vise's interactive session.

The independent fixture binds only numeric `127.0.0.1` on an ephemeral port. Payload bytes come from a seeded, seekable counter-based SplitMix64 PRNG, never a disk file or a whole-file allocation. Fixed-size buffers generate arbitrary ranges; SHA-256 digests are computed independently before the timed run. HTTP/1.1 GET/HEAD, strong ETags, single closed/open/suffix ranges, If-Range, Content-Length and invalid-range rejection are implemented. Requests, headers, file counts and lengths are bounded; malformed input is rejected without echoing it. No raw headers or request text are logged.

Caps persist across keep-alive requests on each accepted TCP connection. Heterogeneous classification uses the connection's acceptance number, which restarts for every run; it does not assume the tools open or reuse connections identically. Reports show the actual number of slow connections that carried body bytes and their share of all accepted body bytes. Inspect those values before interpreting that scenario.

Fault offsets depend on the seed and file index, not timing. Each offset resets at most once when a non-probe response crosses it. The safe socket2 wrapper requests zero linger to inject a TCP reset. Concurrent scheduling and tool range boundaries still affect which request encounters each fault and how many requests recovery needs.

## Client profiles and fairness

The default `--profiles vise,aria2-matched,aria2-defaults` runs both aria2 profiles in every scenario. Select a subset explicitly, for example `--profiles vise,aria2-matched`. Vise must remain included. aria2 accepts up to sixteen connections per server, so the harness enforces C=1–16. Actual connections are measured, not assumed.

| Setting | Vise | Both aria2 profiles |
| --- | --- | --- |
| Per-file connection budget C | `--connections C` | `--split=C --max-connection-per-server=C --min-split-size=1M --piece-length=1M` |
| Concurrent files J | `--jobs J` | `--max-concurrent-downloads=J` |
| Retries after initial R | `--retries R` | `--max-tries=R+1` |
| Connect timeout | Built-in 15 seconds | `--connect-timeout=15` |
| Idle timeout | `--timeout 30` | `--timeout=30` |
| Destination | `--output DIR` | `--dir=DIR --input-file=-` |
| Console output | Non-terminal stdout/stderr discarded | `--quiet=true --summary-interval=0 --show-console-readout=false --download-result=hide` |

`aria2-matched` additionally uses exactly:

```text
--no-conf --no-netrc=true --file-allocation=none
--allow-overwrite=false --auto-file-renaming=false --continue=false
```

It **does not** set `--disk-cache` or `--enable-mmap`; aria2's cache and mmap defaults remain intact. `aria2-defaults` supplies only the mapped settings in the table. Allocation, cache, mmap, overwrite, renaming, continuation and every other option remain stock for that profile. Fresh isolated homes prevent user config/netrc files from affecting either profile. Both profiles start with empty destinations, so stock overwrite/resume policies cannot reuse prior output.

`--curl` adds a separate C=1 cohort containing the selected profiles plus curl; it never compares single-stream curl against a C>1 Vise run. Curl uses `-q` first, `--http1.1 --silent --show-error --fail`, a generated URL/output config, `--parallel --parallel-max J`, `--connect-timeout 15`, `--speed-time 30 --speed-limit 1`, and `--retry R --retry-all-errors`. Its low-speed timeout is an approximation of idle-read semantics, not an identical timer. Client retry/backoff schedules and range-recovery policies remain native; equal retry counts do not mean equal recovery schedules.

Each run gets a private HOME/USERPROFILE, config/cache/app-data paths and a fresh output directory; all output and resume/control files are deleted afterward. Vise receives a minimal token-free configuration and `--limit-rate off`. Process environments are allowlisted, excluding proxy, token and log variables. Captured output is discarded; reports retain numeric metadata, safe versions, hashes and sanitized arguments, never credentials. The read-only `rustc --version` query may use the real toolchain home; no Vise process accesses the real `~/.vise`.

Native connection pooling, range layouts, probes and synchronization remain enabled. Vise's SQLite WAL transactions, checksum/publication machinery and fsync cost are part of the measurement. aria2's allocation policy differs deliberately between the two profiles. The fixture sends no checksum metadata; external SHA-256 verification runs for **every** output after timing. Fresh files do not imply cold filesystem caches. This is a comparison of complete tools, not isolated HTTP implementations or equal durability guarantees.

## Metrics and reports

Each attempt gets its own JSON file. `results.json` contains attempts, configuration, environment, successful-run aggregates and comparisons; `summary.md` contains Markdown tables. Aggregates report median, min, max and spread (`max − min`). Success rate includes all measured attempts. Any failed attempt, including a warm-up, makes the harness exit nonzero; failures and warm-ups never enter timing aggregates. Comparisons require at least five correct full runs per profile (three only for non-publishable smoke).

| Metric | Definition |
| --- | --- |
| Wall time | Monotonic time immediately before spawning the client through process exit; includes stdin feeding and native shutdown, excludes fixture setup and external hash verification |
| Useful throughput | Sum of verified expected file sizes divided by wall seconds, in MiB/s; successful non-flaky runs only |
| CPU | Client process user and system CPU across its threads; excludes runner and server; total is their sum |
| Peak RSS | Process resident-memory high-water mark, not private allocation, PSS or the sum of sampled RSS values |
| Requests | All parsed HTTP requests, including HEAD, one-byte probes, ranges, errors and recovery; fixture control uses stdin, not HTTP |
| Connections | Accepted TCP connections; also record peak open and peak actively serving connections; idle pools are distinct from data occupancy |
| Body bytes B | Actual response-body prefixes accepted by server socket `write()` calls; excludes headers, TCP retransmissions and kernel delivery confirmation |
| Unique bytes U | Union of accepted byte intervals per file, summed across files |
| Duplicate bytes | B − U, including probes, overlap and repeated accepted payload |
| Wasted bytes | B − sum of verified expected file sizes for a successful run; null on failure, where coverage and duplicate bytes remain diagnostic |
| Observed retry requests | Non-probe body requests starting after a reset and intersecting that reset response's advertised interval; counts resumed ranges even without duplicated bytes, but is not the client's internal attempt counter |
| Tail time | After serving occupancy first reaches budget C×J, time from its **first downward crossing** below budget until the last non-probe body byte; null if budget never reached; C=1 commonly yields zero |

Serving occupancy uses each non-probe response's first/last accepted body-write timestamps, grouped by connection. Rate-limit sleeps inside that interval count as serving; request latency before the first byte does not. Keep-alive gaps and retry gaps can start the tail, and occupancy may rise again afterward. This metric intentionally measures the first drop, not just a final uninterrupted drain. Socket buffering and single-write responses can produce very short intervals. Inspect the numeric per-connection request log to understand null/zero tails and actual C utilization.

The flaky scenario reports success rate, time to correct completion and observed retry requests. It has no throughput value or ratio/ranking because different native retry schedules would obscure the controlled transfer comparison. Overlapping observed min–max ranges always receive the explicit **“no clear difference”** label. Ratios are reference median wall time / Vise median wall time, descriptive only; they are not confidence intervals or evidence of statistical significance.

## Platform measurement and limitations

- **Linux:** a single reaper uses `wait4(pid)` for each client; user/system CPU are per child, and `ru_maxrss` KiB is converted to bytes. `/proc/cpuinfo`, `findmnt` when available, and the soft/hard file descriptor limit supply environment metadata.
- **macOS:** the same per-child `wait4` path uses Darwin's byte-valued `ru_maxrss`; `stat -f %T` supplies filesystem type. This implementation requires native macOS validation before relying on its numbers.
- **Windows:** stdlib ctypes calls `GetProcessTimes` and `GetProcessMemoryInfo` on the child handle. `PeakWorkingSetSize` is polled and queried after exit; if the final query fails, the last observed peak is a lower bound. Unsupported counters are null. Filesystem/storage classification is explicitly unknown unless declared. This implementation requires native Windows validation.

No cumulative `RUSAGE_CHILDREN` subtraction or cross-tool process accounting is used. Clients are expected to remain single processes with worker threads. POSIX timeout cleanup kills the process group; Windows kills the measured process, not an arbitrary descendant tree. The per-run fixture lifetime counters include digest preparation and cannot be treated as server CPU during the timed interval alone.

POSIX `ru_maxrss` is a process-lifetime high-water mark. On systems that retain a fork child's pre-exec high-water mark, it can include inherited runner memory before the client executable starts. Equal RSS plateaus across small clients can reflect that launch floor; do not interpret them as equal steady-state client memory. The JSON retains the counter source so this limitation stays explicit.

The fixture and clients share CPUs, caches and the loopback stack. Fast-loopback runs can be generator-, scheduler- or storage-bound. Per-connection pacing has 64 KiB granularity and kernel buffering; it is not a network emulator. High small-file counts may exhaust a low descriptor limit. No affinity, cache state, CPU frequency, thermal state, filesystem compression or background workload is automatically controlled. Record and disclose these when sharing results. The implementation was exercised on Linux; local macOS/Windows execution and PR-only CodeQL effects are unverified.

## Correctness self-tests and isolation

These deliberate corruption checks must exit **1**, not zero:

```sh
python benchmarks/run.py --self-test all --output target/benchmarks/corruption
```

One wrong byte, truncation and a missing file each produce a failed JSON result, zero aggregate successes and no timing data point. A client exit status of zero never bypasses verification. Use `--self-test wrong-byte`, `truncated` or `missing` to exercise a single case.

```sh
cargo fmt --manifest-path benchmarks/fixture/Cargo.toml --check
cargo clippy --manifest-path benchmarks/fixture/Cargo.toml --all-targets --locked -- -D warnings
cargo test --manifest-path benchmarks/fixture/Cargo.toml --all-targets --locked
python -m unittest discover -s benchmarks/tests -v
```

The fixture declares an empty `[workspace]` and its own tracked lockfile. Root Cargo build/test/Clippy/fmt and the MSRV job continue to select only Vise. Its six direct crates are Tokio (async I/O/timers), httparse (bounded HTTP parsing), serde/serde_json (numeric control records), sha2 (expected digests), and socket2 (safe reset injection). The seekable PRNG needs no additional crate. Vise's dependency tree and runtime code are unchanged.

Only generated output is ignored: root `target/`, `benchmarks/fixture/target/`, `__pycache__/` and `*.pyc`. Benchmark sources, tests, scenario defaults and fixture `Cargo.lock` are tracked. No workflows or security settings are changed. CodeQL default setup is allowed to scan the fixture on the PR; its scan time and alerts cannot be established locally. The root Rust Doctor command remains the one in [AGENTS.md](https://github.com/juic3b0x/vise/blob/main/AGENTS.md).

See [architecture](architecture.md) for why Vise journals and safely publishes files, and [usage](usage.md) for its production connection and retry controls.

## Adaptive tail policy comparison

Run the deterministic, standard-library-only model from the repository root:

```sh
python -B benchmarks/tail_simulation.py
python -B -m unittest benchmarks.tests.test_tail_simulation -v
```

The model uses exactly 512 MiB, four equal initial quarters, three 8 MiB/s workers and one 0.8 MiB/s worker, plus a uniform four-by-8 MiB/s case. It assumes fixed worker capacities, 1 ms per-response setup, a 1 MiB minimum, 256 KiB donor margin, 64 KiB alignment, and **64 KiB duplicate overrun per shortened response**. Several splits of one response still close it once. It excludes journal, disk, hashing, startup and publication costs, TCP reassignment, and actual socket buffering. This is a policy model, not a measured speed claim.

| Policy | Heterogeneous finish / first-drop tail | Splits / body requests | Modeled duplicates | Uniform finish |
| --- | --- | --- | --- | --- |
| Fixed quarters | 160.001 / 144.000 s | 0 / 4 | 0 | 16.001 s |
| Halving largest remainder | 22.189 / 6.188 s | 7 / 11 | 128 KiB | 16.001 s |
| Throughput-weighted tails | 20.782 / 4.781 s | 3 / 7 | 64 KiB | 16.001 s |
| Initial 1 MiB pieces | 21.267 / 21.141 s | 0 / 512 | 0 | 16.128 s |

Weighted allocations include all simultaneously idle capacities. Their average modeled duplication is 21.3 KiB per split (64 KiB per closed shortened response), versus 18.3 KiB for halving. These averages are not enforceable network waste bounds. Increasing the assumed overrun to 256 KiB gives weighted finish/tail 21.017/5.016 s approximately; 1 MiB gives 21.954/5.953 s. A response-delay sensitivity of 80 ms yields weighted heterogeneous finish 20.861 s, whereas initial 1 MiB pieces need 33.415 s and uniform pieces need 26.240 s. Frequent request gaps start the first-drop tail early even when serving occupancy later recovers.

For a change comparison, preserve an optimized baseline binary and use `--vise PATH` with separate fresh output directories, the same fixture, seed 424242, connections 4, jobs 5, and profiles. Smoke remains correctness/harness validation only; do not draw performance conclusions from it. On real hardware, retain warm-ups and six measured full runs. The reference targets are heterogeneous median wall time within about 20% of aria2's 25.6 s (30.72 s), tail under 10 s, no consistent regression beyond baseline spread in large/capped/small/latency, and a 320 KiB server-measured waste target. Inspect actual slow-connection count/share. Flaky remains a correctness/recovery comparison, excluded from throughput ranking.

Use `--min-split-size 0` for manual fixed-layout controls. Runtime `split_discarded_bytes` observes only bytes fetched by the client and is not interchangeable with server accepted bytes minus the verified file length. The real hardware run must establish waste and performance; neither the model nor an 8 MiB smoke run can establish them.

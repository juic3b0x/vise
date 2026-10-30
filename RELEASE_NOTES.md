# Vise 0.8.0

*A firm grip on every download.*

Vise 0.8.0 adds adaptive tail chunk splitting, tightens the internal Rust library boundary, and fixes synchronization in the tail-splitting tests. Direct `vise URL` downloads keep their familiar workflow.

## Adaptive tail chunk splitting

- Idle workers take portions of unfinished HTTP ranges, using observed throughput to choose boundaries and conservative halving when observations are unreliable. Splitting requires range support and a strong ETag or Last-Modified validator.
- Tune `--min-split-size BYTES` or `downloads.min_split_size` (default 1048576); zero disables splitting. A task is capped at 1,024 chunks, with refusal cooldowns and bounded retries.
- Follow per-attempt `tail_splits` and `split_discarded_bytes` in snapshots, the dashboard, and WebUI terminal output. Range ownership commits atomically in SQLite, while proxy routing, credential boundaries, shared rate limiting, and checksum verification remain in force.

## Internal library boundary

The Rust library surface is internal and unstable throughout 0.x, including its public items; Vise does not provide a supported embedding API. This refactor reduces implementation visibility while retaining the items needed by the binary and integration tests. Runtime behavior and serialized snapshots are unchanged by the boundary refactor.

The credential logging policy now compiles once in the library, with the binary using the shared initializer at the same startup point. Cookie import stays private to the networking module, and the legacy stream-throttling helper compiles only for its existing unit tests. Production transfers continue to use the shared token bucket.

## Build identification and developer tools

`vise --version` and `-V` include the build commit and committer date when available, with a dirty marker scoped to build inputs. Packager overrides and a package-version-only fallback remain available without runtime Git access.

A developer-only benchmark harness and deterministic tail-splitting simulation provide local fixtures, output-integrity checks, and reproducible reports. Reports now record each available tool’s resolved absolute executable path alongside its version and SHA-256.

## Tail-splitting test fixes

Crash-recovery tests read the complete split-phase acknowledgment before terminating the child, avoiding socket-reset races. The donor-retry fixture waits for the split child’s actual request instead of depending on frame timing. Regression coverage includes acknowledgment validation and a virtual-clock check of donor synchronization.

## Upgrade notes

Stop Vise and take a consistent backup of `~/.vise/` and retain each output directory’s staging files before upgrading. Upgrading from 0.7.0 retains **schema 6** without a schema migration. Unfinished tasks recover as paused; resume them explicitly or with `--resume-all`. Supplied request credentials remain plaintext private journal data, including WAL/SHM and backups; protect those files.

**Browser extension 0.3.0 is unchanged** and remains bundled with Vise. Rust **1.88** remains the minimum supported toolchain. Dependency requirements and resolved dependency versions are unchanged; Cargo.lock changes only for Vise’s root package version.

See [installation](INSTALL.md), the [complete changelog](CHANGELOG.md#080---2026-10-30), and the [security policy](SECURITY.md).

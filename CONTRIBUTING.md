# Contributing to Vise

Vise is built for fast transfers, durable recovery, and predictable operation. Contributions must preserve those properties while keeping the executable and its dependencies lean.

## Start here

Read the root [AGENTS.md](AGENTS.md) before changing code. Its architecture, persistence, testing, and commit standards apply to every contribution. Use the [README](README.md) for supported behavior and [architecture guide](docs/architecture.md) for implementation boundaries.

For a substantial feature, dependency addition, or behavioral change, open an issue describing the problem and proposed approach before investing in a large patch. Small fixes and documentation improvements can go directly to a pull request. Report suspected vulnerabilities through [SECURITY.md](SECURITY.md), without publishing exploit details in an issue or pull request.

## Development setup

Use the latest stable Rust toolchain with Rustfmt and Clippy. The package uses Rust edition 2024 and declares Rust 1.88 as its minimum version; changes to that minimum must be explicit. A C build toolchain is required for bundled SQLite. No external database service or frontend build system is needed.

```sh
git clone https://github.com/juic3b0x/vise.git
cd vise
rustup component add rustfmt clippy
cargo build --locked
```

Work on a focused branch and keep unrelated formatting, refactoring, and lockfile changes out of the patch.

## Engineering requirements

- **Keep the engine independent.** Download workers must remain decoupled from the interactive CLI and Axum handlers. Both interfaces use the same manager and worker pool; communicate through shared state and events rather than duplicating transfer logic.
- **Preserve durable recovery.** Download state and chunk offsets belong in SQLite transactions. Maintain WAL mode and the ordering between durable file writes and saved offsets. Schema changes need migrations that preserve existing tasks and settings. Do not introduce flat-file progress tracking.
- **Respect async execution.** Do not block Tokio workers with database operations, hashing, or blocking filesystem work. Use the existing blocking-work boundaries and never hold a synchronous lock across an await.
- **Handle failures explicitly.** Propagate useful errors, preserve recoverable partial data, and avoid panics on user input, network responses, or filesystem failures. Keep retries bounded and cancellation responsive.
- **Preserve security boundaries.** Keep TLS validation, explicit proxy routing without direct fallback, credential redaction, loopback WebUI guards, and checksum verification before final publication intact. See [SECURITY.md](SECURITY.md).
- **Keep interfaces consistent.** Changes to commands, API payloads, events, or persisted state must account for both CLI and WebUI consumers. Document user-visible behavior and compatibility implications.

### Zero unnecessary dependencies

Vise's zero-bloat philosophy means zero **unnecessary** dependencies. Reuse the standard library and existing crates before adding another dependency. Do not add a framework or package for a small convenience.

Any dependency proposal must explain the concrete need, alternatives considered, enabled features, transitive dependencies, binary-size impact, maintenance status, license compatibility, and effect on the minimum Rust version. Keep test-only tools in dev-dependencies. Commit intentional `Cargo.lock` changes with the corresponding manifest change. Keep the dashboard embedded and lightweight; avoid introducing external CDNs, runtime asset fetching, or a frontend toolchain without a demonstrated need.

## Required checks

Code must compile without warnings, Clippy must report no warnings, and the full test suite must pass before a feature or fix is considered complete. Run from the repository root:

```sh
cargo fmt --all --check
cargo clippy --all-targets --locked -- -D warnings
cargo test --all-targets --locked
cargo test --doc --locked
cargo build --release --locked
```

The CI workflow checks formatting, Clippy, tests, and local installation on Linux, macOS, and Windows. All required CI jobs must pass before merge. For changes affecting packaging or startup, also verify installation in an isolated directory:

```sh
cargo install --path . --locked --root ./target/install-check
./target/install-check/bin/vise --help
```

On Windows, use the installed `vise.exe` executable. Do not silence a warning or weaken a check merely to make CI pass; any narrowly scoped lint allowance needs a documented reason.

Add meaningful regression tests for fixes and behavior changes. Use deterministic local servers and temporary directories instead of public download hosts. Cover relevant failure paths: interrupted transfers, pause/resume, SQLite migration and recovery, range validation, checksum mismatch, rate changes, proxy failure, or sensitive-data redaction. Documentation-only changes do not require artificial tests.

## Commit messages

Use conventional commits with a type, scope, and concise imperative description:

```text
feat(engine): support a new transfer option
fix(database): preserve chunk offsets during recovery
refactor(web): simplify telemetry serialization
test(proxy): cover authentication failure without direct fallback
docs(security): clarify local journal storage
```

Use `feat`, `fix`, `refactor`, `test`, `docs`, `perf`, `build`, `ci`, or `chore` as appropriate. Keep commits coherent and reviewable. Mark intentional breaking changes with `!` after the scope and explain them in a `BREAKING CHANGE:` footer, including the migration path.

## Pull requests

Submit focused pull requests against `main`. Include:

- The problem, resulting behavior, and any related issue.
- The design decisions needed to review the change, including persistence or API compatibility and migration details.
- Validation performed and its results; identify anything untested and why.
- Updated documentation and an entry under `Unreleased` in [CHANGELOG.md](CHANGELOG.md) for notable user-facing changes. Use the appropriate Keep a Changelog category rather than copying commit subjects.
- Screenshots for visible dashboard changes and reproducible measurements for performance claims.

Remove debugging output and sensitive data before submission. Keep credentials, private URLs, downloaded files, local journals, and generated build artifacts out of commits. Address review feedback and rerun relevant checks after changes. A green build does not replace architectural or security review.

## Bug reports

Use [GitHub Issues](https://github.com/juic3b0x/vise/issues) for ordinary bugs. Include the Vise version or commit, operating system, Rust version when building from source, sanitized invocation or API request, expected and actual behavior, and minimal reproduction steps. Describe server range support, proxy scheme, output filesystem, and whether the problem survives a restart when relevant.

Prefer a small local fixture over a private or expiring download link. Scrub credentials, signed URL query parameters, private hostnames, and personal paths from logs and screenshots. Do not attach a real SQLite journal: it can contain authenticated proxy URLs and other sensitive metadata. Follow [SECURITY.md](SECURITY.md) if the bug exposes data or bypasses a security boundary.

## Licensing

Contributions must be compatible with Vise's `MIT OR Apache-2.0` licensing. By submitting a contribution, you agree to provide it under the project's existing [MIT](LICENSE) and [Apache-2.0](LICENSE-APACHE) licensing terms. Identify any third-party material and retain required notices.

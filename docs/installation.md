# Installation

Install Vise from source with Cargo, verify the executable, then choose a [terminal](usage.md), [dashboard](webui.md), or [daemon](daemon.md) workflow. This guide expands the repository's [INSTALL.md](https://github.com/juic3b0x/vise/blob/main/INSTALL.md).

This checkout pairs **Vise 0.8.0** with **extension 0.3.0** and retains Rust **1.88** support. Default-branch installation commands are not pinned to an unpublished tag.

## System requirements

- Rust **1.88 or newer**, including Cargo. Use a current stable toolchain for normal builds.
- Git for cloning or installing directly from the repository.
- A working C compiler and the platform's linker/build tools for bundled SQLite.
- A writable home directory for configuration and the global journal, plus writable destination directories.

Install Rust using the instructions at [rustup.rs](https://rustup.rs/). SQLite is bundled: a separate SQLite runtime or database server is unnecessary. The embedded dashboard and browser extension require no Node.js installation to use.

Vise's release workflow targets Linux x86-64, macOS Intel and Apple Silicon, and Windows x86-64. See the [GitHub releases](https://github.com/juic3b0x/vise/releases) for published archives; the commands below build from source.

## Install with Cargo

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

### Use the dependency lockfile

Pass `--locked` to honor the repository's `Cargo.lock` instead of resolving new dependency versions:

```sh
cargo install --path . --locked
```

Or install directly without a separate checkout:

```sh
cargo install --git https://github.com/juic3b0x/vise.git --locked
```

The Git form builds the repository's default branch. To select a release, add `--tag` with an existing tag from the [release list](https://github.com/juic3b0x/vise/releases).

## Find and verify the executable

Cargo's default executable directory is `~/.cargo/bin/` on Unix and `%USERPROFILE%\.cargo\bin\` on Windows. Ensure it is on `PATH`. A custom `CARGO_HOME` or `cargo install --root` changes that location; see the [Cargo installation reference](https://doc.rust-lang.org/cargo/commands/cargo-install.html).

```sh
vise --version
vise --help
```

The prepared binary's version starts with `vise 0.8.0`; its help includes `--header`, `--referer`, and `--user-agent`. `--version` and `-V` produce identical output. These checks do not create configuration or journal files. If the command is unavailable, reopen the terminal after installing Rust and verify the Cargo binary directory is on `PATH`.

### Build provenance

Git builds include the seven-character commit and its committer date, for example `vise 0.8.0 (a1b2c3d 2026-10-28)`. The date uses the commit's stored timezone, never the build clock. Staged, unstaged, and untracked non-ignored changes within `src/`, `Cargo.toml`, `Cargo.lock`, `build.rs`, and `build_support.rs` add `-dirty` after the commit. Other file edits do not affect this flag. The information is embedded at compile time; the installed binary never invokes Git for version output.

Missing Git, source-only archives, an enclosing unrelated repository, or Git errors produce the clean fallback `vise 0.8.0`. Detached HEAD, linked worktrees, and shallow clones are supported. Tagged builds in the existing release workflow retain the tagged commit and date even with a shallow checkout; no full history is required. A container without `.git`, or a Git safe-directory error, uses the fallback instead.

For builds without Git, set `VISE_GIT_COMMIT` (7–64 hex characters, shortened to seven), optionally `VISE_GIT_DATE` (`YYYY-MM-DD`) and `VISE_GIT_DIRTY` (`true`/`false` or `1`/`0`) during compilation. An explicit commit bypasses Git; an omitted date stays omitted and dirty defaults to false. An empty commit forces package-version-only output. Without a commit override, date/dirty overrides win over successful Git discovery. Invalid overrides safely fall back without failing compilation or echoing values.

```sh
VISE_GIT_COMMIT=deadbee VISE_GIT_DATE=2026-01-02 cargo install --path . --locked
# vise --version: vise 0.8.0 (deadbee 2026-01-02)
```

These are build variables, not daemon configuration keys. The output never includes build-host names, paths, usernames, or build timestamps. Consumers should accept the stable `vise X.Y.Z` prefix plus an optional suffix.

## First startup

```sh
vise --interactive
```

Vise creates the following under your home directory:

| Path | Purpose |
| --- | --- |
| `~/.vise/config.toml` | Persistent preferences and a randomly generated daemon token |
| `~/.vise/state.sqlite3` | Global download history, chunk offsets, and saved rate limit |
| `~/.vise/engine.lock` | Prevents conflicting engine processes |

On Unix, newly created private configuration directories use `0700` permissions and the configuration file uses `0600`. Windows resolves the home from `USERPROFILE`, with `HOMEDRIVE`/`HOMEPATH` as a fallback. Keep this directory on a local filesystem; SQLite WAL is unsuitable for network shares.

The initial destination is `~/Downloads/Vise`. Override it for new tasks with `-o`; saved transfers retain their original absolute destinations. Type `quit` to checkpoint the queue and exit. See [configuration precedence](daemon.md#configuration-and-precedence) before changing server settings.

!!! important "One engine per home directory"
    Stop an interactive, batch, or WebUI session before launching the daemon. While a daemon is running, control it through [vise remote](remote.md), the dashboard, extension, or API.

## Run as a service

Linux systems with systemd can use the supplied [user service unit](vise.service). It runs the daemon in the foreground, resumes saved transfers, and checkpoints on shutdown.

From a checkout, after installing the executable with Cargo:

```sh
install -D -m 644 docs/vise.service ~/.config/systemd/user/vise.service
systemctl --user daemon-reload
systemctl --user enable --now vise.service
```

The unit assumes the default `~/.cargo/bin/vise` installation path. Adjust `ExecStart` if your Cargo installation root differs. The [daemon guide](daemon.md#systemd-user-service) covers authentication, logs, restarts, and operation after logout.

## Set up the browser extension

The repository includes an unpacked Manifest V3 extension for Chrome/Chromium and Firefox. Run the authenticated daemon first, then follow the [browser extension guide](extension.md). The extension is distributed as source here; this documentation does not imply a published browser-store listing.

## Upgrade and recover existing transfers

Stop Vise before replacing its executable. Back up `~/.vise/` after a clean shutdown, including the configuration and journal, and retain staging files in each output directory. Do not copy only `state.sqlite3` while the process is running: committed data may still be in its WAL file.

Upgrading from 0.7.0 to 0.8.0 retains schema 6 without a schema migration. Unfinished tasks recover as paused; resume them explicitly or with `--resume-all`. Adaptive tail splitting preserves durable chunk ownership and existing checksum, proxy, and credential protections.

Upgrading from 0.6.0 adds schema 6’s nullable `request_credentials` column. Existing tasks retain legacy redirect behavior. Supplied credentials become private plaintext journal data; protect backups too. Binaries predating schema 6 reject it, so rollback to those versions requires restoring a consistent pre-upgrade backup.

For older per-output-directory journals, stop all older binaries and visit each old directory once. This shell example imports the previous default destination:

```sh
vise -o "$HOME/Downloads/Vise" --interactive
```

Use `-o` with each other old destination as needed. Vise imports the local SQLite or JSON journal transactionally, keeps the saved staging paths, and records an import marker to prevent replay. Old SQLite files remain unchanged as backups. Then use `vise --resume-all` or the dashboard to resume the global queue from any working directory.

See [global journal ownership and imports](architecture.md#global-journal-ownership-and-imports) for migration guarantees and collision handling.

## Next steps

- [Download and control tasks from the CLI](usage.md).
- [Open the local dashboard](webui.md).
- [Configure an authenticated daemon and API client](daemon.md).

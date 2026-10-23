# Installation

Install Vise from source with Cargo, verify the executable, then choose a [terminal](usage.md), [dashboard](webui.md), or [daemon](daemon.md) workflow. This guide expands the repository's [INSTALL.md](https://github.com/juic3b0x/vise/blob/main/INSTALL.md).

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

These checks do not create configuration or journal files. If the command is unavailable, reopen the terminal after installing Rust and verify the Cargo binary directory is on `PATH`.

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
    Stop an interactive, batch, or WebUI session before launching the daemon. While a daemon is running, control it through the dashboard, extension, or API.

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

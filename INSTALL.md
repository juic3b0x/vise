# Installing Vise

Return to the [README](README.md) for features and usage, or continue to the [quick start](README.md#quick-start) after installation.

This checkout contains **Vise 0.7.0** and **extension 0.3.0**. The commands below use a local checkout or GitHub’s default branch; they do not assume that the new tag has been published.

## System requirements

Install a Rust toolchain with Cargo. Vise declares Rust **1.88 or newer** as its minimum version. Git is also required to clone the repository. A working C build toolchain is needed for bundled SQLite; no separate SQLite installation or database server is required.

## Cargo installation

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

## Verify installation

For the prepared release, version output starts with `vise 0.7.0` and includes build provenance when available; `vise --help` lists the per-task request flags. Confirm the installation:

```sh
vise --version
vise --help
```

### Build identification

`--version` and `-V` produce the same line:

```text
vise 0.7.0 (a1b2c3d 2026-10-28)
vise 0.7.0 (a1b2c3d-dirty 2026-10-28)
```

These are examples: the seven lowercase hexadecimal characters identify the commit actually built. The date is that commit's **committer date**, using its stored timezone, never the build time. Dirty means staged, unstaged, or untracked non-ignored changes within `src/`, `Cargo.toml`, `Cargo.lock`, `build.rs`, or `build_support.rs`. Editing docs, benchmarks, or extension files alone neither marks the build dirty nor reruns the build script; a new commit still refreshes the embedded commit ID.

The information is embedded at compile time. Running the installed binary needs no Git checkout or Git executable and creates no local state. Builds without Git, without a checkout rooted at the package directory, or with failed Git discovery fall back to `vise 0.7.0`. This includes source archives and container builds that do not copy Git metadata. Detached HEAD and shallow checkouts retain the checked-out commit and its date, including ordinary tagged CI builds. Git ownership/safe-directory failures also fall back safely.

Packagers can supply these build-time environment variables without Git:

| Variable | Meaning |
| --- | --- |
| `VISE_GIT_COMMIT` | 7–64 hexadecimal characters; output uses the first seven in lowercase. Setting this selects authoritative override mode and bypasses Git entirely. An empty value selects package-version-only output. |
| `VISE_GIT_DATE` | Optional valid `YYYY-MM-DD` committer date. In override mode, no date is inferred when omitted. Otherwise it overrides the discovered date. |
| `VISE_GIT_DIRTY` | Optional `true`/`false` or `1`/`0`. Overrides discovered dirty state; defaults to false in authoritative override mode. |

Invalid override values produce package-version-only output without failing the build or echoing values. For example, in a POSIX shell:

```sh
VISE_GIT_COMMIT=deadbee VISE_GIT_DATE=2026-01-02 cargo install --path . --locked
# Installed output: vise 0.7.0 (deadbee 2026-01-02)
```

These variables affect compilation, not runtime settings. No machine paths, usernames, hostnames, or build timestamps are embedded in the version string. The `vise X.Y.Z` prefix remains stable; tools should allow an optional provenance suffix.

Local source installation and Git-source installation have been verified in isolated installation directories. The Git check used a local repository; installation from the live GitHub repository has not yet been verified.

## Systemd user service

For a systemd user service, install Vise with Cargo, then install the provided unit:

```sh
install -D -m 644 docs/vise.service ~/.config/systemd/user/vise.service
systemctl --user daemon-reload
systemctl --user enable --now vise.service
```

The example loads `~/.vise/config.toml`, creating it on first startup, and resumes saved transfers. Edit the file's host, port, default destination for new tasks, or token and restart the service to apply changes. All modes share `~/.vise/state.sqlite3`; changing the default destination does not move existing tasks. Run one engine per home directory and use the daemon API/WebUI while the service is active. An optional `~/.config/vise/daemon.env` file can override `VISE_API_TOKEN`. User services that must survive logout also need user lingering enabled by the system administrator. See [docs/vise.service](docs/vise.service).

## Terminal control of a running daemon

While the daemon is active, `vise remote list`, `vise remote add URL`, and `vise remote watch` provide native terminal control without opening another engine. See the [remote CLI guide](docs/remote.md).

## Browser extension

The source includes a dependency-free Manifest V3 extension in [extension/](extension/). Start an updated daemon built from this checkout; older binaries may not support authenticated extension origins.

### Chrome / Chromium

1. Open `chrome://extensions` in Chrome/Chromium 121 or newer.
2. Enable **Developer mode**, choose **Load unpacked**, and select the repository's `extension/` directory.
3. Click the Vise toolbar icon to configure the URL and Bearer token. Test the connection and save settings.

### Firefox

1. Open `about:debugging#/runtime/this-firefox` in Firefox 142 or newer.
2. Choose **Load Temporary Add-on** and select `extension/manifest.json`.
3. Open the extension's **Preferences**, or click its toolbar icon, then configure, test, and save the daemon connection.

Temporary Firefox add-ons are removed when Firefox closes. Persistent distribution requires a signed package; this repository does not claim a published Chrome Web Store or Mozilla Add-ons listing.

If your browser requests host/site access, allow it for the configured `127.0.0.1` or `localhost` address. Copy the token from `daemon.VISE_API_TOKEN` in `~/.vise/config.toml`, or use the `VISE_API_TOKEN` environment override used to launch the daemon. Right-click an HTTP(S) link and select **Send to Vise**. Credential forwarding is off by default. To opt in, independently enable cookie/referer forwarding, grant the download site with **Grant site access**, and save preferences. Runtime host grants cover that scheme/host on all ports; cookie permission is optional. Cookie forwarding requires an identifiable tab store and partition (Chrome 132+ lookup); current Firefox safely refuses this path. **Send URL only to Vise** always bypasses browser credential capture. The daemon journals supplied credentials as plaintext, so protect `~/.vise/`, its WAL/SHM files, and backups. See the [extension guide](extension/README.md) for permissions, privacy, and troubleshooting.

## Upgrading to 0.7.0

Stop Vise and make a SQLite-consistent backup of `~/.vise/`, including its configuration, WAL/SHM where applicable, and credentials, before upgrading. First startup upgrades schema 5 to schema 6 additively. Existing tasks retain their legacy request behavior; newly supplied request credentials are saved privately for resume. Older binaries cannot read schema 6: restore the pre-upgrade backup before rolling back rather than opening the upgraded journal with an older executable.

## Upgrading from per-directory journals

Vise 0.4.0 stores its database at `~/.vise/state.sqlite3`. For each older output directory, stop older Vise processes and start the new version once with `vise -o /absolute/path/to/downloads --interactive`. Its local SQLite or JSON journal is imported automatically; staging data stays in place. Afterwards, `vise --resume-all` and the daemon/WebUI recover every imported destination without repeating `-o`. Old SQLite journals remain unchanged as backups, and imports are recorded transactionally to prevent replay after task deletion. See [recovery and migration](README.md#interactive-mode).

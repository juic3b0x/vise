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

For the prepared release, `vise --version` reports `vise 0.7.0`; `vise --help` lists the per-task request flags. Confirm the installation:

```sh
vise --version
vise --help
```

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

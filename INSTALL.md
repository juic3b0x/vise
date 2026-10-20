# Installing Vise

Return to the [README](README.md) for features and usage, or continue to the [quick start](README.md#quick-start) after installation.

## System requirements

Install a Rust toolchain with Cargo. Vise declares Rust **1.85 or newer** as its minimum version. Git is also required to clone the repository. A working C build toolchain is needed for bundled SQLite; no separate SQLite installation or database server is required.

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

Confirm the installation:

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

The example loads `~/.vise/config.toml`, creating it on first startup, and resumes saved transfers. Edit the file's host, port, output path, or token and restart the service to apply changes. An optional `~/.config/vise/daemon.env` file can override `VISE_API_TOKEN`. User services that must survive logout also need user lingering enabled by the system administrator. See [docs/vise.service](docs/vise.service).

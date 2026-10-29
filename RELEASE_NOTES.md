# Vise 0.7.0

*A firm grip on every download.*

Vise 0.7.0 adds restart-safe authenticated downloads across the CLI, API, dashboard, and browser extension. Direct `vise URL` downloads keep their familiar workflow.

## Authenticated downloads that survive restart

- Add repeatable `--header 'Name: value'`, `--referer URL`, and `--user-agent STRING` to local downloads and `vise remote add`. Set a default user agent with `[downloads].user_agent`.
- Supply structured API cookies with domain, hostOnly, path, secure, expiry, and httpOnly metadata. Saved task credentials cover probes, chunks, retries, and resume.
- Keep credentialed batches on one origin. Cross-origin redirects permanently strip credentials and custom user agents for that redirect chain, reducing a supplied referer to its origin.

## Private controls in the dashboard and extension

The dashboard’s collapsed **Advanced request settings** accepts write-only credentials, clears them after submission, and shows only public names/counts and presence flags.

**Extension 0.3.0** adds independent cookie and referer opt-ins, both off by default, with per-site consent and runtime permissions. Cookie forwarding requires a trustworthy tab-store/frame-partition lookup. Chrome 132+ supports that lookup; current Firefox and older supported Chrome versions safely refuse cookie forwarding. URL-only and independent referer forwarding remain available. **Send URL only to Vise** explicitly bypasses capture.

## Security and fixes

This release fixes cookie exposure in dependency debug/trace logging that existed before this update: Netscape cookie imports and response cookies could appear in CLI/daemon logs. The binary now suppresses the audited cookie-store logging target regardless of `RUST_LOG`, including bridged records, and protects newly supplied cookies as well. Other networking diagnostics remain available.

It also fixes Windows Clippy compatibility, a macOS-sensitive WAL snapshot test, and dashboard rendering of flattened credential-summary fields. The compatible dependency refresh updates `unicode-width` 0.2.2 → 0.2.3 without raising the Rust 1.88 minimum.

## Upgrade notes

Stop Vise and take a consistent backup of `~/.vise/` before upgrading. Schema 5 upgrades additively to **schema 6**; existing tasks with no request context retain legacy behavior. Older binaries cannot read schema 6, so restore a pre-upgrade backup before rolling back.

Supplied request credentials are **plaintext private journal data**, including WAL/SHM and backups. Protect those files and browser profiles. Unix credential persistence requires mode `0600`; Windows relies on protected account ACLs. Daemon/WebUI startup rejects process-wide headers and referers; submit them per task. Use HTTPS for authenticated download destinations. Browser network inspectors, shell history, and process listings remain outside application log redaction.

See [installation](INSTALL.md), the [complete changelog](CHANGELOG.md#070---2026-10-28), and the [security policy](SECURITY.md).

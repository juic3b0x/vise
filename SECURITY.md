# Security Policy

Vise handles remote content, local files, and potentially authenticated proxy routes. Report suspected security issues privately so maintainers can investigate and coordinate a fix without exposing users.

## Reporting a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/juic3b0x/vise/security/advisories/new) when it is enabled: open the repository's security advisories and choose **Report a vulnerability**. Keep reproduction details, exploit code, and affected user data inside the private report.

If that option is unavailable, use a verified private contact published by [the maintainer](https://github.com/juic3b0x). If no private contact is listed, open a [minimal issue requesting a private security contact](https://github.com/juic3b0x/vise/issues/new), with no vulnerability details, affected component, logs, or proof of concept. Wait for a private channel before sharing details. This fallback follows [GitHub's reporting guidance](https://docs.github.com/en/code-security/how-tos/report-and-fix-vulnerabilities/report-privately).

Do not disclose a vulnerability in a public issue, pull request, discussion, or commit before coordination. Ordinary bugs that do not affect security can follow [CONTRIBUTING.md](CONTRIBUTING.md).

Include in the private report:

- A concise description of the vulnerability, affected boundary, and potential impact.
- The affected Vise version or commit, operating system, and relevant configuration.
- Minimal reproduction steps or a safe local proof of concept, with expected and actual results.
- Sanitized logs or requests and any proposed mitigation or fix.
- A way to contact you privately and your preference for public credit.

Use synthetic credentials and files. Do not send real secrets, private download content, or an unsanitized journal. Test only systems you own or have permission to assess; avoid accessing other users' data or disrupting public services.

## Handling and disclosure

Maintainers will triage reports, seek clarification as needed, and coordinate remediation and disclosure through the private channel. Publication should follow an available fix or a mutually agreed disclosure date. Discuss any urgent disclosure constraint in the report. Credit is offered with the reporter's consent.

There is no guaranteed response deadline, paid bounty, or published long-term support schedule. Security reports are accepted for the current `main` development branch. Older snapshots receive best-effort investigation; fixes target current development unless a backport is explicitly announced. Reproducing against current code helps triage, but is not required to submit a report.

## Transport encryption

HTTPS downloads use Reqwest with Rustls and certificate verification enabled. HTTPS proxy connections also validate the proxy's certificate. Contributions must not bypass certificate validation or silently downgrade HTTPS to HTTP.

HTTP downloads are supported and transmit content without TLS. An HTTP proxy connection does not encrypt the client-to-proxy hop; HTTPS destinations use a CONNECT tunnel with destination TLS. An HTTPS proxy encrypts the proxy hop. SOCKS5 and SOCKS5h provide routing, not encryption by themselves: use HTTPS destinations for encrypted content transport. Proxy authentication over an unencrypted proxy connection can be exposed to observers of that hop.

## Proxy bypass prevention

An explicitly configured per-transfer proxy is a required route. Probes, chunk requests, redirects, retries, and resumed transfers must use it. An unavailable or rejected proxy must cause a routed retry or failure, never a direct fallback. Explicit routes override environment proxy and `NO_PROXY` settings.

Each transfer journals its effective route. A later `--proxy` value sets the default for new tasks and does not replace saved routes. With no explicit proxy, Reqwest's environment-proxy behavior applies; Vise does not promise proxy-only operation for such tasks.

`socks5://` resolves destination names locally. Use `socks5h://` when destination DNS must be delegated to the SOCKS proxy. These options are not a system-wide anonymity service; resolving the proxy's own hostname may still require local DNS. Any bypass of an explicit route or unexpected direct destination connection is a security issue.

## Credentials and local data

Proxy credentials must be scrubbed from public API/telemetry snapshots, application log messages, and display/debug output. Current proxy snapshots expose the route without user information plus a `proxy_authenticated` flag. Invalid proxy diagnostics must not echo the raw input. Changes to error handling or dependency logging must preserve these protections.

This redaction does not cover every kind of secret. Download URLs, including signed query parameters, and local paths can appear in transfer metadata, dashboard output, and logs. Avoid sharing those values. Command-line credentials can also enter shell history or process listings. Sanitize reports and screenshots even when application redaction is present.

The SQLite journal is **not encrypted**. It stores full authenticated proxy URLs to resume transfers, download URLs, output paths, chunk offsets, and checksum records. Staging files and final downloads are also unencrypted by Vise. Protect the output directory and `.vise` directory with appropriate filesystem permissions; use filesystem or disk encryption when encryption at rest is required. Treat SQLite `-wal` and `-shm` files and legacy journal backups as sensitive too. Do not commit journals, credentials, or private downloads to source control.

Vise assumes its journal and staging directory are trusted local state. Do not import another person's journal into a privileged or sensitive environment. Vise is not a sandbox or malware scanner; downloaded files remain untrusted content.

## Integrity and final publication

When expected checksums are available, Vise verifies the complete staging file before publishing the final destination. All supplied algorithms must match. A mismatch is recorded as `corrupt`, expected and actual hashes are retained, and the staging file remains available for inspection or explicit re-download. Final publication must not overwrite an existing output file.

Use SHA-256 obtained through a trusted publisher channel for security-sensitive checks. MD5 supports legacy integrity checks and accidental-corruption detection; it is not collision-resistant. A checksum delivered by the same untrusted server does not independently establish authenticity. Downloads without an expected checksum do not receive cryptographic verification. SQLite durability and range validation protect recovery consistency, not the trustworthiness of downloaded content.

## WebUI trust boundary

The WebUI runs only with `--webui`, defaults to `127.0.0.1:8080`, and accepts loopback binds only. Use the exact numeric loopback URL printed at startup. Host, Origin, and cross-site checks, JSON-only mutations, and the Content Security Policy must remain enabled.

The local API has no built-in user authentication. Other local processes capable of reaching it can control downloads and access transfer metadata. Run it only in a trusted local environment; do not expose it through a public reverse proxy or disable the loopback checks. Use an authenticated SSH tunnel if remote access is needed, while preserving the expected host and origin. These browser protections do not provide isolation between local users or replace authentication.

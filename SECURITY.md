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

## WebUI and daemon trust boundaries

The interactive WebUI runs with `--webui`, defaults to `127.0.0.1:8080`, and accepts loopback binds only. Use the exact numeric loopback URL printed at startup. Host, Origin, and cross-site checks, JSON-only mutations, and the Content Security Policy must remain enabled.

The `--webui` API and a loopback daemon without a configured file or environment token have no user authentication. Other local processes capable of reaching them can control downloads and access transfer metadata. Run these configurations only in a trusted local environment. Use an authenticated SSH tunnel for remote access to the local WebUI, preserving its expected host and origin.

`--daemon` additionally supports remote numeric IPv4/IPv6 binds. A non-loopback bind requires a valid token before any transfer journal or listener is opened. First startup creates a random 64-character token in `~/.vise/config.toml` under `daemon.VISE_API_TOKEN`; the environment variable `VISE_API_TOKEN` overrides it. The original `auth_token` key is accepted as an alias. Tokens must contain 32–256 ASCII letters, digits, `-`, or `_`. A configured token also enables authentication on loopback; an explicitly empty or omitted token in an existing file disables authentication there only.

Every authenticated daemon route, including static assets and WebSocket upgrades, requires the configured token via Bearer authentication or HTTP Basic authentication (username `vise`, password equal to the token). Invalid credentials receive a generic HTTP 401 response without echoing the supplied value. Tokens are hashed in server state and are not written to the transfer journal or snapshots. The TOML file contains the plaintext token and any configured proxy credentials. Newly generated Unix files use mode `0600` inside a `0700` directory; existing permissions are preserved. On Windows, protect the home directory with account ACLs. Keep configuration files and service environment files private; rotate a token by changing its source and restarting the service. TOML parse errors omit source values so malformed credentials cannot appear in diagnostics.

The embedded daemon listener is HTTP, not HTTPS. Use an HTTPS reverse proxy or authenticated encrypted tunnel before sending credentials or sensitive metadata across an untrusted network. Preserve the original Host header and browser origin when proxying; cross-origin requests remain forbidden, and no permissive CORS policy is added. Daemon authentication is a single shared administrative credential, not per-user authorization. Authorized clients can control all transfers in that output directory.

The browser extension uses narrow loopback host permissions and an explicit Bearer token. Daemon requests with a valid serialized `chrome-extension://` or `moz-extension://` origin may bypass same-origin/cross-site checks only after Bearer authentication; unauthenticated servers and cached Basic authentication never qualify. Other website origins remain forbidden and do not receive CORS access. Extension settings keep the token in unencrypted local browser storage, without syncing it to an account. Protect the browser profile as well as Vise's configuration. The extension forwards only explicitly selected HTTP(S) links, omits browser cookies/referrers, rejects remote API endpoints and redirects, and never retries an ambiguous POST automatically.

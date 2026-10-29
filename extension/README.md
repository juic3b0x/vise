# Vise browser extension

Version **0.3.0**, bundled with Vise **0.7.0**. Use the matching daemon for the request-credential API.

Right-click an HTTP(S) link and select **Send to Vise** to queue it in your local download manager. Clicking the toolbar icon opens settings. There is no automatic download interception, page scraping, analytics, or third-party service.

## Setup

1. Build/install Vise from this checkout and start `vise --daemon`. This checkout includes the authenticated extension-origin support; an older daemon build may reject requests with HTTP 403.
2. Load the extension in Chrome/Chromium 121+ or Firefox 142+ using the [installation guide](../INSTALL.md#browser-extension).
3. Click the Vise toolbar icon, enter the daemon URL (initially `http://127.0.0.1:8080`), and paste `daemon.VISE_API_TOKEN` from `~/.vise/config.toml`. If you launched Vise with an environment token, use that value instead.
4. Select **Test connection**, then **Save settings**. Right-click a link to send it.

The options page accepts a base URL or its `/api/downloads` endpoint, with any nonzero port. The extension connects only to `http://127.0.0.1` and `http://localhost`, matching its narrow host permissions. Configure a local tunnel if your daemon runs elsewhere. Browser host/site permissions must be enabled for the selected loopback host.

## Privacy and failure handling

- Address/token settings use `chrome.storage.local` (also supported by Firefox's Chrome-compatible API), never sync storage. This storage is not encrypted: protect your browser profile. No content scripts or externally accessible message handlers are installed. Chromium storage access is restricted to trusted extension contexts where that API is available.
- By default, only the clicked link URL is sent, as `POST /api/downloads` with JSON and an explicit Bearer token. Cookie and referer forwarding have independent opt-in switches, both off by default. Page content is never read. An explicit **Send URL only to Vise** menu action bypasses both switches. Embedded URL credentials and non-HTTP(S) links are rejected.
- Endpoint validation is repeated before each request. Remote hosts, redirects, query strings, and embedded endpoint credentials are rejected to keep the token on loopback.
- Notifications report queue acceptance or a readable failure without displaying tokens, link query strings, or raw server error bodies. A success notification means queued, not finished.
- Failed or timed-out POSTs are never retried automatically. A timed-out request may already be queued; check Vise before sending the link again.
- `blob:` URLs and page-generated downloads are unsupported. Forwarded cookies are a one-time snapshot, not a live browser session; expired or refreshed sessions need a new admission.

The daemon permits serialized Chrome/Firefox extension origins only when an explicit Bearer token is valid. Unauthenticated local WebUI origins, cached Basic credentials, and unrelated website origins do not receive this exception. No permissive website CORS policy is enabled.

## Optional cookie and referer forwarding

In settings, enable each preference separately. Enter the download site and press **Grant site access** to request its exact scheme/host permission during the button gesture; no wildcard site grant is requested. Browser match patterns cover all ports and paths of that host. Cookie permission is requested only when its switch is checked. Then save preferences. **Revoke forwarded-site access** removes optional site/cookie access while preserving the required loopback connection. Preferences alone do not grant permission, and grants alone do not enable forwarding.

On each context-menu click, the extension revalidates permissions and reads cookies only for that link URL in the clicked tab’s identified cookie store and frame partition. `hostOnly`, domain, path, secure, expiry/session, and httpOnly are preserved. Referer comes from the click’s page URL, with its fragment removed. Capture failures stop the credentialed send and offer **Send URL only to Vise**, without automatically submitting or retrying. That fallback sends no browser credentials but does not disable daemon-side defaults.

A trustworthy `cookies.getPartitionKey` lookup is required: Chrome 132+ exposes it; Chrome 121–131 and current Firefox cannot satisfy this capture path, so cookie forwarding is refused there. Firefox URL-only and independent referer forwarding continue to work. Ambiguous container/private stores, missing partitions, and first-party-isolation results are never guessed. See the [full compatibility and privacy guide](../docs/extension.md#browser-compatibility-and-safe-fallback).

Cookies and referers go only in admission JSON, never the daemon Authorization header. Storage contains connection settings, forwarding booleans, and explicit site-consent patterns; the browser maintains actual grants. Both consent and a current grant are required, even for loopback download sites. Captured values are never saved in extension storage, logs, notifications, or URLs. The daemon **does** persist supplied credentials in its private plaintext SQLite journal for resume, including WAL/SHM and backups. Protect the journal and browser profile. Network inspectors may show the outbound request body. Redirects use the daemon’s initial-origin policy and cannot restore credentials after a cross-origin hop.

## Development

No build step, npm dependencies, or frontend framework is required. With Node.js 22+:

```sh
node extension/tests/extension.test.cjs
node --check extension/common.js
node --check extension/background.js
node --check extension/options.js
```

`icons/icon.svg` extends Vise's existing jaw-and-arrow mark. The PNG files are bundled at 16, 32, 48, and 128 pixels for browser compatibility.

One Manifest V3 manifest declares both `background.service_worker` (Chrome) and `background.scripts` (Firefox). Firefox's validator may report that `service_worker` is ignored; its `scripts` entry supplies the event page. See [MDN's cross-browser background guidance](https://developer.mozilla.org/en-US/docs/Mozilla/Add-ons/WebExtensions/manifest.json/background#browser_support) and [host permissions](https://developer.mozilla.org/en-US/docs/Mozilla/Add-ons/WebExtensions/manifest.json/host_permissions).

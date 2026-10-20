# Vise browser extension

Right-click an HTTP(S) link and select **Send to Vise** to queue it in your local download manager. Clicking the toolbar icon opens settings. There is no automatic download interception, page scraping, analytics, or third-party service.

## Setup

1. Build/install Vise from this checkout and start `vise --daemon`. This checkout includes the authenticated extension-origin support; an older daemon build may reject requests with HTTP 403.
2. Load the extension in Chrome/Chromium 121+ or Firefox 142+ using the [installation guide](../INSTALL.md#browser-extension).
3. Click the Vise toolbar icon, enter the daemon URL (initially `http://127.0.0.1:8080`), and paste `daemon.VISE_API_TOKEN` from `~/.vise/config.toml`. If you launched Vise with an environment token, use that value instead.
4. Select **Test connection**, then **Save settings**. Right-click a link to send it.

The options page accepts a base URL or its `/api/downloads` endpoint, with any nonzero port. This first version deliberately connects only to `http://127.0.0.1` and `http://localhost`, matching its narrow host permissions. Configure a local tunnel if your daemon runs elsewhere. Browser host/site permissions must be enabled for the selected loopback host.

## Privacy and failure handling

- Address/token settings use `chrome.storage.local` (also supported by Firefox's Chrome-compatible API), never sync storage. This storage is not encrypted: protect your browser profile. No content scripts or externally accessible message handlers are installed. Chromium storage access is restricted to trusted extension contexts where that API is available.
- Only the clicked link URL is sent, as `POST /api/downloads` with JSON and an explicit Bearer token. Cookies, page content, and page referrers are omitted. Embedded URL credentials and non-HTTP(S) links are rejected.
- Endpoint validation is repeated before each request. Remote hosts, redirects, query strings, and embedded endpoint credentials are rejected to keep the token on loopback.
- Notifications report queue acceptance or a readable failure without displaying tokens, link query strings, or raw server error bodies. A success notification means queued, not finished.
- Failed or timed-out POSTs are never retried automatically. A timed-out request may already be queued; check Vise before sending the link again.
- Sites that require browser cookies, expiring sessions, `blob:` URLs, or special request headers may not provide a directly downloadable link. The extension does not transfer browser sessions to Vise.

The daemon permits serialized Chrome/Firefox extension origins only when an explicit Bearer token is valid. Unauthenticated local WebUI origins, cached Basic credentials, and unrelated website origins do not receive this exception. No permissive website CORS policy is enabled.

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

# Browser extension

The lightweight Manifest V3 extension adds **Send to Vise** to HTTP(S) link context menus in Chrome/Chromium and Firefox. It sends the selected URL to your authenticated local daemon and reports queue admission through browser notifications.

## Requirements

- An updated Vise executable built from this repository, with authenticated extension-origin support.
- Chrome/Chromium **121 or newer**, or Firefox **142 or newer**, as declared in the manifest.
- A repository checkout or release archive containing the `extension/` directory.
- A daemon listening at `http://127.0.0.1:8080` or another supported loopback port.

Start the daemon, keeping it in the foreground unless a service manager already runs it:

```sh
vise --daemon --resume-all
```

Copy the generated token from `daemon.VISE_API_TOKEN` in `~/.vise/config.toml`, or use the environment override with which you launched the service. Keep it private. The unauthenticated `--webui` mode does not grant extension-origin access; use `--daemon` for this integration.

## Chrome and Chromium

1. Open `chrome://extensions`.
2. Enable **Developer mode**.
3. Choose **Load unpacked** and select the repository's `extension/` directory.
4. Click the Vise toolbar icon to open settings; pin the icon if convenient.
5. Follow [connection setup](#configure-and-test-the-connection).

## Firefox

1. Open `about:debugging#/runtime/this-firefox`.
2. Choose **Load Temporary Add-on**.
3. Select `extension/manifest.json` from the checkout.
4. Click its toolbar icon, or open the add-on's **Preferences**.
5. Follow [connection setup](#configure-and-test-the-connection).

Temporary Firefox add-ons disappear when the browser closes. Persistent distribution requires Mozilla signing. The repository does not claim a published Chrome Web Store or Mozilla Add-ons listing.

## Configure and test the connection

Enter the following in the extension's settings:

| Field | Value |
| --- | --- |
| Daemon URL | `http://127.0.0.1:8080` for the initial daemon settings |
| Authentication token | The actual generated or environment-supplied Vise token |

The settings page also accepts the URL ending in `/api/downloads`. If you change the daemon port, update the extension URL to match. Click **Test connection**, then save the settings. If the browser requests host/site access, allow the configured `127.0.0.1` or `localhost` endpoint.

Now right-click an HTTP(S) link and choose **Send to Vise**. A success notification means the daemon accepted the task, not that the file finished downloading. Follow progress in the [dashboard](webui.md) served by the daemon.

## Forward request credentials

Forwarding starts **off**, including after upgrading an existing installation. The normal **Send to Vise** action keeps URL-only behavior until you explicitly enable one or both settings:

1. Open extension settings. Independently check **Forward cookies for the clicked link** and/or **Forward the context-menu page URL as referer**.
2. Enter the download site, for example `https://downloads.example.com`, and select **Grant site access**. The browser permission prompt runs only from this button gesture. It requests this scheme/host, without a wildcard subdomain; browser host grants necessarily cover all paths and ports on that host. Cookie API permission is requested only when its switch is checked. Granting a site does not enable either preference.
3. Save your preferences, then right-click the link and choose **Send to Vise**. The action rechecks the actual browser permissions on every click. Revoke access with **Revoke forwarded-site access**, or through the browser’s permissions UI. Revoking does not remove required loopback daemon access.

The referer comes from the context-menu event’s **page URL**, not a later tab lookup or a scraped page. Fragments are removed; a non-HTTP(S) or unavailable page URL stops credential forwarding. Only the selected link is queued. No content scripts, page scraping, or automatic interception are installed.

Cookie capture uses only the clicked link’s URL, with an explicitly identified store containing the clicked tab ID; Firefox’s `cookieStoreId`, when supplied, must agree. It also requires the browser’s frame-specific `cookies.getPartitionKey` result. Unpartitioned cookies and cookies for that exact partition are read separately, without enumerating other partitions. Domain/hostOnly, path, secure, expiry/session and httpOnly metadata are preserved in structured admission JSON. Unexpected store, partition, first-party-isolation, or scope results stop the whole credentialed send; the extension never substitutes its default store.

### Browser compatibility and safe fallback

Cookie forwarding requires `cookies.getPartitionKey` (Chrome 132+) with a complete partition key, including `hasCrossSiteAncestor`. **Firefox currently lacks this lookup, so this extension refuses cookie forwarding there**, including container/private contexts it cannot prove safe. Chrome 121–131 also uses this refusal. Firefox still supports URL-only and separately enabled referer forwarding. This restriction is deliberate: deriving a partition from a hostname or reading every container’s cookies could forward a different session. See the official [Chrome cookie API](https://developer.chrome.com/docs/extensions/reference/api/cookies) and [Firefox partitioning documentation](https://developer.mozilla.org/en-US/docs/Mozilla/Add-ons/WebExtensions/API/cookies).

When a store/partition is unavailable, permission is missing/revoked, or capture fails, the generic notification offers **Send URL only to Vise**. That separate context-menu action sends only `{ "url": ... }`, ignoring both forwarding switches and reading no cookies. It never silently downgrades or automatically retries the original credentialed send. URL-only tasks still inherit any cookie-file/user-agent defaults configured in the daemon; the extension cannot erase those server settings.

Forwarded cookies are a one-time snapshot, not a live browser session. Vise merges them after daemon cookie-file imports and persists supplied credentials for resume. Response cookies stay in memory. The task’s origin-aware redirect policy, rather than browser SameSite/partition behavior, governs later requests; credentials stay disabled after leaving the admission origin. Browser-only metadata such as SameSite and partition keys is used for capture selection, not persisted as a browser browsing context. See the [request credential threat model](https://github.com/juic3b0x/vise/blob/main/SECURITY.md#per-task-request-credentials).

## Privacy and security

Required permissions remain context menu, local storage, notifications, and loopback hosts. Optional manifest declarations permit HTTP(S) site grants and cookie access, but do not grant broad access at installation; the settings button requests one site at a time. The extension has no content scripts or analytics and does not collect browsing history. Page content is never read.

Daemon connection settings and the two boolean preferences are stored in `storage.local`, not browser sync. Explicit site-consent patterns are also saved (scheme and host only); actual permission grants are maintained by the browser and rechecked before capture. Captured cookie values and referers are never stored, logged, placed in notifications, or embedded in URLs. They travel only in the admission JSON body, while the daemon token remains exclusively in its Authorization header; task credentials never become daemon authentication. The token is intentionally saved so the extension can connect. This storage is not an encrypted secret vault. Treat access to your browser profile as access to the token, and rotate the daemon token if the profile is compromised. Restart the daemon and update extension settings after rotating it.

The daemon stores forwarded credentials in its plaintext private journal, WAL/SHM files, and backups for restart-safe resume; protect these files. Browser developer tools may expose admission bodies in network inspectors, which are outside the application’s log redaction.

Every request revalidates the endpoint. Only loopback HTTP destinations are supported by the extension's host permissions and content security policy. Redirects are refused. Endpoint URLs cannot carry credentials, query strings, or fragments; link URLs with embedded credentials and non-HTTP(S) schemes are rejected.

For a remote Vise service, use an authenticated local tunnel rather than broadening browser permissions. Protect the tunneled service with its daemon token as well. The [daemon guide](daemon.md#authentication-and-remote-access) explains encrypted remote access.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Cannot reach Vise | Daemon is running; URL/port match; browser grants loopback access |
| Unauthorized | Token matches the effective daemon configuration/environment |
| Forbidden | Use authenticated daemon mode; an unauthenticated WebUI does not allow extension-origin requests |
| No menu item | Right-click an actual HTTP(S) link, not page background or a `blob:`/`file:` URL |
| Timed out after sending | Inspect the queue before trying again; the task may already have been admitted |
| Credentials not sent | Grant the download site; verify the store/partition is supported, or use **Send URL only to Vise** |
| Firefox extension vanished | Temporary add-ons must be reloaded after browser restart |

The extension deliberately avoids automatically retrying an ambiguous POST, which could queue duplicates. Queue errors appear as notifications without printing credentials.

## Extension development

Source and further implementation notes live in [extension/](https://github.com/juic3b0x/vise/tree/main/extension). There is no frontend build step or npm dependency tree. From the repository root, run the same checks as CI with Node.js 22:

```sh
node extension/tests/extension.test.cjs
node --check extension/background.js
node --check extension/common.js
node --check extension/options.js
```

Reload an unpacked/temporary extension after editing its files. For backend behavior, see the [queue API](daemon.md#rest-api) and [architecture](architecture.md).

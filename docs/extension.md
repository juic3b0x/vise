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

## Privacy and security

The extension requests only context menu, local storage, notification, and loopback host permissions. It has no content scripts or analytics and does not collect browsing history. Only the clicked link URL is submitted; browser cookies, page contents, and referrers are not forwarded.

Settings are stored in `storage.local`, not browser sync. This storage is not an encrypted secret vault. Treat access to your browser profile as access to the token, and rotate the daemon token if the profile is compromised. Restart the daemon and update extension settings after rotating it.

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

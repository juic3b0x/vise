---
hide:
  - navigation
  - toc
---

<div class="vise-hero" markdown>

![Vise logo: a download arrow held by vise jaws](logo.png){ width="96" height="96" }

<span class="vise-eyebrow">Rust download manager / CLI · daemon · WebUI</span>

# Vise

<p class="vise-tagline">A firm grip on every download.</p>

Parallel transfers. Durable checkpoints. Precise bandwidth control.
Vise brings a fast terminal workflow and an embedded industrial dashboard to the same download engine.

[Get started](installation.md){ .md-button .md-button--primary }
[Explore the CLI](usage.md){ .md-button }
[View on GitHub](https://github.com/juic3b0x/vise){ .md-button }

</div>

## Built to keep going

<div class="grid cards" markdown>

- **Parallel where it counts**

    Tokio and Reqwest stream eligible files across concurrent chunks. Configure connections per file and the number of simultaneous downloads.

    [Understand chunking →](architecture.md#transfers-and-recovery)

- **Recovery you can inspect**

    SQLite transactions preserve chunk offsets and absolute destinations. Restart, review the saved queue, and resume without moving your files.

    [Explore the journal →](architecture.md#sqlite-journal)

- **One shared speed budget**

    A global token bucket controls all active files and chunks. Adjust it live from the terminal, dashboard, or REST API.

    [Control bandwidth →](usage.md#bandwidth-control)

- **Verify before publishing**

    SHA-256 and MD5 checks run before the final filename becomes available. Mismatches retain staging data and mark the task corrupt.

    [Verify a download →](usage.md#checksum-verification)

- **Your preferred controller**

    Indicatif keeps terminal progress readable. Axum serves the embedded WebUI and authenticated daemon API only when explicitly requested.

    [Choose a mode →](usage.md#core-modes)

- **Authenticated routes**

    Import Netscape cookies and save per-task HTTP, HTTPS, SOCKS5, or SOCKS5h proxy routes. Explicit proxy failures never fall back to a direct route.

    [Configure sessions and proxies →](usage.md#cookies-and-proxies)

</div>

## Start in the terminal

Install from GitHub with the repository's dependency lockfile:

```sh
cargo install --git https://github.com/juic3b0x/vise.git --locked
vise https://www.rust-lang.org/static/images/rust-logo-blk.svg
```

Tune a transfer or open the browser controller:

```sh
vise -c 8 --limit-rate 5M -o ./downloads https://www.rust-lang.org/static/images/rust-logo-blk.svg
vise --webui --bind 127.0.0.1:8080
```

Open [http://127.0.0.1:8080](http://127.0.0.1:8080) for the dashboard. Stop the first Vise session before starting another: each home directory has one engine owner.

For unattended use, run `vise --daemon --resume-all` under a service manager. The daemon stays in the foreground, uses a persistent authentication token, and shares the same global journal. See the [daemon guide](daemon.md).

## See your transfers

### Terminal precision

![Vise CLI showing progress, downloaded bytes, speed, and ETA](cli-demo.png){ loading=lazy }

### Industrial browser control

![Vise WebUI with live speed graphs, chunk grids, and bandwidth controls](webui-industrial.jpg){ loading=lazy }

The [WebUI guide](webui.md) explains the telemetry and controls. To send links straight from your browser, install the [Chrome or Firefox extension](extension.md).

## Lean by design

| Component | Responsibility |
| --- | --- |
| Rust + Tokio | Concurrent execution and asynchronous I/O |
| Reqwest | HTTP(S), streaming bodies, ranges, cookies, and proxies |
| Indicatif + Clap | Terminal progress and command-line controls |
| Axum | Embedded dashboard, REST API, and WebSockets |
| rusqlite + bundled SQLite | Transactional recovery journal in WAL mode |

The dashboard ships inside the executable with vanilla HTML, CSS, and JavaScript. No separate frontend build or database service is needed to run Vise.

Read the [architecture guide](architecture.md), review the [contribution standards](https://github.com/juic3b0x/vise/blob/main/CONTRIBUTING.md), or report vulnerabilities through the [security policy](https://github.com/juic3b0x/vise/security/policy). Vise is available under your choice of [MIT](https://github.com/juic3b0x/vise/blob/main/LICENSE) or [Apache-2.0](https://github.com/juic3b0x/vise/blob/main/LICENSE-APACHE).

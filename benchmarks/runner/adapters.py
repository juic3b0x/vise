"""Exact client profiles; no shell, inherited configuration or credentials."""
import os

PROFILES = ("vise", "aria2-matched", "aria2-defaults")


def environment(home):
    # Allowlist, not a blacklist: proxy, token, log and cookie variables cannot leak in.
    allowed = ("PATH", "SystemRoot", "WINDIR", "COMSPEC", "PATHEXT", "LD_LIBRARY_PATH",
               "DYLD_LIBRARY_PATH", "TMP", "TEMP", "TMPDIR")
    env = {key: os.environ[key] for key in allowed if key in os.environ}
    env.update(HOME=str(home), USERPROFILE=str(home), XDG_CONFIG_HOME=str(home / "config"),
               XDG_CACHE_HOME=str(home / "cache"), APPDATA=str(home / "appdata"),
               LOCALAPPDATA=str(home / "localappdata"), LC_ALL="C", NO_COLOR="1")
    return env


def isolate(home):
    home.mkdir(parents=True, mode=0o700)
    private = home / ".vise"
    private.mkdir(parents=True, mode=0o700)
    config = private / "config.toml"
    # Avoid generating a random API token. No credentials are used by this harness.
    config.write_text('[daemon]\nVISE_API_TOKEN = ""\n', encoding="utf-8")
    config.chmod(0o600)


def command(profile, executable, urls, output, work, connections, jobs, retries, timeout, many):
    common = ["--connections", str(connections), "--jobs", str(jobs), "--retries", str(retries),
              "--timeout", str(timeout), "--output", str(output), "--limit-rate", "off"]
    text = "\n".join(urls) + "\n"
    if profile == "vise":
        if many:
            return [executable, "--interactive", *common], text.encode()
        listing = work / "urls.txt"
        listing.write_text(text, encoding="utf-8")
        return [executable, "--batch", *common, "--input", str(listing)], None
    if profile.startswith("aria2-"):
        flags = [f"--split={connections}", f"--max-connection-per-server={connections}",
                 "--min-split-size=1M", "--piece-length=1M", f"--max-concurrent-downloads={jobs}",
                 f"--max-tries={retries + 1}", "--connect-timeout=15", f"--timeout={timeout}",
                 f"--dir={output}", "--input-file=-", "--quiet=true", "--summary-interval=0",
                 "--show-console-readout=false", "--download-result=hide"]
        if profile == "aria2-matched":
            flags += ["--no-conf", "--no-netrc=true", "--file-allocation=none",
                      "--allow-overwrite=false", "--auto-file-renaming=false", "--continue=false"]
        return [executable, *flags], text.encode()
    if profile != "curl" or connections != 1:
        raise ValueError("invalid client profile or curl connection budget")
    config = work / "curl.conf"
    def quote(value):
        return value.replace("\\", "\\\\").replace('"', '\\"')
    config.write_text("\n".join(f'url = "{quote(url)}"\noutput = "{quote((output / f"{i}.bin").as_posix())}"'
                                for i, url in enumerate(urls)) + "\n", encoding="utf-8")
    return [executable, "-q", "--http1.1", "--silent", "--show-error", "--fail",
            "--connect-timeout", "15", "--speed-time", str(timeout), "--speed-limit", "1",
            "--retry", str(retries), "--retry-all-errors", "--parallel", "--parallel-max", str(jobs),
            "--config", str(config)], None


def order(profiles, round_index):
    """Rotating, reversing order; paired rounds use identical content/fault seeds."""
    offset = round_index % len(profiles)
    rotated = profiles[offset:] + profiles[:offset]
    return list(reversed(rotated)) if (round_index // len(profiles)) % 2 else rotated

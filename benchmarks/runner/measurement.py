"""Per-process counters; never subtract cumulative RUSAGE_CHILDREN snapshots."""
from pathlib import Path
import os
import signal
import subprocess
import sys
import threading
import time


def process_cpu(pid):
    """Linux process-wide CPU ticks; no hash-preparation or child subtraction."""
    if not sys.platform.startswith("linux"):
        return None
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        ticks = os.sysconf("SC_CLK_TCK")
        return (int(fields[11]) / ticks, int(fields[12]) / ticks)
    except (OSError, ValueError, IndexError):
        return None


def cpu_delta(before, after):
    if before is None or after is None:
        return None
    return tuple(max(0, end - start) for start, end in zip(before, after, strict=True))


def windows_counters(process):
    import ctypes
    from ctypes import wintypes

    class Memory(ctypes.Structure):
        _fields_ = [("cb", wintypes.DWORD), ("faults", wintypes.DWORD)] + [
            (name, ctypes.c_size_t) for name in ("peak", "working", "paged_peak", "paged",
                                               "nonpaged_peak", "nonpaged", "pagefile", "pagefile_peak")]

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    kernel.GetProcessTimes.argtypes = [wintypes.HANDLE] + [ctypes.POINTER(wintypes.FILETIME)] * 4
    psapi.GetProcessMemoryInfo.argtypes = [wintypes.HANDLE, ctypes.POINTER(Memory), wintypes.DWORD]
    times = [wintypes.FILETIME() for _ in range(4)]
    memory = Memory()
    memory.cb = ctypes.sizeof(memory)
    handle = wintypes.HANDLE(int(process._handle))
    cpu = kernel.GetProcessTimes(handle, *(ctypes.byref(value) for value in times))
    ram = psapi.GetProcessMemoryInfo(handle, ctypes.byref(memory), memory.cb)
    seconds = lambda item: ((item.dwHighDateTime << 32) | item.dwLowDateTime) / 10_000_000
    return {"cpu_user_s": seconds(times[3]) if cpu else None,
            "cpu_system_s": seconds(times[2]) if cpu else None,
            "peak_rss_bytes": int(memory.peak) if ram else None}


def reap(process, timeout):
    deadline = time.monotonic() + timeout
    timed_out = False
    if os.name == "posix":
        finished = threading.Event()
        def expire():
            nonlocal timed_out
            if not finished.is_set():
                timed_out = True
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
        timer = threading.Timer(timeout, expire)
        timer.daemon = True
        timer.start()
        try:
            # Blocking wait avoids adding a polling interval to short loopback runs.
            _, status, usage = os.wait4(process.pid, 0)
        finally:
            finished.set()
            timer.cancel()
            timer.join()
        process.returncode = os.waitstatus_to_exitcode(status)
        scale = 1 if sys.platform == "darwin" else 1024
        return {"returncode": process.returncode, "cpu_user_s": usage.ru_utime,
                "cpu_system_s": usage.ru_stime, "peak_rss_bytes": usage.ru_maxrss * scale,
                "timed_out": timed_out, "rss_source": "wait4 high-water mark"}
    peak = 0
    sample = {}
    while process.poll() is None:
        sample = windows_counters(process)
        peak = max(peak, sample.get("peak_rss_bytes") or 0)
        if time.monotonic() > deadline:
            process.kill()
            timed_out = True
        try:
            # On Windows this waits on the process handle and wakes at exit.
            process.wait(timeout=0.005)
        except subprocess.TimeoutExpired:
            pass
    final = windows_counters(process)
    peak = max(peak, final.get("peak_rss_bytes") or 0)
    sample.update({key: value for key, value in final.items() if value is not None})
    return {**sample, "peak_rss_bytes": peak or None, "returncode": process.returncode,
            "timed_out": timed_out, "rss_source": "Windows PeakWorkingSetSize; sampled lower bound if final unavailable"}


def launch(command, env, cwd, input_bytes=None, timeout=300):
    start = time.perf_counter()
    with subprocess.Popen(command, cwd=cwd, env=env, stdin=subprocess.PIPE,
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                          start_new_session=os.name == "posix") as process:
        def feed():
            try:
                if input_bytes:
                    process.stdin.write(input_bytes)
                process.stdin.close()
            except (BrokenPipeError, OSError):
                pass
        sender = threading.Thread(target=feed, daemon=True)
        sender.start()
        counters = reap(process, timeout)
        wall = time.perf_counter() - start
        sender.join(timeout=2)
    return {**counters, "wall_s": wall}

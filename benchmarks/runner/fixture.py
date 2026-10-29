"""Bounded JSON control channel, separate from the measured HTTP traffic."""
import json
import queue
import subprocess
import threading

from .measurement import reap


class Fixture:
    def __init__(self, executable, config, work, env):
        path = work / "fixture.json"
        path.write_text(json.dumps(config), encoding="utf-8")
        self.process = subprocess.Popen([str(executable), str(path)], cwd=work, env=env,
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.DEVNULL, start_new_session=True)
        self.lines = queue.Queue()
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        try:
            self.ready = self._next(120)
            port = self.ready["port"]
            if not isinstance(port, int) or not 1 <= port <= 65535:
                raise ValueError("invalid fixture port")
        except BaseException:
            self.abort()
            raise

    def _read(self):
        try:
            # At most 64 MiB of numeric logs per run; refuse runaway fixture output.
            for _ in range(2):
                line = self.process.stdout.readline(64 * 1024 * 1024 + 1)
                if not line or len(line) > 64 * 1024 * 1024:
                    break
                self.lines.put(json.loads(line))
        except (ValueError, OSError):
            pass
        finally:
            self.lines.put(None)

    def _next(self, timeout):
        try:
            value = self.lines.get(timeout=timeout)
        except queue.Empty:
            raise TimeoutError("fixture control channel timeout") from None
        if value is None:
            raise RuntimeError("fixture control channel closed")
        return value

    def finish(self):
        try:
            self.process.stdin.write(b"stop\n")
            self.process.stdin.close()
            stats = self._next(15)
            counters = reap(self.process, 10)
            if counters["returncode"]:
                raise RuntimeError("fixture exit failure")
            stats["process_lifetime_counters_including_preparation"] = counters
            return stats
        finally:
            self.abort()

    def abort(self):
        if self.process.returncode is None:
            self.process.kill()
            self.process.wait()
        for stream in (self.process.stdin, self.process.stdout):
            if stream and not stream.closed:
                stream.close()
        self.reader.join(timeout=2)

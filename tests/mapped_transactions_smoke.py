#!/usr/bin/env python3
"""Real Night mapped OW controller + H2 transaction integration smoke."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
OW = ROOT / "ow-lang" / "target" / "OwIDE.jar"
PLATFORM = ROOT / "platform" / "target" / "night-platform-0.2.0-SNAPSHOT.jar"
APP = ROOT / "examples" / "mapped-transactions" / "main.ow"
BASE = "http://127.0.0.1:8080"


def request(path: str, *, method: str = "GET", body: str | None = None):
    payload = body.encode("utf-8") if body is not None else None
    req = urllib.request.Request(BASE + path, data=payload, method=method)
    try:
        with urllib.request.urlopen(req, timeout=4) as response:
            status = response.status
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        status = error.code
        raw = error.read().decode("utf-8")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = raw
    return status, parsed


def expect(path, status, *, method="GET", body=None):
    actual, data = request(path, method=method, body=body)
    assert actual == status, (path, status, actual, data)
    return data


def main():
    with tempfile.TemporaryDirectory(prefix="night-mapped-tx-") as tmp:
        env = os.environ.copy()
        env.update({
            "OW_NO_BROWSER": "1",
            "OW_DB_PATH": str(Path(tmp) / "state.json"),
            "NIGHT_JDBC_URL": "jdbc:h2:mem:night_mapped_tx;DB_CLOSE_DELAY=-1",
            "NIGHT_JDBC_USER": "sa",
            "NIGHT_JDBC_PASSWORD": "",
        })
        log = Path(tmp) / "server.log"
        command = [
            "java", "-cp",
            os.pathsep.join((str(OW), str(PLATFORM))),
            "dev.oreo.Main", "run", str(APP),
        ]
        with log.open("w", encoding="utf-8") as output:
            process = subprocess.Popen(
                command, cwd=ROOT, env=env,
                stdout=output, stderr=subprocess.STDOUT
            )
            try:
                for _ in range(100):
                    if process.poll() is not None:
                        raise AssertionError(f"Night exited early:\n{log.read_text()}")
                    try:
                        if request("/_night/health")[0] == 200:
                            break
                    except (OSError, TimeoutError):
                        time.sleep(0.25)
                else:
                    raise AssertionError(f"Server did not start:\n{log.read_text()}")

                assert expect("/api/entries/1", 200) == {"count": 0}
                assert expect("/api/entries/1", 201, method="POST",
                              body="persisted") == {"id": 1, "created": 1}
                assert expect("/api/entries/1", 200) == {"count": 1}

                # Exception escaping the annotated service triggers rollback.
                expect("/api/entries/2/fail", 500, method="POST", body="rollback")
                assert expect("/api/entries/2", 200) == {"count": 0}

                # Outer annotated method catches the nested error; REQUIRED
                # propagation must still reject commit (rollback-only).
                expect("/api/entries/3/caught", 500, method="POST", body="rollback-only")
                assert expect("/api/entries/3", 200) == {"count": 0}

                # Transactional failure must not poison later independent calls.
                assert expect("/api/entries/4", 201, method="POST",
                              body="recovered")["id"] == 4
                assert expect("/api/entries/4", 200) == {"count": 1}
            finally:
                process.terminate()
                try:
                    process.wait(timeout=6)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=4)
        contents = log.read_text(encoding="utf-8")
        assert "Night mapped controller with transactions ready" in contents, contents
        print("PASS Night OW mapping: GET/POST, commit, rollback, REQUIRED rollback-only, recovery")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(str(error), file=sys.stderr)
        raise

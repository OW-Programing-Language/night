#!/usr/bin/env python3
"""Real OW @Repository HTTP CRUD with H2 + REQUIRED JDBC transaction regression."""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
OW = ROOT / "ow-lang" / "target" / "OwIDE.jar"
PLATFORM = ROOT / "platform" / "target" / "night-platform-0.2.0-SNAPSHOT.jar"
APP = ROOT / "examples" / "repositories" / "main.ow"
BASE = "http://127.0.0.1:8080"


def request(path, *, method="GET", body=None):
    payload = body.encode("utf-8") if body is not None else None
    req = urllib.request.Request(BASE + path, data=payload, method=method)
    try:
        with urllib.request.urlopen(req, timeout=4) as response:
            status, raw = response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        status, raw = error.code, error.read().decode("utf-8")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        parsed = raw
    return status, parsed


def expect(path, code, *, method="GET", body=None):
    actual, data = request(path, method=method, body=body)
    assert actual == code, (path, code, actual, data)
    return data


def main():
    with tempfile.TemporaryDirectory(prefix="night-repositories-") as tmp:
        env = dict(os.environ)
        env.update({
            "OW_NO_BROWSER": "1",
            "OW_DB_PATH": str(Path(tmp) / "state.json"),
            "NIGHT_JDBC_URL": "jdbc:h2:mem:night_repo_http;DB_CLOSE_DELAY=-1",
            "NIGHT_JDBC_USER": "sa",
            "NIGHT_JDBC_PASSWORD": "",
        })
        log = Path(tmp) / "server.log"
        cmd = [
            "java", "-cp", os.pathsep.join((str(OW), str(PLATFORM))),
            "dev.oreo.Main", "run", str(APP),
        ]
        with log.open("w", encoding="utf-8") as output:
            proc = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=output,
                                    stderr=subprocess.STDOUT)
            try:
                for _ in range(100):
                    if proc.poll() is not None:
                        raise AssertionError(f"OW/Night server exited:\n{log.read_text()}")
                    try:
                        if request("/_night/health")[0] == 200:
                            break
                    except (OSError, TimeoutError):
                        time.sleep(0.25)
                else:
                    raise AssertionError(f"Server startup failed:\n{log.read_text()}")

                assert expect("/api/users", 200) == []
                assert expect("/api/users/3", 201, method="POST",
                              body="gamma") == {"id": 3, "created": 1}
                assert expect("/api/users/1", 201, method="POST",
                              body="alpha") == {"id": 1, "created": 1}
                assert expect("/api/users/2", 201, method="POST",
                              body="beta") == {"id": 2, "created": 1}
                user = expect("/api/users/2", 200)
                assert user["id"] == 2 and user["username"] == "beta", user
                found = expect("/api/users", 200)
                assert [row["id"] for row in found] == [1, 2, 3], found
                sorted_page = expect("/api/users/page?limit=2&offset=0", 200)
                assert [row["id"] for row in sorted_page] == [3, 2], sorted_page

                # Update is explicit, returns row count and shares @Transactional.
                assert expect("/api/users/1/rename", 200, method="POST",
                              body="renamed") == {"updated": 1}
                assert expect("/api/users/1", 200)["username"] == "renamed"

                # Duplicate inserts do not silently overwrite existing data.
                expect("/api/users/2", 500, method="POST", body="overwrite")
                assert expect("/api/users/2", 200)["username"] == "beta"

                # One annotated service throws after writing: must roll back.
                expect("/api/users/4/fail", 500, method="POST", body="rollback")
                assert expect("/api/users/4", 200) == 0

                # Nested caught failure marks outer transaction rollback-only.
                expect("/api/users/5/caught", 500, method="POST", body="rollback-only")
                assert expect("/api/users/5", 200) == 0

                # Subsequent requests can commit after failed transactions.
                assert expect("/api/users/6", 201, method="POST",
                              body="recovered")["id"] == 6
                assert expect("/api/users/6", 200)["username"] == "recovered"
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=6)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=4)
        logs = log.read_text(encoding="utf-8")
        assert "Night repository CRUD ready" in logs, logs
        print("PASS @Repository CRUD, paging, JDBC commit/rollback, REQUIRED rollback-only")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""Boot real Night + OW extension JAR, exercise security, JDBC and Actuator."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
OW = ROOT / "ow-lang" / "target" / "OwIDE.jar"
PLATFORM = ROOT / "platform" / "target" / "night-platform-0.2.0-SNAPSHOT.jar"
APP = ROOT / "examples" / "production" / "main.ow"
BASE = "http://127.0.0.1:8080"

def request(path, method="GET", token=None, payload=None):
    headers = {}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = payload.encode("utf-8") if payload is not None else None
    req = urllib.request.Request(BASE + path, method=method, headers=headers, data=data)
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, r.read().decode(), dict(r.headers)
    except urllib.error.HTTPError as ex:
        return ex.code, ex.read().decode(), dict(ex.headers)

def expect(path, status, *, token=None, method="GET", payload=None):
    code, raw, headers = request(path, method, token, payload)
    assert code == status, (path, code, status, raw)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # OW deliberately sends plaintext for transport-level 413/500.
        return {"raw": raw}

def main():
    assert OW.exists(), "Missing OW build"
    assert PLATFORM.exists(), "Missing Night platform build"
    tokens = {role: secrets.token_urlsafe(48) for role in ("ADMIN", "READER", "WRITER")}
    hashes = ";".join(
        role + ":" + hashlib.sha256(secret.encode()).hexdigest()
        for role, secret in tokens.items()
    )
    with tempfile.TemporaryDirectory(prefix="night-platform-") as dirname:
        env = dict(os.environ)
        env["OW_NO_BROWSER"] = "1"
        env["NIGHT_BEARER_KEYS"] = hashes
        env["NIGHT_JDBC_URL"] = (
            "jdbc:h2:file:" + str(Path(dirname) / "purchases") +
            ";DATABASE_TO_UPPER=false"
        )
        env["NIGHT_JDBC_USER"] = "sa"
        env["NIGHT_JDBC_PASSWORD"] = ""
        log_path = Path(dirname) / "platform-server.log"
        with log_path.open("w", encoding="utf-8") as output:
            proc = subprocess.Popen(
                ["java", "-cp", os.pathsep.join((str(OW), str(PLATFORM))),
                 "dev.oreo.Main", "run", str(APP)],
                cwd=ROOT, env=env, stdout=output, stderr=subprocess.STDOUT,
            )
            try:
                for _ in range(100):
                    if proc.poll() is not None:
                        raise AssertionError("Night failed to start:\n" + log_path.read_text())
                    try:
                        code, _, _ = request("/_night/health")
                        if code == 200: break
                    except Exception:
                        time.sleep(0.20)
                else:
                    raise AssertionError("Night platform startup timed out:\n" +
                                         log_path.read_text())
                assert expect("/_night/health", 200)["framework"] == "Night"
                expect("/api/purchases", 401)
                expect("/api/purchases", 401, token="invalid-token")
                expect("/api/purchases", 200, token=tokens["READER"])
                expect("/_night/metrics", 401)
                expect("/_night/metrics", 403, token=tokens["READER"])
                expect("/api/purchases/1", 404, token=tokens["READER"])
                expect("/api/purchases/1", 403, token=tokens["READER"], method="POST", payload="2")
                expect("/api/purchases/1", 400, token=tokens["WRITER"], method="POST", payload="0")
                expect("/api/purchases/1", 400, token=tokens["WRITER"], method="POST", payload="oops")
                expect("/api/purchases/not-an-id", 400, token=tokens["WRITER"], method="POST", payload="2")
                expect("/api/purchases/99", 404, token=tokens["WRITER"], method="POST", payload="2")
                created = expect("/api/purchases/1", 201, token=tokens["WRITER"],
                                 method="POST", payload="2")
                assert created["item"] == 1 and created["quantity"] == 2, created
                rows = expect("/api/purchases", 200, token=tokens["READER"])
                assert len(rows) == 1 and rows[0]["total"] == 24, rows
                assert expect("/api/purchases/1", 200, token=tokens["READER"])["total"] == 24
                assert expect("/_night/ready", 200, token=tokens["ADMIN"])["database"] == "UP"
                metrics = expect("/_night/metrics", 200, token=tokens["ADMIN"])
                assert metrics["requests"] >= 12, metrics
                assert metrics["access_denied"] >= 4, metrics
                code, _, headers = request("/api/purchases", token=tokens["READER"])
                assert {key.lower(): value for key, value in headers.items()}.get("x-content-type-options") == "nosniff", headers
                huge = "1" * (1024 * 1024 + 1)
                expect("/api/purchases/1", 413, token=tokens["WRITER"],
                       method="POST", payload=huge)
            finally:
                proc.terminate()
                try: proc.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=4)
        log = log_path.read_text()
        assert "Night: booted 2 module(s)" in log, log[-3000:]
        assert "Night platform ready" in log, log[-3000:]
        assert tokens["ADMIN"] not in log
    print("PASS Night platform: bearer RBAC, SQL migrations/prepared queries, health, metrics, 413")

if __name__ == "__main__":
    main()

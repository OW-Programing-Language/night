#!/usr/bin/env python3
"""Exercise a real OW Night HTTP process, not a mocked route dispatcher."""
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
JAR = ROOT / "ow-lang" / "target" / "OwIDE.jar"
APP = ROOT / "examples" / "bookstore" / "main.ow"
BASE = "http://127.0.0.1:8080"

def request(path, *, method="GET", data=None):
    payload = data.encode() if data is not None else None
    request = urllib.request.Request(BASE + path, data=payload, method=method)
    try:
        with urllib.request.urlopen(request, timeout=3) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as ex:
        return ex.code, json.loads(ex.read())

def expect(path, status, *, method="GET", data=None):
    code, body = request(path, method=method, data=data)
    assert code == status, (path, code, body)
    return body

def main():
    with tempfile.TemporaryDirectory(prefix="night-http-") as dirname:
        env = os.environ.copy()
        env["OW_NO_BROWSER"] = "1"
        env["OW_DB_PATH"] = str(Path(dirname) / "night-test-data.json")
        logs = Path(dirname) / "server.log"
        with logs.open("w", encoding="utf-8") as logfile:
            proc = subprocess.Popen(
                ["java", "-jar", str(JAR), "run", str(APP)],
                cwd=ROOT, stdout=logfile, stderr=subprocess.STDOUT, env=env
            )
            try:
                for _ in range(80):
                    if proc.poll() is not None:
                        raise RuntimeError(f"Night exited early:\n{logs.read_text()}")
                    try:
                        status, data = request("/_night/health")
                        if status == 200:
                            break
                    except (OSError, urllib.error.URLError, TimeoutError):
                        time.sleep(0.25)
                else:
                    raise RuntimeError(f"Night HTTP startup failed:\n{logs.read_text()}")

                assert data["framework"] == "Night", data
                assert data["started"] == 3, data
                modules = expect("/_night/modules", 200)
                assert len(modules) == 3, modules
                assert all(item["started"] == 1 for item in modules), modules

                catalog = expect("/api/catalog", 200)
                assert len(catalog) == 2, catalog
                assert expect("/api/catalog/1", 200)["price"] == 12
                expect("/api/catalog/999", 404)
                assert expect("/api/orders", 200) == []
                expect("/api/orders/1", 404)
                expect("/api/orders/1", 400, method="POST", data="0")
                expect("/api/orders/999", 404, method="POST", data="2")

                created = expect("/api/orders/1", 201, method="POST", data="2")
                assert created["total"] == 24 and created["quantity"] == 2, created
                purchases = expect("/api/orders", 200)
                assert len(purchases) == 1, purchases
                assert purchases[0]["total"] == 24, purchases
                assert expect("/api/orders/1", 200)["item"] == 1
                assert expect("/api/notifications/1", 200)["state"] == "ready"
                health = expect("/_night/health", 200)
                assert health["events"] == 1, health
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=6)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=4)
        content = logs.read_text(encoding="utf-8")
        for marker in ("Catalog: started", "Orders: started",
                       "Notifications: started",
                       "Notifications: order.placed for item 1"):
            assert marker in content, f"Missing {marker}:\n{content}"
        print("PASS Night live HTTP: health, typed REST, persistence, events, 400/404/201")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise

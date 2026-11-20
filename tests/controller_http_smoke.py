#!/usr/bin/env python3
"""Exercise real @Controller/@Service injection through an OW HTTP server."""
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
APP = ROOT / "examples" / "controllers" / "main.ow"
BASE = "http://127.0.0.1:8080"

def get(path):
    with urllib.request.urlopen(BASE + path, timeout=3) as response:
        return response.status, json.loads(response.read())

def main():
    with tempfile.TemporaryDirectory(prefix="night-controller-") as temp:
        env = os.environ.copy()
        env["OW_NO_BROWSER"] = "1"
        env["OW_DB_PATH"] = str(Path(temp) / "state.json")
        logfile = Path(temp) / "server.log"
        with logfile.open("w", encoding="utf-8") as logs:
            proc = subprocess.Popen(
                ["java", "-jar", str(JAR), "run", str(APP)],
                cwd=ROOT, stdout=logs, stderr=subprocess.STDOUT, env=env
            )
            try:
                for _ in range(80):
                    if proc.poll() is not None:
                        raise RuntimeError(f"Controller server exited:\n{logfile.read_text()}")
                    try:
                        code, data = get("/_night/health")
                        if code == 200:
                            break
                    except (OSError, urllib.error.URLError, TimeoutError):
                        time.sleep(0.25)
                else:
                    raise RuntimeError(f"Controller HTTP startup failed:\n{logfile.read_text()}")
                assert data["framework"] == "Night", data
                code, data = get("/api/greeting")
                assert code == 200, (code, data)
                assert data == {"message": "Hello from Night and OW"}, data
            finally:
                proc.terminate()
                try:
                    proc.wait(timeout=6)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=4)
        logs = logfile.read_text(encoding="utf-8")
        assert "Night annotated controller ready" in logs, logs
        print("PASS Night @Controller real HTTP + OW @Inject service singleton")

if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(str(error), file=sys.stderr)
        raise

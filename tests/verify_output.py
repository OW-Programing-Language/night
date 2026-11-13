#!/usr/bin/env python3
"""Regression assertions on Night module lifecycle order and event filtering."""
from pathlib import Path
import sys

life = Path(sys.argv[1]).read_text(encoding="utf-8")
rollback = Path(sys.argv[2]).read_text(encoding="utf-8")
restart = Path(sys.argv[3]).read_text(encoding="utf-8")

def ordered(content, *markers):
    offsets = [content.index(marker) for marker in markers]
    assert offsets == sorted(offsets), f"Wrong Night event order: {markers}"

ordered(life, "START base", "START middle", "START leaf")
ordered(life, "STOP leaf", "STOP middle", "STOP base")
assert "EVENT leaf 7" in life
assert "EVENT leaf 8" not in life, "disabled subscriber got an event"
assert "EVENT leaf 9" in life
assert "PASS Night lifecycle and events" in life
ordered(rollback, "START base", "FAIL failing", "STOP base")
assert "ERROR: failed component cannot be stopped" not in rollback
assert "PASS Night rollback" in rollback
assert "PASS Night 70 restart cycles" in restart
print("PASS Night lifecycle ordering, rollback, and event filtering")

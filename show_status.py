#!/usr/bin/env python3
"""
Prints current download progress from progress.json.
Usage: python3 show_status.py <playlist_file>
"""
import sys
import json
from pathlib import Path

playlist = sys.argv[1] if len(sys.argv) > 1 else ""
total_urls = 0

if playlist and Path(playlist).exists():
    content = Path(playlist).read_text(encoding="utf-8", errors="replace")
    total_urls = content.count("https://")

print(f"Playlist : {playlist}  (~{total_urls} URLs)")

if Path("progress.json").exists():
    d = json.loads(Path("progress.json").read_text())
    done  = len(d.get("completed_urls", []))
    fail  = len(d.get("failed_urls", []))
    runs  = d.get("total_runs", 0)
    rem   = d.get("remaining_count", "?")
    comp  = d.get("completed", False)
    print(f"  Completed  : {done}")
    print(f"  Failed     : {fail}")
    print(f"  Remaining  : {rem}")
    print(f"  Total runs : {runs}")
    print(f"  All done   : {comp}")
else:
    print("  No progress.json - fresh start")

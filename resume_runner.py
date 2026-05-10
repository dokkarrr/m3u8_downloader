#!/usr/bin/env python3
"""
Resume-aware wrapper for the M3U8 batch downloader.
Reads  : progress.json  (set of already-completed URLs)
Writes : progress.json  (updated after every episode)
Exit   : 0 = all done | 1 = errors | 2 = timed-out (auto-resume next run)
"""
import os
import re
import sys
import json
import time
import subprocess
from pathlib import Path
from datetime import datetime, timezone

PLAYLIST_FILE      = os.environ["PLAYLIST"]
OUTPUT_DIR         = os.environ.get("OUTPUT_DIR", "downloads")
QUALITY            = os.environ.get("QUALITY", "best")
WORKERS            = os.environ.get("WORKERS", "4")
SCRIPT             = os.environ["SCRIPT"]
PROGRESS_FILE      = "progress.json"
# 345 min = 5h 45m  (leaves 4 min for the commit step before job is killed)
TIME_LIMIT_SECONDS = int(os.environ.get("TIME_LIMIT_SECONDS", str(345 * 60)))


# ── progress helpers ───────────────────────────────────────────────────────────

def load_progress():
    if Path(PROGRESS_FILE).exists():
        try:
            return json.loads(Path(PROGRESS_FILE).read_text())
        except Exception:
            pass
    return {
        "playlist_file": PLAYLIST_FILE,
        "quality": QUALITY,
        "completed_urls": [],
        "failed_urls": [],
        "total_runs": 0,
        "last_run": None,
        "completed": False,
    }


def save_progress(state):
    state["last_run"] = datetime.now(timezone.utc).isoformat()
    Path(PROGRESS_FILE).write_text(json.dumps(state, indent=2))
    done = len(state["completed_urls"])
    print(f"\n  Saved -> {PROGRESS_FILE}  ({done} done so far)")


# ── playlist parser ────────────────────────────────────────────────────────────

def parse_urls(filepath):
    """Return ordered list of (raw_title, url) from the .txt playlist."""
    raw  = Path(filepath).read_text(encoding="utf-8", errors="replace")
    norm = re.sub(r'\s+#', '\n#', raw)
    norm = re.sub(r'(https?://\S+)', r'\n\1', norm)
    lines = [l.strip() for l in norm.splitlines() if l.strip()]
    entries = []
    current_title = None
    for line in lines:
        if line.startswith("#EXTINF"):
            current_title = line.split(",", 1)[1].strip() if "," in line else line
        elif re.match(r'https?://', line) and ".m3u8" in line:
            title = current_title or "episode_{:03d}".format(len(entries) + 1)
            entries.append((title, line))
            current_title = None
    return entries


def clean_title(raw):
    t = re.sub(r'\s*[\[\(][^\]\)]*[\]\)]', '', raw)
    t = re.sub(r'\.m3u8\s*$', '', t, flags=re.IGNORECASE)
    t = re.sub(r'\s+', ' ', t).strip()
    t = re.sub(r'[\\/:*?"<>|]', '', t)
    return t or "episode"


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    start_time = time.time()
    state = load_progress()
    state["total_runs"]    = state.get("total_runs", 0) + 1
    state["playlist_file"] = PLAYLIST_FILE
    state["quality"]       = QUALITY

    completed_urls = set(state.get("completed_urls", []))
    failed_urls    = set(state.get("failed_urls", []))
    all_entries    = parse_urls(PLAYLIST_FILE)
    total          = len(all_entries)
    pending        = [(t, u) for t, u in all_entries if u not in completed_urls]

    print(f"\n  Playlist       : {PLAYLIST_FILE}")
    print(f"  Total URLs     : {total}")
    print(f"  Already done   : {len(completed_urls)}")
    print(f"  This run queue : {len(pending)}")
    print(f"  Time budget    : {TIME_LIMIT_SECONDS // 60} min\n")

    if not pending:
        print("  All episodes already downloaded!")
        state["completed"] = True
        state["remaining_count"] = 0
        save_progress(state)
        return 0

    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
    timed_out   = False
    this_done   = 0
    this_failed = 0

    url_to_index = {u: i for i, (t, u) in enumerate(all_entries)}

    for idx, (raw_title, url) in enumerate(pending):
        elapsed   = time.time() - start_time
        remaining = TIME_LIMIT_SECONDS - elapsed

        if remaining < 90:
            print(f"\n  Only {remaining:.0f}s left - stopping safely")
            print(f"  Paused at queue item {idx + 1}/{len(pending)}")
            timed_out = True
            break

        title   = clean_title(raw_title)
        out_mp4 = Path(OUTPUT_DIR) / (title + ".mp4")
        out_ts  = Path(OUTPUT_DIR) / (title + ".ts")

        if (out_mp4.exists() and out_mp4.stat().st_size > 1000) or \
           (out_ts.exists()  and out_ts.stat().st_size  > 1000):
            print(f"  File on disk, marking done: {title}")
            completed_urls.add(url)
            state["completed_urls"] = list(completed_urls)
            save_progress(state)
            continue

        overall = url_to_index.get(url, idx) + 1
        print("=" * 68)
        print(f"  [{overall}/{total}]  {title}")
        print(f"  {url[:72]}")
        print(f"  Elapsed {elapsed / 60:.1f}min | Remaining {remaining / 60:.1f}min")
        print("=" * 68)

        cmd = [
            sys.executable, SCRIPT,
            url,
            "-o", str(out_mp4),
            "-w", WORKERS,
            "-q", QUALITY,
            "--retries", "5",
            "--report",
        ]

        try:
            result = subprocess.run(cmd, timeout=max(remaining - 60, 30))
            if result.returncode == 0:
                completed_urls.add(url)
                this_done += 1
                print(f"\n  Done: {title}")
            else:
                failed_urls.add(url)
                this_failed += 1
                print(f"\n  Failed (exit {result.returncode}): {title}")
        except subprocess.TimeoutExpired:
            print(f"\n  Episode timed out: {title}")
            failed_urls.add(url)
            this_failed += 1
            timed_out = True
        except Exception as exc:
            print(f"\n  Error: {exc}")
            failed_urls.add(url)
            this_failed += 1

        remaining_count = total - len(completed_urls)
        state["completed_urls"]  = list(completed_urls)
        state["failed_urls"]     = list(failed_urls)
        state["remaining_count"] = remaining_count
        state["completed"]       = (remaining_count == 0)
        state["timed_out"]       = timed_out
        save_progress(state)

        if timed_out:
            break

    remaining_count = total - len(completed_urls)
    state["completed_urls"]  = list(completed_urls)
    state["failed_urls"]     = list(failed_urls)
    state["remaining_count"] = remaining_count
    state["completed"]       = (remaining_count == 0 and not timed_out)
    state["timed_out"]       = timed_out
    save_progress(state)

    elapsed_total = time.time() - start_time
    print("\n" + "=" * 68)
    print("  SESSION SUMMARY")
    print("=" * 68)
    print(f"  Done this run   : {this_done}")
    print(f"  Failed this run : {this_failed}")
    print(f"  Total done      : {len(completed_urls)}/{total}")
    print(f"  Still pending   : {remaining_count}")
    print(f"  Time used       : {elapsed_total / 60:.1f} min")
    if timed_out and remaining_count > 0:
        print(f"\n  Auto-resuming next run for {remaining_count} episode(s)...")
    elif state["completed"]:
        print("\n  ALL EPISODES COMPLETE!")
    print()

    if timed_out and remaining_count > 0:
        return 2
    elif failed_urls:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

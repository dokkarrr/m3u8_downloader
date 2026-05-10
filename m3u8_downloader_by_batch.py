#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║        Advanced M3U8 / HLS Batch Downloader  v3.0              ║
║  New in v3.0:                                                    ║
║    • Reads multiple URLs from m3u8files.txt playlist             ║
║    • Parses #EXTINF title → "Naruto Shippuden - 001 - ..."      ║
║    • Strips [tags] from title for clean filenames                ║
║    • Sophisticated 403 handling: rotate UA, retry with backoff   ║
║    • Cookie jar rotation on 403                                  ║
║    • Referer spoofing on 403                                     ║
║    • Per-episode skip if already downloaded                      ║
║    • Batch queue with progress summary                           ║
║  Carried from v2.0:                                              ║
║    • Binary-safe m3u8 parsing (latin-1 decode)                   ║
║    • AES-128 decryption, resumable segments                      ║
║    • Concurrent segment downloading (thread pool)                ║
║    • FFmpeg mux → MP4                                            ║
║    • Live / VOD stream support                                   ║
║    • Rich progress bar & ETA                                     ║
╚══════════════════════════════════════════════════════════════════╝
"""

import os
import re
import sys
import json
import time
import uuid
import shutil
import hashlib
import logging
import argparse
import tempfile
import threading
import signal
import subprocess
import random
from datetime import datetime
from pathlib import Path
from queue import Queue, Empty
from urllib.parse import urljoin, urlparse, quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, List, Tuple, Dict

# ── third-party ───────────────────────────────────────────────────────────────
try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from Crypto.Cipher import AES
    HAS_CRYPTO = True
    HAS_CRYPTOGRAPHY = False
except ImportError:
    HAS_CRYPTO = False
    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        from cryptography.hazmat.backends import default_backend
        HAS_CRYPTOGRAPHY = True
    except ImportError:
        HAS_CRYPTOGRAPHY = False

try:
    import browser_cookie3
    HAS_BROWSER_COOKIE3 = True
except ImportError:
    HAS_BROWSER_COOKIE3 = False

# ── logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("m3u8dl")

# ── Global abort flag ─────────────────────────────────────────────────────────
_ABORT = threading.Event()

def _sigint_handler(sig, frame):
    if not _ABORT.is_set():
        print("\n\n  ⚠️  Interrupted — saving partial download …\n")
        _ABORT.set()

signal.signal(signal.SIGINT, _sigint_handler)


# ─────────────────────────────────────────────────────────────────────────────
# USER-AGENT POOL  (for 403 rotation)
# ─────────────────────────────────────────────────────────────────────────────

USER_AGENTS = [
    # Chrome Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    # Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (X11; Linux x86_64; rv:123.0) Gecko/20100101 Firefox/123.0",
    # Safari macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Safari/605.1.15",
    # Edge
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    # Mobile Chrome
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.82 Mobile Safari/537.36",
    # Chrome macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

REFERER_CANDIDATES = [
    "https://www.google.com/",
    "https://www.bing.com/",
    "https://duckduckgo.com/",
    "",  # no referer
]


# ─────────────────────────────────────────────────────────────────────────────
# PLAYLIST FILE PARSER  (reads m3u8files.txt)
# ─────────────────────────────────────────────────────────────────────────────

class EpisodeEntry:
    """Represents one parsed episode from the playlist file."""
    __slots__ = ("title", "url", "subtitle_url", "episode_num")

    def __init__(self, title: str, url: str,
                 subtitle_url: Optional[str] = None,
                 episode_num: Optional[str] = None):
        self.title = title
        self.url = url
        self.subtitle_url = subtitle_url
        self.episode_num = episode_num

    def __repr__(self):
        return f"<Episode {self.episode_num}: {self.title}>"


def _clean_title(raw: str) -> str:
    """
    Given a raw #EXTINF label like:
      'Naruto Shippuden - 001 - Homecoming.m3u8 [Dub & S-Sub - Server 2]'
    Returns a clean output name like:
      'Naruto Shippuden - 001 - Homecoming'
    Steps:
      1. Strip [bracketed tags] and (parenthesised tags)
      2. Strip .m3u8 extension suffix if present
      3. Collapse whitespace
      4. Strip illegal filename chars
    """
    title = raw.strip()

    # Remove bracketed/parenthesised suffixes like [Dub & S-Sub - Server 2]
    title = re.sub(r'\s*[\[\(][^\]\)]*[\]\)]', '', title)

    # Remove .m3u8 extension if baked into the title
    title = re.sub(r'\.m3u8\s*$', '', title, flags=re.IGNORECASE)

    # Collapse whitespace
    title = re.sub(r'\s+', ' ', title).strip()

    # Strip chars illegal on Windows/Linux/macOS
    title = re.sub(r'[\\/:*?"<>|]', '', title)

    return title or "episode"


def _extract_episode_num(title: str) -> Optional[str]:
    """
    Try to extract a 3-digit episode number from title.
    Pattern: ' - 001 - '  or  ' - 001'  or  'EP001'
    """
    m = re.search(r'[-\s](\d{2,4})[-\s]', title)
    if m:
        return m.group(1).zfill(3)
    m = re.search(r'EP\s*(\d+)', title, re.IGNORECASE)
    if m:
        return m.group(1).zfill(3)
    return None


def parse_playlist_file(filepath: str) -> List[EpisodeEntry]:
    """
    Parse a custom playlist file (.txt) in the format:

      #EXTINF:-1,<Title> [optional tags]
      https://host/path/list.m3u8
      #EXT-X-MEDIA:TYPE=SUBTITLES,...,URI="https://..."

    Blank lines between entries are ignored.
    Lines that are not #EXTINF, #EXT-X-MEDIA, or URLs are skipped.
    Only .m3u8 URLs are collected.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Playlist file not found: {filepath}")

    entries: List[EpisodeEntry] = []
    current_title: Optional[str] = None
    current_url: Optional[str] = None
    current_sub: Optional[str] = None

    def flush():
        nonlocal current_title, current_url, current_sub
        if current_url:
            title = _clean_title(current_title or "Unknown")
            ep_num = _extract_episode_num(current_title or "")
            entries.append(EpisodeEntry(
                title=title,
                url=current_url,
                subtitle_url=current_sub,
                episode_num=ep_num,
            ))
        current_title = None
        current_url = None
        current_sub = None

    raw = path.read_text(encoding="utf-8", errors="replace")

    # Normalize: the file can be one long line or multiline.
    # Split on # to tokenize tags, then reconstruct lines.
    # Better: split on whitespace-separated tokens while keeping URLs whole.
    # Strategy: replace ' #' with '\n#' to create clean lines, then parse.
    normalized = re.sub(r'\s+#', '\n#', raw)
    # Also split bare URLs that follow a tag on the same line
    normalized = re.sub(r'(https?://\S+)', r'\n\1', normalized)

    lines = [l.strip() for l in normalized.splitlines()]

    for line in lines:
        if not line:
            continue

        if line.startswith("#EXTINF"):
            # Save previous entry if pending
            if current_url:
                flush()
            # Extract title after the comma
            if "," in line:
                current_title = line.split(",", 1)[1].strip()
            else:
                current_title = line.strip()
            continue

        if line.startswith("#EXT-X-MEDIA") and "SUBTITLES" in line:
            m = re.search(r'URI="([^"]+)"', line)
            if m:
                current_sub = m.group(1)
            continue

        if line.startswith("#"):
            # Other tags — ignore
            continue

        if re.match(r'https?://', line):
            if line.lower().endswith(".m3u8") or "list.m3u8" in line or ".m3u8" in line:
                if current_url is None:
                    current_url = line
                # If we already have a URL for this entry, it's a new entry without EXTINF
                # (shouldn't happen with well-formed files, but handle gracefully)
            continue

    # Flush last entry
    if current_url:
        flush()

    return entries


# ─────────────────────────────────────────────────────────────────────────────
# BINARY-SAFE FETCH HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def fetch_playlist_text(session, url: str, timeout: int = 20) -> str:
    """Fetch an m3u8 playlist and decode it byte-transparently with latin-1."""
    resp = session.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.content.decode("latin-1")


def _safe_urljoin(base: str, uri: str) -> str:
    if uri.startswith("http://") or uri.startswith("https://"):
        return uri
    parsed = urlparse(base)
    base_path = parsed.path.rsplit("/", 1)[0] + "/"
    raw_bytes = uri.encode("latin-1")
    encoded_uri = quote(raw_bytes, safe="/:@!$&'()*+,;=?#%")
    if encoded_uri.startswith("/"):
        full_path = encoded_uri
    else:
        full_path = base_path + encoded_uri
    return parsed._replace(path=full_path, query="", fragment="").geturl()


# ─────────────────────────────────────────────────────────────────────────────
# SOPHISTICATED 403 HANDLER
# ─────────────────────────────────────────────────────────────────────────────

class ForbiddenError(Exception):
    """Raised when a 403 persists after all recovery strategies."""
    pass


class AntiBlock403:
    """
    Handles 403 Forbidden errors with multiple escalating strategies:
      1. Retry with same headers (might be transient)
      2. Rotate User-Agent
      3. Add / rotate Referer
      4. Add Accept-Language header
      5. Clear cookies and retry
      6. Add Origin header matching the host
      7. Exponential back-off between each attempt
    Raises ForbiddenError if all strategies exhausted.
    """

    def __init__(self, session, max_attempts: int = 7):
        self._session = session
        self._max_attempts = max_attempts
        self._ua_pool = USER_AGENTS.copy()
        random.shuffle(self._ua_pool)
        self._ua_idx = 0

    def _next_ua(self) -> str:
        ua = self._ua_pool[self._ua_idx % len(self._ua_pool)]
        self._ua_idx += 1
        return ua

    def fetch_with_403_recovery(
        self,
        url: str,
        extra_headers: Optional[Dict] = None,
        timeout: int = 30,
        stream: bool = False,
    ) -> "requests.Response":
        """
        GET `url` with automatic 403 recovery.
        Returns response on success, raises ForbiddenError after exhaustion.
        """
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}"

        strategies = [
            # (ua, referer, extra)
            (self._next_ua(), "", {}),
            (self._next_ua(), REFERER_CANDIDATES[0], {"Accept-Language": "en-US,en;q=0.9"}),
            (self._next_ua(), origin + "/", {"Origin": origin}),
            (self._next_ua(), REFERER_CANDIDATES[1], {"Accept-Language": "en-GB,en;q=0.8"}),
            (self._next_ua(), "", {"Cache-Control": "no-cache", "Pragma": "no-cache"}),
            (self._next_ua(), REFERER_CANDIDATES[0], {"Origin": origin, "Accept-Language": "en-US,en;q=0.9"}),
            (self._next_ua(), "", {}),  # final attempt bare
        ]

        backoff = 1.0
        for attempt, (ua, referer, xtra) in enumerate(strategies[:self._max_attempts]):
            headers = {
                "User-Agent": ua,
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            }
            if referer:
                headers["Referer"] = referer
            headers.update(xtra)
            if extra_headers:
                headers.update(extra_headers)

            if attempt == 4:
                # Strategy 5: clear cookies
                log.info("[403-Recovery] Clearing cookies on attempt %d", attempt + 1)
                self._session.cookies.clear()

            try:
                resp = self._session.get(
                    url, headers=headers, timeout=timeout,
                    stream=stream, allow_redirects=True,
                )
                if resp.status_code == 403:
                    log.warning(
                        "[403-Recovery] Attempt %d/%d → still 403 (UA: %s…)",
                        attempt + 1, self._max_attempts, ua[:40]
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 20)
                    continue
                resp.raise_for_status()
                if attempt > 0:
                    log.info("[403-Recovery] Recovered on attempt %d", attempt + 1)
                return resp

            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code == 403:
                    log.warning(
                        "[403-Recovery] Attempt %d/%d → 403 HTTPError",
                        attempt + 1, self._max_attempts
                    )
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 20)
                    continue
                raise

        raise ForbiddenError(
            f"403 Forbidden persisted after {self._max_attempts} recovery "
            f"strategies for URL: {url}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# HTTP SESSION
# ─────────────────────────────────────────────────────────────────────────────

def build_session(
    headers: Optional[Dict] = None,
    proxy: Optional[str] = None,
    cookie_browser: Optional[str] = None,
    throttle_kbps: Optional[int] = None,
) -> "requests.Session":
    if not HAS_REQUESTS:
        raise RuntimeError("pip install requests")
    s = requests.Session()

    retry = Retry(
        total=3,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=50)
    s.mount("https://", adapter)
    s.mount("http://", adapter)

    default_headers = {
        "User-Agent": USER_AGENTS[0],
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    if headers:
        default_headers.update(headers)
    s.headers.update(default_headers)

    if proxy:
        s.proxies = {"http": proxy, "https": proxy}

    if cookie_browser and HAS_BROWSER_COOKIE3:
        try:
            getter = getattr(browser_cookie3, cookie_browser.lower(), None)
            if getter:
                s.cookies.update(getter())
                log.info("Injected cookies from browser: %s", cookie_browser)
            else:
                log.warning("Unknown browser '%s'.", cookie_browser)
        except Exception as e:
            log.warning("Could not load browser cookies: %s", e)
    elif cookie_browser and not HAS_BROWSER_COOKIE3:
        log.warning("browser_cookie3 not installed. pip install browser-cookie3")

    if throttle_kbps and throttle_kbps > 0:
        _patch_session_throttle(s, throttle_kbps)

    return s


def _patch_session_throttle(session, kbps: int):
    original_get = session.get
    bytes_per_sec = kbps * 1024

    def throttled_get(*args, **kwargs):
        kwargs.setdefault("stream", True)
        resp = original_get(*args, **kwargs)
        chunks = []
        start = time.time()
        downloaded = 0
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                chunks.append(chunk)
                downloaded += len(chunk)
                elapsed = time.time() - start
                expected = downloaded / bytes_per_sec
                if expected > elapsed:
                    time.sleep(expected - elapsed)
        resp._content = b"".join(chunks)
        resp._content_consumed = True
        return resp

    session.get = throttled_get
    log.info("Bandwidth throttle active: %d KB/s", kbps)


# ─────────────────────────────────────────────────────────────────────────────
# M3U8 PARSER  (binary-safe)
# ─────────────────────────────────────────────────────────────────────────────

class Segment:
    __slots__ = ("url", "index", "duration", "key_url", "iv",
                 "method", "byterange", "discontinuity")

    def __init__(self, url, index, duration=0.0,
                 key_url=None, iv=None, method="NONE",
                 byterange=None, discontinuity=False):
        self.url = url
        self.index = index
        self.duration = duration
        self.key_url = key_url
        self.iv = iv
        self.method = method
        self.byterange = byterange
        self.discontinuity = discontinuity


class M3U8Parser:
    def __init__(self, content: str, base_url: str):
        self.content = content
        self.base_url = base_url
        self.segments: List[Segment] = []
        self.streams: List[Dict] = []
        self.is_master = False
        self.is_live = True
        self.target_duration = 0
        self.total_duration = 0.0
        self._parse()

    def _abs(self, uri: str) -> str:
        return _safe_urljoin(self.base_url, uri)

    def _parse(self):
        lines = self.content.splitlines()
        current_key_url = None
        current_iv = None
        current_method = "NONE"
        current_byterange = None
        byterange_offset = 0
        seg_index = 0
        seg_duration = 0.0
        discontinuity_next = False

        i = 0
        while i < len(lines):
            line = lines[i].strip()
            i += 1

            if not line or line == "#EXTM3U":
                continue

            if line.startswith("#EXT-X-STREAM-INF"):
                self.is_master = True
                info = self._parse_attributes(line)
                bw = int(info.get("BANDWIDTH", 0))
                res = info.get("RESOLUTION", "?x?")
                codecs = info.get("CODECS", "")
                for j in range(i, len(lines)):
                    candidate = lines[j].strip()
                    if candidate and not candidate.startswith("#"):
                        self.streams.append({
                            "bandwidth": bw,
                            "resolution": res,
                            "codecs": codecs,
                            "uri": self._abs(candidate),
                        })
                        i = j + 1
                        break
                continue

            if line.startswith("#EXT-X-ENDLIST"):
                self.is_live = False
                continue

            if line.startswith("#EXT-X-TARGETDURATION"):
                try:
                    self.target_duration = int(line.split(":")[1])
                except Exception:
                    pass
                continue

            if line.startswith("#EXT-X-DISCONTINUITY"):
                discontinuity_next = True
                continue

            if line.startswith("#EXT-X-KEY"):
                attrs = self._parse_attributes(line)
                current_method = attrs.get("METHOD", "NONE")
                uri = attrs.get("URI", "").strip('"')
                current_key_url = self._abs(uri) if uri else None
                iv_hex = attrs.get("IV", "")
                if iv_hex.startswith(("0x", "0X")):
                    current_iv = bytes.fromhex(iv_hex[2:].zfill(32))
                else:
                    current_iv = None
                continue

            if line.startswith("#EXT-X-BYTERANGE"):
                val = line.split(":")[1]
                if "@" in val:
                    length, offset = val.split("@")
                    byterange_offset = int(offset)
                else:
                    length = val
                current_byterange = (int(length), byterange_offset)
                byterange_offset += int(length)
                continue

            if line.startswith("#EXTINF"):
                try:
                    seg_duration = float(line.split(":")[1].rstrip(","))
                    self.total_duration += seg_duration
                except Exception:
                    seg_duration = 0.0
                continue

            if line and not line.startswith("#"):
                if not self.is_master:
                    iv = current_iv if current_iv else seg_index.to_bytes(2, "big")
                    self.segments.append(Segment(
                        url=self._abs(line),
                        index=seg_index,
                        duration=seg_duration,
                        key_url=current_key_url,
                        iv=iv,
                        method=current_method,
                        byterange=current_byterange,
                        discontinuity=discontinuity_next,
                    ))
                    seg_index += 1
                    current_byterange = None
                    seg_duration = 0.0
                    discontinuity_next = False

    @staticmethod
    def _parse_attributes(line: str) -> Dict[str, str]:
        attrs: Dict[str, str] = {}
        body = line.split(":", 1)[1] if ":" in line else ""
        for part in re.split(r',(?=(?:[^"]*"[^"]*")*[^"]*$)', body):
            if "=" in part:
                k, v = part.split("=", 1)
                attrs[k.strip()] = v.strip()
        return attrs


# ─────────────────────────────────────────────────────────────────────────────
# DECRYPTION
# ─────────────────────────────────────────────────────────────────────────────

def decrypt_aes128(data: bytes, key: bytes, iv: bytes) -> bytes:
    if HAS_CRYPTO:
        cipher = AES.new(key, AES.MODE_CBC, iv)
        return cipher.decrypt(data)
    elif HAS_CRYPTOGRAPHY:
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
        d = cipher.decryptor()
        return d.update(data) + d.finalize()
    else:
        raise RuntimeError("pip install pycryptodome  OR  pip install cryptography")


def strip_pkcs7(data: bytes) -> bytes:
    if not data:
        return data
    pad = data[-1]
    if 1 <= pad <= 16 and data[-pad:] == bytes([pad]) * pad:
        return data[:-pad]
    return data


# ─────────────────────────────────────────────────────────────────────────────
# SEGMENT INTEGRITY
# ─────────────────────────────────────────────────────────────────────────────

def is_valid_ts(data: bytes) -> bool:
    if len(data) < 188:
        return len(data) > 0
    for start in range(188):
        if start >= len(data):
            break
        if data[start] == 0x47:
            valid = True
            for i in range(1, min(5, (len(data) - start) // 188)):
                if data[start + i * 188] != 0x47:
                    valid = False
                    break
            if valid:
                return True
    return len(data) > 0


# ─────────────────────────────────────────────────────────────────────────────
# FFMPEG HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def find_ffmpeg_tool(name: str) -> Optional[str]:
    if shutil.which(name):
        return name
    win_locations = [
        rf"C:\ffmpeg\bin\{name}.exe",
        rf"C:\Program Files\ffmpeg\bin\{name}.exe",
        rf"C:\Program Files (x86)\ffmpeg\bin\{name}.exe",
        os.path.expanduser(rf"~\ffmpeg\bin\{name}.exe"),
        os.path.expanduser(rf"~\AppData\Local\Programs\ffmpeg\bin\{name}.exe"),
    ]
    for loc in win_locations:
        if os.path.isfile(loc):
            return loc
    return None


def detect_resolution_ffprobe(filepath: Path) -> Optional[Tuple[int, int]]:
    ffprobe_bin = find_ffmpeg_tool("ffprobe")
    if not ffprobe_bin:
        return None
    popen_kwargs = {}
    if sys.platform == "win32":
        popen_kwargs["creationflags"] = 0x08000000
    try:
        cmd = [
            ffprobe_bin, "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height",
            "-of", "csv=p=0",
            str(filepath),
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10, **popen_kwargs)
        if result.returncode == 0 and result.stdout.strip():
            parts = result.stdout.strip().split(",")
            if len(parts) >= 2:
                return (int(parts[0]), int(parts[1]))
    except Exception:
        pass
    return None


def classify_resolution(width: int, height: int) -> str:
    h = min(width, height) if width > 0 and height > 0 else max(width, height)
    if h >= 2160:   return "4K (2160p) 🔵"
    elif h >= 1440: return "1440p (2K) 🟣"
    elif h >= 1080: return "1080p (Full HD) 🟢"
    elif h >= 720:  return "720p (HD) 🟡"
    elif h >= 480:  return "480p (SD) 🟠"
    elif h >= 360:  return "360p 🔴"
    elif h >= 240:  return "240p 🔴"
    else:           return f"{h}p (Low) 🔴"


def detect_resolution_from_url(url: str) -> Optional[str]:
    known = {2160, 1440, 1080, 720, 480, 360, 240}
    patterns = [r"[/_\-](\d{3,4})p", r"[/_\-](\d{3,4})[/_\-]", r"(\d{3,4})x(\d{3,4})"]
    for pat in patterns:
        m = re.search(pat, url, re.IGNORECASE)
        if m:
            try:
                val = int(m.group(1))
                if val in known:
                    return classify_resolution(0, val)
            except Exception:
                pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# PROGRESS TRACKER
# ─────────────────────────────────────────────────────────────────────────────

class Progress:
    def __init__(self, total: int):
        self.total = total
        self.done = 0
        self.failed = 0
        self.bytes_dl = 0
        self._lock = threading.Lock()
        self._start = time.time()
        self._speeds: List[float] = []

    def update(self, n_bytes: int = 0, failed: bool = False):
        with self._lock:
            if failed:
                self.failed += 1
            else:
                self.done += 1
                self.bytes_dl += n_bytes
                elapsed = time.time() - self._start
                if elapsed > 0:
                    self._speeds.append(n_bytes / elapsed)
                    if len(self._speeds) > 20:
                        self._speeds.pop(0)
            self._render()

    def _avg_speed(self) -> float:
        return sum(self._speeds) / len(self._speeds) if self._speeds else 0

    def _render(self):
        done = self.done + self.failed
        pct = done / self.total if self.total else 0
        bar_w = 35
        filled = int(bar_w * pct)
        bar = "█" * filled + "░" * (bar_w - filled)
        elapsed = time.time() - self._start
        speed = self._avg_speed()
        eta = ((self.total - done) / (done / elapsed)) if done and elapsed else 0
        mb = self.bytes_dl / 1_048_576
        line = (
            f"\r  [{bar}] {pct*100:5.1f}%  "
            f"{self.done}/{self.total} segs  "
            f"{mb:6.1f} MB  "
            f"{speed/1024:.0f} KB/s  "
            f"ETA {eta:.0f}s  ✗{self.failed}"
        )
        print(line, end="", flush=True)

    def finish(self) -> Dict:
        elapsed = time.time() - self._start
        print(
            f"\n  ✔  {self.done} segments in {elapsed:.1f}s  "
            f"({self.bytes_dl/1_048_576:.2f} MB)  Failed: {self.failed}"
        )
        return {
            "segments_ok": self.done,
            "segments_failed": self.failed,
            "bytes_downloaded": self.bytes_dl,
            "elapsed_seconds": round(elapsed, 2),
        }


# ─────────────────────────────────────────────────────────────────────────────
# KEY CACHE
# ─────────────────────────────────────────────────────────────────────────────

class KeyCache:
    def __init__(self, session, anti403: "AntiBlock403"):
        self._cache: Dict[str, bytes] = {}
        self._lock = threading.Lock()
        self._session = session
        self._anti403 = anti403

    def get(self, url: str) -> bytes:
        with self._lock:
            if url in self._cache:
                return self._cache[url]
        try:
            resp = self._anti403.fetch_with_403_recovery(url, timeout=15)
        except ForbiddenError:
            raise
        key = resp.content
        with self._lock:
            self._cache[url] = key
        return key


# ─────────────────────────────────────────────────────────────────────────────
# SEGMENT DOWNLOADER  (with 403 recovery)
# ─────────────────────────────────────────────────────────────────────────────

def download_segment(
    seg: Segment,
    tmp_dir: Path,
    session,
    anti403: AntiBlock403,
    key_cache: KeyCache,
    progress: Progress,
    max_retries: int = 5,
) -> Optional[Path]:
    if _ABORT.is_set():
        return None

    out_path = tmp_dir / f"seg_{seg.index:08d}.ts"

    if out_path.exists() and out_path.stat().st_size > 0:
        progress.update(out_path.stat().st_size)
        return out_path

    req_headers = {}
    if seg.byterange:
        length, offset = seg.byterange
        req_headers["Range"] = f"bytes={offset}-{offset+length-1}"

    backoff = 1.0

    for attempt in range(max_retries):
        if _ABORT.is_set():
            return None
        try:
            # Use 403-aware fetcher for segment downloads
            resp = anti403.fetch_with_403_recovery(
                seg.url, extra_headers=req_headers, timeout=30
            )
            raw = resp.content

            if seg.method in ("AES-128", "AES-128-CTR"):
                key = key_cache.get(seg.key_url)
                raw = decrypt_aes128(raw, key, seg.iv)
                raw = strip_pkcs7(raw)
            elif seg.method == "SAMPLE-AES":
                log.warning("SAMPLE-AES on segment %d — skipping frame-level decrypt.", seg.index)

            if not is_valid_ts(raw):
                raise ValueError(f"Integrity check failed: size={len(raw)}")

            out_path.write_bytes(raw)
            progress.update(len(raw))
            return out_path

        except ForbiddenError as exc:
            log.error("Segment %d: 403 unrecoverable — %s", seg.index, exc)
            progress.update(failed=True)
            return None

        except Exception as exc:
            if attempt == max_retries - 1:
                log.warning("Segment %d failed after %d attempts: %s",
                             seg.index, max_retries, exc)
                progress.update(failed=True)
                return None
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    return None


# ─────────────────────────────────────────────────────────────────────────────
# LIVE STREAM POLLER
# ─────────────────────────────────────────────────────────────────────────────

def live_segment_generator(playlist_url: str, session, anti403: AntiBlock403,
                            poll_interval: float = 2.0,
                            stall_timeout: float = 120.0):
    seen: Dict[str, int] = {}
    last_new_seg_time = time.time()
    prev_max_index = -1

    while not _ABORT.is_set():
        try:
            resp = anti403.fetch_with_403_recovery(playlist_url, timeout=20)
            content = resp.content.decode("latin-1")
            parser = M3U8Parser(content, playlist_url)

            new_count = 0
            for seg in parser.segments:
                if seg.url not in seen:
                    if prev_max_index >= 0 and seg.index > prev_max_index + 1:
                        gap = seg.index - prev_max_index - 1
                        log.warning("Live gap: %d missing segs (idx %d–%d)",
                                    gap, prev_max_index + 1, seg.index - 1)
                    seen[seg.url] = seg.index
                    prev_max_index = max(prev_max_index, seg.index)
                    new_count += 1
                    last_new_seg_time = time.time()
                    yield seg

            if not parser.is_live:
                log.info("EXT-X-ENDLIST – stream ended.")
                return

            if new_count == 0:
                stall_secs = time.time() - last_new_seg_time
                if stall_secs > stall_timeout:
                    log.warning("Stream stalled for %.0f s – stopping.", stall_secs)
                    return

        except ForbiddenError as e:
            log.error("Live poll 403 unrecoverable: %s", e)
            return
        except Exception as e:
            log.warning("Playlist poll error: %s", e)

        time.sleep(poll_interval)


# ─────────────────────────────────────────────────────────────────────────────
# MUXER
# ─────────────────────────────────────────────────────────────────────────────

def mux_to_mp4(segment_paths: List[Path], output: Path, ffmpeg: str = "ffmpeg") -> bool:
    ffmpeg_bin = find_ffmpeg_tool("ffmpeg") if ffmpeg == "ffmpeg" else ffmpeg
    if not ffmpeg_bin:
        log.warning("FFmpeg not found – skipping mux.")
        return False

    popen_kwargs = {}
    if sys.platform == "win32":
        popen_kwargs["creationflags"] = 0x08000000

    combined_ts = output.parent / f"_combined_{uuid.uuid4().hex}.ts"
    try:
        log.info("Concatenating %d segments …", len(segment_paths))
        with open(combined_ts, "wb") as fout:
            for p in segment_paths:
                fout.write(p.read_bytes())

        cmd = [
            ffmpeg_bin, "-y",
            "-i", str(combined_ts.resolve()).replace("\\", "/"),
            "-c", "copy",
            "-movflags", "+faststart",
            "-v", "warning",
            str(output.resolve()).replace("\\", "/"),
        ]
        log.info("Remuxing TS → MP4: %s", output.name)
        result = subprocess.run(
            cmd, capture_output=True, text=True,
            encoding="utf-8", errors="replace", **popen_kwargs,
        )
        if result.returncode != 0:
            log.error("FFmpeg failed (exit %d):\n%s", result.returncode, result.stderr[-3000:])
            return False
        log.info("FFmpeg remux successful.")
        return True
    finally:
        if combined_ts.exists():
            combined_ts.unlink(missing_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# SINGLE-EPISODE DOWNLOADER
# ─────────────────────────────────────────────────────────────────────────────

class M3U8Downloader:
    def __init__(
        self,
        url: str,
        output: Path,
        workers: int = 2,
        prefer_quality: str = "best",
        headers: Optional[Dict] = None,
        proxy: Optional[str] = None,
        tmp_dir: Optional[Path] = None,
        keep_ts: bool = False,
        max_retries: int = 5,
        ffmpeg: str = "ffmpeg",
        cookie_browser: Optional[str] = None,
        throttle_kbps: Optional[int] = None,
        write_report: bool = False,
        session=None,
    ):
        self.url = url
        self.output = output
        self.workers = workers
        self.prefer_quality = prefer_quality
        self.keep_ts = keep_ts
        self.max_retries = max_retries
        self.ffmpeg = ffmpeg
        self.write_report = write_report
        self._report: Dict = {"url": url, "started_at": datetime.now().isoformat()}

        if session is not None:
            self.session = session
        else:
            self.session = build_session(
                headers=headers,
                proxy=proxy,
                cookie_browser=cookie_browser,
                throttle_kbps=throttle_kbps,
            )

        self.anti403 = AntiBlock403(self.session)

        if tmp_dir:
            self.tmp_dir = tmp_dir
        else:
            self.tmp_dir = Path(tempfile.mkdtemp(prefix="m3u8_"))
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

    def run(self) -> bool:
        """Returns True on success, False on failure."""
        try:
            log.info("Fetching playlist: %s", self.url)
            resp = self.anti403.fetch_with_403_recovery(self.url, timeout=20)
            content = resp.content.decode("latin-1")
            parser = M3U8Parser(content, self.url)
        except ForbiddenError as e:
            log.error("Cannot fetch playlist (403 unrecoverable): %s", e)
            return False
        except Exception as e:
            log.error("Cannot fetch playlist: %s", e)
            return False

        playlist_url = self.url
        if parser.is_master:
            playlist_url = self._select_stream(parser.streams)
            log.info("Selected stream: %s", playlist_url)
            try:
                resp = self.anti403.fetch_with_403_recovery(playlist_url, timeout=20)
                content = resp.content.decode("latin-1")
                parser = M3U8Parser(content, playlist_url)
            except ForbiddenError as e:
                log.error("Cannot fetch sub-playlist (403): %s", e)
                return False

        if parser.is_live:
            log.info("Live stream – polling mode.")
            self._download_live(parser, playlist_url)
        else:
            mins = parser.total_duration / 60
            log.info("VOD – %d segments (~%.1f min)", len(parser.segments), mins)
            self._download_vod(parser.segments)

        return True

    def _select_stream(self, streams: List[Dict]) -> str:
        if not streams:
            raise ValueError("No streams in master playlist.")
        streams_sorted = sorted(streams, key=lambda s: s["bandwidth"])

        if self.prefer_quality == "best":
            chosen = streams_sorted[-1]
        elif self.prefer_quality == "worst":
            chosen = streams_sorted[0]
        else:
            height = int(self.prefer_quality.lower().replace("p", ""))
            matches = [s for s in streams if str(height) in s["resolution"]]
            chosen = matches[0] if matches else streams_sorted[-1]

        res_str = chosen.get("resolution", "?x?")
        if "x" in res_str:
            try:
                w, h = map(int, res_str.split("x"))
                label = classify_resolution(w, h)
                print(f"\n  📐  Stream quality: {res_str}  →  {label}")
            except Exception:
                pass

        self._report["selected_quality"] = chosen.get("resolution", "?")
        self._report["selected_bandwidth"] = chosen.get("bandwidth", 0)
        return chosen["uri"]

    def _download_vod(self, segments: List[Segment]):
        key_cache = KeyCache(self.session, self.anti403)
        progress = Progress(len(segments))
        ordered_paths: Dict[int, Optional[Path]] = {}

        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = {
                pool.submit(
                    download_segment,
                    seg, self.tmp_dir, self.session,
                    self.anti403, key_cache, progress, self.max_retries,
                ): seg.index
                for seg in segments
            }
            for fut in as_completed(futures):
                idx = futures[fut]
                ordered_paths[idx] = fut.result()
                if _ABORT.is_set():
                    pool.shutdown(wait=False, cancel_futures=True)
                    break

        stats = progress.finish()
        self._report.update(stats)
        seg_paths = [ordered_paths[i] for i in sorted(ordered_paths)
                     if ordered_paths.get(i) is not None]
        self._finalize(seg_paths)

    def _download_live(self, parser: M3U8Parser, playlist_url: str):
        key_cache = KeyCache(self.session, self.anti403)
        queue: Queue = Queue(maxsize=300)
        seg_paths: List[Tuple[int, Path]] = []
        done_event = threading.Event()

        def producer():
            for seg in live_segment_generator(playlist_url, self.session, self.anti403):
                if _ABORT.is_set():
                    break
                queue.put(seg)
            queue.put(None)

        def consumer():
            progress = Progress(total=0)
            with ThreadPoolExecutor(max_workers=self.workers) as pool:
                futures = {}
                while not _ABORT.is_set():
                    try:
                        seg = queue.get(timeout=60)
                    except Empty:
                        break
                    if seg is None:
                        break
                    progress.total += 1
                    fut = pool.submit(
                        download_segment,
                        seg, self.tmp_dir, self.session,
                        self.anti403, key_cache, progress, self.max_retries,
                    )
                    futures[fut] = seg.index
                for fut in as_completed(futures):
                    p = fut.result()
                    if p:
                        seg_paths.append((futures[fut], p))
            stats = progress.finish()
            self._report.update(stats)
            done_event.set()

        t_prod = threading.Thread(target=producer, daemon=True)
        t_cons = threading.Thread(target=consumer, daemon=True)
        t_prod.start()
        t_cons.start()
        done_event.wait()

        ordered = [p for _, p in sorted(seg_paths)]
        self._finalize(ordered)

    def _finalize(self, seg_paths: List[Path]):
        if not seg_paths:
            log.error("No segments downloaded.")
            return

        total_raw_mb = sum(p.stat().st_size for p in seg_paths) / 1_048_576
        log.info("Raw segments: %.2f MB (%d files)", total_raw_mb, len(seg_paths))

        mp4_out = self.output.with_suffix(".mp4")
        success = mux_to_mp4(seg_paths, mp4_out, self.ffmpeg)

        if success and mp4_out.exists():
            mp4_mb = mp4_out.stat().st_size / 1_048_576
            if mp4_mb < total_raw_mb * 0.50:
                log.warning("MP4 (%.2f MB) << raw (%.2f MB) — falling back to .ts",
                             mp4_mb, total_raw_mb)
                success = False
                mp4_out.unlink(missing_ok=True)

        if not success:
            ts_out = self.output.with_suffix(".ts")
            log.info("Writing raw .ts → %s", ts_out)
            with open(ts_out, "wb") as fout:
                for p in seg_paths:
                    fout.write(p.read_bytes())
            final = ts_out
            print(f"\n  ⚠️  Saved as .ts  →  ffmpeg -i \"{ts_out}\" -c copy \"{mp4_out}\"")
        else:
            final = mp4_out
            probe = detect_resolution_ffprobe(final)
            if probe:
                w, h = probe
                label = classify_resolution(w, h)
                print(f"  📐  Final resolution: {w}x{h}  →  {label}")

        size_mb = final.stat().st_size / 1_048_576
        log.info("Saved: %s  (%.2f MB)", final, size_mb)
        print(f"\n  ✅  Output : {final}")
        print(f"  📦  Size   : {size_mb:.2f} MB\n")

        self._report["output_file"] = str(final)
        self._report["output_size_mb"] = round(size_mb, 2)
        self._report["finished_at"] = datetime.now().isoformat()

        if self.write_report:
            report_path = final.with_suffix(".json")
            report_path.write_text(json.dumps(self._report, indent=2))
            log.info("Report: %s", report_path)

        if not self.keep_ts:
            shutil.rmtree(self.tmp_dir, ignore_errors=True)
        else:
            log.info("Temp segments kept: %s", self.tmp_dir)


# ─────────────────────────────────────────────────────────────────────────────
# BATCH RUNNER
# ─────────────────────────────────────────────────────────────────────────────

class BatchDownloader:
    """
    Reads m3u8files.txt, parses all episode entries, downloads sequentially.
    Shares a single HTTP session across all episodes.
    Skips episodes whose output file already exists (resumable batch).
    """

    def __init__(
        self,
        playlist_file: str,
        output_dir: str = ".",
        workers: int = 2,
        prefer_quality: str = "best",
        headers: Optional[Dict] = None,
        proxy: Optional[str] = None,
        keep_ts: bool = False,
        max_retries: int = 5,
        ffmpeg: str = "ffmpeg",
        cookie_browser: Optional[str] = None,
        throttle_kbps: Optional[int] = None,
        write_report: bool = False,
        episode_delay: float = 2.0,
    ):
        self.playlist_file = playlist_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.workers = workers
        self.prefer_quality = prefer_quality
        self.keep_ts = keep_ts
        self.max_retries = max_retries
        self.ffmpeg = ffmpeg
        self.write_report = write_report
        self.episode_delay = episode_delay

        # Shared session for all episodes
        self.session = build_session(
            headers=headers,
            proxy=proxy,
            cookie_browser=cookie_browser,
            throttle_kbps=throttle_kbps,
        )

    def run(self):
        print(f"\n  📂  Parsing playlist file: {self.playlist_file}")
        try:
            entries = parse_playlist_file(self.playlist_file)
        except FileNotFoundError as e:
            print(f"\n  ❌  {e}")
            sys.exit(1)

        if not entries:
            print("\n  ❌  No valid m3u8 entries found in playlist file.")
            sys.exit(1)

        print(f"  📋  Found {len(entries)} episode(s)\n")
        for i, ep in enumerate(entries):
            print(f"    [{i+1:03d}] {ep.title}")
        print()

        results = {"ok": [], "skipped": [], "failed": []}
        total = len(entries)

        for idx, ep in enumerate(entries):
            if _ABORT.is_set():
                print("\n  ⚠️  Batch aborted by user.\n")
                break

            print("=" * 70)
            print(f"  🎬  [{idx+1}/{total}]  {ep.title}")
            print(f"  🔗  {ep.url}")
            print("=" * 70)

            out_path = self.output_dir / f"{ep.title}.mp4"
            ts_path  = self.output_dir / f"{ep.title}.ts"

            # Skip if already downloaded
            if out_path.exists() and out_path.stat().st_size > 1_000:
                print(f"  ⏭️  Already exists, skipping: {out_path.name}\n")
                results["skipped"].append(ep.title)
                continue
            if ts_path.exists() and ts_path.stat().st_size > 1_000:
                print(f"  ⏭️  Already exists (.ts), skipping: {ts_path.name}\n")
                results["skipped"].append(ep.title)
                continue

            tmp_dir = Path(tempfile.mkdtemp(prefix=f"m3u8_{idx:03d}_"))

            dl = M3U8Downloader(
                url=ep.url,
                output=out_path,
                workers=self.workers,
                prefer_quality=self.prefer_quality,
                keep_ts=self.keep_ts,
                max_retries=self.max_retries,
                ffmpeg=self.ffmpeg,
                write_report=self.write_report,
                session=self.session,
                tmp_dir=tmp_dir,
            )

            ok = dl.run()
            if ok:
                results["ok"].append(ep.title)
            else:
                results["failed"].append(ep.title)
                print(f"  ❌  Episode failed: {ep.title}\n")

            if idx < total - 1 and not _ABORT.is_set():
                print(f"  ⏳  Waiting {self.episode_delay:.0f}s before next episode …\n")
                time.sleep(self.episode_delay)

        # ── Batch summary ─────────────────────────────────────────────────────
        print("\n" + "=" * 70)
        print("  📊  BATCH SUMMARY")
        print("=" * 70)
        print(f"  ✅  Success  : {len(results['ok'])}")
        print(f"  ⏭️   Skipped  : {len(results['skipped'])}")
        print(f"  ❌  Failed   : {len(results['failed'])}")
        if results["failed"]:
            print("\n  Failed episodes:")
            for t in results["failed"]:
                print(f"    • {t}")
        print()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Advanced M3U8 / HLS Batch Downloader  v3.0",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Source
    source = p.add_mutually_exclusive_group()
    source.add_argument("url", nargs="?",
                        help="Single M3U8 URL (overrides --playlist-file)")
    source.add_argument("-f", "--playlist-file", default="m3u8files.txt",
                        metavar="FILE",
                        help="Playlist file with multiple episodes (default: m3u8files.txt)")

    # Output
    p.add_argument("-o", "--output", default="output.mp4",
                   help="Output filename for single-URL mode (default: output.mp4)")
    p.add_argument("-d", "--output-dir", default=".",
                   help="Output directory for batch mode (default: current dir)")

    # Download settings
    p.add_argument("-w", "--workers", type=int, default=2,
                   help="Concurrent segment download threads (default: 2)")
    p.add_argument("-q", "--quality", default="best",
                   help="Stream quality: best | worst | 1080p | 720p (default: best)")
    p.add_argument("--retries", type=int, default=5,
                   help="Max retries per segment (default: 5)")
    p.add_argument("--episode-delay", type=float, default=2.0,
                   help="Seconds to wait between episodes in batch (default: 2)")

    # Network
    p.add_argument("--header", action="append", metavar="K:V",
                   help="Custom request header (repeatable)")
    p.add_argument("--proxy", help="HTTP/S proxy (e.g. http://127.0.0.1:8080)")
    p.add_argument("--cookies-from-browser", metavar="BROWSER",
                   help="Auto-inject cookies: chrome | firefox | edge | safari")
    p.add_argument("--throttle", type=int, metavar="KB/S",
                   help="Limit download speed in KB/s")

    # Misc
    p.add_argument("--ffmpeg", default="ffmpeg",
                   help="Path to ffmpeg binary (default: ffmpeg)")
    p.add_argument("--keep-ts", action="store_true",
                   help="Keep raw TS segments after download")
    p.add_argument("--report", action="store_true",
                   help="Write a JSON download report for each episode")
    p.add_argument("-v", "--verbose", action="store_true",
                   help="Enable debug logging")

    return p.parse_args()


def main():
    args = parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not HAS_REQUESTS:
        print("Missing dependency: pip install requests")
        sys.exit(1)

    headers = {}
    if args.header:
        for h in args.header:
            if ":" in h:
                k, v = h.split(":", 1)
                headers[k.strip()] = v.strip()

    # ── Single URL mode ────────────────────────────────────────────────────────
    if args.url:
        print(f"""
╔══════════════════════════════════════════════════════════╗
║       Advanced M3U8 Downloader  v3.0  – single URL       ║
╠══════════════════════════════════════════════════════════╣
║  URL     : {args.url[:54]:<54}║
║  Output  : {args.output:<54}║
║  Workers : {args.workers:<54}║
║  Quality : {args.quality:<54}║
╚══════════════════════════════════════════════════════════╝
""")
        session = build_session(
            headers=headers or None,
            proxy=args.proxy,
            cookie_browser=args.cookies_from_browser,
            throttle_kbps=args.throttle,
        )
        dl = M3U8Downloader(
            url=args.url,
            output=Path(args.output),
            workers=args.workers,
            prefer_quality=args.quality,
            keep_ts=args.keep_ts,
            max_retries=args.retries,
            ffmpeg=args.ffmpeg,
            write_report=args.report,
            session=session,
        )
        dl.run()
        return

    # ── Batch mode ─────────────────────────────────────────────────────────────
    playlist_file = args.playlist_file

    print(f"""
╔══════════════════════════════════════════════════════════╗
║       Advanced M3U8 Downloader  v3.0  – BATCH MODE       ║
╠══════════════════════════════════════════════════════════╣
║  Playlist : {playlist_file:<53}║
║  Out dir  : {args.output_dir:<53}║
║  Workers  : {args.workers:<53}║
║  Quality  : {args.quality:<53}║
╚══════════════════════════════════════════════════════════╝
""")

    batch = BatchDownloader(
        playlist_file=playlist_file,
        output_dir=args.output_dir,
        workers=args.workers,
        prefer_quality=args.quality,
        headers=headers or None,
        proxy=args.proxy,
        keep_ts=args.keep_ts,
        max_retries=args.retries,
        ffmpeg=args.ffmpeg,
        cookie_browser=args.cookies_from_browser,
        throttle_kbps=args.throttle,
        write_report=args.report,
        episode_delay=args.episode_delay,
    )
    batch.run()


# ─────────────────────────────────────────────────────────────────────────────
# DIRECT RUN  (edit settings below for quick use without CLI)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":

    # ── Quick batch run (no CLI args needed) ──────────────────────────────────
    # Point PLAYLIST_FILE to your m3u8files.txt and OUTPUT_DIR to where you
    # want the downloaded episodes saved.

    PLAYLIST_FILE = r"C:\Users\AC\Desktop\m3u8.txt"   # ← your playlist file
    OUTPUT_DIR    = "downloads"        # ← where episodes are saved
    WORKERS       = 2                  # ← concurrent segment threads
    QUALITY       = "best"             # ← best | worst | 1080p | 720p | …

    if len(sys.argv) == 1:
        # No CLI args → run batch directly
        batch = BatchDownloader(
            playlist_file=PLAYLIST_FILE,
            output_dir=OUTPUT_DIR,
            workers=WORKERS,
            prefer_quality=QUALITY,
            write_report=True,
        )
        batch.run()
    else:
        main()

"""Production-grade YouTube audio downloader with structured error handling.

Architecture
-----------
- ``classify_error`` / ``should_retry``: separate permanent errors (auth
  required, private/unavailable videos) from transient retryable errors
  (timeouts, 5xx, connection resets).
- ``fetch_metadata`` / ``download_media``: modular wrapper around yt-dlp.
- ``process_track``: orchestrates metadata -> download -> compress -> upload
  -> save, applying exponential backoff only to retryable failures and
  aborting immediately on ``AuthenticationRequiredError``.

The public interface ``process_all`` and ``expand_all_urls`` is preserved so
the Flask app (``main.py``) continues to work unchanged.
"""

import base64
import datetime
import logging
import os
import random
import shutil
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import lru_cache
from typing import Callable, Dict, List, Optional, Tuple, Union

import requests
import yt_dlp
from dotenv import load_dotenv
from PIL import Image
from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor

from download_errors import (
    AuthenticationRequiredError,
    DownloadError,
    MetadataError,
    RetryableDownloadError,
    VideoUnavailableError,
    extract_video_id,
)

load_dotenv()

# ========== CONFIG ==========
CLOUD_NAME = os.getenv("CLOUD_NAME")
UPLOAD_PRESET = os.getenv("UPLOAD_PRESET")
DOWNLOAD_FOLDER = os.getenv("DOWNLOAD_FOLDER", "downloads")
MAX_IMAGE_SIZE = 800
JPEG_QUALITY = 85
AUDIO_BITRATE = "256k"

FIREBASE_API_KEY = os.getenv("FIREBASE_API_KEY")
FIREBASE_PROJECT_ID = os.getenv("FIREBASE_PROJECT_ID")
OWNER_ID = os.getenv("OWNER_ID")

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", 5))

# --- Authentication configuration ---
# 1) A Netscape-format cookies file (e.g. exported from "Get cookies.txt LOCALLY"
#    or exported via `yt-dlp --cookies-from-browser chrome --cookies cookies.txt`).
#    Works reliably inside Docker since it is just a file on disk.
#    The first line must be "# HTTP Cookie File" or "# Netscape HTTP Cookie File".
COOKIES_FILE = os.getenv("COOKIES_FILE", "")
# 2) Browser cookies from a local browser profile. This is a convenience for
#    local/dev use; the browser must be installed on the machine running the
#    code. It is NOT available inside a headless Docker container by default.
COOKIES_FROM_BROWSER = os.getenv("COOKIES_FROM_BROWSER", "")  # e.g. "chrome" or "firefox"
# 3) Base64-encoded cookies.txt content. This is the SECURE way to ship cookies
#    to a cloud host (Render) without committing the file to the repo. Set it as
#    a Render Secret/Environment Variable:
#        COOKIES_BASE64=$(base64 -w0 cookies.txt)
#    At startup we decode it and write it to ./cookies.txt, then COOKIES_FILE
#    points at it. Do NOT commit cookies.txt to version control.
COOKIES_BASE64 = os.getenv("COOKIES_BASE64", "")

# 4) Optional SOCKS5/HTTP proxy to route YouTube traffic through.
#    This is the alternative to cookies for bypassing IP reputation blocks.
#    Use a RESIDENTIAL or rotating proxy (e.g. BrightData, Oxylabs, Webshare,
#    or a self-hosted proxy). Datacenter proxies usually won't help.
#    Format: socks5://user:pass@host:port  or  http://user:pass@host:port
#    Example: PROXY_URL="socks5://user:pass@host:1080"
PROXY_URL = os.getenv("PROXY_URL", "")

# yt-dlp needs a JavaScript runtime and the EJS challenge solver for current
# YouTube extraction. Node is installed in the Docker image and can also be
# selected explicitly for local runs.
YTDLP_JS_RUNTIME = os.getenv("YTDLP_JS_RUNTIME", "node")
YTDLP_REMOTE_COMPONENTS = os.getenv("YTDLP_REMOTE_COMPONENTS", "ejs:github")

# --- Retry / rate-limit policy ---
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
BASE_BACKOFF = float(os.getenv("BASE_BACKOFF", 2.0))
MAX_BACKOFF = float(os.getenv("MAX_BACKOFF", 30.0))
# Seconds to sleep between individual track downloads to avoid hammering YouTube.
TRACK_DELAY = float(os.getenv("TRACK_DELAY", 1.0))

# Set DEBUG=1 to log generated yt-dlp options, extractor args, auth config,
# and container environment details (excluding sensitive cookie contents).
DEBUG = os.getenv("DEBUG", "0").lower() in ("1", "true", "yes", "on")

# Cookie file materialized from COOKIES_BASE64 at startup.
COOKIES_MATERIALIZED_PATH = os.getenv("COOKIES_MATERIALIZED_PATH", "cookies.txt")

# Structured logging instead of print().
logger = logging.getLogger("uploader")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    ))
    logger.addHandler(handler)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


class _YtDlpLogger:
    def debug(self, message):
        logger.debug("yt-dlp: %s", message)

    def warning(self, message):
        logger.debug("yt-dlp warning: %s", message)

    def error(self, message):
        logger.debug("yt-dlp attempt: %s", message)


if not all([CLOUD_NAME, UPLOAD_PRESET, FIREBASE_API_KEY, FIREBASE_PROJECT_ID, OWNER_ID]):
    logger.error("Missing environment variables. Check .env")
    sys.exit(1)

os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
session = requests.Session()


def _validate_cookies_file(path: str) -> None:
    """Validate that a cookies file is in correct Mozilla/Netscape format.

    Per the yt-dlp FAQ, the first line must be exactly either
    ``# HTTP Cookie File`` or ``# Netscape HTTP Cookie File``, and the newline
    style must match the OS (LF on Unix, CRLF on Windows) — an HTTP 400 when
    using ``--cookies`` is a common sign of invalid newline format.
    """
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            first_line = f.readline().rstrip("\r\n")
    except OSError as e:
        logger.warning("Could not read COOKIES_FILE '%s': %s", path, e)
        return

    if first_line not in ("# HTTP Cookie File", "# Netscape HTTP Cookie File"):
        logger.warning(
            "COOKIES_FILE '%s' does not start with a valid Netscape header "
            "('# HTTP Cookie File' or '# Netscape HTTP Cookie File'). "
            "yt-dlp may reject it with HTTP 400. Export cookies with the "
            "'Get cookies.txt LOCALLY' extension or via "
            "`yt-dlp --cookies-from-browser chrome --cookies cookies.txt`.",
            path,
        )


def _materialize_cookies() -> None:
    """Copy configured cookies into a writable runtime file when needed.

    Render Secret Files are read-only, but yt-dlp may update its cookie file.
    Copy mounted files to COOKIES_MATERIALIZED_PATH before passing them to yt-dlp.
    COOKIES_FILE takes precedence over COOKIES_BASE64 when its source exists.
    """
    global COOKIES_FILE
    path = os.path.abspath(COOKIES_MATERIALIZED_PATH)
    if COOKIES_FILE and os.path.exists(COOKIES_FILE):
        source = os.path.abspath(COOKIES_FILE)
        if source != path:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            shutil.copyfile(source, path)
            os.chmod(path, 0o600)
            logger.info("COOKIES_FILE copied to writable runtime path %s", path)
        COOKIES_FILE = path
        return

    if not COOKIES_BASE64:
        return
    try:
        content = base64.b64decode(COOKIES_BASE64.encode("utf-8")).decode("utf-8")
    except Exception as e:  # noqa: BLE001
        logger.warning("COOKIES_BASE64 could not be decoded: %s", e)
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    os.chmod(path, 0o600)
    COOKIES_FILE = path
    logger.info("COOKIES_BASE64 decoded and written to %s", path)


def _normalize_cookies_text(text: str) -> str:
    """Sanitize pasted cookies into a clean Netscape cookies.txt body."""
    lines = [ln.rstrip("\r") for ln in text.splitlines()]
    while lines and not lines[-1].strip():
        lines.pop()
    return "\n".join(lines) + "\n"


def set_cookies_from_text(cookies_text: str) -> str:
    """
    Persist user-supplied cookies (pasted in the web UI) to disk and activate
    them globally for all subsequent yt-dlp calls.

    Returns a human-readable status message.
    """
    global COOKIES_FILE
    content = _normalize_cookies_text(cookies_text)
    if not content.strip():
        raise ValueError("No cookies provided")

    # Basic validation: ensure it looks like a Netscape cookies file
    # (a header line starting with '#' + 'cookie') or loose cookie rows
    # that contain at least one tab-separated line.
    first_line = content.splitlines()[0].strip()
    is_netscape = first_line.startswith("#") and "cookie" in first_line.lower()
    has_tab_rows = "\t" in content
    if not (is_netscape or has_tab_rows):
        raise ValueError(
            "Invalid cookies format. Paste a Netscape cookies.txt file "
            "(from the 'Get cookies.txt LOCALLY' extension) or a "
            "browser-exported cookies file."
        )

    path = os.path.join(os.getcwd(), "cookies.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    COOKIES_FILE = path
    logger.info("Cookies updated from web UI (%d chars).", len(content))
    return f"Cookies saved and activated ({len(content)} chars)."


def clear_cookies() -> str:
    """Remove the runtime cookies file and deactivate authentication."""
    global COOKIES_FILE
    path = os.path.join(os.getcwd(), "cookies.txt")
    if os.path.exists(path):
        try:
            os.remove(path)
        except OSError:
            pass
    COOKIES_FILE = ""
    logger.info("Cookies cleared from web UI.")
    return "Cookies cleared."


def get_cookie_status() -> Dict[str, object]:
    """Return a safe summary of the current cookie configuration for the UI."""
    cookies_exist = bool(COOKIES_FILE and os.path.exists(COOKIES_FILE))
    return {
        "configured": bool(COOKIES_FILE or COOKIES_BASE64 or COOKIES_FROM_BROWSER),
        "active": cookies_exist,
        "mode": "file" if cookies_exist else "none",
        "size": os.path.getsize(COOKIES_FILE) if cookies_exist else 0,
    }


def log_auth_config_warning() -> None:
    """Log a clear startup reminder if no authentication is configured.

    YouTube frequently requires authenticated cookies (especially on datacenter
    IPs like Render). This check surfaces the requirement early so the user is
    not surprised by per-track ``AuthenticationRequiredError`` failures later.
    """
    if not COOKIES_FILE and not COOKIES_FROM_BROWSER and not COOKIES_BASE64:
        logger.warning(
            "No YouTube authentication configured. Video downloads may fail with "
            "'Sign in to confirm you're not a bot'. Set COOKIES_FILE (path to a "
            "cookies.txt), COOKIES_BASE64 (base64-encoded cookies.txt), or "
            "COOKIES_FROM_BROWSER (e.g. 'chrome'/'firefox') to authenticate. "
            "In Docker/cloud, use COOKIES_BASE64 or a mounted cookies.txt file."
        )
    elif COOKIES_FILE and not os.path.exists(COOKIES_FILE):
        logger.warning(
            "COOKIES_FILE is set to '%s' but the file does not exist. "
            "Check the path is correct and the file is mounted into the container.",
            COOKIES_FILE,
        )
    elif COOKIES_FILE:
        _validate_cookies_file(COOKIES_FILE)


def _command_version(cmd: str) -> str:
    """Return the version string of a CLI binary, or an error message."""
    try:
        result = subprocess.run(
            [cmd, "--version"], capture_output=True, text=True, timeout=30
        )
        out = (result.stdout or result.stderr or "").strip().splitlines()
        return out[0] if out else "(no output)"
    except FileNotFoundError:
        return "NOT FOUND"
    except Exception as e:  # noqa: BLE001
        return f"ERROR: {e}"


def _yt_dlp_version() -> str:
    """Return the version from the active Python environment."""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "yt_dlp", "--version"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        out = (result.stdout or result.stderr or "").strip().splitlines()
        return out[0] if out else "(no output)"
    except Exception as e:  # noqa: BLE001 - diagnostics must not stop startup
        return f"ERROR: {e}"


def run_startup_diagnostics() -> None:
    """Log environment diagnostics and run a cookie/YTDL self-test.

    Logs (excluding sensitive cookie contents):
      - yt-dlp and ffmpeg versions
      - cookie configuration mode and file existence/readability/header
      - whether the cookie option reaches a constructed YoutubeDL instance
      - (when DEBUG=1) full generated yt-dlp options and container details
    """
    logger.info("===== STARTUP DIAGNOSTICS =====")
    logger.info("yt-dlp version: %s", _yt_dlp_version())
    logger.info("ffmpeg version: %s", _command_version("ffmpeg"))

    # Cookie configuration.
    cookies_exist = bool(COOKIES_FILE and os.path.exists(COOKIES_FILE))
    cookies_readable = False
    header_ok = None
    if cookies_exist:
        try:
            with open(COOKIES_FILE, "r", encoding="utf-8", errors="replace") as f:
                first_line = f.readline().rstrip("\r\n")
            cookies_readable = True
            header_ok = first_line in ("# HTTP Cookie File", "# Netscape HTTP Cookie File")
        except OSError:
            cookies_readable = False

    if COOKIES_FILE:
        logger.info("Cookie mode: COOKIES_FILE=%s", COOKIES_FILE)
    elif COOKIES_BASE64:
        logger.info("Cookie mode: COOKIES_BASE64 (decoded at startup)")
    elif COOKIES_FROM_BROWSER:
        logger.info("Cookie mode: COOKIES_FROM_BROWSER=%s (NOT available in headless Docker)", COOKIES_FROM_BROWSER)
    else:
        logger.info("Cookie mode: NONE")

    logger.info("Cookie file exists: %s", cookies_exist)
    logger.info("Cookie file readable: %s", cookies_readable)
    logger.info("Cookie header valid Netscape: %s", header_ok)

    # Verify the cookie option reaches a YoutubeDL instance.
    opts = make_ydl_opts()
    has_cookie_opt = "cookiefile" in opts or "cookiesfrombrowser" in opts
    logger.info("YoutubeDL receives cookie option: %s", has_cookie_opt)

    if DEBUG:
        # Log non-sensitive option keys (redact any keys that could contain
        # credential material).
        safe_opts = {}
        for k, v in opts.items():
            if k in ("cookiefile", "http_headers", "extractor_args"):
                continue  # path/headers could hint at local state; skip
            safe_opts[k] = v
        logger.info("yt-dlp options (redacted): %s", safe_opts)
        logger.info("extractor_args: %s", opts.get("extractor_args"))
        logger.info("PLAYER_CLIENT_STRATEGIES: %s", PLAYER_CLIENT_STRATEGIES)
        logger.info("Debug: DEBUG=1, cwd=%s, user=%s", os.getcwd(), (os.getenv("USER") or os.getenv("USERNAME") or "unknown"))

    logger.info("===== END STARTUP DIAGNOSTICS =====")


# ========== YOUTUBE BOT-DETECTION BYPASS ==========
# Render uses datacenter IPs that YouTube aggressively blocks with
# "Sign in to confirm you're not a bot". We try multiple player clients and
# user-agent strategies. Cookies remain the most reliable fix.
#
# NOTE (2024+): The classic clients (android, tv, ios, web_embedded) are now
# heavily fingerprinted and blocked from datacenter IPs. Newer / less-common
# clients are tried first because they are not yet as aggressively targeted.
# Combined strategies (comma-separated) let yt-dlp fall back across clients
# within a single extraction, which often passes the bot check when a single
# client fails.
PLAYER_CLIENT_STRATEGIES: List[str] = [
    # `default` reliably returns full format lists (verified with cookies).
    # It uses yt-dlp's own intelligent client selection.
    "default",
    # Additional fallbacks tried in order if `default` is blocked.
    "android",
    "tv",
    "ios",
    "web_embedded",
    "android_vr",
    "tv_embedded",
    "web_safari",
    "mweb",
    "android_prod",
    "ios_safarivp",
    # Combined multi-client fallbacks (yt-dlp will try each in order internally).
    "android_vr,tv_embedded",
    "tv,web_embedded",
    "android,ios",
]

def _rate_limit_delay() -> None:
    """Sleep a small randomized delay before a YouTube request.

    Aggressive, burst-y request patterns (especially from parallel workers on a
    datacenter IP) strongly correlate with YouTube's bot detection. A short
    jittered delay between requests makes the traffic look more human and
    reduces the chance of triggering the "Sign in to confirm you're not a bot"
    block. The base delay is configurable via TRACK_DELAY.
    """
    if TRACK_DELAY <= 0:
        return
    # Jitter between 0.5x and 1.5x of TRACK_DELAY to avoid a fixed cadence.
    time.sleep(TRACK_DELAY * random.uniform(0.5, 1.5))


# ========== ERROR CLASSIFICATION ==========
def classify_error(error_text: str) -> type:
    """
    Map an error message to the appropriate exception class.

    Returns a ``DownloadError`` subclass. Permanent conditions (auth required,
    private/unavailable videos) are never classified as retryable.
    """
    text = error_text or ""

    # Authentication / bot detection — permanent.
    # HTTP 400 when using --cookies usually means an invalid cookies file
    # (wrong header or newline format), so it is treated as an auth problem.
    auth_markers = (
        "Sign in to confirm you're not a bot",
        "not a bot",
        "--cookies",
        "--cookies-from-browser",
        "HTTP Error 400",
        "ERROR: Unable to load cookies",
    )
    if any(marker in text for marker in auth_markers):
        return AuthenticationRequiredError

    # Video unavailable / private / removed / geoblocked — permanent.
    unavailable_markers = (
        "Private video",
        "Video unavailable",
        "This video has been removed",
        "This video is unavailable",
        "Join this channel",
        "members only",
        "not available in your country",
        "Whoa there, partner",
        "playback on other websites",
    )
    if any(marker in text for marker in unavailable_markers):
        return VideoUnavailableError

    # Transient failures — retryable.
    retryable_markers = (
        "HTTP Error 403",
        "403: Forbidden",
        "HTTP Error 429",
        "429",
        "500",
        "502",
        "503",
        "504",
        "timed out",
        "Connection reset",
        "Temporary failure",
        "Too Many Requests",
        "Read timed out",
    )
    if any(marker in text for marker in retryable_markers):
        return RetryableDownloadError

    # Default: treat unknown errors as metadata errors (non-retryable to be safe).
    return MetadataError


def should_retry(error: Union[str, Exception]) -> bool:
    """Return True if the error is transient and retrying may help."""
    if isinstance(error, Exception):
        # RetryableDownloadError is explicitly retryable; everything else that
        # derives from DownloadError is permanent.
        if isinstance(error, RetryableDownloadError):
            return True
        if isinstance(error, DownloadError):
            return False
        text = str(error)
    else:
        text = error
    return classify_error(text) is RetryableDownloadError


def _raise_classified(
    error_text: str,
    *,
    url: str = "",
    video_id: Optional[str] = None,
) -> None:
    """Raise the appropriate custom exception for a yt-dlp error message."""
    cls = classify_error(error_text)
    message = f"yt-dlp error: {error_text.strip()}"
    if "http error 403" in error_text.lower() or "403: forbidden" in error_text.lower():
        message += (
            " YouTube denied the media request. The stream URL may have expired, "
            "or this server/session may not have access. Update yt-dlp, verify "
            "configured cookies, and check whether the host can access the video."
        )
    raise cls(
        message,
        url=url,
        video_id=video_id,
        original_message=error_text,
    )


# ========== yt-dlp OPTIONS ==========
def _cookie_opts() -> Dict[str, Union[str, Tuple[str]]]:
    """Build yt-dlp cookie options from environment configuration."""
    opts: Dict[str, Union[str, Tuple[str]]] = {}
    if COOKIES_FILE and os.path.exists(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE
    if COOKIES_FROM_BROWSER:
        opts["cookiesfrombrowser"] = (COOKIES_FROM_BROWSER,)
    return opts


def _proxy_opts() -> Dict[str, str]:
    """Build yt-dlp proxy options from environment configuration."""
    if not PROXY_URL:
        return {}
    return {"proxy": PROXY_URL}


def make_ydl_opts(**overrides) -> Dict[str, object]:
    """
    Build yt-dlp options for the Python API, merged with overrides.

    Includes cookie config and multi-player-client fallback to reduce bot
    detection. yt-dlp selects the matching user-agent for each player client.
    """
    opts: Dict[str, object] = {
        "quiet": True,
        "no_warnings": True,
        "no_check_certificates": True,
        "geo_bypass": True,
        "noupdate": True,
        "retries": 1,  # let our own retry policy handle backoff
        "socket_timeout": 30,
        "logger": _YtDlpLogger(),
        "extractor_args": {
            "youtube": {"player_client": PLAYER_CLIENT_STRATEGIES},
        },
        "js_runtimes": {YTDLP_JS_RUNTIME: {}},
        "remote_components": [YTDLP_REMOTE_COMPONENTS],
    }
    opts.update(_cookie_opts())
    opts.update(_proxy_opts())
    opts.update(overrides)
    return opts


def _build_ydl_opts_for_client(
    player_client: str,
    **overrides,
) -> Dict[str, object]:
    """Build yt-dlp options pinned to a SINGLE player client.

    Passing the whole client list at once makes yt-dlp fall back internally, but
    it often stops at the first blocked client (e.g. returning the bot-check
    error) instead of trying the next one. Forcing a single client per attempt
    lets us loop over strategies ourselves and skip blocked clients.
    """
    opts: Dict[str, object] = {
        "quiet": True,
        "no_warnings": True,
        "no_check_certificates": True,
        "geo_bypass": True,
        "noupdate": True,
        "retries": 0,  # rely on our own per-client + backoff handling
        "socket_timeout": 30,
        "logger": _YtDlpLogger(),
        # Download fragments in parallel for much faster media fetches.
        "concurrent_fragment_downloads": int(os.getenv("CONCURRENT_FRAGMENTS", 4)),
        "extractor_args": {
            "youtube": {"player_client": [player_client]},
        },
        "js_runtimes": {YTDLP_JS_RUNTIME: {}},
        "remote_components": [YTDLP_REMOTE_COMPONENTS],
    }
    opts.update(_cookie_opts())
    opts.update(_proxy_opts())
    opts.update(overrides)
    return opts


def _base_cli_args() -> List[str]:
    """Common CLI args appended to every yt-dlp CLI call."""
    args = [
        "--no-warnings",
        "--no-check-certificates",
        "--geo-bypass",
        "--no-update",
        "--sleep-requests",
        "1.0",
        "--sleep-interval",
        "1.0",
    ]
    if COOKIES_FILE and os.path.exists(COOKIES_FILE):
        args += ["--cookies", COOKIES_FILE]
    if COOKIES_FROM_BROWSER:
        args += ["--cookies-from-browser", COOKIES_FROM_BROWSER]
    if PROXY_URL:
        args += ["--proxy", PROXY_URL]
    if YTDLP_JS_RUNTIME:
        args += ["--js-runtimes", YTDLP_JS_RUNTIME]
    if YTDLP_REMOTE_COMPONENTS:
        args += ["--remote-components", YTDLP_REMOTE_COMPONENTS]
    return args


def build_ytdlp_cli(base_cmd: List[str], player_client: str) -> List[str]:
    """Assemble a full yt-dlp CLI command for a given player client."""
    cmd = list(base_cmd) + _base_cli_args()
    cmd += ["--extractor-args", f"youtube:player_client={player_client}"]
    return cmd


def run_ytdlp_cli(base_cmd: List[str]) -> Tuple[int, str, str]:
    """
    Run a yt-dlp CLI command, trying multiple player-client strategies.

    Returns ``(returncode, stdout, stderr)``. If every strategy fails, the last
    stderr is returned. Used for metadata/playlist resolution where the Python
    API may be overkill.
    """
    last_err = ""
    if base_cmd and base_cmd[0] == "yt-dlp":
        base_cmd = [sys.executable, "-m", "yt_dlp", *base_cmd[1:]]
    for player_client in PLAYER_CLIENT_STRATEGIES:
        cmd = build_ytdlp_cli(base_cmd, player_client)
        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=120
            )
            if result.returncode == 0:
                return result.returncode, result.stdout, result.stderr
            last_err = result.stderr
        except subprocess.TimeoutExpired as e:
            last_err = f"yt-dlp timed out: {e}"
        except Exception as e:  # noqa: BLE001 - capture any subprocess failure
            last_err = str(e)
    return 1, "", last_err


# ========== MODELS ==========
@dataclass
class TrackMetadata:
    """Lightweight metadata for a downloaded track."""
    title: str
    artist: str
    thumbnail: str = ""
    webpage_url: str = ""
    video_id: Optional[str] = None


# ========== MODULAR DOWNLOADER ==========
def fetch_metadata(
    url: str,
    *,
    max_retries: int = MAX_RETRIES,
    base_backoff: float = BASE_BACKOFF,
    retry_callback: Optional[Callable[[int, int], None]] = None,
) -> TrackMetadata:
    """
    Fetch track metadata (title, uploader, thumbnail) for a video URL.

    Transient failures are retried with exponential backoff; permanent errors
    (auth required, unavailable video) are raised immediately.
    """
    video_id = extract_video_id(url)

    # Remember the last error seen so we can raise something meaningful if every
    # player client fails. We suppress transient per-client failures and try the
    # next client, but abort immediately on permanent (non-bot) errors.
    last_error: Optional[Exception] = None
    auth_seen = False

    for attempt in range(1, max_retries + 1):
        for player_client in PLAYER_CLIENT_STRATEGIES:
            _rate_limit_delay()
            try:
                with yt_dlp.YoutubeDL(_build_ydl_opts_for_client(player_client)) as ydl:
                    info = ydl.extract_info(url, download=False)
                if info is None:
                    raise MetadataError(
                        "yt-dlp returned no metadata",
                        url=url,
                        video_id=video_id,
                    )
                title = info.get("title") or ""
                uploader = info.get("uploader") or info.get("channel") or ""
                thumb = info.get("thumbnail") or ""
                return TrackMetadata(
                    title=title,
                    artist=uploader,
                    thumbnail=thumb,
                    webpage_url=info.get("webpage_url") or url,
                    video_id=video_id or info.get("id"),
                )
            except AuthenticationRequiredError:
                # Don't give up immediately — a different client may not be
                # blocked. Remember it and continue to the next client.
                auth_seen = True
                last_error = last_error or None
                logger.debug("Client '%s' blocked by auth for %s", player_client, url)
                continue
            except VideoUnavailableError:
                # Permanent: the video itself is unavailable/private/removed.
                raise
            except RetryableDownloadError as e:
                last_error = e
                logger.debug(
                    "Transient failure with client '%s' for %s: %s",
                    player_client, url, e,
                )
                continue
            except yt_dlp.utils.DownloadError as e:
                text = str(e)
                # If this client hit the bot-check, treat it as auth and move on.
                if "not a bot" in text or "cookies" in text:
                    auth_seen = True
                    last_error = None
                    continue
                # A "Requested format is not available" means we reached the
                # player but got no streamable formats — try the next client.
                last_error = e
                continue
            except Exception as e:  # noqa: BLE001
                last_error = e
                continue

        # Exhausted all clients for this overall attempt.
        if attempt < max_retries:
            delay = min(base_backoff * (2 ** (attempt - 1)), MAX_BACKOFF) + random.uniform(0, 0.5)
            logger.info(
                "Metadata attempt %d/%d exhausted player clients; retrying in %.1fs",
                attempt, max_retries, delay,
            )
            if retry_callback:
                retry_callback(attempt, max_retries)
            time.sleep(delay)

    # All retries exhausted.
    if auth_seen:
        raise AuthenticationRequiredError(
            "YouTube blocked all player clients (Sign in to confirm you're not "
            "a bot). Configure COOKIES_FILE / COOKIES_BASE64 / COOKIES_FROM_BROWSER "
            "or use a residential proxy.",
            url=url,
            video_id=video_id,
        )
    if last_error is not None:
        _raise_classified(str(last_error), url=url, video_id=video_id)
    raise MetadataError("Metadata fetch failed", url=url, video_id=video_id)


def download_media(
    url: str,
    outtmpl: str,
    *,
    max_retries: int = MAX_RETRIES,
    base_backoff: float = BASE_BACKOFF,
    retry_callback: Optional[Callable[[int, int], None]] = None,
    progress_callback: Optional[Callable[..., None]] = None,
) -> str:
    """
    Download the best audio for a video URL into ``outtmpl``.

    Returns the path of the downloaded file. Retries only transient failures.
    """
    video_id = extract_video_id(url)

    last_error: Optional[Exception] = None
    forbidden_error: Optional[Exception] = None
    auth_seen = False

    def report_download_progress(data):
        if not progress_callback:
            return
        downloaded = int(data.get("downloaded_bytes", 0) or 0)
        total = int(data.get("total_bytes") or data.get("total_bytes_estimate") or 0)
        progress_callback(
            status=data.get("status", "downloading"),
            downloaded_bytes=downloaded,
            total_bytes=total,
            percent=(downloaded / total * 100) if total else None,
            speed=data.get("speed"),
            eta=data.get("eta"),
        )

    for attempt in range(1, max_retries + 1):
        for player_client in PLAYER_CLIENT_STRATEGIES:
            _rate_limit_delay()
            try:
                ydl_opts = _build_ydl_opts_for_client(
                    player_client,
                    format="bestaudio/best",
                    outtmpl=outtmpl,
                    progress_hooks=[report_download_progress],
                )
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([url])
                # Locate the downloaded file (yt-dlp may append an extension).
                return _locate_downloaded(outtmpl)
            except AuthenticationRequiredError:
                auth_seen = True
                logger.debug("Client '%s' blocked by auth for download %s", player_client, url)
                continue
            except VideoUnavailableError:
                # Permanent: the video itself is unavailable/private/removed.
                raise
            except RetryableDownloadError as e:
                last_error = e
                logger.debug("Transient download failure with client '%s': %s", player_client, e)
                continue
            except yt_dlp.utils.DownloadError as e:
                text = str(e)
                if "not a bot" in text or "cookies" in text:
                    auth_seen = True
                    last_error = None
                    continue
                if "http error 403" in text.lower() or "403: forbidden" in text.lower():
                    forbidden_error = e
                last_error = e
                continue
            except Exception as e:  # noqa: BLE001
                last_error = e
                continue

        # Exhausted all clients for this overall attempt.
        if attempt < max_retries:
            delay = min(base_backoff * (2 ** (attempt - 1)), MAX_BACKOFF) + random.uniform(0, 0.5)
            logger.info(
                "Download attempt %d/%d exhausted player clients; retrying in %.1fs",
                attempt, max_retries, delay,
            )
            if retry_callback:
                retry_callback(attempt, max_retries)
            time.sleep(delay)

    # All retries exhausted.
    if auth_seen:
        raise AuthenticationRequiredError(
            "YouTube blocked all player clients for download (Sign in to confirm "
            "you're not a bot). Configure COOKIES_FILE / COOKIES_BASE64 / "
            "COOKIES_FROM_BROWSER or use a residential proxy.",
            url=url,
            video_id=video_id,
        )
    if forbidden_error is not None:
        _raise_classified(str(forbidden_error), url=url, video_id=video_id)
    if last_error is not None:
        _raise_classified(str(last_error), url=url, video_id=video_id)
    raise RetryableDownloadError(
        "Download failed after retries", url=url, video_id=video_id
    )


def _locate_downloaded(outtmpl: str) -> str:
    """Find the actual file written by yt-dlp for an outtmpl pattern."""
    base = outtmpl.replace("%(ext)s", "")
    candidates = []
    for f in os.listdir(DOWNLOAD_FOLDER):
        if f.startswith(os.path.basename(base)):
            candidates.append(os.path.join(DOWNLOAD_FOLDER, f))
    if not candidates:
        raise MetadataError("Downloaded file not found")
    # Prefer the most recently modified candidate.
    return max(candidates, key=os.path.getmtime)


# ========== UTILITIES ==========
def human_size(size_bytes: int) -> str:
    """Human-readable bytes."""
    if size_bytes >= 1024 * 1024:
        return f"{size_bytes / (1024*1024):.1f} MB"
    elif size_bytes >= 1024:
        return f"{size_bytes / 1024:.1f} KB"
    else:
        return f"{size_bytes} B"


def _compress_to_mp3(
    raw_path: str,
    final_mp3: str,
    progress_callback: Optional[Callable[[float], None]] = None,
) -> None:
    """Compress an audio file to MP3 with ffmpeg."""
    probe = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration", "-of", "default=noprint_wrappers=1:nokey=1", raw_path],
        check=False,
        capture_output=True,
        text=True,
    )
    try:
        duration = float(probe.stdout.strip())
    except (TypeError, ValueError):
        duration = 0

    cmd = [
        "ffmpeg", "-y", "-i", raw_path,
        "-codec:a", "libmp3lame", "-b:a", AUDIO_BITRATE,
        "-map_metadata", "0", "-id3v2_version", "3",
        "-progress", "pipe:1", "-nostats", final_mp3,
    ]
    output_tail = []
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    if process.stdout is not None:
        for line in process.stdout:
            key, separator, value = line.strip().partition("=")
            if separator and key == "out_time_us" and duration > 0 and progress_callback:
                percent = min(100.0, int(value) / (duration * 1_000_000) * 100)
                progress_callback(percent)
            elif line.strip() and not separator:
                output_tail.append(line.strip())
                output_tail = output_tail[-8:]
    return_code = process.wait()
    if return_code:
        raise RuntimeError("FFmpeg compression failed: " + " | ".join(output_tail))
    if progress_callback:
        progress_callback(100.0)


# ========== FIREBASE ==========
def save_to_firestore_rest(collection: str, data: Dict) -> None:
    url = (
        f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
        f"/databases/(default)/documents/{collection}"
    )
    fields = {}
    for k, v in data.items():
        if isinstance(v, datetime.datetime):
            fields[k] = {"timestampValue": v.isoformat() + "Z"}
        elif isinstance(v, str):
            fields[k] = {"stringValue": v}
        else:
            fields[k] = {"stringValue": str(v)}
    res = requests.post(f"{url}?key={FIREBASE_API_KEY}", json={"fields": fields})
    if res.status_code not in (200, 201):
        raise Exception(f"Firestore error {res.status_code}: {res.text}")


def track_exists(title: str, artist: str) -> bool:
    url = (
        f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
        f"/databases/(default)/documents:runQuery?key={FIREBASE_API_KEY}"
    )
    structured_query = {
        "structuredQuery": {
            "from": [{"collectionId": "tracks"}],
            "where": {
                "compositeFilter": {
                    "op": "AND",
                    "filters": [
                        {"fieldFilter": {"field": {"fieldPath": "ownerId"}, "op": "EQUAL", "value": {"stringValue": OWNER_ID}}},
                        {"fieldFilter": {"field": {"fieldPath": "title"}, "op": "EQUAL", "value": {"stringValue": title}}},
                        {"fieldFilter": {"field": {"fieldPath": "artist"}, "op": "EQUAL", "value": {"stringValue": artist}}},
                    ],
                }
            },
            "limit": 1,
        }
    }
    res = requests.post(url, json=structured_query)
    if res.status_code != 200:
        return False
    results = res.json()
    return any("document" in r for r in results)


# ========== CLOUDINARY UPLOAD ==========
def upload_to_cloudinary(
    file_path: str,
    resource_type: str = "video",
    progress_callback: Optional[Callable[[int, int], None]] = None,
) -> str:
    url = f"https://api.cloudinary.com/v1_1/{CLOUD_NAME}/{resource_type}/upload"
    for attempt in range(3):
        try:
            with open(file_path, "rb") as f:
                encoder = MultipartEncoder(fields={
                    "file": (os.path.basename(file_path), f, "application/octet-stream"),
                    "upload_preset": UPLOAD_PRESET,
                })
                monitor = MultipartEncoderMonitor(
                    encoder,
                    lambda current: progress_callback(current.bytes_read, current.len)
                    if progress_callback else None,
                )
                res = session.post(
                    url,
                    data=monitor,
                    headers={"Content-Type": monitor.content_type},
                    timeout=600,
                )
                if res.status_code == 200:
                    return res.json()["secure_url"]
        except Exception:  # noqa: BLE001
            if attempt == 2:
                raise
            time.sleep(2)
    raise Exception("Cloudinary upload failed")


# ========== SPOTIFY SINGLE TRACK ==========
def resolve_spotify(url: str) -> Tuple[str, str, str]:
    res = requests.get("https://open.spotify.com/oembed", params={"url": url})
    if res.status_code != 200:
        raise Exception(f"Spotify oembed failed: {res.status_code}")
    title_full = res.json().get("title", "")
    if " - " in title_full:
        title, artist = title_full.split(" - ", 1)
    else:
        title, artist = title_full, ""
    query = f"{title} {artist} official audio" if artist else title
    rc, out, err = run_ytdlp_cli(["yt-dlp", f"ytsearch1:{query}", "--print", "id"])
    if rc != 0:
        raise MetadataError(f"No YouTube result for: {query} ({err.strip()})")
    lines = out.strip().split("\n")
    if not lines or not lines[0]:
        raise MetadataError(f"No YouTube result for: {query}")
    vid = lines[0]
    return f"https://www.youtube.com/watch?v={vid}", title, artist


# ========== PLAYLIST EXPANSION ==========
def expand_youtube_playlist(url: str) -> List[str]:
    rc, out, err = run_ytdlp_cli(["yt-dlp", "--flat-playlist", "--print", "url", url])
    if rc != 0:
        raise MetadataError(f"yt-dlp failed: {err.strip()}")
    urls = [line.strip() for line in out.strip().split("\n") if line.strip()]
    valid_urls = [u for u in urls if "watch?v=" in u]
    return valid_urls


def get_spotify_token() -> str:
    auth = f"{SPOTIFY_CLIENT_ID}:{SPOTIFY_CLIENT_SECRET}"
    b64 = base64.b64encode(auth.encode()).decode()
    res = requests.post(
        "https://accounts.spotify.com/api/token",
        headers={"Authorization": f"Basic {b64}", "Content-Type": "application/x-www-form-urlencoded"},
        data={"grant_type": "client_credentials"},
    )
    if res.status_code != 200:
        raise Exception(f"Spotify token error {res.status_code}: {res.text}")
    data = res.json()
    if "access_token" not in data:
        raise Exception(f"Spotify auth failed: {data}")
    return data["access_token"]


@lru_cache(maxsize=1000)
def search_youtube(query: str) -> str:
    rc, out, err = run_ytdlp_cli(["yt-dlp", f"ytsearch1:{query}", "--print", "id"])
    if rc != 0:
        raise MetadataError(f"No YouTube result for: {query} ({err.strip()})")
    lines = out.strip().split("\n")
    if not lines or not lines[0]:
        raise MetadataError(f"No YouTube result for: {query}")
    vid = lines[0]
    return f"https://www.youtube.com/watch?v={vid}"


def expand_spotify_playlist(url: str, token: str) -> List[str]:
    playlist_id = url.split("playlist/")[1].split("?")[0]
    headers = {"Authorization": f"Bearer {token}"}

    all_tracks_meta = []
    offset = 0
    while True:
        api_url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks?limit=100&offset={offset}"
        r = requests.get(api_url, headers=headers)
        if r.status_code != 200:
            raise Exception(f"Spotify API error: {r.text}")
        res = r.json()
        items = res.get("items", [])
        if not items:
            break
        for item in items:
            track = item.get("track")
            if track:
                name = track["name"]
                artist = track["artists"][0]["name"]
                all_tracks_meta.append((name, artist))
        offset += 100

    yt_urls = [None] * len(all_tracks_meta)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_index = {}
        for idx, (name, artist) in enumerate(all_tracks_meta):
            query = f"{name} {artist} official audio"
            future = executor.submit(search_youtube, query)
            future_to_index[future] = idx
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                yt_urls[idx] = future.result()
            except Exception:  # noqa: BLE001
                yt_urls[idx] = None

    valid_urls = [url for url in yt_urls if url]
    return valid_urls


def expand_all_urls(raw_urls: List[str]) -> List[str]:
    final_urls = []
    spotify_token = None

    for url in raw_urls:
        if "youtube.com/playlist" in url:
            final_urls.extend(expand_youtube_playlist(url))
        elif "spotify.com/playlist" in url:
            if not SPOTIFY_CLIENT_ID or SPOTIFY_CLIENT_ID == "your_client_id":
                continue  # silently skip
            if not spotify_token:
                spotify_token = get_spotify_token()
            final_urls.extend(expand_spotify_playlist(url, spotify_token))
        else:
            final_urls.append(url)
    return list(dict.fromkeys(final_urls))


def fetch_all_tracks_rest() -> List[Dict]:
    """Fetch every 'tracks' doc belonging to OWNER_ID (paginated)."""
    url = (
        f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
        f"/databases/(default)/documents:runQuery?key={FIREBASE_API_KEY}"
    )
    tracks: List[Dict] = []
    offset = 0
    page_size = 300
    while True:
        sq = {
            "structuredQuery": {
                "from": [{"collectionId": "tracks"}],
                "where": {
                    "fieldFilter": {
                        "field": {"fieldPath": "ownerId"},
                        "op": "EQUAL",
                        "value": {"stringValue": OWNER_ID},
                    }
                },
                "orderBy": [
                    {"field": {"fieldPath": "createdAt"}, "direction": "DESCENDING"}
                ],
                "offset": offset,
                "limit": page_size,
            }
        }
        res = requests.post(url, json=sq, timeout=30)
        if res.status_code != 200:
            raise Exception(f"Firestore fetch error {res.status_code}: {res.text}")
        docs = [x["document"] for x in res.json() if "document" in x]
        if not docs:
            break
        for d in docs:
            f = d.get("fields", {})

            def gv(field):
                v = f.get(field, {})
                for k in ("stringValue", "timestampValue", "integerValue"):
                    if k in v:
                        return v[k]
                return None

            tracks.append({
                "docId": d["name"].rsplit("/", 1)[-1],
                "title": gv("title") or "",
                "artist": gv("artist") or "",
                "audioUrl": gv("audioUrl") or "",
                "coverUrl": gv("coverUrl") or "",
                "createdAt": gv("createdAt") or "",
            })
        if len(docs) < page_size:
            break
        offset += page_size
    return tracks


def delete_firestore_doc(doc_id: str) -> bool:
    url = (
        f"https://firestore.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}"
        f"/databases/(default)/documents/tracks/{doc_id}?key={FIREBASE_API_KEY}"
    )
    try:
        return requests.delete(url, timeout=15).status_code == 200
    except Exception:  # noqa: BLE001
        return False


# ========== THUMBNAIL ==========
def compress_image(image_path: str, output_path: str) -> None:
    with Image.open(image_path) as img:
        if img.mode in ("RGBA", "LA", "P"):
            img = img.convert("RGB")
        if max(img.size) > MAX_IMAGE_SIZE:
            ratio = MAX_IMAGE_SIZE / max(img.size)
            new_size = (int(img.size[0] * ratio), int(img.size[1] * ratio))
            img = img.resize(new_size, Image.LANCZOS)
        img.save(output_path, "JPEG", quality=JPEG_QUALITY, optimize=True)


def upload_thumbnail(thumb_url: str) -> Optional[str]:
    if not thumb_url:
        return None
    # Unique temp names per call — parallel workers must not clobber each
    # other's thumbnail files in the shared downloads folder.
    uid = str(uuid.uuid4())[:8]
    temp_raw = os.path.join(DOWNLOAD_FOLDER, f"thumb_raw_{uid}.jpg")
    temp_comp = os.path.join(DOWNLOAD_FOLDER, f"thumb_comp_{uid}.jpg")
    try:
        r = session.get(thumb_url, timeout=10)
        if r.status_code != 200:
            return None
        with open(temp_raw, "wb") as f:
            f.write(r.content)
        compress_image(temp_raw, temp_comp)
        return upload_to_cloudinary(temp_comp, "image")
    finally:
        for f in [temp_raw, temp_comp]:
            if os.path.exists(f):
                try:
                    os.remove(f)
                except OSError:
                    pass


# ========== PROCESS SINGLE TRACK ==========
def process_track(
    url: str,
    i: int,
    total: int,
    report_callback: Callable[..., None],
    *,
    stop_event: Optional[threading.Event] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Process a single track: resolve -> download -> compress -> upload -> save.

    Returns ``(success, stop)`` where ``stop`` is a non-empty status string
    (e.g. ``"auth_required"``) when processing should halt entirely, otherwise
    ``None``.

    On ``AuthenticationRequiredError`` we return immediately with a clean
    message and never retry, because the same request will always fail.

    If ``stop_event`` is set (user cancellation), the track aborts cleanly
    before starting and between major steps; in-flight downloads finish their
    current step so partial files can be removed safely.
    """
    def _cancelled() -> bool:
        return stop_event is not None and stop_event.is_set()

    def report_stage(stage: str, percent: Optional[float], title: str = "", **details) -> None:
        report_callback(
            type="track_stage",
            index=i,
            total=total,
            stage=stage,
            percent=percent,
            title=title,
            **details,
        )

    report_callback(type="log", message=f"Track {i}/{total}: {url[:80]}")
    report_callback(type="track_start", index=i, total=total, title="")
    report_stage("metadata", 0, url)

    # Files created by THIS track only. Cleanup must never touch files that
    # belong to other tracks running concurrently in the same downloads folder.
    cleanup_files: List[str] = []

    if _cancelled():
        report_callback(type="log", message=f"⏹️ Track {i}/{total} cancelled before start.")
        report_callback(type="track_done", index=i, ok=False, title="Cancelled")
        return False, None

    try:
        if "spotify.com" in url:
            yt_url, title, artist = resolve_spotify(url)
        else:
            yt_url, title, artist = url, "", ""

        if title and artist and track_exists(title, artist):
            report_callback(type="log", message="⏭️ Already exists, skipping.")
            report_stage("complete", 100, title)
            report_callback(type="track_done", index=i, ok=True, title=title)
            report_callback(type="progress", processed=i, title=title)
            return True, None

        report_callback(type="log", message="Fetching metadata...")
        meta = fetch_metadata(yt_url)
        title = title or meta.title
        artist = artist or meta.artist
        report_stage("metadata", 100, title)

        if _cancelled():
            report_callback(type="log", message=f"⏹️ Track {i}/{total} cancelled after metadata.")
            report_callback(type="track_done", index=i, ok=False, title="Cancelled")
            return False, None

        uid = str(uuid.uuid4())[:8]
        raw_template = os.path.join(DOWNLOAD_FOLDER, f"raw_{uid}.%(ext)s")
        final_mp3 = os.path.join(DOWNLOAD_FOLDER, f"audio_{uid}.mp3")

        report_callback(type="log", message=f"Downloading {title}...")
        report_stage("download", 0, title)
        raw_path = download_media(
            yt_url,
            raw_template,
            progress_callback=lambda **progress: report_stage(
                "download",
                progress.get("percent"),
                title,
                **{key: value for key, value in progress.items() if key != "percent"},
            ),
        )
        cleanup_files.append(raw_path)
        cleanup_files.append(final_mp3)

        if _cancelled():
            report_callback(type="log", message=f"⏹️ Track {i}/{total} cancelled after download.")
            report_callback(type="track_done", index=i, ok=False, title="Cancelled")
            return False, None

        report_callback(type="log", message="Compressing to MP3...")
        report_stage("compress", 0, title)
        _compress_to_mp3(
            raw_path,
            final_mp3,
            progress_callback=lambda percent: report_stage("compress", percent, title),
        )

        if _cancelled():
            report_callback(type="log", message=f"⏹️ Track {i}/{total} cancelled after compression.")
            report_callback(type="track_done", index=i, ok=False, title="Cancelled")
            return False, None

        report_stage("upload", 0, title)
        audio_url = upload_to_cloudinary(
            final_mp3,
            progress_callback=lambda sent, size: report_stage(
                "upload",
                (sent / size * 100) if size else None,
                title,
                uploaded_bytes=sent,
                total_bytes=size,
            ),
        )
        cover_url = upload_thumbnail(meta.thumbnail or "")
        report_stage("upload", 100, title)

        report_stage("save", 0, title)
        save_to_firestore_rest("tracks", {
            "title": title,
            "artist": artist,
            "audioUrl": audio_url,
            "coverUrl": cover_url,
            "ownerId": OWNER_ID,
            "createdAt": datetime.datetime.now(),
        })

        report_stage("save", 100, title)
        report_stage("complete", 100, title)
        report_callback(type="log", message=f"✅ Track added: {title} - {artist}")
        report_callback(type="track_done", index=i, ok=True, title=title)
        report_callback(type="progress", processed=i, title=title)
        return True, None

    except AuthenticationRequiredError as e:
        logger.error("Authentication required: %s", e)
        report_callback(type="log", message=(
            "❌ Authentication required. YouTube blocked this request. "
            "Configure COOKIES_FILE (a cookies.txt) or COOKIES_FROM_BROWSER "
            "to authenticate. Stopping further processing.\n"
            f"Details: {e}"
        ))
        report_stage("failed", 100, "Authentication required")
        report_callback(type="track_done", index=i, ok=False, title="Authentication required")
        report_callback(type="progress", processed=i, title="Auth required")
        return False, "auth_required"

    except VideoUnavailableError as e:
        logger.warning("Video unavailable: %s", e)
        report_callback(type="log", message=f"❌ Video unavailable/permanent: {e}")
        report_stage("failed", 100, "Failed")
        report_callback(type="track_done", index=i, ok=False, title="Failed")
        report_callback(type="progress", processed=i, title="Failed")
        return False, None

    except RetryableDownloadError as e:
        logger.error("Giving up after retries: %s", e)
        report_callback(type="log", message=f"❌ Failed after retries: {e}")
        report_stage("failed", 100, "Failed")
        report_callback(type="track_done", index=i, ok=False, title="Failed")
        report_callback(type="progress", processed=i, title="Failed")
        return False, None

    except DownloadError as e:
        logger.error("Download error: %s", e)
        report_callback(type="log", message=f"❌ Download error: {e}")
        report_stage("failed", 100, "Failed")
        report_callback(type="track_done", index=i, ok=False, title="Failed")
        report_callback(type="progress", processed=i, title="Failed")
        return False, None

    except Exception as e:  # noqa: BLE001
        logger.exception("Unexpected error processing track %s", url)
        report_callback(type="log", message=f"❌ Unexpected error: {e}")
        report_stage("failed", 100, "Failed")
        report_callback(type="track_done", index=i, ok=False, title="Failed")
        report_callback(type="progress", processed=i, title="Failed")
        return False, None

    finally:
        # Resource cleanup: remove ONLY this track's own files. Never wipe the
        # whole downloads folder — other tracks in a playlist may still be
        # downloading in parallel threads.
        for f in cleanup_files:
            try:
                if f and os.path.exists(f):
                    os.remove(f)
            except OSError:
                pass


# ========== PROCESS ALL ==========
def process_all(
    urls: List[str],
    report_callback: Callable[..., None],
    *,
    cancel_event: Optional[threading.Event] = None,
) -> None:
    """
    Process a list of URLs, reporting progress via callbacks.

    Tracks are processed concurrently (up to ``MAX_WORKERS``) for much faster
    throughput. If authentication is required, processing stops immediately and
    a clean error is reported to avoid hammering YouTube with unauthenticated
    requests.

    ``cancel_event`` may be supplied by the caller (e.g. the web server's
    cancel endpoint); when set, no further tracks are submitted and in-flight
    tracks abort at the next checkpoint.
    """
    total = len(urls)
    report_callback(type="start", total=total)

    # Cancel support: an external thread (e.g. the web server's cancel
    # endpoint) may set this event to abort pending tracks. Tracks already
    # mid-download are allowed to finish their current step and clean up;
    # nothing further is submitted afterwards.
    stop_event = threading.Event()
    external_cancel = cancel_event  # alias for clarity in closures

    def _cancelled() -> bool:
        return stop_event.is_set() or (external_cancel is not None and external_cancel.is_set())

    # Bridge the external cancel event (from the web server) into the internal
    # stop_event so ALL in-flight tracks observe cancellation, regardless of
    # single- or multi-track path.
    if external_cancel is not None:
        def _bridge():
            external_cancel.wait()
            stop_event.set()
        threading.Thread(target=_bridge, daemon=True).start()

    if total == 1:
        # Single track: process inline (keeps backoff/stop semantics simple).
        ok, stop = process_track(urls[0], 1, total, report_callback, stop_event=stop_event)
        report_callback(type="finish", success=int(ok), total=total)
        return

    success_count = 0
    success_lock = threading.Lock()
    processed_count = [0]  # box mutable counter in a list for closure access
    processed_lock = threading.Lock()

    def worker(url: str, index: int) -> None:
        nonlocal success_count
        # Skip tasks that were queued in the executor but never started before
        # cancellation arrived.
        if _cancelled():
            report_callback(type="log", message=f"⏹️ Track {index}/{total} skipped (cancelled before start).")
            with processed_lock:
                processed_count[0] += 1
            return
        try:
            ok, stop = process_track(url, index, total, report_callback, stop_event=stop_event)
            if ok:
                with success_lock:
                    success_count += 1
            if stop:
                stop_event.set()
        except Exception as e:  # noqa: BLE001
            logger.exception("Unexpected error processing track %s", url)
            report_callback(type="log", message=f"❌ Unexpected error: {e}")
            report_callback(type="track_done", index=index, ok=False, title="Failed")
            report_callback(type="progress", processed=index, title="Failed")
        finally:
            with processed_lock:
                processed_count[0] += 1
                done = processed_count[0]
            report_callback(type="progress", processed=done, title="")

    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, total)) as executor:
        futures = []
        for i, url in enumerate(urls, 1):
            if _cancelled():  # checks both internal stop and external cancel
                break
            futures.append(executor.submit(worker, url, i))
        # Wait for all submitted futures.
        for future in futures:
            try:
                future.result()
            except Exception:  # noqa: BLE001
                pass

    if _cancelled():
        report_callback(type="log", message="⚠️ Task cancelled — no further tracks submitted.")

    report_callback(type="finish", success=success_count, total=total)


# ========== STARTUP ==========
# Materialize base64 cookies (if provided) before warning/validation.
_materialize_cookies()

# Warn on startup so auth issues are visible immediately (not just per-track).
log_auth_config_warning()

# Log environment diagnostics and self-test at startup.
run_startup_diagnostics()

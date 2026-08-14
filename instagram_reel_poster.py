#!/usr/bin/env python3
"""
Reliable Google Drive -> Instagram Reel posting engine for Railway.

Designed for a public Google Drive folder and Instagram API with Instagram Login.
One invocation posts at most one Reel. The companion app.py scheduler controls execution.

Required environment variables:
  DRIVE_FOLDER_ID
  INSTAGRAM_USER_ID
  INSTAGRAM_ACCESS_TOKEN   (INSTAGRAM_TOKEN is also accepted)

Useful optional variables:
  INSTAGRAM_API_VERSION=v23.0
  INSTAGRAM_SHARE_TO_FEED=true
  GDOWN_BIN=gdown
  REELS_DB_PATH=/opt/data/instagram_reels.sqlite3
  REELS_LOCK_PATH=/opt/data/instagram_reel_poster.lock
  REELS_TEMP_DIR=/opt/data/instagram_reels_tmp
  MAX_ATTEMPTS=3
  DRY_RUN=1
  SEED_POSTED_THROUGH=1

The script uses gdown >= 6.1.0's --folder --json mode to inspect a large
public Drive folder without downloading it, numerically sorts the Reel number
from each filename, downloads only the next candidate, uploads the local video
binary to Meta using the resumable-upload URI returned by Instagram, polls the
container, and publishes only after the container reaches FINISHED.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import fcntl
import hashlib
import http.client
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Optional


# ----------------------------- Configuration -----------------------------

DRIVE_FOLDER_ID = os.getenv("DRIVE_FOLDER_ID", "").strip()
INSTAGRAM_USER_ID = os.getenv("INSTAGRAM_USER_ID", "").strip()
INSTAGRAM_ACCESS_TOKEN = (
    os.getenv("INSTAGRAM_ACCESS_TOKEN", "").strip()
    or os.getenv("INSTAGRAM_TOKEN", "").strip()
    or os.getenv("INSTAGRAM_USER_ACCESS_TOKEN", "").strip()
)
API_VERSION = os.getenv("INSTAGRAM_API_VERSION", "v23.0").strip().lstrip("/")
GRAPH_BASE = f"https://graph.instagram.com/{API_VERSION}"

DATA_DIR = Path(
    os.getenv("REELS_DATA_DIR", "").strip()
    or os.getenv("RAILWAY_VOLUME_MOUNT_PATH", "").strip()
    or "/opt/data"
)
DB_PATH = Path(os.getenv("REELS_DB_PATH", str(DATA_DIR / "instagram_reels.sqlite3")))
LOCK_PATH = Path(os.getenv("REELS_LOCK_PATH", str(DATA_DIR / "instagram_reel_poster.lock")))
TEMP_DIR = Path(os.getenv("REELS_TEMP_DIR", str(DATA_DIR / "instagram_reels_tmp")))
GDOWN_BIN = os.getenv("GDOWN_BIN", "").strip()

DRY_RUN = os.getenv("DRY_RUN", "0").strip().lower() in {"1", "true", "yes", "on"}
SHARE_TO_FEED = os.getenv("INSTAGRAM_SHARE_TO_FEED", "true").strip().lower() in {
    "1", "true", "yes", "on"
}
MAX_ATTEMPTS = max(1, int(os.getenv("MAX_ATTEMPTS", "3")))
SEED_POSTED_THROUGH = max(0, int(os.getenv("SEED_POSTED_THROUGH", "1")))
PROCESSING_STALE_SECONDS = max(300, int(os.getenv("PROCESSING_STALE_SECONDS", "3600")))
GRAPH_TIMEOUT = max(10, int(os.getenv("GRAPH_TIMEOUT_SECONDS", "60")))
UPLOAD_TIMEOUT = max(60, int(os.getenv("UPLOAD_TIMEOUT_SECONDS", "900")))
POLL_INTERVAL = max(2, int(os.getenv("CONTAINER_POLL_SECONDS", "5")))
POLL_TIMEOUT = max(30, int(os.getenv("CONTAINER_TIMEOUT_SECONDS", "180")))
FOLDER_LIST_TIMEOUT = max(30, int(os.getenv("FOLDER_LIST_TIMEOUT_SECONDS", "240")))
DOWNLOAD_TIMEOUT = max(60, int(os.getenv("DOWNLOAD_TIMEOUT_SECONDS", "900")))
MAX_FILE_BYTES = 1_000_000_000
ALLOWED_EXTENSIONS = {".mp4", ".mov", ".m4v"}


HOOKS = [
    "That transition deserved a replay.",
    "The timing on this edit is unreal.",
    "This one hits different.",
    "A clean edit never gets old.",
    "The final cut was worth it.",
    "Watch the transition closely.",
    "This scene was made for an edit.",
    "The sync is too clean.",
    "One more for the anime edit collection.",
    "This edit has no wasted frames.",
    "The energy in this scene is unmatched.",
    "A few seconds of pure anime energy.",
    "The beat and the scene lined up perfectly.",
    "This moment needed an edit.",
    "Clean cuts. Strong scene.",
    "The replay value on this one is high.",
    "Some scenes just edit themselves.",
    "This sequence goes hard.",
    "A small edit with big energy.",
    "This is why anime edits are addictive.",
    "The transition landed perfectly.",
    "A sharp cut can change the whole scene.",
    "This one belongs on repeat.",
    "Anime edits just keep getting better.",
]

CTAS = [
    "Follow for more anime edits.",
    "More edits are on the way.",
    "Save it if you want to replay it later.",
    "Follow for the next edit.",
    "Send this to an anime fan.",
    "More anime moments, every day.",
    "Keep this one in your saved edits.",
    "Follow if edits like this are your thing.",
    "Share it with someone who would appreciate the cut.",
    "More clean edits coming soon.",
    "Stay for the next anime edit.",
    "Drop this into your anime edit collection.",
]

HASHTAG_GROUPS = [
    ("#anime", "#animeedit", "#amv", "#animereels", "#reels"),
    ("#anime", "#animeedits", "#videoedit", "#animereels", "#otaku"),
    ("#amv", "#animeedit", "#edit", "#reelsinstagram", "#animefans"),
    ("#anime", "#amvedit", "#animeedits", "#reels", "#otaku"),
    ("#animeedit", "#animereels", "#videoedit", "#animefans", "#reels"),
    ("#anime", "#amv", "#edit", "#animeedits", "#reelsinstagram"),
    ("#animeedits", "#animefans", "#animereels", "#edit", "#reels"),
    ("#anime", "#animeedit", "#videoedit", "#otaku", "#reelsinstagram"),
    ("#amvedit", "#anime", "#animereels", "#animefans", "#reels"),
    ("#animeedit", "#amv", "#animeedits", "#videoedit", "#reels"),
    ("#anime", "#edit", "#animereels", "#otaku", "#reelsinstagram"),
    ("#animefans", "#animeedit", "#amvedit", "#animeedits", "#reels"),
]


# ----------------------------- Data types -----------------------------

@dataclass(frozen=True)
class DriveVideo:
    file_id: str
    name: str
    url: str
    path: str
    reel_number: Optional[int]


class ReelPosterError(RuntimeError):
    pass


class GraphHTTPError(ReelPosterError):
    def __init__(self, status: int, message: str, payload: Any = None):
        super().__init__(f"Instagram API HTTP {status}: {message}")
        self.status = status
        self.payload = payload


class NetworkError(ReelPosterError):
    pass


# ----------------------------- Logging/helpers -----------------------------

def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")


def log(message: str) -> None:
    print(f"[{now_iso()}] {message}", flush=True)


def sanitize(text: Any) -> str:
    s = str(text)
    if INSTAGRAM_ACCESS_TOKEN:
        s = s.replace(INSTAGRAM_ACCESS_TOKEN, "<redacted>")
    s = re.sub(r"(?i)(access_token=)[^&\s]+", r"\1<redacted>", s)
    return s[:1500]


def require_config() -> None:
    missing = []
    if not DRIVE_FOLDER_ID:
        missing.append("DRIVE_FOLDER_ID")
    if not INSTAGRAM_USER_ID:
        missing.append("INSTAGRAM_USER_ID")
    if not INSTAGRAM_ACCESS_TOKEN:
        missing.append("INSTAGRAM_ACCESS_TOKEN")
    if missing:
        raise ReelPosterError("Missing required environment variable(s): " + ", ".join(missing))


def resolve_gdown() -> str:
    candidates = [GDOWN_BIN, shutil.which("gdown")]
    for candidate in candidates:
        if candidate and Path(candidate).is_file() and os.access(candidate, os.X_OK):
            return candidate
    raise ReelPosterError(
        "gdown executable not found. Install requirements.txt or set GDOWN_BIN."
    )


def extract_file_id(url: str) -> str:
    patterns = [
        r"/d/([A-Za-z0-9_-]{10,})",
        r"[?&]id=([A-Za-z0-9_-]{10,})",
        r"/file/d/([A-Za-z0-9_-]{10,})",
    ]
    for pattern in patterns:
        m = re.search(pattern, url)
        if m:
            return m.group(1)
    return "url_" + hashlib.sha256(url.encode("utf-8")).hexdigest()[:32]


def extract_reel_number(name: str) -> Optional[int]:
    stem = Path(name).stem
    numbers = re.findall(r"\d+", stem)
    return int(numbers[-1]) if numbers else None


def natural_sort_key(video: DriveVideo) -> tuple[Any, ...]:
    if video.reel_number is not None:
        return (0, video.reel_number, video.name.casefold(), video.file_id)
    return (1, float("inf"), video.name.casefold(), video.file_id)


def deterministic_number(video: DriveVideo) -> int:
    if video.reel_number is not None:
        return video.reel_number
    return int(hashlib.sha256(video.file_id.encode()).hexdigest()[:8], 16)


def make_caption(video: DriveVideo, previous_caption: Optional[str] = None) -> str:
    n = deterministic_number(video)
    hook_idx = n % len(HOOKS)
    cta_idx = (n * 7 + 3) % len(CTAS)
    tag_idx = (n * 11 + 5) % len(HASHTAG_GROUPS)

    caption = f"{HOOKS[hook_idx]}\n{CTAS[cta_idx]}\n\n{' '.join(HASHTAG_GROUPS[tag_idx])}"
    if previous_caption and caption == previous_caption:
        hook_idx = (hook_idx + 1) % len(HOOKS)
        caption = f"{HOOKS[hook_idx]}\n{CTAS[cta_idx]}\n\n{' '.join(HASHTAG_GROUPS[tag_idx])}"
    return caption


# ----------------------------- File lock -----------------------------

@contextlib.contextmanager
def single_instance_lock() -> Iterable[None]:
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    fp = open(LOCK_PATH, "a+")
    try:
        try:
            fcntl.flock(fp.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("Another instagram_reel_poster.py run is already active; exiting safely.")
        fp.seek(0)
        fp.truncate()
        fp.write(f"pid={os.getpid()} started={now_iso()}\n")
        fp.flush()
        yield
    finally:
        with contextlib.suppress(Exception):
            fcntl.flock(fp.fileno(), fcntl.LOCK_UN)
        fp.close()


# ----------------------------- SQLite -----------------------------

def connect_db() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reel_posts_v2 (
            file_id TEXT PRIMARY KEY,
            reel_number INTEGER,
            filename TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            caption TEXT,
            container_id TEXT,
            media_id TEXT,
            permalink TEXT,
            last_error TEXT,
            first_seen_at TEXT NOT NULL,
            last_attempt_at TEXT,
            posted_at TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reel_posts_v2_status ON reel_posts_v2(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reel_posts_v2_number ON reel_posts_v2(reel_number)")
    conn.commit()
    migrate_legacy_posted_rows(conn)
    return conn


def migrate_legacy_posted_rows(conn: sqlite3.Connection) -> None:
    """Best-effort migration of posted file IDs from any older table schema."""
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    for table in tables:
        if table in {"reel_posts_v2", "sqlite_sequence"}:
            continue
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table):
            continue
        try:
            cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            if not {"file_id", "status"}.issubset(cols):
                continue
            filename_col = "filename" if "filename" in cols else None
            media_col = "media_id" if "media_id" in cols else (
                "instagram_media_id" if "instagram_media_id" in cols else None
            )
            caption_col = "caption" if "caption" in cols else None
            select_cols = ["file_id", "status"]
            for c in (filename_col, media_col, caption_col):
                if c:
                    select_cols.append(c)
            query = f"SELECT {', '.join(select_cols)} FROM {table} WHERE lower(status)='posted'"
            for row in conn.execute(query):
                row = dict(row)
                file_id = str(row.get("file_id") or "").strip()
                if not file_id:
                    continue
                filename = str(row.get(filename_col) or "legacy-posted") if filename_col else "legacy-posted"
                reel_number = extract_reel_number(filename)
                ts = now_iso()
                conn.execute(
                    """
                    INSERT OR IGNORE INTO reel_posts_v2
                    (file_id, reel_number, filename, status, attempts, caption, media_id,
                     first_seen_at, posted_at, updated_at)
                    VALUES (?, ?, ?, 'posted', 0, ?, ?, ?, ?, ?)
                    """,
                    (
                        file_id,
                        reel_number,
                        filename,
                        row.get(caption_col) if caption_col else None,
                        row.get(media_col) if media_col else None,
                        ts,
                        ts,
                        ts,
                    ),
                )
        except sqlite3.Error:
            continue
    conn.commit()


def seed_known_posted(conn: sqlite3.Connection, videos: list[DriveVideo]) -> None:
    if SEED_POSTED_THROUGH <= 0:
        return
    ts = now_iso()
    for video in videos:
        if video.reel_number is not None and video.reel_number <= SEED_POSTED_THROUGH:
            conn.execute(
                """
                INSERT OR IGNORE INTO reel_posts_v2
                (file_id, reel_number, filename, status, attempts, first_seen_at, posted_at, updated_at)
                VALUES (?, ?, ?, 'posted', 0, ?, ?, ?)
                """,
                (video.file_id, video.reel_number, video.name, ts, ts, ts),
            )
    conn.commit()


def upsert_seen(conn: sqlite3.Connection, video: DriveVideo) -> None:
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO reel_posts_v2
        (file_id, reel_number, filename, status, attempts, first_seen_at, updated_at)
        VALUES (?, ?, ?, 'pending', 0, ?, ?)
        ON CONFLICT(file_id) DO UPDATE SET
            reel_number=excluded.reel_number,
            filename=excluded.filename,
            updated_at=excluded.updated_at
        """,
        (video.file_id, video.reel_number, video.name, ts, ts),
    )


def reset_stale_processing(conn: sqlite3.Connection) -> None:
    cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=PROCESSING_STALE_SECONDS)).isoformat()
    conn.execute(
        """
        UPDATE reel_posts_v2
        SET status='failed', last_error='Recovered stale processing state after previous run ended', updated_at=?
        WHERE status='processing' AND (last_attempt_at IS NULL OR last_attempt_at < ?)
        """,
        (now_iso(), cutoff),
    )
    conn.commit()


def previous_posted_caption(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute(
        """
        SELECT caption FROM reel_posts_v2
        WHERE status='posted' AND caption IS NOT NULL AND caption <> ''
        ORDER BY posted_at DESC LIMIT 1
        """
    ).fetchone()
    return row[0] if row else None


def select_candidate(conn: sqlite3.Connection, videos: list[DriveVideo]) -> Optional[DriveVideo]:
    reset_stale_processing(conn)
    for video in videos:
        upsert_seen(conn, video)
    conn.commit()

    for video in videos:
        row = conn.execute(
            "SELECT status, attempts, last_attempt_at FROM reel_posts_v2 WHERE file_id=?",
            (video.file_id,),
        ).fetchone()
        if not row:
            return video
        status = str(row["status"]).lower()
        attempts = int(row["attempts"] or 0)
        if status in {"posted", "skipped", "uncertain"}:
            continue
        if status == "processing":
            continue
        if attempts >= MAX_ATTEMPTS:
            conn.execute(
                "UPDATE reel_posts_v2 SET status='skipped', updated_at=? WHERE file_id=?",
                (now_iso(), video.file_id),
            )
            conn.commit()
            continue
        return video
    return None


def mark_processing(conn: sqlite3.Connection, video: DriveVideo, caption: str) -> int:
    ts = now_iso()
    conn.execute(
        """
        UPDATE reel_posts_v2
        SET status='processing', attempts=attempts+1, caption=?, last_error=NULL,
            last_attempt_at=?, updated_at=?
        WHERE file_id=?
        """,
        (caption, ts, ts, video.file_id),
    )
    conn.commit()
    row = conn.execute("SELECT attempts FROM reel_posts_v2 WHERE file_id=?", (video.file_id,)).fetchone()
    return int(row[0])


def mark_container(conn: sqlite3.Connection, file_id: str, container_id: str) -> None:
    conn.execute(
        "UPDATE reel_posts_v2 SET container_id=?, updated_at=? WHERE file_id=?",
        (container_id, now_iso(), file_id),
    )
    conn.commit()


def mark_failed(conn: sqlite3.Connection, file_id: str, error: str) -> None:
    row = conn.execute("SELECT attempts FROM reel_posts_v2 WHERE file_id=?", (file_id,)).fetchone()
    attempts = int(row[0] or 0) if row else 0
    status = "skipped" if attempts >= MAX_ATTEMPTS else "failed"
    conn.execute(
        "UPDATE reel_posts_v2 SET status=?, last_error=?, updated_at=? WHERE file_id=?",
        (status, sanitize(error), now_iso(), file_id),
    )
    conn.commit()


def mark_uncertain(conn: sqlite3.Connection, file_id: str, error: str) -> None:
    conn.execute(
        """
        UPDATE reel_posts_v2
        SET status='uncertain', last_error=?, updated_at=?
        WHERE file_id=?
        """,
        (sanitize(error), now_iso(), file_id),
    )
    conn.commit()


def mark_posted(
    conn: sqlite3.Connection,
    file_id: str,
    media_id: str,
    permalink: Optional[str] = None,
) -> None:
    ts = now_iso()
    conn.execute(
        """
        UPDATE reel_posts_v2
        SET status='posted', media_id=?, permalink=?, last_error=NULL,
            posted_at=?, updated_at=?
        WHERE file_id=?
        """,
        (media_id, permalink, ts, ts, file_id),
    )
    conn.commit()


# ----------------------------- Google Drive -----------------------------

def run_command(args: list[str], timeout: int) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            args,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise ReelPosterError(f"Command timed out after {timeout}s: {Path(args[0]).name}") from exc


def list_drive_videos() -> list[DriveVideo]:
    gdown = resolve_gdown()
    folder_url = f"https://drive.google.com/drive/folders/{DRIVE_FOLDER_ID}"
    proc = run_command([gdown, folder_url, "--folder", "--json", "--quiet"], FOLDER_LIST_TIMEOUT)
    if proc.returncode != 0:
        raise ReelPosterError("gdown folder listing failed: " + sanitize(proc.stderr.strip() or proc.stdout.strip()))
    try:
        entries = json.loads(proc.stdout)
    except json.JSONDecodeError as exc:
        raise ReelPosterError("gdown did not return valid JSON. Ensure gdown >= 6.1.0 is installed.") from exc
    if not isinstance(entries, list):
        raise ReelPosterError("Unexpected gdown JSON format: expected an array")

    candidates: list[tuple[dict[str, Any], PurePosixPath]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        url = str(entry.get("url") or "").strip()
        path_text = str(entry.get("path") or "").strip().replace("\\", "/")
        if not url or not path_text:
            continue
        p = PurePosixPath(path_text)
        if p.suffix.lower() not in ALLOWED_EXTENSIONS:
            continue
        candidates.append((entry, p))

    if not candidates:
        raise ReelPosterError("No direct video files found in the Drive folder")

    # gdown's JSON path may include the root folder name. Direct files always
    # have the shallowest path depth; nested subfolder files are deeper.
    direct_depth = min(len(p.parts) for _, p in candidates)
    videos: list[DriveVideo] = []
    for entry, p in candidates:
        if len(p.parts) != direct_depth:
            continue
        url = str(entry["url"]).strip()
        name = p.name
        videos.append(
            DriveVideo(
                file_id=extract_file_id(url),
                name=name,
                url=url,
                path=str(p),
                reel_number=extract_reel_number(name),
            )
        )

    videos.sort(key=natural_sort_key)
    return videos


def download_video(video: DriveVideo) -> Path:
    gdown = resolve_gdown()
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    safe_suffix = Path(video.name).suffix.lower() or ".mp4"
    fd, tmp_name = tempfile.mkstemp(prefix="ig_reel_", suffix=safe_suffix, dir=TEMP_DIR)
    os.close(fd)
    tmp_path = Path(tmp_name)
    with contextlib.suppress(FileNotFoundError):
        tmp_path.unlink()

    proc = run_command([gdown, video.url, "-O", str(tmp_path), "--quiet"], DOWNLOAD_TIMEOUT)
    if proc.returncode != 0 or not tmp_path.exists():
        with contextlib.suppress(FileNotFoundError):
            tmp_path.unlink()
        raise ReelPosterError("gdown video download failed: " + sanitize(proc.stderr.strip() or proc.stdout.strip()))

    size = tmp_path.stat().st_size
    if size <= 0:
        tmp_path.unlink(missing_ok=True)
        raise ReelPosterError("Downloaded video is empty")
    if size > MAX_FILE_BYTES:
        tmp_path.unlink(missing_ok=True)
        raise ReelPosterError(f"Video is {size} bytes, above Instagram's 1 GB Reel limit")
    return tmp_path


# ----------------------------- Instagram API -----------------------------

def graph_request(
    method: str,
    path: str,
    params: Optional[dict[str, Any]] = None,
    *,
    timeout: int = GRAPH_TIMEOUT,
) -> dict[str, Any]:
    params = {k: v for k, v in (params or {}).items() if v is not None}
    url = path if path.startswith("http://") or path.startswith("https://") else f"{GRAPH_BASE}/{path.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {INSTAGRAM_ACCESS_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "BotNerva-Railway-InstagramReels/4.0",
    }
    data: Optional[bytes] = None
    if method.upper() == "GET":
        if params:
            sep = "&" if "?" in url else "?"
            url += sep + urllib.parse.urlencode(params)
    else:
        data = urllib.parse.urlencode(params).encode("utf-8")
        headers["Content-Type"] = "application/x-www-form-urlencoded"

    req = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
            payload = json.loads(raw) if raw else {}
            if not isinstance(payload, dict):
                raise ReelPosterError("Unexpected Instagram API response format")
            return payload
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"raw": raw[:1000]}
        error_obj = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error_obj, dict):
            parts = [
                str(error_obj.get("message") or "").strip(),
                f"type={error_obj.get('type')}" if error_obj.get("type") else "",
                f"code={error_obj.get('code')}" if error_obj.get("code") is not None else "",
                f"subcode={error_obj.get('error_subcode')}" if error_obj.get("error_subcode") is not None else "",
                f"trace={error_obj.get('fbtrace_id')}" if error_obj.get("fbtrace_id") else "",
            ]
            detail = " | ".join(p for p in parts if p)
        else:
            detail = raw or str(exc.reason)
        raise GraphHTTPError(exc.code, sanitize(detail), payload) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise NetworkError(f"Instagram API network error: {sanitize(exc)}") from exc


def verify_instagram_auth() -> dict[str, Any]:
    me = graph_request("GET", "me", {"fields": "id,username"})
    actual_id = str(me.get("id") or "")
    if actual_id != INSTAGRAM_USER_ID:
        raise ReelPosterError(
            f"Authenticated Instagram account ID {actual_id or '<missing>'} does not match INSTAGRAM_USER_ID"
        )
    return me


def create_reel_container(caption: str) -> tuple[str, str]:
    payload = graph_request(
        "POST",
        f"{INSTAGRAM_USER_ID}/media",
        {
            "media_type": "REELS",
            "upload_type": "resumable",
            "caption": caption,
            "share_to_feed": "true" if SHARE_TO_FEED else "false",
        },
    )
    container_id = str(payload.get("id") or "").strip()
    upload_uri = str(payload.get("uri") or "").strip()
    if not container_id or not upload_uri:
        raise ReelPosterError("Instagram resumable container response did not include both id and uri")
    return container_id, upload_uri


def upload_binary(upload_uri: str, video_path: Path) -> None:
    parsed = urllib.parse.urlsplit(upload_uri)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ReelPosterError("Instagram returned an invalid resumable upload URI")
    path = urllib.parse.urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
    size = video_path.stat().st_size
    conn = http.client.HTTPSConnection(parsed.hostname, parsed.port or 443, timeout=UPLOAD_TIMEOUT)
    try:
        conn.putrequest("POST", path)
        conn.putheader("Authorization", f"OAuth {INSTAGRAM_ACCESS_TOKEN}")
        conn.putheader("offset", "0")
        conn.putheader("file_size", str(size))
        conn.putheader("Content-Type", "application/octet-stream")
        conn.putheader("Content-Length", str(size))
        conn.putheader("User-Agent", "BotNerva-Railway-InstagramReels/4.0")
        conn.endheaders()
        with video_path.open("rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                conn.send(chunk)
        resp = conn.getresponse()
        body = resp.read().decode("utf-8", errors="replace")
        if not 200 <= resp.status < 300:
            raise GraphHTTPError(resp.status, sanitize(body or resp.reason))
    except GraphHTTPError:
        raise
    except (TimeoutError, OSError, http.client.HTTPException) as exc:
        raise NetworkError(f"Instagram video upload network error: {sanitize(exc)}") from exc
    finally:
        conn.close()


def poll_container(container_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + POLL_TIMEOUT
    last: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last = graph_request(
            "GET",
            container_id,
            {"fields": "id,status,status_code,video_status"},
        )
        status = str(last.get("status_code") or "").upper()
        if status == "FINISHED":
            return last
        if status in {"ERROR", "EXPIRED"}:
            raise ReelPosterError(
                f"Instagram container {container_id} ended with {status}: {sanitize(last.get('status') or last)}"
            )
        time.sleep(POLL_INTERVAL)
    raise ReelPosterError(
        f"Instagram container {container_id} did not reach FINISHED within {POLL_TIMEOUT}s; last status={sanitize(last)}"
    )


def publish_container(container_id: str) -> str:
    # Deliberately no automatic retry here: a network timeout after Meta accepts
    # the publish request could otherwise create a duplicate on the next attempt.
    payload = graph_request(
        "POST",
        f"{INSTAGRAM_USER_ID}/media_publish",
        {"creation_id": container_id},
        timeout=GRAPH_TIMEOUT,
    )
    media_id = str(payload.get("id") or "").strip()
    if not media_id:
        raise ReelPosterError("Instagram media_publish response did not contain a media id")
    return media_id


def get_permalink(media_id: str) -> Optional[str]:
    try:
        payload = graph_request("GET", media_id, {"fields": "permalink"})
        return str(payload.get("permalink") or "").strip() or None
    except ReelPosterError:
        return None


# ----------------------------- Main flow -----------------------------

def print_dry_run(videos: list[DriveVideo], conn: sqlite3.Connection) -> None:
    print("=== DRY RUN MODE ===")
    print(f"Found {len(videos)} direct video files.")
    print("First 15 files after numeric sorting:")
    for video in videos[:15]:
        print(f"  {video.reel_number if video.reel_number is not None else '-'}: {video.name} ({video.file_id})")

    candidate = select_candidate(conn, videos)
    if candidate is None:
        print("Next candidate: NONE (all eligible files are posted/skipped/blocked)")
        print("=== DRY RUN RESULT: PASS ===")
        return

    caption = make_caption(candidate, previous_posted_caption(conn))
    print(f"Next candidate: {candidate.name} (ID: {candidate.file_id})")
    print("Generated caption:")
    print("---")
    print(caption)
    print("---")
    print("No video was downloaded, no media container was created, and nothing was published.")
    print("=== DRY RUN RESULT: PASS ===")


def main() -> int:
    require_config()
    with single_instance_lock():
        conn = connect_db()
        try:
            log("Verifying Instagram authentication...")
            me = verify_instagram_auth()
            log(f"Authenticated as @{me.get('username', '<unknown>')} (ID: {me.get('id')})")

            log("Fetching Google Drive folder list...")
            videos = list_drive_videos()
            log(f"Found {len(videos)} direct video files.")
            seed_known_posted(conn, videos)

            if DRY_RUN:
                print_dry_run(videos, conn)
                return 0

            candidate = select_candidate(conn, videos)
            if candidate is None:
                log("No eligible Reel remains to post.")
                return 0

            caption = make_caption(candidate, previous_posted_caption(conn))
            attempt = mark_processing(conn, candidate, caption)
            log(
                f"Selected Reel #{candidate.reel_number if candidate.reel_number is not None else '?'}: "
                f"{candidate.name} (attempt {attempt}/{MAX_ATTEMPTS})"
            )

            video_path: Optional[Path] = None
            try:
                log("Downloading selected Reel from Google Drive...")
                video_path = download_video(candidate)
                log(f"Downloaded {video_path.stat().st_size} bytes. Creating Instagram container...")

                container_id, upload_uri = create_reel_container(caption)
                mark_container(conn, candidate.file_id, container_id)
                log(f"Container {container_id} created. Uploading video binary to Meta...")

                upload_binary(upload_uri, video_path)
                log("Upload accepted. Waiting for Instagram processing...")
                poll_container(container_id)
                log("Container FINISHED. Publishing Reel...")

                try:
                    media_id = publish_container(container_id)
                except NetworkError as exc:
                    # We cannot know whether Meta accepted a publish request when
                    # the response is lost. Block auto-retry to prevent duplicates.
                    mark_uncertain(conn, candidate.file_id, str(exc))
                    raise ReelPosterError(
                        "Publish response was lost/uncertain; this file was blocked from automatic retry to prevent a duplicate. "
                        "Inspect Instagram before changing its DB status."
                    ) from exc
                except GraphHTTPError as exc:
                    # A 5xx after sending media_publish can also be ambiguous:
                    # Meta may have accepted the publish before the error surfaced.
                    if exc.status >= 500:
                        mark_uncertain(conn, candidate.file_id, str(exc))
                        raise ReelPosterError(
                            "Instagram returned a server error during media_publish; publish state is uncertain. "
                            "This file was blocked from automatic retry to prevent a duplicate. Inspect Instagram first."
                        ) from exc
                    raise

                permalink = get_permalink(media_id)
                mark_posted(conn, candidate.file_id, media_id, permalink)
                print(f"Reel posted successfully | {candidate.name} | IG media {media_id}", flush=True)
                return 0

            except GraphHTTPError as exc:
                mark_failed(conn, candidate.file_id, str(exc))
                raise
            except ReelPosterError as exc:
                # mark_uncertain already owns its state; do not overwrite it.
                row = conn.execute("SELECT status FROM reel_posts_v2 WHERE file_id=?", (candidate.file_id,)).fetchone()
                if not row or row[0] != "uncertain":
                    mark_failed(conn, candidate.file_id, str(exc))
                raise
            except Exception as exc:
                mark_failed(conn, candidate.file_id, f"Unexpected error: {type(exc).__name__}: {exc}")
                raise ReelPosterError(f"Unexpected error: {type(exc).__name__}: {sanitize(exc)}") from exc
            finally:
                if video_path is not None:
                    with contextlib.suppress(FileNotFoundError):
                        video_path.unlink()

        finally:
            conn.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception as exc:
        print(f"Instagram Reel automation failed | {sanitize(exc)}", file=sys.stderr, flush=True)
        raise SystemExit(1)

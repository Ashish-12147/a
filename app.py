#!/usr/bin/env python3
"""
Railway Instagram Reels Automation Control Plane

No Hermes. No LLM.

Features
--------
- Long-running Railway service
- Persistent SQLite state on a Railway Volume
- 4x/day default scheduler in Asia/Kolkata
- Telegram owner-only control bot
- Inline dashboard buttons
- Success/failure/startup/daily reports
- Pause/resume without redeploying
- Change schedule from Telegram
- Post-next-now with confirmation
- Dry-run / next-candidate / caption preview
- Skip-next with confirmation
- Retry a failed/skipped reel by reel number
- Stats, history and error inspection
- Health endpoint for Railway
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import os
import sqlite3
import sys
import threading
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from zoneinfo import ZoneInfo

import instagram_reel_poster as poster


# --------------------------- Configuration ---------------------------

IST = ZoneInfo(os.getenv("AUTOMATION_TIMEZONE", "Asia/Kolkata"))
DEFAULT_SCHEDULE = os.getenv(
    "REEL_POST_TIMES",
    "09:30,13:30,17:30,21:30",
).strip()
DEFAULT_DAILY_REPORT_TIME = os.getenv("DAILY_REPORT_TIME", "22:15").strip()

TG_TOKEN = (
    os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    or os.getenv("TELEGRAM_TOKEN", "").strip()
    or os.getenv("TG_BOT_TOKEN", "").strip()
)
TG_REPORT_CHAT = (
    os.getenv("TELEGRAM_CHAT_ID", "").strip()
    or os.getenv("TG_CHAT_ID", "").strip()
)
TG_ALLOWED_RAW = os.getenv("TELEGRAM_ALLOWED_CHAT_IDS", "").strip()
DAILY_REPORT_ENABLED = os.getenv("DAILY_REPORT_ENABLED", "true").strip().lower() in {
    "1", "true", "yes", "on"
}
STARTUP_REPORT_ENABLED = os.getenv("STARTUP_REPORT_ENABLED", "true").strip().lower() in {
    "1", "true", "yes", "on"
}
PORT = int(os.getenv("PORT", "8080"))

APP_VERSION = "4.0.0"
BOT_API = f"https://api.telegram.org/bot{TG_TOKEN}" if TG_TOKEN else ""
POST_MUTEX = threading.Lock()
CATALOG_MUTEX = threading.Lock()
CATALOG_CACHE: dict[str, Any] = {"at": 0.0, "videos": None}
CATALOG_TTL = max(30, int(os.getenv("DRIVE_CATALOG_CACHE_SECONDS", "600")))
STOP_EVENT = threading.Event()


def now_ist() -> dt.datetime:
    return dt.datetime.now(IST)


def iso_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def short_error(exc: BaseException) -> str:
    text = poster.sanitize(f"{type(exc).__name__}: {exc}")
    return text[:1200]


# --------------------------- SQLite control tables ---------------------------

def db() -> sqlite3.Connection:
    conn = poster.connect_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS automation_settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS automation_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT NOT NULL,
            status TEXT NOT NULL,
            trigger_source TEXT,
            reel_number INTEGER,
            file_id TEXT,
            filename TEXT,
            detail TEXT,
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS automation_slots (
            slot_key TEXT PRIMARY KEY,
            claimed_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def get_setting(key: str, default: str) -> str:
    conn = db()
    try:
        row = conn.execute(
            "SELECT value FROM automation_settings WHERE key=?",
            (key,),
        ).fetchone()
        return str(row[0]) if row else default
    finally:
        conn.close()


def set_setting(key: str, value: str) -> None:
    conn = db()
    try:
        conn.execute(
            """
            INSERT INTO automation_settings(key,value,updated_at)
            VALUES(?,?,?)
            ON CONFLICT(key) DO UPDATE SET
                value=excluded.value,
                updated_at=excluded.updated_at
            """,
            (key, value, iso_utc()),
        )
        conn.commit()
    finally:
        conn.close()


def is_paused() -> bool:
    return get_setting("paused", "0") == "1"


def set_paused(paused: bool) -> None:
    set_setting("paused", "1" if paused else "0")


def log_event(
    event_type: str,
    status: str,
    trigger_source: str = "",
    reel_number: Optional[int] = None,
    file_id: str = "",
    filename: str = "",
    detail: str = "",
) -> None:
    conn = db()
    try:
        conn.execute(
            """
            INSERT INTO automation_events
            (event_type,status,trigger_source,reel_number,file_id,filename,detail,created_at)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (
                event_type,
                status,
                trigger_source,
                reel_number,
                file_id,
                filename,
                poster.sanitize(detail)[:2500],
                iso_utc(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def claim_slot(slot_key: str) -> bool:
    conn = db()
    try:
        try:
            conn.execute(
                "INSERT INTO automation_slots(slot_key,claimed_at) VALUES(?,?)",
                (slot_key, iso_utc()),
            )
            conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False
    finally:
        conn.close()


# --------------------------- Schedule ---------------------------

def validate_time(value: str) -> str:
    try:
        parsed = dt.datetime.strptime(value, "%H:%M")
    except ValueError as exc:
        raise ValueError(f"Invalid time '{value}'. Use HH:MM in 24-hour format.") from exc
    return parsed.strftime("%H:%M")


def schedule_times() -> list[str]:
    raw = get_setting("post_times", DEFAULT_SCHEDULE)
    values = [v.strip() for v in raw.replace(" ", ",").split(",") if v.strip()]
    clean = sorted({validate_time(v) for v in values})
    if not clean:
        clean = ["09:30", "13:30", "17:30", "21:30"]
    return clean


def update_schedule(values: list[str]) -> list[str]:
    clean = sorted({validate_time(v) for v in values})
    if not 1 <= len(clean) <= 12:
        raise ValueError("Schedule must contain between 1 and 12 posting times.")
    set_setting("post_times", ",".join(clean))
    return clean


def next_scheduled_time() -> Optional[dt.datetime]:
    if is_paused():
        return None
    now = now_ist()
    for add_days in range(0, 8):
        day = (now + dt.timedelta(days=add_days)).date()
        for value in schedule_times():
            hh, mm = map(int, value.split(":"))
            candidate = dt.datetime.combine(day, dt.time(hh, mm), IST)
            if candidate > now:
                return candidate
    return None


def fmt_next_run() -> str:
    nxt = next_scheduled_time()
    if nxt is None:
        return "PAUSED"
    return nxt.strftime("%d %b %Y, %I:%M %p IST")


# --------------------------- Drive catalog / queue helpers ---------------------------

def get_catalog(force: bool = False) -> list[poster.DriveVideo]:
    with CATALOG_MUTEX:
        cached = CATALOG_CACHE.get("videos")
        age = time.monotonic() - float(CATALOG_CACHE.get("at") or 0)
        if not force and cached is not None and age < CATALOG_TTL:
            return cached
        videos = poster.list_drive_videos()
        CATALOG_CACHE["videos"] = videos
        CATALOG_CACHE["at"] = time.monotonic()
        return videos


def prepare_catalog(force: bool = False) -> tuple[list[poster.DriveVideo], sqlite3.Connection]:
    videos = get_catalog(force=force)
    conn = db()
    poster.seed_known_posted(conn, videos)
    for video in videos:
        poster.upsert_seen(conn, video)
    conn.commit()
    return videos, conn


def preview_next(force: bool = False) -> tuple[Optional[poster.DriveVideo], Optional[str]]:
    videos, conn = prepare_catalog(force=force)
    try:
        candidate = poster.select_candidate(conn, videos)
        if candidate is None:
            return None, None
        caption = poster.make_caption(candidate, poster.previous_posted_caption(conn))
        return candidate, caption
    finally:
        conn.close()


def get_row_for_file(file_id: str) -> Optional[sqlite3.Row]:
    conn = db()
    try:
        return conn.execute(
            "SELECT * FROM reel_posts_v2 WHERE file_id=?",
            (file_id,),
        ).fetchone()
    finally:
        conn.close()


def latest_attempt_row(since: Optional[str] = None) -> Optional[sqlite3.Row]:
    conn = db()
    try:
        if since:
            return conn.execute(
                """
                SELECT * FROM reel_posts_v2
                WHERE last_attempt_at IS NOT NULL AND last_attempt_at>=?
                ORDER BY last_attempt_at DESC
                LIMIT 1
                """,
                (since,),
            ).fetchone()
        return conn.execute(
            """
            SELECT * FROM reel_posts_v2
            WHERE last_attempt_at IS NOT NULL
            ORDER BY last_attempt_at DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        conn.close()


def queue_counts() -> dict[str, int]:
    conn = db()
    try:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM reel_posts_v2 GROUP BY status"
        ).fetchall()
        result = {str(r["status"]): int(r["n"]) for r in rows}
        result["known_total"] = sum(result.values())
        return result
    finally:
        conn.close()


def stats_text(include_next: bool = True) -> str:
    conn = db()
    try:
        counts = {
            str(r["status"]): int(r["n"])
            for r in conn.execute(
                "SELECT status, COUNT(*) AS n FROM reel_posts_v2 GROUP BY status"
            ).fetchall()
        }
        today = now_ist().date()
        start_utc = dt.datetime.combine(today, dt.time.min, IST).astimezone(dt.timezone.utc).isoformat()
        end_utc = dt.datetime.combine(today + dt.timedelta(days=1), dt.time.min, IST).astimezone(dt.timezone.utc).isoformat()
        posted_today = conn.execute(
            "SELECT COUNT(*) FROM reel_posts_v2 WHERE posted_at>=? AND posted_at<?",
            (start_utc, end_utc),
        ).fetchone()[0]
        failures_today = conn.execute(
            """
            SELECT COUNT(*) FROM automation_events
            WHERE event_type='post' AND status='failed'
              AND created_at>=? AND created_at<?
            """,
            (start_utc, end_utc),
        ).fetchone()[0]
    finally:
        conn.close()

    known = sum(counts.values())
    lines = [
        "AUTOMATION STATS",
        f"Posted today: {posted_today}",
        f"Failures today: {failures_today}",
        f"Posted total: {counts.get('posted', 0)}",
        f"Pending/failed: {counts.get('pending', 0) + counts.get('failed', 0)}",
        f"Skipped: {counts.get('skipped', 0)}",
        f"Uncertain: {counts.get('uncertain', 0)}",
        f"Known files in DB: {known}",
        f"Automation: {'PAUSED' if is_paused() else 'RUNNING'}",
        f"Schedule: {', '.join(schedule_times())} IST",
        f"Next scheduled: {fmt_next_run()}",
    ]
    if include_next:
        try:
            candidate, _ = preview_next()
            lines.append(
                "Next candidate: "
                + (
                    f"#{candidate.reel_number} — {candidate.name}"
                    if candidate else "NONE"
                )
            )
        except Exception as exc:
            lines.append(f"Next candidate: unavailable ({short_error(exc)})")
    return "\n".join(lines)


def history_text(limit: int = 8) -> str:
    limit = max(1, min(limit, 20))
    conn = db()
    try:
        rows = conn.execute(
            """
            SELECT reel_number,filename,status,attempts,media_id,permalink,posted_at,last_error,last_attempt_at
            FROM reel_posts_v2
            WHERE status IN ('posted','failed','skipped','uncertain')
            ORDER BY COALESCE(posted_at,last_attempt_at,updated_at) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        return "HISTORY\nNo completed/failed attempts yet."
    lines = ["HISTORY"]
    for r in rows:
        when = r["posted_at"] or r["last_attempt_at"] or ""
        when = when.replace("T", " ")[:19]
        label = f"#{r['reel_number']}" if r["reel_number"] is not None else "?"
        lines.append(f"{label} | {r['status'].upper()} | attempts={r['attempts']} | {when}")
        if r["permalink"]:
            lines.append(str(r["permalink"]))
        elif r["last_error"]:
            lines.append("  " + str(r["last_error"])[:180])
    return "\n".join(lines)


def errors_text(limit: int = 8) -> str:
    limit = max(1, min(limit, 20))
    conn = db()
    try:
        rows = conn.execute(
            """
            SELECT reel_number,filename,status,attempts,last_error,last_attempt_at
            FROM reel_posts_v2
            WHERE last_error IS NOT NULL AND last_error<>''
            ORDER BY last_attempt_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()
    if not rows:
        return "ERRORS\nNo recorded Reel errors."
    lines = ["RECENT ERRORS"]
    for r in rows:
        label = f"#{r['reel_number']}" if r["reel_number"] is not None else "?"
        lines.append(f"{label} | {r['status'].upper()} | attempts={r['attempts']}")
        lines.append("  " + str(r["last_error"])[:500])
    return "\n".join(lines)


# --------------------------- Posting workflow ---------------------------

def post_report(row: Optional[sqlite3.Row], elapsed: float, trigger: str, error: str = "") -> str:
    if row is None:
        return (
            "POST RESULT\n"
            f"Trigger: {trigger}\n"
            f"Result: {'FAILED' if error else 'NO ELIGIBLE REEL'}\n"
            f"Error: {error or 'No new Reel attempt was created; the eligible queue may be exhausted.'}\n"
            f"Next scheduled: {fmt_next_run()}"
        )
    status = str(row["status"]).upper()
    reel = f"#{row['reel_number']}" if row["reel_number"] is not None else "?"
    lines = [
        "POST RESULT",
        f"Trigger: {trigger}",
        f"Result: {status}",
        f"Reel: {reel}",
        f"File: {row['filename']}",
        f"Attempts: {row['attempts']}/{poster.MAX_ATTEMPTS}",
        f"Duration: {elapsed:.1f}s",
    ]
    if row["media_id"]:
        lines.append(f"Instagram media ID: {row['media_id']}")
    if row["permalink"]:
        lines.append(f"Permalink: {row['permalink']}")
    if row["caption"]:
        lines.append("Caption:")
        lines.append(str(row["caption"])[:900])
    if error:
        lines.append(f"Error: {error[:1200]}")
    elif row["last_error"]:
        lines.append(f"Error: {str(row['last_error'])[:1200]}")
    counts = queue_counts()
    lines.append(f"Posted total: {counts.get('posted', 0)}")
    lines.append(f"Next scheduled: {fmt_next_run()}")
    return "\n".join(lines)


def execute_post(trigger: str, report_chat: Optional[str] = None) -> None:
    if not POST_MUTEX.acquire(blocking=False):
        if report_chat:
            send_message(report_chat, "A Reel posting job is already running.")
        return

    started = time.monotonic()
    run_started_at = iso_utc()
    try:
        if trigger == "schedule" and is_paused():
            return

        log_event("post", "started", trigger_source=trigger)
        if report_chat and trigger != "schedule":
            send_message(report_chat, f"Posting next Reel now.\nTrigger: {trigger}")

        try:
            rc = poster.main()
            if rc != 0:
                raise RuntimeError(f"Poster returned exit code {rc}")
            row = latest_attempt_row(run_started_at)
            elapsed = time.monotonic() - started
            if row is not None:
                log_event(
                    "post",
                    str(row["status"]),
                    trigger_source=trigger,
                    reel_number=row["reel_number"],
                    file_id=row["file_id"],
                    filename=row["filename"],
                    detail=row["last_error"] or row["permalink"] or "",
                )
            target = report_chat or TG_REPORT_CHAT
            if target:
                send_message(target, post_report(row, elapsed, trigger))
        except BaseException as exc:
            elapsed = time.monotonic() - started
            row = latest_attempt_row(run_started_at)
            err = short_error(exc)
            log_event(
                "post",
                "failed",
                trigger_source=trigger,
                reel_number=(row["reel_number"] if row else None),
                file_id=(row["file_id"] if row else ""),
                filename=(row["filename"] if row else ""),
                detail=err,
            )
            target = report_chat or TG_REPORT_CHAT
            if target:
                send_message(target, post_report(row, elapsed, trigger, err))
            print("Posting job failed:", err, file=sys.stderr, flush=True)
    finally:
        POST_MUTEX.release()


def start_post_job(trigger: str, report_chat: Optional[str] = None) -> bool:
    if POST_MUTEX.locked():
        return False
    t = threading.Thread(
        target=execute_post,
        args=(trigger, report_chat),
        name=f"post-{trigger}",
        daemon=True,
    )
    t.start()
    return True


# --------------------------- Telegram API ---------------------------

def parse_allowed_chat_ids() -> set[str]:
    values = []
    if TG_ALLOWED_RAW:
        values.extend(re.split(r"[\s,;]+", TG_ALLOWED_RAW))
    if TG_REPORT_CHAT:
        values.append(TG_REPORT_CHAT)
    return {v.strip() for v in values if v.strip()}


# Imported late only for the simple split regex above.
import re

ALLOWED_CHAT_IDS = parse_allowed_chat_ids()


def tg_api(method: str, params: Optional[dict[str, Any]] = None, timeout: int = 60) -> Any:
    if not TG_TOKEN:
        raise RuntimeError("Telegram token is not configured")
    body: dict[str, Any] = {}
    for key, value in (params or {}).items():
        if isinstance(value, (dict, list)):
            body[key] = json.dumps(value, separators=(",", ":"))
        elif value is not None:
            body[key] = str(value)
    data = urllib.parse.urlencode(body).encode("utf-8")
    req = urllib.request.Request(
        f"{BOT_API}/{method}",
        data=data,
        method="POST",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Telegram HTTP {exc.code}: {raw[:500]}") from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise RuntimeError(f"Telegram network error: {exc}") from exc
    if not payload.get("ok"):
        raise RuntimeError(f"Telegram API error: {payload}")
    return payload.get("result")


def send_message(chat_id: str, text: str, reply_markup: Optional[dict[str, Any]] = None) -> None:
    if not TG_TOKEN or not chat_id:
        return
    chunks = []
    remaining = str(text)
    while remaining:
        if len(remaining) <= 3900:
            chunks.append(remaining)
            break
        cut = remaining.rfind("\n", 0, 3900)
        if cut < 1000:
            cut = 3900
        chunks.append(remaining[:cut])
        remaining = remaining[cut:].lstrip()
    for i, chunk in enumerate(chunks):
        params: dict[str, Any] = {
            "chat_id": chat_id,
            "text": chunk,
            "disable_web_page_preview": "true",
        }
        if reply_markup is not None and i == len(chunks) - 1:
            params["reply_markup"] = reply_markup
        tg_api("sendMessage", params, timeout=30)


def answer_callback(callback_id: str, text: str = "") -> None:
    with contextlib.suppress(Exception):
        tg_api(
            "answerCallbackQuery",
            {"callback_query_id": callback_id, "text": text[:180]},
            timeout=15,
        )


def dashboard_keyboard() -> dict[str, Any]:
    paused = is_paused()
    return {
        "inline_keyboard": [
            [
                {"text": "Status", "callback_data": "status"},
                {"text": "Next", "callback_data": "next"},
            ],
            [
                {"text": "Stats", "callback_data": "stats"},
                {"text": "History", "callback_data": "history"},
            ],
            [
                {"text": "Dry run", "callback_data": "dryrun"},
                {"text": "Errors", "callback_data": "errors"},
            ],
            [
                {
                    "text": "Resume" if paused else "Pause",
                    "callback_data": "resume" if paused else "pause",
                },
                {"text": "Post now", "callback_data": "post_confirm"},
            ],
            [
                {"text": "Schedule", "callback_data": "schedule"},
                {"text": "Daily report", "callback_data": "report"},
            ],
        ]
    }


def dashboard_text() -> str:
    return (
        "INSTAGRAM REELS CONTROL\n"
        f"Version: {APP_VERSION}\n"
        f"State: {'PAUSED' if is_paused() else 'RUNNING'}\n"
        f"Schedule: {', '.join(schedule_times())} IST\n"
        f"Next run: {fmt_next_run()}\n"
        f"Posting job: {'BUSY' if POST_MUTEX.locked() else 'IDLE'}"
    )


def send_dashboard(chat_id: str) -> None:
    send_message(chat_id, dashboard_text(), dashboard_keyboard())


def require_authorized(chat_id: str) -> bool:
    return bool(chat_id and chat_id in ALLOWED_CHAT_IDS)


def command_help() -> str:
    return (
        "COMMANDS\n"
        "/panel - control dashboard\n"
        "/status - automation state + next run\n"
        "/next - next Reel + caption preview\n"
        "/stats - totals and today's activity\n"
        "/queue - queue summary + next Reel\n"
        "/refresh - force-refresh Drive catalog\n"
        "/history [n] - recent results\n"
        "/errors [n] - recent errors\n"
        "/postnow - confirm an immediate post\n"
        "/dryrun - full safe next-candidate check\n"
        "/pause - stop scheduled posting\n"
        "/resume - resume scheduled posting\n"
        "/schedule - show current schedule\n"
        "/schedule HH:MM HH:MM ... - replace schedule\n"
        "/skipnext - confirm skipping the next Reel\n"
        "/retry REEL_NUMBER - reset a failed/skipped Reel\n"
        "/report - send an on-demand report\n"
        "/health - bot/service health\n"
        "/help - this command list"
    )


def next_text(force: bool = False) -> str:
    candidate, caption = preview_next(force=force)
    if not candidate:
        return "NEXT REEL\nNo eligible Reel remains."
    return (
        "NEXT REEL\n"
        f"Reel: #{candidate.reel_number}\n"
        f"File: {candidate.name}\n"
        f"Drive ID: {candidate.file_id}\n"
        "Caption:\n"
        f"{caption}"
    )


def dryrun_text() -> str:
    poster.require_config()
    me = poster.verify_instagram_auth()
    videos, conn = prepare_catalog(force=True)
    try:
        candidate = poster.select_candidate(conn, videos)
        first = ", ".join(
            str(v.reel_number) if v.reel_number is not None else "?"
            for v in videos[:15]
        )
        lines = [
            "DRY RUN: PASS",
            f"Instagram: @{me.get('username')} ({me.get('id')})",
            f"Drive direct videos: {len(videos)}",
            f"First 15 order: {first}",
        ]
        if candidate:
            caption = poster.make_caption(candidate, poster.previous_posted_caption(conn))
            lines.extend(
                [
                    f"Next: #{candidate.reel_number} — {candidate.name}",
                    "Caption:",
                    caption,
                ]
            )
        else:
            lines.append("Next: NONE")
        lines.append("No video downloaded. Nothing published.")
        return "\n".join(lines)
    finally:
        conn.close()


def daily_report_text() -> str:
    header = f"DAILY REPORT — {now_ist().strftime('%d %b %Y')} IST"
    return header + "\n\n" + stats_text(include_next=True)


def health_text() -> str:
    return (
        "SERVICE HEALTH\n"
        f"Version: {APP_VERSION}\n"
        f"Poster config: {'OK' if poster.DRIVE_FOLDER_ID and poster.INSTAGRAM_USER_ID and poster.INSTAGRAM_ACCESS_TOKEN else 'MISSING'}\n"
        f"Telegram: {'OK' if TG_TOKEN else 'DISABLED'}\n"
        f"Authorized chats: {len(ALLOWED_CHAT_IDS)}\n"
        f"DB: {poster.DB_PATH}\n"
        f"Posting worker: {'BUSY' if POST_MUTEX.locked() else 'IDLE'}\n"
        f"Scheduler: {'PAUSED' if is_paused() else 'RUNNING'}\n"
        f"Next run: {fmt_next_run()}"
    )


def retry_reel(reel_number: int) -> str:
    conn = db()
    try:
        rows = conn.execute(
            "SELECT file_id,filename,status FROM reel_posts_v2 WHERE reel_number=? ORDER BY filename",
            (reel_number,),
        ).fetchall()
        if not rows:
            return f"Reel #{reel_number} is not known in the local DB yet. Run /next first."
        changed = 0
        for row in rows:
            if row["status"] in {"failed", "skipped"}:
                conn.execute(
                    """
                    UPDATE reel_posts_v2
                    SET status='pending', attempts=0, last_error=NULL, container_id=NULL,
                        media_id=NULL, permalink=NULL, last_attempt_at=NULL, updated_at=?
                    WHERE file_id=?
                    """,
                    (iso_utc(), row["file_id"]),
                )
                changed += 1
        conn.commit()
        return f"Retry reset complete for Reel #{reel_number}. Rows reset: {changed}."
    finally:
        conn.close()


def skip_next() -> str:
    videos, conn = prepare_catalog(force=True)
    try:
        candidate = poster.select_candidate(conn, videos)
        if candidate is None:
            return "No eligible Reel exists to skip."
        conn.execute(
            """
            UPDATE reel_posts_v2
            SET status='skipped', last_error='Skipped manually from Telegram',
                updated_at=?
            WHERE file_id=?
            """,
            (iso_utc(), candidate.file_id),
        )
        conn.commit()
        log_event(
            "queue",
            "skipped",
            trigger_source="telegram",
            reel_number=candidate.reel_number,
            file_id=candidate.file_id,
            filename=candidate.name,
            detail="Manual skip",
        )
        return f"Skipped Reel #{candidate.reel_number}: {candidate.name}"
    finally:
        conn.close()


def handle_command(chat_id: str, text: str) -> None:
    parts = text.strip().split()
    command = parts[0].split("@", 1)[0].lower()

    if command in {"/start", "/panel"}:
        send_dashboard(chat_id)
        return
    if command == "/help":
        send_message(chat_id, command_help(), dashboard_keyboard())
        return
    if command == "/status":
        send_message(chat_id, dashboard_text(), dashboard_keyboard())
        return
    if command == "/next":
        send_message(chat_id, next_text(), dashboard_keyboard())
        return
    if command == "/stats":
        send_message(chat_id, stats_text(), dashboard_keyboard())
        return
    if command == "/queue":
        send_message(chat_id, stats_text(include_next=True), dashboard_keyboard())
        return
    if command == "/refresh":
        send_message(chat_id, "Refreshing Google Drive catalog.")
        try:
            videos = get_catalog(force=True)
            candidate, _ = preview_next(force=False)
            result = (
                f"DRIVE CATALOG REFRESHED\nDirect videos: {len(videos)}\n"
                + (f"Next: #{candidate.reel_number} — {candidate.name}" if candidate else "Next: NONE")
            )
            send_message(chat_id, result, dashboard_keyboard())
        except Exception as exc:
            send_message(chat_id, "Refresh failed.\n" + short_error(exc), dashboard_keyboard())
        return
    if command == "/history":
        limit = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 8
        send_message(chat_id, history_text(limit), dashboard_keyboard())
        return
    if command == "/errors":
        limit = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 8
        send_message(chat_id, errors_text(limit), dashboard_keyboard())
        return
    if command == "/dryrun":
        send_message(chat_id, "Running safe dry-run. This can take a few seconds.")
        try:
            send_message(chat_id, dryrun_text(), dashboard_keyboard())
        except Exception as exc:
            send_message(chat_id, "DRY RUN: FAIL\n" + short_error(exc), dashboard_keyboard())
        return
    if command == "/pause":
        set_paused(True)
        log_event("control", "paused", trigger_source="telegram")
        send_message(chat_id, "Scheduled posting is now PAUSED.", dashboard_keyboard())
        return
    if command == "/resume":
        set_paused(False)
        log_event("control", "resumed", trigger_source="telegram")
        send_message(chat_id, "Scheduled posting is now RUNNING.", dashboard_keyboard())
        return
    if command == "/schedule":
        if len(parts) == 1:
            send_message(
                chat_id,
                "CURRENT SCHEDULE\n"
                + ", ".join(schedule_times())
                + " IST\n\n"
                + "Change it with:\n/schedule 09:30 13:30 17:30 21:30",
                dashboard_keyboard(),
            )
        else:
            try:
                clean = update_schedule(parts[1:])
                log_event("control", "schedule_changed", trigger_source="telegram", detail=",".join(clean))
                send_message(
                    chat_id,
                    "Schedule updated:\n" + ", ".join(clean) + " IST\nNext: " + fmt_next_run(),
                    dashboard_keyboard(),
                )
            except Exception as exc:
                send_message(chat_id, "Schedule not changed.\n" + short_error(exc))
        return
    if command == "/postnow":
        send_message(
            chat_id,
            "POST NEXT REEL NOW?\nThis performs a real Instagram publish.",
            {
                "inline_keyboard": [[
                    {"text": "Confirm post", "callback_data": "post_do"},
                    {"text": "Cancel", "callback_data": "cancel"},
                ]]
            },
        )
        return
    if command == "/skipnext":
        try:
            candidate, _ = preview_next()
            if not candidate:
                send_message(chat_id, "No eligible Reel exists to skip.")
                return
            send_message(
                chat_id,
                f"SKIP Reel #{candidate.reel_number}?\n{candidate.name}",
                {
                    "inline_keyboard": [[
                        {"text": "Confirm skip", "callback_data": "skip_do"},
                        {"text": "Cancel", "callback_data": "cancel"},
                    ]]
                },
            )
        except Exception as exc:
            send_message(chat_id, "Unable to inspect queue.\n" + short_error(exc))
        return
    if command == "/retry":
        if len(parts) != 2 or not parts[1].isdigit():
            send_message(chat_id, "Usage: /retry REEL_NUMBER")
            return
        send_message(chat_id, retry_reel(int(parts[1])), dashboard_keyboard())
        return
    if command == "/report":
        send_message(chat_id, daily_report_text(), dashboard_keyboard())
        return
    if command == "/health":
        send_message(chat_id, health_text(), dashboard_keyboard())
        return

    send_message(chat_id, "Unknown command.\n\n" + command_help(), dashboard_keyboard())


def handle_callback(query: dict[str, Any]) -> None:
    callback_id = str(query.get("id") or "")
    data = str(query.get("data") or "")
    message = query.get("message") or {}
    chat = message.get("chat") or {}
    chat_id = str(chat.get("id") or "")
    if not require_authorized(chat_id):
        answer_callback(callback_id, "Not authorized")
        return

    answer_callback(callback_id)

    if data == "cancel":
        send_message(chat_id, "Cancelled.", dashboard_keyboard())
    elif data == "status":
        send_message(chat_id, dashboard_text(), dashboard_keyboard())
    elif data == "next":
        try:
            send_message(chat_id, next_text(), dashboard_keyboard())
        except Exception as exc:
            send_message(chat_id, "Unable to load next Reel.\n" + short_error(exc))
    elif data == "stats":
        send_message(chat_id, stats_text(), dashboard_keyboard())
    elif data == "history":
        send_message(chat_id, history_text(), dashboard_keyboard())
    elif data == "errors":
        send_message(chat_id, errors_text(), dashboard_keyboard())
    elif data == "dryrun":
        send_message(chat_id, "Running safe dry-run.")
        try:
            send_message(chat_id, dryrun_text(), dashboard_keyboard())
        except Exception as exc:
            send_message(chat_id, "DRY RUN: FAIL\n" + short_error(exc), dashboard_keyboard())
    elif data == "pause":
        set_paused(True)
        send_message(chat_id, "Scheduled posting is now PAUSED.", dashboard_keyboard())
    elif data == "resume":
        set_paused(False)
        send_message(chat_id, "Scheduled posting is now RUNNING.", dashboard_keyboard())
    elif data == "post_confirm":
        send_message(
            chat_id,
            "POST NEXT REEL NOW?\nThis performs a real Instagram publish.",
            {
                "inline_keyboard": [[
                    {"text": "Confirm post", "callback_data": "post_do"},
                    {"text": "Cancel", "callback_data": "cancel"},
                ]]
            },
        )
    elif data == "post_do":
        if start_post_job("telegram", report_chat=chat_id):
            send_message(chat_id, "Manual Reel posting job started.")
        else:
            send_message(chat_id, "A posting job is already running.")
    elif data == "skip_do":
        try:
            send_message(chat_id, skip_next(), dashboard_keyboard())
        except Exception as exc:
            send_message(chat_id, "Skip failed.\n" + short_error(exc), dashboard_keyboard())
    elif data == "schedule":
        send_message(
            chat_id,
            "CURRENT SCHEDULE\n"
            + ", ".join(schedule_times())
            + " IST\n\nChange with:\n/schedule 09:30 13:30 17:30 21:30",
            dashboard_keyboard(),
        )
    elif data == "report":
        send_message(chat_id, daily_report_text(), dashboard_keyboard())


def telegram_loop() -> None:
    if not TG_TOKEN:
        print("Telegram disabled: no bot token configured.", flush=True)
        return
    if not ALLOWED_CHAT_IDS:
        print(
            "Telegram command handling disabled: TELEGRAM_CHAT_ID or TELEGRAM_ALLOWED_CHAT_IDS is required.",
            flush=True,
        )
        return

    # getUpdates and webhooks are mutually exclusive; this service is the long-polling owner.
    with contextlib.suppress(Exception):
        tg_api("deleteWebhook", {"drop_pending_updates": "false"}, timeout=20)
    with contextlib.suppress(Exception):
        tg_api(
            "setMyCommands",
            {
                "commands": [
                    {"command": "panel", "description": "Open control dashboard"},
                    {"command": "status", "description": "Automation state"},
                    {"command": "next", "description": "Preview next Reel"},
                    {"command": "stats", "description": "Posting stats"},
                    {"command": "queue", "description": "Queue summary"},
                    {"command": "refresh", "description": "Refresh Drive catalog"},
                    {"command": "postnow", "description": "Post next Reel now"},
                    {"command": "pause", "description": "Pause schedule"},
                    {"command": "resume", "description": "Resume schedule"},
                    {"command": "schedule", "description": "View/change schedule"},
                    {"command": "history", "description": "Recent results"},
                    {"command": "errors", "description": "Recent errors"},
                    {"command": "dryrun", "description": "Safe validation"},
                    {"command": "report", "description": "Current report"},
                    {"command": "help", "description": "All commands"},
                ]
            },
            timeout=20,
        )

    offset: Optional[int] = None
    while not STOP_EVENT.is_set():
        try:
            params: dict[str, Any] = {
                "timeout": 45,
                "allowed_updates": ["message", "callback_query"],
            }
            if offset is not None:
                params["offset"] = offset
            updates = tg_api("getUpdates", params, timeout=55) or []
            for update in updates:
                offset = int(update["update_id"]) + 1
                if "callback_query" in update:
                    handle_callback(update["callback_query"])
                    continue
                message = update.get("message") or {}
                chat = message.get("chat") or {}
                chat_id = str(chat.get("id") or "")
                if not require_authorized(chat_id):
                    continue
                text = str(message.get("text") or "").strip()
                if text.startswith("/"):
                    try:
                        handle_command(chat_id, text)
                    except Exception as exc:
                        send_message(chat_id, "Command failed.\n" + short_error(exc))
        except Exception as exc:
            print("Telegram loop error:", short_error(exc), flush=True)
            time.sleep(5)


# --------------------------- Scheduler ---------------------------

def scheduler_loop() -> None:
    print(
        "Scheduler started:",
        ", ".join(schedule_times()),
        "IST",
        flush=True,
    )
    while not STOP_EVENT.is_set():
        try:
            now = now_ist()
            hhmm = now.strftime("%H:%M")

            if not is_paused() and hhmm in schedule_times() and not POST_MUTEX.locked():
                slot = f"post:{now.date().isoformat()}:{hhmm}"
                if claim_slot(slot):
                    print(f"Claimed scheduled slot {slot}", flush=True)
                    start_post_job("schedule", report_chat=TG_REPORT_CHAT or None)

            if DAILY_REPORT_ENABLED and hhmm == validate_time(DEFAULT_DAILY_REPORT_TIME):
                slot = f"report:{now.date().isoformat()}:{hhmm}"
                if claim_slot(slot):
                    if TG_REPORT_CHAT:
                        send_message(TG_REPORT_CHAT, daily_report_text(), dashboard_keyboard())
                    log_event("report", "sent", trigger_source="schedule")

        except Exception as exc:
            print("Scheduler error:", short_error(exc), flush=True)
        STOP_EVENT.wait(10)


# --------------------------- Health server ---------------------------

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in {"/", "/health"}:
            self.send_response(404)
            self.end_headers()
            return
        payload = {
            "ok": True,
            "version": APP_VERSION,
            "paused": is_paused(),
            "post_worker_busy": POST_MUTEX.locked(),
            "schedule_ist": schedule_times(),
            "next_run_ist": fmt_next_run(),
            "database": str(poster.DB_PATH),
            "telegram_enabled": bool(TG_TOKEN),
        }
        raw = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def log_message(self, fmt: str, *args: Any) -> None:
        return


def health_server_loop() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), HealthHandler)
    print(f"Health server listening on 0.0.0.0:{PORT}", flush=True)
    server.serve_forever()


# --------------------------- Startup ---------------------------

def startup_validation() -> str:
    poster.require_config()
    poster.resolve_gdown()
    conn = db()
    conn.close()

    lines = [
        "REELS AUTOMATION ONLINE",
        f"Version: {APP_VERSION}",
        f"Database: {poster.DB_PATH}",
        f"Schedule: {', '.join(schedule_times())} IST",
        f"Next run: {fmt_next_run()}",
        f"Telegram controls: {'ENABLED' if TG_TOKEN and ALLOWED_CHAT_IDS else 'DISABLED'}",
    ]
    try:
        me = poster.verify_instagram_auth()
        lines.append(f"Instagram: @{me.get('username')} ({me.get('id')})")
    except Exception as exc:
        lines.append(f"Instagram auth check: FAIL — {short_error(exc)}")
    return "\n".join(lines)


def main() -> int:
    print(startup_validation(), flush=True)

    threading.Thread(target=health_server_loop, name="health-server", daemon=True).start()
    threading.Thread(target=scheduler_loop, name="scheduler", daemon=True).start()
    threading.Thread(target=telegram_loop, name="telegram", daemon=True).start()

    if STARTUP_REPORT_ENABLED and TG_REPORT_CHAT and TG_TOKEN:
        with contextlib.suppress(Exception):
            send_message(TG_REPORT_CHAT, startup_validation(), dashboard_keyboard())

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        STOP_EVENT.set()
        raise SystemExit(0)
    except BaseException as exc:
        print("Fatal service error:", short_error(exc), file=sys.stderr, flush=True)
        traceback.print_exc()
        raise SystemExit(1)

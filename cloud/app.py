#!/usr/bin/env python3
"""EWNext cloud: ingest gateway telemetry, store history, queue setpoint jobs."""

from __future__ import annotations

import hashlib
import hmac
import html
import json
import math
import os
import queue
import secrets
import smtplib
import sqlite3
import threading
import time
import uuid
from email.message import EmailMessage
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

MAX_INSTRUMENTS_PER_SITE = 15
SUBSCRIPTION_DAYS = 365
SUBSCRIPTION_DUE_DAYS = 30
SETPOINT_REGISTER = 32769  # EWNext SEt, 0.1 C
POWER_REGISTER = 4123  # Ronon / RoFFoFF / Att_Sbr command word
POWER_ON_MASK = 4  # Ronon Device on
POWER_OFF_MASK = 8  # RoFFoFF Device off
DEFROST_MASK = 4096  # Att_Sbr Manual Defrost activation
STANDBY_MASK = 1  # STD-BY in analog RAM 4115
NAME_MAX_LEN = 80
EMAIL_MAX_LEN = 120
PASSWORD_MIN_LEN = 8
PENDING_SETPOINT_TTL_SEC = 10
HASH_ROUNDS = 200_000
HISTORY_RANGES = {
    "1h": 3600,
    "24h": 86400,
    "2d": 172800,
    "7d": 604800,
    "1m": 2592000,
}
HISTORY_MAX_POINTS = 720
TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="Keyview Cloud")
app.add_middleware(
    SessionMiddleware,
    secret_key=os.environ.get("SESSION_SECRET") or secrets.token_hex(32),
    same_site="lax",
    https_only=os.environ.get("RENDER") == "true",
)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _pg() -> str | None:
    url = os.environ.get("DATABASE_URL")
    if not url:
        return None
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://") :]
    return url


def _is_pg() -> bool:
    return _pg() is not None


@contextmanager
def db() -> Iterator[Any]:
    url = _pg()
    if url:
        import psycopg

        conn = psycopg.connect(url)
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()
    else:
        conn = sqlite3.connect(Path(__file__).parent / "ewnext.db")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()


def q(sql: str) -> str:
    if _is_pg():
        return sql.replace("?", "%s")
    return sql


def init_db() -> None:
    serial = "SERIAL PRIMARY KEY" if _is_pg() else "INTEGER PRIMARY KEY AUTOINCREMENT"
    ts = "TIMESTAMPTZ" if _is_pg() else "TEXT"
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS readings (
                id {serial},
                device_id TEXT NOT NULL,
                instrument_id INTEGER NOT NULL,
                server_name TEXT,
                register_address INTEGER,
                value TEXT,
                raw_data TEXT,
                values_json TEXT NOT NULL,
                probe1 REAL,
                probe2 REAL,
                flags INTEGER,
                stale INTEGER NOT NULL DEFAULT 0,
                device_ts INTEGER,
                received_at {ts} NOT NULL,
                created_at {ts},
                setpoint REAL
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS instruments (
                device_id TEXT NOT NULL,
                instrument_id INTEGER NOT NULL,
                server_name TEXT,
                last_probe1 REAL,
                last_probe2 REAL,
                last_setpoint REAL,
                last_flags INTEGER,
                last_stale INTEGER NOT NULL DEFAULT 0,
                last_seen {ts},
                PRIMARY KEY (device_id, instrument_id)
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS jobs (
                id {serial},
                device_id TEXT NOT NULL,
                instrument_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                register INTEGER NOT NULL,
                raw_value INTEGER NOT NULL,
                display_value REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT,
                created_at {ts} NOT NULL,
                completed_at {ts}
            )
            """
        )
        conn.commit()
        _ensure_column(cur, "instruments", "last_flags", "INTEGER")
        _ensure_column(cur, "instruments", "last_stale", "INTEGER DEFAULT 0")
        _ensure_column(cur, "instruments", "label", "TEXT")
        _ensure_column(cur, "instruments", "alarm_high_temp", "REAL")
        _ensure_column(cur, "instruments", "alarm_high_delay_min", "REAL")
        _ensure_column(cur, "instruments", "alarm_reset_temp", "REAL")
        _ensure_column(cur, "instruments", "alarm_notify_delay_min", "REAL")
        _ensure_column(cur, "instruments", "alarm_high_since", ts)
        _ensure_column(cur, "instruments", "alarm_active", "INTEGER DEFAULT 0")
        _ensure_column(cur, "instruments", "alarm_active_since", ts)
        _ensure_column(cur, "instruments", "alarm_email_sent", "INTEGER DEFAULT 0")
        _ensure_column(cur, "instruments", "offline_history_built", "INTEGER DEFAULT 0")
        _ensure_column(cur, "readings", "cloud_alarm", "INTEGER DEFAULT 0")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS alarm_events (
                id {serial},
                device_id TEXT NOT NULL,
                instrument_id INTEGER NOT NULL,
                started_at {ts} NOT NULL,
                cleared_at {ts},
                peak_temp REAL,
                start_temp REAL,
                clear_temp REAL,
                high_temp REAL,
                reset_temp REAL,
                email_sent INTEGER NOT NULL DEFAULT 0,
                email_sent_at {ts}
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS alarm_events_lookup ON alarm_events (device_id, instrument_id, started_at)"
        )
        _ensure_column(cur, "alarm_events", "kind", "TEXT DEFAULT 'high'")
        _ensure_column(cur, "readings", "register_address", "INTEGER")
        _ensure_column(cur, "readings", "value", "TEXT")
        _ensure_column(cur, "readings", "raw_data", "TEXT")
        _ensure_column(cur, "readings", "created_at", ts)
        _ensure_column(cur, "readings", "setpoint", "REAL")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS readings_lookup ON readings (device_id, instrument_id, received_at)"
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS app_users (
                id {serial},
                name TEXT NOT NULL,
                email TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'operator',
                created_at {ts} NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_sites (
                user_id INTEGER NOT NULL,
                device_id TEXT NOT NULL,
                PRIMARY KEY (user_id, device_id)
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS organisations (
                id {serial},
                name TEXT NOT NULL,
                parent_id INTEGER,
                locked INTEGER NOT NULL DEFAULT 0,
                created_at {ts} NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS org_sites (
                organisation_id INTEGER NOT NULL,
                device_id TEXT NOT NULL,
                PRIMARY KEY (organisation_id, device_id)
            )
            """
        )
        _ensure_column(cur, "app_users", "organisation_id", "INTEGER")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS sites (
                device_id TEXT PRIMARY KEY,
                uid TEXT NOT NULL UNIQUE,
                store_name TEXT,
                created_at {ts} NOT NULL
            )
            """
        )
        _ensure_column(cur, "sites", "subscription_start", ts)
        _ensure_column(cur, "sites", "subscription_until", ts)
        _ensure_column(cur, "sites", "subscription_locked", "INTEGER DEFAULT 0")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS org_invites (
                id {serial},
                organisation_id INTEGER NOT NULL,
                email TEXT NOT NULL,
                role TEXT NOT NULL,
                token TEXT NOT NULL UNIQUE,
                created_at {ts} NOT NULL,
                expires_at {ts} NOT NULL,
                accepted_at {ts}
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS password_resets (
                id {serial},
                user_id INTEGER NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                created_at {ts} NOT NULL,
                expires_at {ts} NOT NULL,
                used_at {ts}
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS push_vapid (
                id INTEGER PRIMARY KEY,
                public_key TEXT NOT NULL,
                private_key TEXT NOT NULL
            )
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS push_subscriptions (
                id {serial},
                user_id INTEGER,
                super_admin INTEGER NOT NULL DEFAULT 0,
                endpoint TEXT NOT NULL UNIQUE,
                p256dh TEXT NOT NULL,
                auth TEXT NOT NULL,
                created_at {ts} NOT NULL
            )
            """
        )
        conn.commit()
        _ensure_org_tree(cur)
        _backfill_sites(cur)
        _backfill_subscriptions(cur)
        _backfill_instrument_labels(cur)
        conn.commit()
        _log_store_counts(cur)


def _count_rows(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    row = cur.fetchone()
    if row is None:
        return 0
    if isinstance(row, dict) or hasattr(row, "keys"):
        return int(list(dict(row).values())[0] or 0)
    return int(row[0] or 0)


def _log_store_counts(cur) -> None:
    kind = "postgres" if _is_pg() else "sqlite"
    print(
        "ewnext store="
        + kind
        + f" sites={_count_rows(cur, 'sites')}"
        + f" instruments={_count_rows(cur, 'instruments')}"
        + f" readings={_count_rows(cur, 'readings')}"
        + f" users={_count_rows(cur, 'app_users')}"
    )


def _ensure_column(cur, table: str, column: str, typedef: str) -> None:
    if _is_pg():
        cur.execute(
            q(
                """
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = current_schema()
                  AND table_name = ? AND column_name = ?
                """
            ),
            (table, column),
        )
        if cur.fetchone():
            return
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column} {typedef}")
        return
    cur.execute(f"PRAGMA table_info({table})")
    rows = cur.fetchall()
    names = []
    for row in rows:
        if isinstance(row, dict):
            names.append(row.get("name"))
        elif hasattr(row, "keys"):
            names.append(row["name"])
        else:
            names.append(row[1])
    if column not in names:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {typedef}")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


ORG_ADMIN_ROLES = {"manager", "org_admin", "administrator", "admin"}
SUB_OPERATOR_ROLES = {"operator", "sub_operator"}


def normalize_role(value: Any) -> str:
    role = str(value or "").strip().lower()
    if role in ORG_ADMIN_ROLES:
        return "org_admin"
    return "sub_operator"


def role_label(value: Any) -> str:
    if normalize_role(value) == "org_admin":
        return "Organisation administrator"
    return "Sub-organisation operator"


def _row_get(row: Any, key: str, index: int = 0) -> Any:
    if isinstance(row, dict) or hasattr(row, "keys"):
        try:
            return row[key]
        except Exception:
            return None
    try:
        return row[index]
    except Exception:
        return None


def _ensure_org_tree(cur) -> None:
    cur.execute("SELECT id, name, parent_id FROM organisations ORDER BY id")
    rows = fetchall(cur)
    if not rows:
        cur.execute(
            q("INSERT INTO organisations (name, parent_id, locked, created_at) VALUES (?, ?, 0, ?)"),
            ("Keyview", None, now_iso()),
        )
        root_id = int(cur.lastrowid) if not _is_pg() else None
        if root_id is None:
            cur.execute(q("SELECT id FROM organisations WHERE parent_id IS NULL ORDER BY id LIMIT 1"))
            found = cur.fetchone()
            root_id = int(_row_get(found, "id", 0))
        cur.execute(
            q("INSERT INTO organisations (name, parent_id, locked, created_at) VALUES (?, ?, 0, ?)"),
            ("Main", root_id, now_iso()),
        )
        main_id = int(cur.lastrowid) if not _is_pg() else None
        if main_id is None:
            cur.execute(q("SELECT id FROM organisations WHERE parent_id=? ORDER BY id LIMIT 1"), (root_id,))
            found = cur.fetchone()
            main_id = int(_row_get(found, "id", 0))
        cur.execute("SELECT DISTINCT device_id FROM instruments")
        for row in fetchall(cur):
            device_id = str(_row_get(row, "device_id", 0) or "").strip()
            if device_id:
                cur.execute(
                    q("INSERT INTO org_sites (organisation_id, device_id) VALUES (?, ?)"),
                    (main_id, device_id),
                )
        return
    cur.execute("SELECT organisation_id, device_id FROM org_sites")
    linked = {str(_row_get(row, "device_id", 1)) for row in fetchall(cur)}
    cur.execute("SELECT id FROM organisations WHERE parent_id IS NOT NULL ORDER BY id LIMIT 1")
    child = cur.fetchone()
    if child is None:
        cur.execute("SELECT id FROM organisations WHERE parent_id IS NULL ORDER BY id LIMIT 1")
        root = cur.fetchone()
        if root is None:
            return
        root_id = int(_row_get(root, "id", 0))
        cur.execute(
            q("INSERT INTO organisations (name, parent_id, locked, created_at) VALUES (?, ?, 0, ?)"),
            ("Main", root_id, now_iso()),
        )
        child_id = int(cur.lastrowid) if not _is_pg() else None
        if child_id is None:
            cur.execute(q("SELECT id FROM organisations WHERE parent_id=? ORDER BY id LIMIT 1"), (root_id,))
            found = cur.fetchone()
            child_id = int(_row_get(found, "id", 0))
    else:
        child_id = int(_row_get(child, "id", 0))
    cur.execute("SELECT DISTINCT device_id FROM instruments")
    for row in fetchall(cur):
        device_id = str(_row_get(row, "device_id", 0) or "").strip()
        if device_id and device_id not in linked:
            cur.execute(
                q("INSERT INTO org_sites (organisation_id, device_id) VALUES (?, ?)"),
                (child_id, device_id),
            )
            linked.add(device_id)


def normalize_device_id(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "unknown"
    return raw[:64]


def new_site_uid() -> str:
    return "KV-" + uuid.uuid4().hex[:16].upper()


def site_display_name(store_name: Any, device_id: str) -> str:
    name = str(store_name or "").strip()
    return name if name else device_id


def subscription_period_end(start: datetime) -> datetime:
    return start + timedelta(days=SUBSCRIPTION_DAYS)


def ensure_site_record(cur, device_id: str) -> str:
    device_id = normalize_device_id(device_id)
    cur.execute(q("SELECT uid FROM sites WHERE device_id=?"), (device_id,))
    row = cur.fetchone()
    existing = _row_get(row, "uid", 0) if row is not None else None
    if existing:
        return str(existing)
    start = now_iso()
    until = subscription_period_end(datetime.now(timezone.utc)).isoformat()
    for _ in range(8):
        uid = new_site_uid()
        try:
            cur.execute(
                q(
                    """
                    INSERT INTO sites (
                        device_id, uid, store_name, created_at,
                        subscription_start, subscription_until, subscription_locked
                    ) VALUES (?, ?, ?, ?, ?, ?, 0)
                    """
                ),
                (device_id, uid, "", start, start, until),
            )
            return uid
        except Exception:
            cur.execute(q("SELECT uid FROM sites WHERE device_id=?"), (device_id,))
            row = cur.fetchone()
            existing = _row_get(row, "uid", 0) if row is not None else None
            if existing:
                return str(existing)
    uid = "KV-" + uuid.uuid4().hex.upper()
    cur.execute(
        q(
            """
            INSERT INTO sites (
                device_id, uid, store_name, created_at,
                subscription_start, subscription_until, subscription_locked
            ) VALUES (?, ?, ?, ?, ?, ?, 0)
            """
        ),
        (device_id, uid, "", start, start, until),
    )
    return uid


def _backfill_sites(cur) -> None:
    ids: set[str] = set()
    cur.execute("SELECT DISTINCT device_id FROM instruments")
    for row in fetchall(cur):
        device_id = str(_row_get(row, "device_id", 0) or "").strip()
        if device_id:
            ids.add(device_id)
    cur.execute("SELECT device_id FROM org_sites")
    for row in fetchall(cur):
        device_id = str(_row_get(row, "device_id", 0) or "").strip()
        if device_id:
            ids.add(device_id)
    for device_id in sorted(ids):
        ensure_site_record(cur, device_id)


def decorate_subscription(row: dict, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    start = parse_ts(row.get("subscription_start")) or parse_ts(row.get("created_at")) or now
    until = parse_ts(row.get("subscription_until")) or subscription_period_end(start)
    expired = now > until
    locked = bool(int(row.get("subscription_locked") or 0)) or expired
    days_left = int((until - now).total_seconds() // 86400)
    if locked:
        status = "locked"
        status_label = "Locked"
    elif days_left <= SUBSCRIPTION_DUE_DAYS:
        status = "due"
        status_label = f"Renewal due in {max(days_left, 0)} day{'s' if days_left != 1 else ''}"
    else:
        status = "active"
        status_label = "Active"
    row.update(
        {
            "subscription_start": start.isoformat(),
            "subscription_until": until.isoformat(),
            "subscription_locked": locked,
            "subscription_expired": expired,
            "subscription_status": status,
            "subscription_label": status_label,
            "subscription_days_left": days_left,
            "onboarded_label": format_when(start),
            "renewal_label": format_when(until),
        }
    )
    return row


def _backfill_subscriptions(cur) -> None:
    now = datetime.now(timezone.utc)
    cur.execute(
        "SELECT device_id, created_at, subscription_start, subscription_until, subscription_locked FROM sites"
    )
    for row in fetchall(cur):
        start = parse_ts(row.get("subscription_start"))
        until = parse_ts(row.get("subscription_until"))
        if start and until:
            continue
        start = start or parse_ts(row.get("created_at")) or now
        until = until or subscription_period_end(start)
        locked = 1 if now > until or int(row.get("subscription_locked") or 0) else 0
        cur.execute(
            q(
                """
                UPDATE sites
                SET subscription_start=COALESCE(subscription_start, ?),
                    subscription_until=COALESCE(subscription_until, ?),
                    subscription_locked=?
                WHERE device_id=?
                """
            ),
            (start.isoformat(), until.isoformat(), locked, row.get("device_id")),
        )
        if locked:
            cur.execute(
                q(
                    """
                    UPDATE jobs SET status='cancelled', error=?, completed_at=?
                    WHERE device_id=? AND status='pending'
                    """
                ),
                ("subscription locked", now_iso(), row.get("device_id")),
            )


def refresh_site_subscriptions() -> None:
    with db() as conn:
        cur = conn.cursor()
        _backfill_subscriptions(cur)


def load_site_records(device_ids: list[str] | None = None) -> dict[str, dict]:
    refresh_site_subscriptions()
    with db() as conn:
        cur = conn.cursor()
        for device_id in device_ids or []:
            if str(device_id or "").strip():
                ensure_site_record(cur, str(device_id))
        cur.execute(
            """
            SELECT device_id, uid, store_name, created_at,
                   subscription_start, subscription_until, subscription_locked
            FROM sites
            """
        )
        rows = fetchall(cur)
    now = datetime.now(timezone.utc)
    out: dict[str, dict] = {}
    for row in rows:
        device_id = str(row.get("device_id") or "").strip()
        if not device_id:
            continue
        store_name = str(row.get("store_name") or "").strip()
        rec = {
            "uid": str(row.get("uid") or ""),
            "store_name": store_name,
            "display_name": site_display_name(store_name, device_id),
            "created_at": row.get("created_at"),
            "subscription_start": row.get("subscription_start"),
            "subscription_until": row.get("subscription_until"),
            "subscription_locked": row.get("subscription_locked"),
        }
        out[device_id] = decorate_subscription(rec, now)
    return out


def apply_subscription_lock(cur, device_id: str, now: datetime | None = None) -> bool:
    device_id = normalize_device_id(device_id)
    now = now or datetime.now(timezone.utc)
    cur.execute(
        q("SELECT subscription_until, subscription_locked FROM sites WHERE device_id=?"),
        (device_id,),
    )
    row = cur.fetchone()
    if row is None:
        return False
    until = parse_ts(_row_get(row, "subscription_until", 0))
    locked = bool(int(_row_get(row, "subscription_locked", 1) or 0))
    expired = bool(until and now > until)
    if expired and not locked:
        cur.execute(q("UPDATE sites SET subscription_locked=1 WHERE device_id=?"), (device_id,))
        cur.execute(
            q(
                """
                UPDATE jobs SET status='cancelled', error=?, completed_at=?
                WHERE device_id=? AND status='pending'
                """
            ),
            ("subscription locked", now_iso(), device_id),
        )
        locked = True
    return locked or expired


def site_is_subscription_locked(device_id: str) -> bool:
    with db() as conn:
        return apply_subscription_lock(conn.cursor(), device_id)


def renew_device_subscription(device_id: str) -> dict:
    device_id = normalize_device_id(device_id)
    now = datetime.now(timezone.utc)
    with db() as conn:
        cur = conn.cursor()
        ensure_site_record(cur, device_id)
        cur.execute(q("SELECT subscription_until FROM sites WHERE device_id=?"), (device_id,))
        row = cur.fetchone()
        current_until = parse_ts(_row_get(row, "subscription_until", 0)) if row is not None else None
        base = current_until if current_until and current_until > now else now
        until = subscription_period_end(base)
        cur.execute(
            q("UPDATE sites SET subscription_until=?, subscription_locked=0 WHERE device_id=?"),
            (until.isoformat(), device_id),
        )
    recs = load_site_records([device_id])
    return recs.get(device_id) or {}


def row_count(row: Any) -> int:
    if row is None:
        return 0
    if isinstance(row, dict):
        val = next(iter(row.values()))
        return int(val or 0)
    if hasattr(row, "keys"):
        return int(row[0] or 0)
    return int(row[0] or 0)


def site_accepts_instrument(cur, device_id: str, instrument_id: int) -> bool:
    cur.execute(
        q("SELECT 1 FROM instruments WHERE device_id=? AND instrument_id=?"),
        (device_id, instrument_id),
    )
    if cur.fetchone():
        return True
    cur.execute(q("SELECT COUNT(*) FROM instruments WHERE device_id=?"), (device_id,))
    return row_count(cur.fetchone()) < MAX_INSTRUMENTS_PER_SITE


def require_router_key(request: Request) -> None:
    """Optional ingest auth. Data to Server may send only Content-Type.

    If ROUTER_API_KEY is set and the caller sends X-API-Key / Bearer, the
    values must match. Headerless posts from Teltonika Data to Server are allowed.
    """
    expected = os.environ.get("ROUTER_API_KEY") or ""
    got = request.headers.get("X-API-Key") or ""
    auth = request.headers.get("Authorization") or ""
    if auth.lower().startswith("bearer "):
        got = got or auth.split(" ", 1)[1]
    if not got:
        return
    if expected and not secrets.compare_digest(got, expected):
        raise HTTPException(401, "unauthorized")


def hash_password(password: str) -> str:
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), HASH_ROUNDS)
    return f"pbkdf2$sha256${HASH_ROUNDS}${salt}${dk.hex()}"


def verify_password(password: str, stored: str) -> bool:
    try:
        _algo, hash_name, rounds, salt, digest = str(stored).split("$", 4)
        dk = hashlib.pbkdf2_hmac(
            hash_name, password.encode("utf-8"), salt.encode("utf-8"), int(rounds)
        )
        return hmac.compare_digest(dk.hex(), digest)
    except Exception:
        return False


def normalize_email(value: Any) -> str:
    return str(value or "").strip().lower()[:EMAIL_MAX_LEN]


def form_id_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    out: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def logged_in(request: Request) -> bool:
    return bool(request.session.get("admin") or request.session.get("uid") is not None)


def is_manager(request: Request) -> bool:
    if is_super_admin(request):
        return True
    if not request.session.get("admin"):
        return False
    return normalize_role(request.session.get("role")) == "org_admin"


def is_org_lead(request: Request) -> bool:
    return normalize_role(request.session.get("role")) == "org_admin" and not is_manager(request)


def is_super_admin(request: Request | None) -> bool:
    if request is None:
        return False
    if request.session.get("super_admin"):
        return True
    return bool(request.session.get("admin")) and request.session.get("uid") is None


def session_name(request: Request) -> str:
    return str(request.session.get("name") or "Admin")


def org_sites_for(organisation_id: int | None, include_children: bool = False) -> list[str]:
    if not organisation_id:
        return []
    ids = [int(organisation_id)]
    if include_children:
        with db() as conn:
            cur = conn.cursor()
            cur.execute(q("SELECT id FROM organisations WHERE parent_id=?"), (organisation_id,))
            ids.extend(int(_row_get(row, "id", 0)) for row in fetchall(cur))
    with db() as conn:
        cur = conn.cursor()
        placeholders = ",".join("?" * len(ids))
        sql = f"SELECT device_id FROM org_sites WHERE organisation_id IN ({placeholders}) ORDER BY device_id"
        cur.execute(q(sql) if _is_pg() else sql, tuple(ids))
        out: list[str] = []
        for row in fetchall(cur):
            value = str(_row_get(row, "device_id", 0) or "").strip()
            if value and value not in out:
                out.append(value)
        return out


def allowed_site_ids(request: Request) -> set[str] | None:
    if is_manager(request):
        return None
    return {str(s) for s in (request.session.get("sites") or [])}


def can_access_site(request: Request, device_id: str) -> bool:
    allowed = allowed_site_ids(request)
    if allowed is None:
        return True
    return str(device_id) in allowed


def require_admin(request: Request) -> None:
    if not logged_in(request):
        raise HTTPException(401, "login required")


def require_manager(request: Request) -> None:
    require_admin(request)
    if not is_manager(request):
        raise HTTPException(403, "manager only")


def require_site_access(request: Request, device_id: str) -> None:
    require_admin(request)
    if not can_access_site(request, device_id):
        raise HTTPException(403, "no access to this site")


def require_super_admin(request: Request) -> None:
    require_admin(request)
    if not is_super_admin(request):
        raise HTTPException(403, "super admin only")


def require_device_writable(request: Request, device_id: str) -> None:
    require_site_access(request, device_id)
    if site_is_subscription_locked(device_id):
        raise HTTPException(403, "device locked: subscription expired")


def require_subscription_view(request: Request, device_id: str) -> None:
    require_site_access(request, device_id)
    if site_is_subscription_locked(device_id) and not is_super_admin(request):
        raise HTTPException(403, "device locked: subscription expired")


BACK_LINKS = {
    "site": ("/sites", "Sites"),
    "instrument": ("/sites", "Sites"),
    "alarm-history": ("/sites", "Sites"),
    "organisations": ("/", "Dashboard"),
    "users": ("/", "Dashboard"),
    "raw-data": ("/", "Dashboard"),
    "device-admin": ("/", "Dashboard"),
}


def attach_session(ctx: dict, request: Request, page: str) -> dict:
    back = BACK_LINKS.get(page)
    ctx.update(
        {
            "request": request,
            "page": page,
            "admin_name": session_name(request),
            "is_manager": is_manager(request),
            "is_org_lead": is_org_lead(request),
            "is_super_admin": is_super_admin(request),
            "back_href": back[0] if back else None,
            "back_label": back[1] if back else "Back",
        }
    )
    return ctx


def load_user_sites(user_id: int) -> list[str]:
    with db() as conn:
        cur = conn.cursor()
        cur.execute(q("SELECT device_id FROM user_sites WHERE user_id=? ORDER BY device_id"), (user_id,))
        return [str(row.get("device_id") if isinstance(row, dict) else row[0]) for row in fetchall(cur)]


def org_is_locked(organisation_id: int | None) -> bool:
    if not organisation_id:
        return False
    with db() as conn:
        cur = conn.cursor()
        cur.execute(q("SELECT locked FROM organisations WHERE id=?"), (organisation_id,))
        row = cur.fetchone()
    if not row:
        return False
    return bool(int(_row_get(row, "locked", 0) or 0))


def is_root_organisation(org_id: int | None) -> bool:
    if not org_id:
        return False
    with db() as conn:
        cur = conn.cursor()
        cur.execute(q("SELECT parent_id FROM organisations WHERE id=?"), (org_id,))
        row = cur.fetchone()
    if not row:
        return False
    return _row_get(row, "parent_id", 0) is None


def user_portfolio(user: dict) -> list[str] | None:
    role = normalize_role(user.get("role"))
    org_id = as_int(user.get("organisation_id"))
    if role == "org_admin" and (org_id is None or is_root_organisation(org_id)):
        return None
    if org_is_locked(org_id):
        return []
    if role == "org_admin":
        return org_sites_for(org_id, include_children=False)
    portfolio = org_sites_for(org_id, include_children=False)
    assigned = load_user_sites(int(user["id"])) if user.get("id") is not None else []
    if assigned:
        allowed = set(portfolio)
        return [site for site in assigned if site in allowed]
    return portfolio


def start_user_session(request: Request, user: dict) -> None:
    role = normalize_role(user.get("role"))
    request.session.clear()
    request.session["uid"] = int(user["id"])
    request.session["name"] = user["name"]
    request.session["role"] = role
    org_id = as_int(user.get("organisation_id"))
    request.session["admin"] = role == "org_admin" and (org_id is None or is_root_organisation(org_id))
    request.session["org_id"] = org_id
    portfolio = user_portfolio(user)
    request.session["sites"] = [] if portfolio is None else portfolio


def start_admin_session(request: Request) -> None:
    request.session.clear()
    request.session["admin"] = True
    request.session["super_admin"] = True
    request.session["role"] = "org_admin"
    request.session["name"] = "Admin"
    request.session["uid"] = None
    request.session["org_id"] = None
    request.session["sites"] = []


def find_user_by_email(email: str) -> dict | None:
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q("SELECT id, name, email, password_hash, role, organisation_id FROM app_users WHERE email=?"),
            (email,),
        )
        row = cur.fetchone()
    if not row:
        return None
    if isinstance(row, dict) or hasattr(row, "keys"):
        return dict(row)
    return {
        "id": row[0],
        "name": row[1],
        "email": row[2],
        "password_hash": row[3],
        "role": row[4],
        "organisation_id": row[5] if len(row) > 5 else None,
    }


def as_int(value: Any) -> int | None:
    try:
        if value is None or value is False:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def as_float(value: Any) -> float | None:
    if value is None or value is False:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def format_alarm_input(value: Any) -> str:
    n = as_float(value)
    if n is None:
        return ""
    if abs(n - round(n)) < 1e-9:
        return str(int(round(n)))
    return f"{n:.1f}"


INSTRUMENT_SELECT = """
            SELECT device_id, instrument_id, server_name, label, last_probe1, last_probe2,
                   last_setpoint, last_flags, last_stale, last_seen,
                   alarm_high_temp, alarm_high_delay_min, alarm_reset_temp, alarm_notify_delay_min,
                   alarm_high_since, alarm_active, alarm_active_since, alarm_email_sent,
                   offline_history_built
            FROM instruments
"""


def smtp_configured() -> bool:
    return bool((os.environ.get("SMTP_HOST") or "").strip() and (os.environ.get("SMTP_FROM") or os.environ.get("SMTP_USER") or "").strip())


def alarm_email_recipients(device_id: str) -> list[str]:
    device_id = str(device_id or "").strip()
    out: list[str] = []
    extra = normalize_email(os.environ.get("ALARM_NOTIFY_EMAIL") or os.environ.get("ADMIN_EMAIL") or "")
    if extra:
        out.append(extra)
    for user in load_app_users():
        mail = normalize_email(user.get("email"))
        if not mail:
            continue
        portfolio = user_portfolio(user)
        if portfolio is None or device_id in portfolio:
            if mail not in out:
                out.append(mail)
    return out


_mail_queue: queue.Queue = queue.Queue()
_mail_lock = threading.Lock()
_mail_inflight: set[str] = set()
_mail_retry_after: dict[str, float] = {}
_mail_started = False


def _smtp_once(host: str, port: int, starttls: bool, msg: EmailMessage, user: str, password: str) -> None:
    if port == 465:
        with smtplib.SMTP_SSL(host, port, timeout=8) as smtp:
            if user:
                smtp.login(user, password)
            smtp.send_message(msg)
        return
    with smtplib.SMTP(host, port, timeout=8) as smtp:
        if starttls:
            smtp.starttls()
        if user:
            smtp.login(user, password)
        smtp.send_message(msg)


def _deliver_email(recipients: list[str], subject: str, body: str, html_body: str | None = None) -> bool:
    if not smtp_configured() or not recipients:
        return False
    host = (os.environ.get("SMTP_HOST") or "").strip()
    port = as_int(os.environ.get("SMTP_PORT")) or 587
    user = os.environ.get("SMTP_USER") or ""
    password = os.environ.get("SMTP_PASSWORD") or ""
    from_addr = (os.environ.get("SMTP_FROM") or user or "").strip()
    starttls = (os.environ.get("SMTP_STARTTLS") or "1").strip().lower() not in ("0", "false", "no")
    if not from_addr:
        return False
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = from_addr
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)
    if html_body:
        msg.add_alternative(html_body, subtype="html")
    attempts = [(host, port, starttls and port != 465)]
    if port != 587:
        attempts.append((host, 587, True))
    last_error = "error"
    for attempt_host, attempt_port, attempt_tls in attempts:
        try:
            _smtp_once(attempt_host, attempt_port, attempt_tls, msg, user, password)
            print(f"email ok {attempt_port}", flush=True)
            return True
        except Exception as exc:
            last_error = type(exc).__name__
    print(f"email fail {last_error}", flush=True)
    return False


def _ensure_mailer() -> None:
    global _mail_started
    with _mail_lock:
        if _mail_started:
            return
        _mail_started = True
    threading.Thread(target=_mail_loop, name="keyview-mail", daemon=True).start()


def _mail_loop() -> None:
    while True:
        job = _mail_queue.get()
        key = str(job.get("key") or "")
        ok = False
        try:
            ok = _run_mail_job(job)
        except Exception as exc:
            print(f"email fail {type(exc).__name__}", flush=True)
        finally:
            with _mail_lock:
                _mail_inflight.discard(key)
                if not ok and key:
                    _mail_retry_after[key] = time.monotonic() + 90
            _mail_queue.task_done()


def _queue_alarm_mail(job: dict) -> None:
    key = str(job.get("key") or "")
    if not key:
        return
    with _mail_lock:
        if key in _mail_inflight or _mail_retry_after.get(key, 0) > time.monotonic():
            return
        _mail_inflight.add(key)
    _ensure_mailer()
    _mail_queue.put(job)


def flush_alarm_mail(jobs: list[dict]) -> None:
    for job in jobs:
        _queue_alarm_mail(job)


def _mark_high_emailed(device_id: str, instrument_id: Any) -> None:
    sent_at = datetime.now(timezone.utc).isoformat()
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q(
                """
                UPDATE instruments SET alarm_email_sent=1
                WHERE device_id=? AND instrument_id=? AND alarm_active=1
                """
            ),
            (device_id, instrument_id),
        )
        cur.execute(
            q(
                """
                UPDATE alarm_events SET email_sent=1, email_sent_at=?
                WHERE device_id=? AND instrument_id=? AND cleared_at IS NULL
                  AND COALESCE(kind, 'high')='high' AND email_sent=0
                """
            ),
            (sent_at, device_id, instrument_id),
        )


def _high_alarm_still_open(device_id: str, instrument_id: Any) -> bool:
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q(
                """
                SELECT alarm_active, alarm_email_sent
                FROM instruments WHERE device_id=? AND instrument_id=?
                """
            ),
            (device_id, instrument_id),
        )
        row = _sql_row(cur.fetchone(), cur)
    if not row:
        return False
    return bool(int(row.get("alarm_active") or 0)) and not bool(int(row.get("alarm_email_sent") or 0))


def _run_mail_job(job: dict) -> bool:
    inst = job.get("inst") or {}
    kind = job.get("kind")
    if kind == "high":
        if not _high_alarm_still_open(str(job.get("device_id") or ""), job.get("instrument_id")):
            return True
        if send_alarm_email(inst, job.get("temp"), job.get("high"), job.get("started_at"), job.get("now")):
            _mark_high_emailed(str(job.get("device_id") or ""), job.get("instrument_id"))
            return True
        return False
    return send_alarm_resolved_email(inst, job.get("temp"), job.get("high"), job.get("started_at"), job.get("now"))


def _alarm_mail_job(inst: dict, kind: str, temp: float | None, high: float | None, started_at: datetime | None, now: datetime) -> dict:
    device_id = str(inst.get("device_id") or "")
    instrument_id = inst.get("instrument_id")
    started_key = started_at.isoformat() if started_at else ""
    snapshot = {
        "device_id": device_id,
        "instrument_id": instrument_id,
        "label": inst.get("label"),
        "server_name": inst.get("server_name"),
        "site_name": inst.get("site_name"),
        "last_setpoint": inst.get("last_setpoint"),
    }
    return {
        "key": f"{kind}:{device_id}:{instrument_id}:{started_key}",
        "kind": kind,
        "device_id": device_id,
        "instrument_id": instrument_id,
        "inst": snapshot,
        "temp": temp,
        "high": high,
        "started_at": started_at,
        "now": now,
    }


def _alarm_site_name(inst: dict) -> str:
    named = str(inst.get("site_name") or "").strip()
    if named:
        return named
    device_id = str(inst.get("device_id") or "").strip()
    if not device_id:
        return ""
    with db() as conn:
        cur = conn.cursor()
        cur.execute(q("SELECT store_name FROM sites WHERE device_id=?"), (device_id,))
        row = cur.fetchone()
    if not row:
        return device_id
    store = row["store_name"] if isinstance(row, dict) or hasattr(row, "keys") else row[0]
    return str(store or "").strip() or device_id


def _temp_1dp(value: Any) -> str:
    reading = as_float(value)
    if reading is None:
        return "—"
    return f"{reading:.1f}°C"


def _signed_temp(value: float | None) -> str:
    if value is None:
        return "—"
    if value > 0:
        return f"+{value:.1f}°C"
    return f"{value:.1f}°C"


def _alarm_clock(when: datetime | None, now: datetime) -> str:
    if when is None:
        return "—"
    local = when.astimezone(SA_TZ)
    if local.date() == now.astimezone(SA_TZ).date():
        return local.strftime("%H:%M")
    return local.strftime("%d %b %H:%M")


def _alarm_duration_label(start: datetime | None, end: datetime | None) -> str:
    if start is None or end is None:
        return "—"
    mins = max(0, int((end - start).total_seconds()) // 60)
    if mins < 1:
        return "under 1 minute"
    if mins == 1:
        return "1 minute"
    if mins < 60:
        return f"{mins} minutes"
    hours, rem = divmod(mins, 60)
    if hours < 48:
        hour_label = "1 hour" if hours == 1 else f"{hours} hours"
        if not rem:
            return hour_label
        minute_label = "1 minute" if rem == 1 else f"{rem} minutes"
        return f"{hour_label} {minute_label}"
    days, hours = divmod(hours, 24)
    day_label = "1 day" if days == 1 else f"{days} days"
    if not hours:
        return day_label
    hour_label = "1 hour" if hours == 1 else f"{hours} hours"
    return f"{day_label} {hour_label}"


def _temperature_alarm_notice(
    inst: dict,
    temp: float | None,
    high: float | None,
    started_at: datetime | None,
    now: datetime,
    resolved: bool,
) -> tuple[str, str, str]:
    name = display_name(inst.get("label"), inst.get("server_name"), inst.get("instrument_id") or 0)
    site = _alarm_site_name(inst)
    setpoint = as_float(inst.get("last_setpoint"))
    current = as_float(temp)
    deviation = None if current is None or setpoint is None else current - setpoint
    rows = [
        ("Site", site or "—"),
        ("Equipment", name),
        ("Current temperature", _temp_1dp(current)),
        ("Setpoint", _temp_1dp(setpoint)),
        ("Alarm threshold", _temp_1dp(high)),
        ("Temperature deviation", _signed_temp(deviation)),
        ("Alarm triggered", _alarm_clock(started_at, now)),
        ("Alarm duration", _alarm_duration_label(started_at, now)),
        ("Status", "Resolved" if resolved else "Active – Unacknowledged"),
    ]
    if resolved:
        subject = f"Alarm resolved: {name}"
        kicker = "HIGH TEMPERATURE ALERT"
        badge = "RESOLVED"
        badge_bg = "#e8f6ee"
        badge_fg = "#157a3b"
    else:
        subject = f"High temperature alert: {name}"
        kicker = "HIGH TEMPERATURE ALERT"
        badge = "CRITICAL"
        badge_bg = "#fde8ea"
        badge_fg = "#d1263f"
    text_lines = [kicker, name, badge, ""]
    text_lines.extend(f"{label}: {value}" for label, value in rows)
    body = "\n".join(text_lines)
    safe_rows = "".join(
        "<tr>"
        f"<td style=\"padding:12px 16px;border-bottom:1px solid #eceff3;color:#1f2937;\">{html.escape(label)}</td>"
        f"<td style=\"padding:12px 16px;border-bottom:1px solid #eceff3;color:#111827;text-align:right;\">{html.escape(value)}</td>"
        "</tr>"
        for label, value in rows
    )
    html_body = f"""<!DOCTYPE html>
<html>
<body style="margin:0;padding:24px;background:#f4f6f8;font-family:Arial,Helvetica,sans-serif;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="max-width:640px;margin:0 auto;background:#ffffff;border:1px solid #e6e9ee;border-collapse:collapse;">
    <tr>
      <td style="padding:22px 22px 8px 22px;">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
          <tr>
            <td style="font-size:12px;letter-spacing:0.08em;color:#4b5563;font-weight:700;">{kicker}</td>
            <td align="right">
              <span style="display:inline-block;background:{badge_bg};color:{badge_fg};font-size:12px;font-weight:700;letter-spacing:0.04em;padding:4px 10px;border-radius:999px;">{badge}</span>
            </td>
          </tr>
        </table>
      </td>
    </tr>
    <tr>
      <td style="padding:0 22px 18px 22px;font-size:28px;line-height:1.2;font-weight:700;color:#111827;">{html.escape(name)}</td>
    </tr>
    <tr>
      <td style="padding:0 22px 22px 22px;">
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="border-collapse:collapse;">
          <tr>
            <td style="padding:10px 16px;border-bottom:1px solid #e6e9ee;font-weight:700;color:#111827;">Parameter</td>
            <td style="padding:10px 16px;border-bottom:1px solid #e6e9ee;font-weight:700;color:#111827;text-align:right;">Value</td>
          </tr>
          {safe_rows}
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""
    return subject, body, html_body


def send_alarm_email(inst: dict, temp: float, high: float, started_at: datetime | None = None, now: datetime | None = None) -> bool:
    if not smtp_configured():
        return False
    now = now or datetime.now(timezone.utc)
    recipients = alarm_email_recipients(str(inst.get("device_id") or ""))
    subject, body, html_body = _temperature_alarm_notice(inst, temp, high, started_at, now, resolved=False)
    return _deliver_email(recipients, subject, body, html_body)


def send_alarm_resolved_email(inst: dict, temp: float | None, high: float | None, started_at: datetime | None, now: datetime | None = None) -> bool:
    if not smtp_configured():
        return False
    now = now or datetime.now(timezone.utc)
    recipients = alarm_email_recipients(str(inst.get("device_id") or ""))
    subject, body, html_body = _temperature_alarm_notice(inst, temp, high, started_at, now, resolved=True)
    return _deliver_email(recipients, subject, body, html_body)


def _sql_row(row: Any, cur=None) -> dict | None:
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    if hasattr(row, "keys"):
        return dict(row)
    if cur is not None and getattr(cur, "description", None):
        names = [c[0] if isinstance(c, tuple) else getattr(c, "name", c) for c in cur.description]
        return dict(zip(names, row))
    return None


def open_alarm_event(cur, device_id: Any, instrument_id: Any, kind: str = "high") -> dict | None:
    cur.execute(
        q(
            """
            SELECT id, peak_temp, email_sent, kind
            FROM alarm_events
            WHERE device_id=? AND instrument_id=? AND cleared_at IS NULL
              AND COALESCE(kind, 'high')=?
            ORDER BY started_at DESC
            LIMIT 1
            """
        ),
        (device_id, instrument_id, kind),
    )
    return _sql_row(cur.fetchone(), cur)


def record_alarm_history(
    cur,
    inst: dict,
    now: datetime,
    next_active: bool,
    next_emailed: bool,
    temp: float | None,
    high: float | None,
    reset: float | None,
    started_at: datetime | None,
) -> None:
    device_id = inst.get("device_id")
    instrument_id = inst.get("instrument_id")
    if device_id is None or instrument_id is None:
        return
    open_row = open_alarm_event(cur, device_id, instrument_id)
    if next_active:
        if open_row is None:
            start = started_at or now
            cur.execute(
                q(
                    """
                    INSERT INTO alarm_events (
                        device_id, instrument_id, started_at, peak_temp, start_temp,
                        high_temp, reset_temp, email_sent, email_sent_at, kind
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'high')
                    """
                ),
                (
                    device_id,
                    instrument_id,
                    start.isoformat(),
                    temp,
                    temp,
                    high,
                    reset,
                    1 if next_emailed else 0,
                    now.isoformat() if next_emailed else None,
                ),
            )
            return
        peak = as_float(open_row.get("peak_temp"))
        if temp is not None and (peak is None or temp > peak):
            peak = temp
        emailed = 1 if next_emailed or int(open_row.get("email_sent") or 0) else 0
        if next_emailed and not int(open_row.get("email_sent") or 0):
            cur.execute(
                q(
                    """
                    UPDATE alarm_events
                    SET peak_temp=?, high_temp=?, reset_temp=?, email_sent=1, email_sent_at=?
                    WHERE id=?
                    """
                ),
                (peak, high, reset, now.isoformat(), open_row["id"]),
            )
        else:
            cur.execute(
                q(
                    """
                    UPDATE alarm_events
                    SET peak_temp=?, high_temp=?, reset_temp=?, email_sent=?
                    WHERE id=?
                    """
                ),
                (peak, high, reset, emailed, open_row["id"]),
            )
        return
    if open_row is not None:
        cur.execute(
            q("UPDATE alarm_events SET cleared_at=?, clear_temp=? WHERE id=?"),
            (now.isoformat(), temp, open_row["id"]),
        )


OFFLINE_AFTER_SEC = 180


def offline_started_at(inst: dict, now: datetime) -> datetime:
    seen = parse_ts(inst.get("last_seen"))
    if seen is None:
        return now
    start = seen + timedelta(seconds=OFFLINE_AFTER_SEC)
    if start > now:
        return now
    return start


def build_offline_history(cur, inst: dict, now: datetime) -> None:
    if int(inst.get("offline_history_built") or 0):
        return
    device_id = inst.get("device_id")
    instrument_id = inst.get("instrument_id")
    if device_id is None or instrument_id is None:
        return
    cur.execute(
        q(
            """
            SELECT COALESCE(created_at, received_at) AS ts
            FROM readings
            WHERE device_id=? AND instrument_id=?
            ORDER BY received_at ASC
            """
        ),
        (device_id, instrument_id),
    )
    previous = None
    gaps: list[tuple[datetime, datetime]] = []
    for row in fetchall(cur):
        stamp = parse_ts(row.get("ts"))
        if stamp is None:
            continue
        if previous is not None and (stamp - previous).total_seconds() > OFFLINE_AFTER_SEC:
            start = previous + timedelta(seconds=OFFLINE_AFTER_SEC)
            if stamp > start:
                gaps.append((start, stamp))
        previous = stamp
    if len(gaps) > 200:
        gaps = gaps[-200:]
    for start, end in gaps:
        cur.execute(
            q(
                """
                INSERT INTO alarm_events (
                    device_id, instrument_id, started_at, cleared_at, email_sent, kind
                ) VALUES (?, ?, ?, ?, 0, 'offline')
                """
            ),
            (device_id, instrument_id, start.isoformat(), end.isoformat()),
        )
    cur.execute(
        q("UPDATE instruments SET offline_history_built=1 WHERE device_id=? AND instrument_id=?"),
        (device_id, instrument_id),
    )
    inst["offline_history_built"] = 1


def _push_site_label(cur, device_id: str) -> str:
    cur.execute(q("SELECT store_name FROM sites WHERE device_id=?"), (device_id,))
    row = cur.fetchone()
    name = str(_row_get(row, "store_name", 0) or "").strip() if row else ""
    return name or str(device_id or "")


def _other_instruments_online(cur, device_id: str, instrument_id: Any, now: datetime) -> int:
    cur.execute(
        q("SELECT instrument_id, last_seen, last_stale FROM instruments WHERE device_id=?"),
        (device_id,),
    )
    count = 0
    current = int(instrument_id or 0)
    for row in fetchall(cur):
        if int(row.get("instrument_id") or 0) == current:
            continue
        if instrument_online(row, now):
            count += 1
    return count


def _other_open_offline(cur, device_id: str, instrument_id: Any) -> int:
    cur.execute(
        q(
            """
            SELECT COUNT(*) AS n FROM alarm_events
            WHERE device_id=? AND instrument_id<>? AND cleared_at IS NULL
              AND COALESCE(kind, 'high')='offline'
            """
        ),
        (device_id, int(instrument_id or 0)),
    )
    row = cur.fetchone()
    return int(_row_get(row, "n", 0) or 0) if row else 0


def _push_event(device_id: str, title: str, body: str, url: str, tag: str) -> None:
    try:
        import push_notify

        push_notify.enqueue(device_id, title, body, url, tag)
    except Exception:
        return


def _instrument_push_target(cur, inst: dict) -> tuple[str, str, str]:
    device_id = str(inst.get("device_id") or "")
    instrument_id = int(inst.get("instrument_id") or 0)
    name = display_name(inst.get("label"), inst.get("server_name"), instrument_id)
    site = _push_site_label(cur, device_id)
    url = f"/instrument/{instrument_id}?device={quote(device_id, safe='')}"
    return name, site, url


def record_offline_history(cur, inst: dict, now: datetime) -> None:
    device_id = inst.get("device_id")
    instrument_id = inst.get("instrument_id")
    if device_id is None or instrument_id is None:
        return
    build_offline_history(cur, inst, now)
    open_row = open_alarm_event(cur, device_id, instrument_id, "offline")
    if not instrument_online(inst, now):
        if open_row is None:
            cur.execute(
                q(
                    """
                    INSERT INTO alarm_events (
                        device_id, instrument_id, started_at, email_sent, kind
                    ) VALUES (?, ?, ?, 0, 'offline')
                    """
                ),
                (device_id, instrument_id, offline_started_at(inst, now).isoformat()),
            )
            name, site, url = _instrument_push_target(cur, inst)
            import push_notify

            if _other_instruments_online(cur, str(device_id), instrument_id, now) == 0:
                if push_notify.claim_device_offline(str(device_id)):
                    _push_event(
                        str(device_id),
                        "Device offline",
                        f"{site} has stopped reporting.",
                        f"/sites/{quote(str(device_id), safe='')}",
                        f"device-offline:{device_id}",
                    )
            else:
                _push_event(
                    str(device_id),
                    "Instrument offline",
                    f"{name} at {site} has stopped reporting.",
                    url,
                    f"offline:{device_id}:{instrument_id}",
                )
        return
    if open_row is not None:
        cur.execute(
            q("UPDATE alarm_events SET cleared_at=? WHERE id=?"),
            (now.isoformat(), open_row["id"]),
        )
        name, site, url = _instrument_push_target(cur, inst)
        import push_notify

        if _other_open_offline(cur, str(device_id), instrument_id) == 0 and push_notify.device_was_offline(str(device_id)):
            push_notify.release_device_offline(str(device_id))
            _push_event(
                str(device_id),
                "Device online",
                f"{site} is reporting again.",
                f"/sites/{quote(str(device_id), safe='')}",
                f"device-offline:{device_id}",
            )
        elif not push_notify.device_was_offline(str(device_id)):
            _push_event(
                str(device_id),
                "Instrument online",
                f"{name} at {site} is reporting again.",
                url,
                f"offline:{device_id}:{instrument_id}",
            )


# EWNext probe registers are tenths of a degree. -3200 on the controller is -320°C.
PROBE_FAIL_LOW_C = -320.0
PROBE_FAIL_HIGH_C = 110.0


def probe_reading_failed(value: Any) -> bool:
    reading = as_float(value)
    if reading is None:
        return False
    return reading <= PROBE_FAIL_LOW_C or reading >= PROBE_FAIL_HIGH_C


def probe_failure_parts(inst: dict) -> list[str]:
    parts: list[str] = []
    cabinet = as_float(inst.get("last_probe1"))
    defrost = as_float(inst.get("last_probe2"))
    if probe_reading_failed(cabinet):
        parts.append(f"temperature is {format_temp(cabinet)}")
    if probe_reading_failed(defrost):
        parts.append(f"defrost temperature is {format_temp(defrost)}")
    return parts


def probe_failed(inst: dict) -> bool:
    return bool(probe_failure_parts(inst))


def failed_probe_value(inst: dict) -> float | None:
    bad = [
        reading
        for reading in (as_float(inst.get("last_probe1")), as_float(inst.get("last_probe2")))
        if probe_reading_failed(reading)
    ]
    if not bad:
        return None
    return max(bad, key=lambda reading: abs(reading))


def record_probe_history(cur, inst: dict, now: datetime) -> None:
    device_id = inst.get("device_id")
    instrument_id = inst.get("instrument_id")
    if device_id is None or instrument_id is None:
        return
    failed = probe_failed(inst)
    reading = failed_probe_value(inst)
    open_row = open_alarm_event(cur, device_id, instrument_id, "probe")
    if failed:
        if open_row is None:
            cur.execute(
                q(
                    """
                    INSERT INTO alarm_events (
                        device_id, instrument_id, started_at, peak_temp, start_temp,
                        email_sent, kind
                    ) VALUES (?, ?, ?, ?, ?, 0, 'probe')
                    """
                ),
                (device_id, instrument_id, now.isoformat(), reading, reading),
            )
            name, site, url = _instrument_push_target(cur, inst)
            detail = ", ".join(probe_failure_parts(inst))
            _push_event(
                str(device_id),
                "Probe failure",
                f"{name} at {site}: {detail}.",
                url,
                f"probe:{device_id}:{instrument_id}",
            )
            return
        peak = as_float(open_row.get("peak_temp"))
        if reading is not None and (peak is None or abs(reading) > abs(peak)):
            cur.execute(
                q("UPDATE alarm_events SET peak_temp=? WHERE id=?"),
                (reading, open_row["id"]),
            )
        return
    if open_row is not None:
        cur.execute(
            q("UPDATE alarm_events SET cleared_at=?, clear_temp=? WHERE id=?"),
            (now.isoformat(), as_float(inst.get("last_probe1")), open_row["id"]),
        )
        name, site, url = _instrument_push_target(cur, inst)
        _push_event(
            str(device_id),
            "Probe restored",
            f"{name} at {site} is reading normally.",
            url,
            f"probe:{device_id}:{instrument_id}",
        )


def persist_cloud_alarm(cur, inst: dict, active: bool, high_since: datetime | None, active_since: datetime | None, emailed: bool) -> None:
    cur.execute(
        q(
            """
            UPDATE instruments SET
                alarm_active=?, alarm_high_since=?, alarm_active_since=?, alarm_email_sent=?
            WHERE device_id=? AND instrument_id=?
            """
        ),
        (
            1 if active else 0,
            high_since.isoformat() if high_since else None,
            active_since.isoformat() if active_since else None,
            1 if emailed else 0,
            inst.get("device_id"),
            inst.get("instrument_id"),
        ),
    )


def apply_cloud_alarm(cur, inst: dict, now: datetime | None = None, mail_out: list | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    high = as_float(inst.get("alarm_high_temp"))
    delay_min = as_float(inst.get("alarm_high_delay_min")) or 0.0
    reset = as_float(inst.get("alarm_reset_temp"))
    notify_min = as_float(inst.get("alarm_notify_delay_min")) or 0.0
    temp = as_float(inst.get("last_probe1"))
    active = bool(int(inst.get("alarm_active") or 0))
    high_since = parse_ts(inst.get("alarm_high_since"))
    active_since = parse_ts(inst.get("alarm_active_since"))
    emailed = bool(int(inst.get("alarm_email_sent") or 0))
    pending = False

    if high is None:
        next_active, next_high_since, next_active_since, next_emailed = False, None, None, False
    else:
        if reset is None:
            reset = high
        delay_sec = max(0.0, delay_min) * 60.0
        notify_sec = max(0.0, notify_min) * 60.0
        next_active = active
        next_high_since = high_since
        next_active_since = active_since
        next_emailed = emailed
        if temp is None:
            if not active:
                next_high_since = None
        elif active:
            if temp <= reset:
                next_active = False
                next_high_since = None
                next_active_since = None
                next_emailed = False
        elif temp > high:
            if next_high_since is None:
                next_high_since = now
            elapsed = (now - next_high_since).total_seconds()
            if elapsed >= delay_sec:
                next_active = True
                next_active_since = now
                next_emailed = False
            else:
                pending = True
        else:
            next_high_since = None

    cabinet_failed = probe_reading_failed(temp)
    if cabinet_failed:
        next_active = False
        next_high_since = None
        next_active_since = None
        next_emailed = False
        pending = False

    persist_cloud_alarm(cur, inst, next_active, next_high_since, next_active_since, next_emailed)
    if next_active and not next_emailed and next_active_since is not None:
        notify_sec = max(0.0, notify_min) * 60.0
        if (now - next_active_since).total_seconds() >= notify_sec:
            job = _alarm_mail_job(
                inst,
                "high",
                temp if temp is not None else high or 0.0,
                high or 0.0,
                next_active_since,
                now,
            )
            if mail_out is not None:
                mail_out.append(job)
            else:
                _queue_alarm_mail(job)

    record_alarm_history(
        cur,
        inst,
        now,
        next_active=next_active,
        next_emailed=next_emailed,
        temp=temp,
        high=high,
        reset=None if high is None else reset,
        started_at=next_active_since,
    )
    if high is not None and next_active != active and not cabinet_failed:
        name, site, url = _instrument_push_target(cur, inst)
        if next_active:
            _push_event(
                str(inst.get("device_id") or ""),
                "High temperature",
                f"{name} at {site} is {format_temp(temp)}.",
                url,
                f"alarm:{inst.get('device_id')}:{inst.get('instrument_id')}",
            )
        else:
            _push_event(
                str(inst.get("device_id") or ""),
                "Alarm cleared",
                f"{name} at {site} is back to normal.",
                url,
                f"alarm:{inst.get('device_id')}:{inst.get('instrument_id')}",
            )
            if emailed:
                job = _alarm_mail_job(inst, "resolved", temp, high, active_since, now)
                if mail_out is not None:
                    mail_out.append(job)
                else:
                    _queue_alarm_mail(job)
    record_offline_history(cur, inst, now)
    record_probe_history(cur, inst, now)

    inst["alarm_active"] = 1 if next_active else 0
    inst["alarm_high_since"] = next_high_since.isoformat() if next_high_since else None
    inst["alarm_active_since"] = next_active_since.isoformat() if next_active_since else None
    inst["alarm_email_sent"] = 1 if next_emailed else 0
    inst["alarm_pending"] = pending
    return inst


def refresh_cloud_alarms(rows: list[dict]) -> list[dict]:
    if not rows:
        return rows
    now = datetime.now(timezone.utc)
    mail_out: list[dict] = []
    with db() as conn:
        cur = conn.cursor()
        for row in rows:
            apply_cloud_alarm(cur, row, now, mail_out)
    flush_alarm_mail(mail_out)
    return rows


def reading_register_address(body: dict) -> int | None:
    for key in ("register_address", "start_address", "address", "first_reg"):
        n = as_int(body.get(key))
        if n is not None:
            return n
    return None


def reading_value(body: dict, values: Any) -> str:
    if body.get("value") is not None and not isinstance(body.get("value"), (dict, list)):
        return str(body.get("value"))
    if isinstance(values, (list, dict)):
        return json.dumps(values, separators=(",", ":"))
    if values is None:
        return ""
    return str(values)


def decode_values(values: Any) -> tuple[float | None, float | None, int | None, float | None]:
    if isinstance(values, str):
        try:
            values = json.loads(values)
        except json.JSONDecodeError:
            values = []
    if not isinstance(values, list) or not values:
        return None, None, None, None
    nums = []
    for v in values:
        try:
            nums.append(float(v))
        except (TypeError, ValueError):
            nums.append(None)
    probe1 = nums[0] / 10.0 if nums and nums[0] is not None else None
    probe2 = nums[1] / 10.0 if len(nums) > 1 and nums[1] is not None else None
    # EWNext RAM block 4110: [Pb1, Pb2, Pb3, _, _, SEt, status]
    setpoint = nums[5] / 10.0 if len(nums) > 5 and nums[5] is not None else None
    flags = None
    if len(nums) > 6 and nums[6] is not None:
        flags = int(nums[6])
    elif len(nums) > 2 and nums[2] is not None and float(nums[2]) == int(nums[2]):
        flags = int(nums[2])
    return probe1, probe2, flags, setpoint


def format_temp(value: Any) -> str:
    if value is None:
        return "—"
    try:
        n = float(value)
    except (TypeError, ValueError):
        return "—"
    if n == int(n):
        return f"{int(n)}°C"
    return f"{n:.1f}°C"


SA_TZ = timezone(timedelta(hours=2))


def format_when(value: Any) -> str:
    dt = parse_ts(value)
    if dt is None:
        return "—"
    return dt.strftime("%d %b %Y %H:%M UTC")


def format_alarm_when(value: Any) -> str:
    dt = parse_ts(value)
    if dt is None:
        return "—"
    return dt.astimezone(SA_TZ).strftime("%d %b %Y %H:%M SAST")


def format_duration(start: datetime | None, end: datetime | None) -> str:
    if start is None or end is None:
        return "—"
    sec = max(0, int((end - start).total_seconds()))
    if sec < 60:
        return f"{sec}s"
    mins = sec // 60
    if mins < 60:
        return f"{mins} min"
    hours, rem = divmod(mins, 60)
    if hours < 48:
        return f"{hours}h {rem}m" if rem else f"{hours}h"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h" if hours else f"{days}d"


def display_name(label: Any, server_name: Any, instrument_id: int) -> str:
    custom = str(label or "").strip()
    if custom:
        return custom
    raw = str(server_name or "").strip()
    if not raw:
        return f"Instrument {instrument_id}"
    return raw.replace("_", " ")


def _backfill_instrument_labels(cur) -> None:
    cur.execute(
        """
        UPDATE instruments
        SET label = REPLACE(TRIM(server_name), '_', ' ')
        WHERE (label IS NULL OR TRIM(label) = '')
          AND server_name IS NOT NULL
          AND TRIM(server_name) <> ''
        """
    )
    cur.execute(
        q(
            """
            UPDATE instruments
            SET label = 'Instrument ' || CAST(instrument_id AS TEXT)
            WHERE (label IS NULL OR TRIM(label) = '')
            """
        )
    )


def parse_ts(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def instrument_online(row: dict, now: datetime | None = None) -> bool:
    now = now or datetime.now(timezone.utc)
    if int(row.get("last_stale") or 0):
        return False
    seen = parse_ts(row.get("last_seen"))
    if seen is None:
        return False
    return (now - seen).total_seconds() < 180


def relative_ago(value: Any) -> str:
    dt = parse_ts(value)
    if dt is None:
        return ""
    sec = max(0, (datetime.now(timezone.utc) - dt).total_seconds())
    if sec < 60:
        return "Just now"
    if sec < 3600:
        mins = int(sec // 60)
        return f"{mins} min{'s' if mins != 1 else ''} ago"
    if sec < 86400:
        hours = int(sec // 3600)
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    days = int(sec // 86400)
    return f"{days} day{'s' if days != 1 else ''} ago"


def site_status_meta(insts: list[dict]) -> dict[str, Any]:
    online_n = sum(1 for w in insts if w.get("online"))
    alarm_n = sum(1 for w in insts if w.get("alarm"))
    offline_n = len(insts) - online_n
    defrost_n = sum(1 for w in insts if w.get("defrost_active"))
    if alarm_n:
        status_kind = "alarm"
        status_text = f"{alarm_n} Active Alarm{'s' if alarm_n != 1 else ''}"
        expanded = True
    elif offline_n:
        status_kind = "offline"
        status_text = f"{offline_n} Offline"
        expanded = True
    elif defrost_n:
        status_kind = "maint"
        status_text = f"{defrost_n} Maintenance Flag{'s' if defrost_n != 1 else ''}"
        expanded = False
    else:
        status_kind = "ok"
        status_text = "All Systems Normal"
        expanded = False
    return {
        "online_n": online_n,
        "alarm_n": alarm_n,
        "offline_n": offline_n,
        "defrost_n": defrost_n,
        "issues": alarm_n > 0 or offline_n > 0,
        "status_kind": status_kind,
        "status_text": status_text,
        "expanded": expanded,
    }


def compute_kpis(widgets: list[dict], sites: list[dict]) -> dict[str, Any]:
    alarm_n = sum(1 for w in widgets if w.get("alarm"))
    online_n = sum(1 for w in widgets if w.get("online"))
    total = len(widgets)
    issue_sites = [s for s in sites if s.get("issues")]
    defrost_n = sum(1 for w in widgets if w.get("defrost_active"))
    if not issue_sites:
        issue_label = "All clear"
    elif len(issue_sites) == 1:
        issue_label = issue_sites[0].get("display_name") or issue_sites[0]["device_id"]
    else:
        issue_label = f"{len(issue_sites)} sites"
    pct = int(round(100 * online_n / total)) if total else 0
    return {
        "alarm_n": alarm_n,
        "issue_n": len(issue_sites),
        "issue_label": issue_label,
        "online_n": online_n,
        "total_n": total,
        "uptime_pct": pct,
        "maint_n": defrost_n,
    }


def build_alarm_feed(widgets: list[dict]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for widget in widgets:
        name = widget.get("name") or f"Instrument {widget.get('instrument_id')}"
        site = widget.get("site_name") or widget.get("device_id") or "unknown"
        when = relative_ago(widget.get("last_seen"))
        href = widget.get("href") or "/"
        if widget.get("alarm"):
            items.append(
                {
                    "kind": "alarm",
                    "title": f"Probe failure: {name}" if widget.get("probe_fail") else f"Alarm: {name}",
                    "detail": site,
                    "when": when,
                    "href": widget.get("alarm_history_href") or href,
                }
            )
        elif not widget.get("online"):
            items.append(
                {
                    "kind": "offline",
                    "title": f"Device Offline: {name}",
                    "detail": site,
                    "when": when,
                    "href": href,
                }
            )
        elif widget.get("defrost_active"):
            items.append(
                {
                    "kind": "maint",
                    "title": f"Defrost: {name}",
                    "detail": site,
                    "when": when,
                    "href": href,
                }
            )
    order = {"alarm": 0, "offline": 1, "maint": 2}
    items.sort(key=lambda row: order.get(row["kind"], 9))
    return items[:8]


def decode_flags(flags: Any) -> dict[str, bool]:
    """Map EWNext analog RAM 4115 to CP1 / DEF1 / FAN / ALM.

    Eliwell Table of Modbus Resources (EWNext Optimized):
      STD-BY=1, CP1=2, DEF1=16, FAN=64, ALM=256.
    Live word 322 = CP1 + FAN + ALM.
    """
    try:
        bits = int(flags)
    except (TypeError, ValueError):
        return {
            "cooling": False,
            "fan": False,
            "humidity": False,
            "alarm": False,
            "defrost_active": False,
            "standby": False,
        }
    cooling = bool(bits & 0x0002)  # CP1
    defrost_active = bool(bits & 0x0010)  # DEF1
    fan = bool(bits & 0x0040)  # FAN
    alarm = bool(bits & 0x0100)  # ALM
    standby = bool(bits & STANDBY_MASK)  # STD-BY
    return {
        "cooling": cooling,
        "defrost_active": defrost_active,
        "fan": fan,
        "alarm": alarm,
        "humidity": defrost_active,
        "standby": standby,
    }


def same_setpoint(a: Any, b: Any) -> bool:
    try:
        return abs(float(a) - float(b)) < 0.051
    except (TypeError, ValueError):
        return False


def queued_defrost(pending: dict | None, live_on: bool) -> bool | None:
    if not pending:
        return None
    if live_on:
        return None
    created = parse_ts(pending.get("created_at"))
    if created is not None:
        age = (datetime.now(timezone.utc) - created).total_seconds()
        if age >= PENDING_SETPOINT_TTL_SEC:
            return None
    return True


def queued_power(pending: dict | None, live_on: bool) -> bool | None:
    if not pending:
        return None
    try:
        want = bool(int(pending.get("display_value") or 0))
    except (TypeError, ValueError):
        return None
    if want == bool(live_on):
        return None
    created = parse_ts(pending.get("created_at"))
    if created is not None:
        age = (datetime.now(timezone.utc) - created).total_seconds()
        if age >= PENDING_SETPOINT_TTL_SEC:
            return None
    return want


def queued_setpoint(pending: dict | None, live_setpoint: Any) -> float | None:
    if not pending:
        return None
    queued = pending.get("display_value")
    if queued is None:
        return None
    if live_setpoint is not None and same_setpoint(queued, live_setpoint):
        return None
    created = parse_ts(pending.get("created_at"))
    if created is not None:
        age = (datetime.now(timezone.utc) - created).total_seconds()
        if age >= PENDING_SETPOINT_TTL_SEC:
            return None
    return queued


def decorate_instrument(
    row: dict,
    pending: dict | None = None,
    pending_power_job: dict | None = None,
    pending_defrost_job: dict | None = None,
) -> dict:
    inst = dict(row)
    online = instrument_online(inst)
    flags = decode_flags(inst.get("last_flags")) if inst.get("last_flags") is not None else None
    probe1 = inst.get("last_probe1")
    setpoint = inst.get("last_setpoint")
    if flags is None:
        cooling = (
            probe1 is not None
            and setpoint is not None
            and float(probe1) > float(setpoint) + 0.3
        )
        flags = {
            "cooling": cooling,
            "fan": False,
            "humidity": False,
            "alarm": False,
            "defrost_active": False,
            "standby": False,
        }
    pending_value = queued_setpoint(pending, setpoint)
    probe_on = probe_failed(inst)
    probe_text = ", ".join(probe_failure_parts(inst))
    high_on = bool(int(inst.get("alarm_active") or 0))
    live_on = not bool(flags.get("standby"))
    pending_on = queued_power(pending_power_job, live_on)
    live_defrost = bool(flags.get("defrost_active"))
    pending_defrost = queued_defrost(pending_defrost_job, live_defrost)
    inst.update(
        {
            "name": display_name(inst.get("label"), inst.get("server_name"), inst.get("instrument_id") or 0),
            "online": online,
            "cooling": flags["cooling"],
            "fan": flags["fan"],
            "humidity": flags["humidity"],
            "defrost_active": live_defrost if pending_defrost is None else True,
            "live_defrost": live_defrost,
            "pending_defrost": pending_defrost,
            "alarm": high_on or probe_on,
            "probe_fail": probe_on,
            "probe_text": probe_text,
            "alarm_pending": bool(inst.get("alarm_pending")),
            "alarm_label": "PROBE" if probe_on else ("ON" if high_on else ("WAIT" if inst.get("alarm_pending") else "OFF")),
            "alarm_configured": as_float(inst.get("alarm_high_temp")) is not None,
            "alarm_high_temp": as_float(inst.get("alarm_high_temp")),
            "alarm_high_delay_min": as_float(inst.get("alarm_high_delay_min")),
            "alarm_reset_temp": as_float(inst.get("alarm_reset_temp")),
            "alarm_notify_delay_min": as_float(inst.get("alarm_notify_delay_min")),
            "alarm_high_temp_input": format_alarm_input(inst.get("alarm_high_temp")),
            "alarm_high_delay_input": format_alarm_input(inst.get("alarm_high_delay_min")),
            "alarm_reset_temp_input": format_alarm_input(inst.get("alarm_reset_temp")),
            "alarm_notify_delay_input": format_alarm_input(inst.get("alarm_notify_delay_min")),
            "standby": flags["standby"],
            "live_powered": live_on,
            "powered": live_on if pending_on is None else pending_on,
            "pending_power": pending_on,
            "cp1_label": "ON" if flags["cooling"] else "OFF",
            "def1_label": "ON" if (live_defrost if pending_defrost is None else True) else "OFF",
            "fan_label": "ON" if flags["fan"] else "OFF",
            "temperature_label": format_temp(probe1),
            "defrost_label": format_temp(inst.get("last_probe2")),
            "setpoint_label": format_temp(pending_value if pending_value is not None else setpoint),
            "pending_setpoint": pending_value,
            "setpoint_input": format_alarm_input(pending_value if pending_value is not None else setpoint),
            "href": f"/instrument/{inst.get('instrument_id') or 0}?device={quote(str(inst.get('device_id') or ''), safe='')}",
            "alarm_history_href": (
                f"/instrument/{inst.get('instrument_id') or 0}/alarm-history"
                f"?device={quote(str(inst.get('device_id') or ''), safe='')}"
            ),
            "site_href": f"/sites/{quote(str(inst.get('device_id') or ''), safe='')}",
        }
    )
    return inst


def downsample_rows(rows: list[dict], max_points: int) -> list[dict]:
    if len(rows) <= max_points:
        return rows
    step = max(1, math.ceil(len(rows) / max_points))
    sampled = rows[::step]
    if sampled and sampled[-1] is not rows[-1]:
        sampled.append(rows[-1])
    return sampled


def history_point(row: dict) -> dict:
    setpoint = row.get("setpoint")
    if setpoint is None:
        _, _, _, setpoint = decode_values(row.get("values_json"))
    flags = decode_flags(row.get("flags"))
    ts = row.get("received_at") or row.get("created_at")
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        t = ts.astimezone(timezone.utc).isoformat()
    else:
        t = str(ts or "")
    return {
        "t": t,
        "temp": row.get("probe1"),
        "defrost": row.get("probe2"),
        "setpoint": setpoint,
        "cooling": 1 if flags["cooling"] else 0,
        "defrost_on": 1 if flags["defrost_active"] else 0,
        "fan": 1 if flags["fan"] else 0,
        "alarm": 1 if int(row.get("cloud_alarm") or 0) else 0,
    }


def load_history(device_id: str, instrument_id: int, range_key: str) -> list[dict]:
    span = HISTORY_RANGES.get(range_key, HISTORY_RANGES["24h"])
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=span)).isoformat()
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q(
                """
                SELECT probe1, probe2, setpoint, flags, values_json, cloud_alarm,
                       COALESCE(created_at, received_at) AS received_at
                FROM readings
                WHERE device_id=? AND instrument_id=? AND received_at >= ?
                ORDER BY received_at ASC
                """
            ),
            (device_id, instrument_id, cutoff),
        )
        rows = fetchall(cur)
    return downsample_rows([history_point(r) for r in rows], HISTORY_MAX_POINTS)


def decorate_alarm_event(row: dict, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    started = parse_ts(row.get("started_at"))
    cleared = parse_ts(row.get("cleared_at"))
    active = cleared is None
    end = cleared or now
    emailed = bool(int(row.get("email_sent") or 0))
    kind = str(row.get("kind") or "high")
    if kind == "offline":
        status_label = "Offline" if active else "Back online"
        status_kind = "offline" if active else "restored"
        kind_label = "Offline"
        email_label = "—"
    elif kind == "probe":
        status_label = "Probe failure" if active else "Probe ok"
        status_kind = "active" if active else "cleared"
        kind_label = "Probe failure"
        email_label = "—"
    else:
        status_label = "Active" if active else "Cleared"
        status_kind = "active" if active else "cleared"
        kind_label = "High temp"
        email_label = format_alarm_when(row.get("email_sent_at")) if emailed else "Not sent"
    row.update(
        {
            "active": active,
            "kind": kind,
            "kind_label": kind_label,
            "status_label": status_label,
            "status_kind": status_kind,
            "started_label": format_alarm_when(started),
            "cleared_label": "—" if active else format_alarm_when(cleared),
            "duration_label": format_duration(started, end),
            "peak_label": "—" if kind == "offline" else format_temp(row.get("peak_temp")),
            "start_temp_label": format_temp(row.get("start_temp")),
            "clear_temp_label": format_temp(row.get("clear_temp")),
            "high_label": "—" if kind in ("offline", "probe") else format_temp(row.get("high_temp")),
            "reset_label": format_temp(row.get("reset_temp")),
            "email_label": email_label,
        }
    )
    return row


def load_alarm_history(device_id: str, instrument_id: int) -> list[dict]:
    now = datetime.now(timezone.utc)
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q(
                """
                SELECT id, started_at, cleared_at, peak_temp, start_temp, clear_temp,
                       high_temp, reset_temp, email_sent, email_sent_at, kind
                FROM alarm_events
                WHERE device_id=? AND instrument_id=?
                ORDER BY started_at DESC
                LIMIT 200
                """
            ),
            (device_id, instrument_id),
        )
        rows = fetchall(cur)
    return [decorate_alarm_event(r, now) for r in rows]


def find_scoped_instrument(widgets: list[dict], instrument_id: int, device_id: str = "") -> dict | None:
    wanted = (device_id or "").strip()
    for widget in widgets:
        if int(widget.get("instrument_id") or 0) != instrument_id:
            continue
        if not wanted or str(widget.get("device_id")) == wanted:
            return widget
    return None


def fetchall(cur) -> list[dict]:
    rows = cur.fetchall()
    if not rows:
        return []
    if isinstance(rows[0], dict):
        return list(rows)
    if hasattr(rows[0], "keys"):
        return [dict(r) for r in rows]
    desc = [c.name for c in cur.description]
    return [dict(zip(desc, r)) for r in rows]


@app.on_event("startup")
def on_startup() -> None:
    if os.environ.get("RENDER") == "true" and not _is_pg():
        raise RuntimeError("DATABASE_URL is required on Render so names and settings persist across deploys")
    init_db()
    try:
        import push_notify

        push_notify.start()
    except Exception:
        pass


@app.get("/health")
def health() -> dict:
    return {
        "ok": True,
        "service": "ewnext-cloud",
        "store": "postgres" if _is_pg() else "sqlite",
    }


@app.get("/manifest.webmanifest")
def web_manifest():
    return FileResponse(
        STATIC_DIR / "manifest.webmanifest",
        media_type="application/manifest+json",
        headers={"Cache-Control": "no-cache"},
    )


@app.get("/sw.js")
def service_worker():
    return FileResponse(
        STATIC_DIR / "sw.js",
        media_type="application/javascript",
        headers={"Cache-Control": "no-cache", "Service-Worker-Allowed": "/"},
    )


def _session_push_portfolio(request: Request) -> list[str] | None:
    if is_super_admin(request):
        return None
    uid = as_int(request.session.get("uid"))
    if not uid:
        return []
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q("SELECT id, name, email, password_hash, role, organisation_id FROM app_users WHERE id=?"),
            (uid,),
        )
        row = cur.fetchone()
        user = _sql_row(row, cur) if row else None
    if not user:
        return []
    return user_portfolio(user)


def current_push_notes(request: Request) -> list[dict[str, str]]:
    portfolio = _session_push_portfolio(request)
    notes: list[dict[str, str]] = []
    now = datetime.now(timezone.utc)
    with db() as conn:
        cur = conn.cursor()
        cur.execute(INSTRUMENT_SELECT)
        grouped: dict[str, list[dict]] = {}
        for row in fetchall(cur):
            device_id = str(row.get("device_id") or "")
            if portfolio is not None and device_id not in portfolio:
                continue
            grouped.setdefault(device_id, []).append(row)
        for device_id, instruments in grouped.items():
            if instruments and not any(instrument_online(inst, now) for inst in instruments):
                site = _push_site_label(cur, device_id)
                notes.append(
                    {
                        "title": "Device offline",
                        "body": f"{site} has stopped reporting.",
                        "url": f"/sites/{quote(device_id, safe='')}",
                        "tag": f"device-offline:{device_id}",
                    }
                )
                continue
            for inst in instruments:
                name, site, url = _instrument_push_target(cur, inst)
                ident = f"{device_id}:{inst.get('instrument_id')}"
                if probe_failed(inst):
                    notes.append(
                        {
                            "title": "Probe failure",
                            "body": f"{name} at {site}: {', '.join(probe_failure_parts(inst))}.",
                            "url": url,
                            "tag": f"probe:{ident}",
                        }
                    )
                if int(inst.get("alarm_active") or 0):
                    notes.append(
                        {
                            "title": "High temperature",
                            "body": f"{name} at {site} is {format_temp(inst.get('last_probe1'))}.",
                            "url": url,
                            "tag": f"alarm:{ident}",
                        }
                    )
                flags = inst.get("last_flags")
                if flags is not None and decode_flags(flags).get("defrost_active") and not int(inst.get("last_stale") or 0):
                    notes.append(
                        {
                            "title": "Defrost",
                            "body": f"{name} at {site} is in defrost.",
                            "url": url,
                            "tag": f"defrost:{ident}",
                        }
                    )
                if not instrument_online(inst, now):
                    notes.append(
                        {
                            "title": "Instrument offline",
                            "body": f"{name} at {site} has stopped reporting.",
                            "url": url,
                            "tag": f"offline:{ident}",
                        }
                    )
    return notes


@app.get("/api/push/public-key")
def push_public_key(request: Request):
    if not logged_in(request):
        raise HTTPException(401, "login required")
    import push_notify

    return {"publicKey": push_notify.public_application_key()}


@app.post("/api/push/subscribe")
async def push_subscribe(request: Request):
    if not logged_in(request):
        raise HTTPException(401, "login required")
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "invalid json")
    if not isinstance(body, dict):
        raise HTTPException(400, "invalid json")
    endpoint = str(body.get("endpoint") or "").strip()
    keys = body.get("keys") if isinstance(body.get("keys"), dict) else {}
    p256dh = str(keys.get("p256dh") or "").strip()
    auth = str(keys.get("auth") or "").strip()
    if not endpoint.startswith("https://") or not p256dh or not auth:
        raise HTTPException(400, "invalid subscription")
    if len(endpoint) > 2000 or len(p256dh) > 300 or len(auth) > 300:
        raise HTTPException(400, "invalid subscription")
    import push_notify

    uid = as_int(request.session.get("uid"))
    push_notify.save_subscription(uid, is_super_admin(request), endpoint, p256dh, auth)
    if not body.get("welcome"):
        return {"ok": True}
    result = push_notify.deliver_one(
        endpoint,
        p256dh,
        auth,
        "Keyview alerts are on",
        "This phone will get alarms, defrost and offline alerts.",
        "/",
        "push-ready",
    )
    if result != "ok":
        return JSONResponse({"ok": False}, status_code=502)
    for note in current_push_notes(request)[:4]:
        push_notify.enqueue_direct(
            endpoint,
            p256dh,
            auth,
            note["title"],
            note["body"],
            note["url"],
            note["tag"],
        )
    return {"ok": True}


@app.post("/api/modbus")
async def ingest(request: Request) -> JSONResponse:
    require_router_key(request)
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "invalid json")
    if not isinstance(body, dict):
        raise HTTPException(400, "invalid json")

    device_id = normalize_device_id(body.get("device_id"))
    try:
        instrument_id = int(body.get("instrument_id"))
    except (TypeError, ValueError):
        raise HTTPException(400, "instrument_id required")
    values = body.get("values") or []
    probe1, probe2, flags, live_setpoint = decode_values(values)
    received = now_iso()
    values_store = json.dumps(values)
    raw_data = json.dumps(body, default=str)
    register_address = reading_register_address(body)
    value_store = reading_value(body, values)

    mail_out: list[dict] = []
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q(
                """
                INSERT INTO readings (
                    device_id, instrument_id, server_name, register_address, value, raw_data,
                    values_json, probe1, probe2, setpoint, flags, stale, device_ts, received_at, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            ),
            (
                device_id,
                instrument_id,
                body.get("server_name"),
                register_address,
                value_store,
                raw_data,
                values_store,
                probe1,
                probe2,
                live_setpoint,
                flags,
                1 if body.get("stale") else 0,
                body.get("timestamp"),
                received,
                received,
            ),
        )
        cur.execute(
            q("SELECT last_setpoint, last_flags FROM instruments WHERE device_id=? AND instrument_id=?"),
            (device_id, instrument_id),
        )
        prev = _sql_row(cur.fetchone(), cur)
        last_setpoint = live_setpoint
        old_flags = None
        if prev:
            if last_setpoint is None:
                last_setpoint = prev.get("last_setpoint")
            old_flags = prev.get("last_flags")
        stale = 1 if body.get("stale") else 0
        frozen_label = display_name(None, body.get("server_name"), instrument_id)
        if site_accepts_instrument(cur, device_id, instrument_id):
            cur.execute(
                q(
                    """
                    INSERT INTO instruments (
                        device_id, instrument_id, server_name, label, last_probe1, last_probe2,
                        last_setpoint, last_flags, last_stale, last_seen
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (device_id, instrument_id) DO UPDATE SET
                        last_probe1 = EXCLUDED.last_probe1,
                        last_probe2 = EXCLUDED.last_probe2,
                        last_setpoint = COALESCE(EXCLUDED.last_setpoint, instruments.last_setpoint),
                        last_flags = EXCLUDED.last_flags,
                        last_stale = EXCLUDED.last_stale,
                        last_seen = EXCLUDED.last_seen
                    """
                ),
                (
                    device_id,
                    instrument_id,
                    body.get("server_name"),
                    frozen_label,
                    probe1,
                    probe2,
                    last_setpoint,
                    flags,
                    stale,
                    received,
                ),
            )
        cur.execute(
            q(INSTRUMENT_SELECT + " WHERE device_id=? AND instrument_id=?"),
            (device_id, instrument_id),
        )
        found = cur.fetchone()
        inst = _sql_row(found, cur) if found else None
        if inst:
            apply_cloud_alarm(cur, inst, mail_out=mail_out)
            if not body.get("stale") and old_flags is not None and flags is not None:
                was_on = decode_flags(old_flags)["defrost_active"]
                now_on = decode_flags(flags)["defrost_active"]
                if was_on != now_on:
                    name, site, push_url = _instrument_push_target(cur, inst)
                    if now_on:
                        _push_event(
                            device_id,
                            "Defrost",
                            f"{name} at {site} is in defrost.",
                            push_url,
                            f"defrost:{device_id}:{instrument_id}",
                        )
                    else:
                        _push_event(
                            device_id,
                            "Defrost finished",
                            f"{name} at {site} has finished defrost.",
                            push_url,
                            f"defrost:{device_id}:{instrument_id}",
                        )
            cur.execute(
                q(
                    """
                    UPDATE readings SET cloud_alarm=?
                    WHERE device_id=? AND instrument_id=? AND received_at=?
                    """
                ),
                (
                    1 if int(inst.get("alarm_active") or 0) else 0,
                    device_id,
                    instrument_id,
                    received,
                ),
            )
        cur.execute(
            q(
                """
                SELECT id, instrument_id, kind, register, raw_value, display_value
                FROM jobs
                WHERE device_id=? AND status='pending'
                ORDER BY id ASC
                LIMIT 20
                """
            ),
            (device_id,),
        )
        jobs = fetchall(cur)
        uid = ensure_site_record(cur, device_id)
        if apply_subscription_lock(cur, device_id):
            jobs = []

    flush_alarm_mail(mail_out)
    payload_jobs = [
        {
            "id": j["id"],
            "type": j["kind"],
            "instrument_id": j["instrument_id"],
            "register": j["register"],
            "function": 16,
            "value": j["raw_value"],
            "display_value": j["display_value"],
        }
        for j in jobs
    ]
    return JSONResponse(
        {"ok": True, "jobs": payload_jobs, "device_id": device_id, "uid": uid}
    )


@app.post("/api/modbus/jobs/{job_id}/result")
async def job_result(job_id: int, request: Request) -> dict:
    require_router_key(request)
    body = await request.json()
    ok = bool(body.get("ok", True))
    error = body.get("error")
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q("SELECT id, device_id, instrument_id, kind, display_value, status FROM jobs WHERE id=?"),
            (job_id,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "job not found")
        job = dict(row) if hasattr(row, "keys") or isinstance(row, dict) else {
            "id": row[0],
            "device_id": row[1],
            "instrument_id": row[2],
            "kind": row[3],
            "display_value": row[4],
            "status": row[5],
        }
        cur.execute(
            q("UPDATE jobs SET status=?, error=?, completed_at=? WHERE id=?"),
            ("done" if ok else "error", None if ok else str(error or "failed"), now_iso(), job_id),
        )
        if ok and job.get("kind") == "setpoint":
            cur.execute(
                q(
                    """
                    UPDATE instruments SET last_setpoint=?
                    WHERE device_id=? AND instrument_id=?
                    """
                ),
                (job["display_value"], job["device_id"], job["instrument_id"]),
            )
        elif ok and job.get("kind") == "power":
            want_on = bool(int(job.get("display_value") or 0))
            flag_sql = (
                "COALESCE(last_flags, 0) & 65534" if want_on else "COALESCE(last_flags, 0) | 1"
            )
            cur.execute(
                q(
                    f"""
                    UPDATE instruments SET last_flags={flag_sql}
                    WHERE device_id=? AND instrument_id=?
                    """
                ),
                (job["device_id"], job["instrument_id"]),
            )
        elif ok and job.get("kind") == "defrost":
            cur.execute(
                q(
                    """
                    UPDATE instruments SET last_flags=COALESCE(last_flags, 0) | 16
                    WHERE device_id=? AND instrument_id=?
                    """
                ),
                (job["device_id"], job["instrument_id"]),
            )
    return {"ok": True}


def dashboard_shell(request: Request | None = None) -> dict:
    with db() as conn:
        cur = conn.cursor()
        cur.execute(INSTRUMENT_SELECT + " ORDER BY device_id, instrument_id")
        instruments = fetchall(cur)
    allowed = allowed_site_ids(request) if request is not None else None
    if allowed is not None:
        instruments = [row for row in instruments if str(row.get("device_id")) in allowed]
    instruments = refresh_cloud_alarms(instruments)
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, device_id, instrument_id, kind, display_value, status, created_at
            FROM jobs
            WHERE status='pending' AND kind IN ('setpoint', 'power', 'defrost')
            ORDER BY id ASC
            """
        )
        pending_jobs = fetchall(cur)
    pending_setpoints = {}
    pending_power = {}
    pending_defrost = {}
    for job in pending_jobs:
        key = (job.get("device_id"), job.get("instrument_id"))
        if job.get("kind") == "power":
            pending_power[key] = job
        elif job.get("kind") == "defrost":
            pending_defrost[key] = job
        else:
            pending_setpoints[key] = job
    widgets = [
        decorate_instrument(
            inst,
            pending_setpoints.get((inst.get("device_id"), inst.get("instrument_id"))),
            pending_power.get((inst.get("device_id"), inst.get("instrument_id"))),
            pending_defrost.get((inst.get("device_id"), inst.get("instrument_id"))),
        )
        for inst in instruments
    ]
    records = load_site_records(
        [str(inst.get("device_id") or "") for inst in instruments]
    )
    for widget in widgets:
        device_id = str(widget.get("device_id") or "")
        rec = records.get(device_id) or {}
        widget["site_name"] = rec.get("display_name") or device_id
        widget["site_uid"] = rec.get("uid") or ""
        widget["subscription_locked"] = bool(rec.get("subscription_locked"))
        widget["subscription_status"] = rec.get("subscription_status") or "active"
        widget["subscription_label"] = rec.get("subscription_label") or "Active"
        widget["renewal_label"] = rec.get("renewal_label") or "—"
    sites: list[dict[str, Any]] = []
    by_site: dict[str, list[dict]] = {}
    for widget in widgets:
        key = str(widget.get("device_id") or "unknown")
        by_site.setdefault(key, []).append(widget)
    for device_id, insts in by_site.items():
        meta = site_status_meta(insts)
        encoded = quote(device_id, safe="")
        rec = records.get(device_id) or {}
        store_name = rec.get("store_name") or ""
        device_online = any(w.get("online") for w in insts)
        locked = bool(rec.get("subscription_locked"))
        row = {
            "device_id": device_id,
            "store_name": store_name,
            "display_name": rec.get("display_name") or device_id,
            "uid": rec.get("uid") or "",
            "device_online": device_online,
            "device_status": "Online" if device_online else "Offline",
            "instruments": insts,
            "count": len(insts),
            "max_n": MAX_INSTRUMENTS_PER_SITE,
            "href": f"/sites/{encoded}",
            "subscription_locked": locked,
            "subscription_expired": bool(rec.get("subscription_expired")),
            "subscription_status": rec.get("subscription_status") or "active",
            "subscription_label": rec.get("subscription_label") or "Active",
            "onboarded_label": rec.get("onboarded_label") or "—",
            "renewal_label": rec.get("renewal_label") or "—",
            "subscription_days_left": rec.get("subscription_days_left"),
            **meta,
        }
        if locked:
            row["status_kind"] = "locked"
            row["status_text"] = "Subscription locked"
            row["device_status"] = "Locked"
        elif rec.get("subscription_status") == "due" and row.get("status_kind") == "ok":
            row["status_kind"] = "due"
            row["status_text"] = rec.get("subscription_label") or "Renewal due"
        sites.append(row)
    visible = widgets
    if request is not None and not is_super_admin(request):
        visible = [w for w in widgets if not w.get("subscription_locked")]
    online_n = sum(1 for w in visible if w.get("online"))
    alarm_n = sum(1 for w in visible if w.get("alarm"))
    offline_n = len(visible) - online_n
    if not visible:
        health = {
            "level": "warning",
            "label": "Waiting",
            "detail": "No controller telemetry yet",
        }
    elif alarm_n:
        health = {
            "level": "critical",
            "label": "Critical",
            "detail": f"{alarm_n} instrument alarm{'s' if alarm_n != 1 else ''}",
        }
    elif offline_n:
        health = {
            "level": "warning",
            "label": "Warning",
            "detail": f"{offline_n} controller{'s' if offline_n != 1 else ''} offline",
        }
    else:
        health = {
            "level": "ok",
            "label": "OK",
            "detail": f"{online_n} controller{'s' if online_n != 1 else ''} online",
        }
    visible_sites = [row for row in sites if not row.get("subscription_locked")]
    if request is not None and is_super_admin(request):
        visible_sites = sites
        kpis_widgets = widgets
        kpis_sites = sites
        feed_widgets = widgets
    else:
        kpis_widgets = visible
        kpis_sites = visible_sites
        feed_widgets = visible
    return {
        "instruments": kpis_widgets,
        "sites": visible_sites,
        "all_sites": sites,
        "health": health,
        "online_n": online_n,
        "alarm_n": alarm_n,
        "site_n": len(visible_sites),
        "kpis": compute_kpis(kpis_widgets, kpis_sites),
        "alarm_feed": build_alarm_feed(feed_widgets),
        "site_names": {row["device_id"]: row["display_name"] for row in sites},
        "smtp_configured": smtp_configured(),
        "admin_name": session_name(request) if request is not None else "Admin",
        "is_manager": is_manager(request) if request is not None else True,
        "is_super_admin": is_super_admin(request) if request is not None else False,
        "device_locked": False,
    }


@app.get("/api/instruments")
def instruments_api(request: Request):
    require_admin(request)
    ctx = dashboard_shell(request)
    return [
        {
            "device_id": w.get("device_id"),
            "instrument_id": w.get("instrument_id"),
            "last_setpoint": w.get("last_setpoint"),
            "pending_setpoint": w.get("pending_setpoint"),
            "live_powered": w.get("live_powered"),
            "powered": w.get("powered"),
            "pending_power": w.get("pending_power"),
            "live_defrost": w.get("live_defrost"),
            "defrost_active": w.get("defrost_active"),
            "pending_defrost": w.get("pending_defrost"),
        }
        for w in ctx["instruments"]
    ]


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    ctx = attach_session(dashboard_shell(request), request, "dashboard")
    return templates.TemplateResponse("dashboard.html", ctx)


@app.get("/sites", response_class=HTMLResponse)
def sites_page(request: Request):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    ctx = attach_session(dashboard_shell(request), request, "sites")
    return templates.TemplateResponse("sites.html", ctx)


@app.get("/sites/{device_id}", response_class=HTMLResponse)
def site_detail(request: Request, device_id: str):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not can_access_site(request, device_id):
        raise HTTPException(403, "no access to this site")
    require_subscription_view(request, device_id)
    ctx = dashboard_shell(request)
    site = next((row for row in (ctx.get("all_sites") or ctx["sites"]) if row.get("device_id") == device_id), None)
    if site is None:
        raise HTTPException(404, "site not found")
    scoped = list(site.get("instruments") or [])
    ctx["kpis"] = compute_kpis(scoped, [site])
    ctx["alarm_feed"] = build_alarm_feed(scoped)
    attach_session(ctx, request, "site")
    ctx.update(
        {
            "site": site,
            "sites": [site],
            "device_locked": bool(site.get("subscription_locked")),
            "back_href": "/sites",
            "back_label": "Sites",
        }
    )
    return templates.TemplateResponse("site.html", ctx)


def _instrument_page_context(request: Request, instrument_id: int, device: str = "") -> tuple[dict, dict]:
    ctx = dashboard_shell(request)
    inst = find_scoped_instrument(ctx["instruments"], instrument_id, device)
    if inst is None:
        raise HTTPException(404, "instrument not found")
    if not can_access_site(request, str(inst.get("device_id") or "")):
        raise HTTPException(403, "no access to this site")
    require_subscription_view(request, str(inst.get("device_id") or ""))
    return ctx, inst


@app.get("/instrument/{instrument_id}", response_class=HTMLResponse)
def instrument_page(request: Request, instrument_id: int, device: str = "", range: str = "24h"):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    ctx, inst = _instrument_page_context(request, instrument_id, device)
    range_key = range if range in HISTORY_RANGES else "24h"
    attach_session(ctx, request, "instrument")
    ctx.update(
        {
            "inst": inst,
            "device_locked": bool(inst.get("subscription_locked")),
            "back_href": inst.get("site_href") or "/sites",
            "back_label": inst.get("site_name") or "Site",
            "range_key": range_key,
            "ranges": [
                ("1h", "1 hr"),
                ("24h", "24 hr"),
                ("2d", "2 days"),
                ("7d", "7 days"),
                ("1m", "1 month"),
            ],
        }
    )
    return templates.TemplateResponse("instrument.html", ctx)


@app.get("/instrument/{instrument_id}/alarm-history", response_class=HTMLResponse)
def instrument_alarm_history(request: Request, instrument_id: int, device: str = ""):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    ctx, inst = _instrument_page_context(request, instrument_id, device)
    events = load_alarm_history(str(inst.get("device_id") or ""), instrument_id)
    attach_session(ctx, request, "alarm-history")
    ctx.update(
        {
            "inst": inst,
            "device_locked": bool(inst.get("subscription_locked")),
            "back_href": inst.get("href") or inst.get("site_href") or "/sites",
            "back_label": inst.get("name") or "Live",
            "alarm_events": events,
            "alarm_active_n": sum(1 for e in events if e.get("active")),
            "alarm_total_n": len(events),
        }
    )
    return templates.TemplateResponse("alarm_history.html", ctx)


@app.get("/api/instruments/{instrument_id}/history")
def instrument_history(request: Request, instrument_id: int, device: str = "", range: str = "24h"):
    require_admin(request)
    device_id = (device or "").strip()
    if not device_id:
        ctx = dashboard_shell(request)
        for widget in ctx["instruments"]:
            if int(widget.get("instrument_id") or 0) == instrument_id:
                device_id = str(widget.get("device_id") or "")
                break
    if not device_id:
        raise HTTPException(404, "instrument not found")
    require_subscription_view(request, device_id)
    range_key = range if range in HISTORY_RANGES else "24h"
    return {
        "instrument_id": instrument_id,
        "device_id": device_id,
        "range": range_key,
        "points": load_history(device_id, instrument_id, range_key),
    }


def _raw_data_page(request: Request):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_manager(request):
        return RedirectResponse("/", status_code=302)
    ctx = dashboard_shell(request)
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, device_id, instrument_id, kind, display_value, status, created_at, error FROM jobs ORDER BY id DESC LIMIT 30"
        )
        jobs = fetchall(cur)
        cur.execute(
            """
            SELECT id, device_id, instrument_id, register_address,
                   COALESCE(value, values_json) AS value,
                   COALESCE(raw_data, values_json) AS raw_data,
                   COALESCE(created_at, received_at) AS created_at
            FROM readings
            ORDER BY id DESC
            LIMIT 40
            """
        )
        readings = fetchall(cur)
    attach_session(ctx, request, "raw-data")
    ctx.update({"jobs": jobs, "readings": readings})
    return templates.TemplateResponse("jobs.html", ctx)


@app.get("/raw-data", response_class=HTMLResponse)
def raw_data_page(request: Request):
    return _raw_data_page(request)


@app.get("/jobs", response_class=HTMLResponse)
def jobs_page(request: Request):
    return RedirectResponse("/raw-data", status_code=302)


@app.get("/device-admin", response_class=HTMLResponse)
def device_admin_page(request: Request):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_manager(request):
        return RedirectResponse("/", status_code=302)
    ctx = attach_session(dashboard_shell(request), request, "device-admin")
    ctx["sites"] = ctx.get("all_sites") or ctx["sites"]
    return templates.TemplateResponse("device_admin.html", ctx)


@app.post("/device/renew")
def renew_device(
    request: Request,
    device_id: str = Form(...),
    next: str = Form(""),
):
    require_super_admin(request)
    device_id = normalize_device_id(device_id)
    rec = renew_device_subscription(device_id)
    if not rec:
        raise HTTPException(404, "device not found")
    if _wants_json(request):
        return JSONResponse(
            {
                "ok": True,
                "device_id": device_id,
                "subscription_locked": False,
                "subscription_until": rec.get("subscription_until"),
                "renewal_label": rec.get("renewal_label"),
            }
        )
    return _redirect_next(next or "/device-admin")


def known_site_ids() -> list[str]:
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT device_id FROM sites
            UNION
            SELECT DISTINCT device_id FROM instruments
            ORDER BY 1
            """
        )
        rows = fetchall(cur)
    ids: list[str] = []
    for row in rows:
        value = str(_row_get(row, "device_id", 0) or "").strip()
        if value and value not in ids:
            ids.append(value)
    return ids


def load_organisations() -> list[dict]:
    with db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id, name, parent_id, locked, created_at FROM organisations ORDER BY name")
        return [dict(row) for row in fetchall(cur)]


def assigned_device_ids() -> set[str]:
    with db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT device_id FROM org_sites")
        return {str(_row_get(row, "device_id", 0) or "").strip() for row in fetchall(cur) if _row_get(row, "device_id", 0)}


def load_app_users() -> list[dict]:
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT id, name, email, role, organisation_id, created_at FROM app_users ORDER BY name, email"
        )
        users = fetchall(cur)
        cur.execute("SELECT user_id, device_id FROM user_sites ORDER BY device_id")
        links = fetchall(cur)
        cur.execute("SELECT id, name FROM organisations")
        orgs = fetchall(cur)
    names = {int(_row_get(row, "id", 0)): str(_row_get(row, "name", 1) or "") for row in orgs}
    by_user: dict[int, list[str]] = {}
    for link in links:
        uid = int(_row_get(link, "user_id", 0))
        site = str(_row_get(link, "device_id", 1) or "")
        by_user.setdefault(uid, []).append(site)
    out = []
    for user in users:
        item = dict(user)
        org_id = as_int(item.get("organisation_id"))
        item["sites"] = by_user.get(int(item["id"]), [])
        item["organisation_id"] = org_id
        item["organisation_name"] = names.get(org_id or 0, "")
        item["role_key"] = normalize_role(item.get("role"))
        item["role_label"] = role_label(item.get("role"))
        out.append(item)
    return out


def users_page_ctx(request: Request, error: str | None = None, notice: str | None = None) -> dict:
    ctx = dashboard_shell(request)
    attach_session(ctx, request, "users")
    orgs = load_organisations()
    sub_orgs = [row for row in orgs if as_int(row.get("parent_id"))]
    known = known_site_ids()
    names = {k: v["display_name"] for k, v in load_site_records(known).items()}
    ctx.update(
        {
            "app_users": load_app_users(),
            "known_sites": known,
            "site_names": names,
            "sub_orgs": sub_orgs,
            "error": error,
            "notice": notice,
        }
    )
    return ctx


def organisations_page_ctx(request: Request, error: str | None = None, notice: str | None = None) -> dict:
    ctx = dashboard_shell(request)
    attach_session(ctx, request, "organisations")
    orgs = load_organisations()
    users = load_app_users()
    assigned = assigned_device_ids()
    all_sites = known_site_ids()
    unassigned = [site for site in all_sites if site not in assigned]
    root = next((row for row in orgs if not as_int(row.get("parent_id"))), None)
    children = [row for row in orgs if as_int(row.get("parent_id")) == as_int(root.get("id") if root else None)]
    site_map: dict[int, list[str]] = {}
    with db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT organisation_id, device_id FROM org_sites ORDER BY device_id")
        for row in fetchall(cur):
            oid = int(_row_get(row, "organisation_id", 0))
            site_map.setdefault(oid, []).append(str(_row_get(row, "device_id", 1) or ""))
    for child in children:
        oid = int(child["id"])
        child["sites"] = site_map.get(oid, [])
        child["locked"] = bool(int(child.get("locked") or 0))
        child["team"] = [user for user in users if user.get("organisation_id") == oid]
        child["href"] = f"/organisations/{oid}"
    names = {k: v["display_name"] for k, v in load_site_records(all_sites).items()}
    ctx.update(
        {
            "root_org": root,
            "sub_orgs": children,
            "unassigned_sites": unassigned,
            "site_names": names,
            "error": error,
            "notice": notice,
        }
    )
    return ctx


@app.get("/organisations", response_class=HTMLResponse)
def organisations_page(request: Request):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_manager(request):
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("organisations.html", organisations_page_ctx(request))


@app.post("/organisations", response_class=HTMLResponse)
def organisations_create(request: Request, name: str = Form(...)):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_manager(request):
        raise HTTPException(403, "manager only")
    display = " ".join(str(name or "").split())
    if not display or len(display) > NAME_MAX_LEN:
        return templates.TemplateResponse(
            "organisations.html",
            organisations_page_ctx(request, error="Sub-organisation name must be 1 to 80 characters"),
            status_code=400,
        )
    orgs = load_organisations()
    root = next((row for row in orgs if not as_int(row.get("parent_id"))), None)
    if root is None:
        return templates.TemplateResponse(
            "organisations.html",
            organisations_page_ctx(request, error="Create the organisation first"),
            status_code=400,
        )
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q("INSERT INTO organisations (name, parent_id, locked, created_at) VALUES (?, ?, 0, ?)"),
            (display, int(root["id"]), now_iso()),
        )
    return templates.TemplateResponse(
        "organisations.html", organisations_page_ctx(request, notice="Sub-organisation added.")
    )


@app.post("/organisations/{org_id}/lock")
def organisations_lock(request: Request, org_id: int, locked: int = Form(1)):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_manager(request):
        raise HTTPException(403, "manager only")
    with db() as conn:
        cur = conn.cursor()
        cur.execute(q("SELECT id, parent_id FROM organisations WHERE id=?"), (org_id,))
        row = cur.fetchone()
        if not row or not as_int(_row_get(row, "parent_id", 1)):
            raise HTTPException(400, "only a sub-organisation can be locked")
        cur.execute(q("UPDATE organisations SET locked=? WHERE id=?"), (1 if int(locked) else 0, org_id))
    return RedirectResponse("/organisations", status_code=303)


@app.post("/organisations/{org_id}/sites", response_class=HTMLResponse)
def organisations_add_site(request: Request, org_id: int, device_id: str = Form(...)):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_manager(request):
        raise HTTPException(403, "manager only")
    site = normalize_device_id(device_id)
    if site == "unknown":
        return templates.TemplateResponse(
            "organisations.html",
            organisations_page_ctx(request, error="Enter a Device ID to add as a site"),
            status_code=400,
        )
    with db() as conn:
        cur = conn.cursor()
        cur.execute(q("SELECT id, parent_id FROM organisations WHERE id=?"), (org_id,))
        row = cur.fetchone()
        if not row or not as_int(_row_get(row, "parent_id", 1)):
            return templates.TemplateResponse(
                "organisations.html",
                organisations_page_ctx(request, error="Sites are added to a sub-organisation"),
                status_code=400,
            )
        ensure_site_record(cur, site)
        cur.execute(q("DELETE FROM org_sites WHERE device_id=?"), (site,))
        cur.execute(q("INSERT INTO org_sites (organisation_id, device_id) VALUES (?, ?)"), (org_id, site))
    return templates.TemplateResponse(
        "organisations.html", organisations_page_ctx(request, notice=f"Site {site} added.")
    )


@app.post("/organisations/{org_id}/sites/delete")
def organisations_delete_site(request: Request, org_id: int, device_id: str = Form(...)):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_manager(request):
        raise HTTPException(403, "manager only")
    site = normalize_device_id(device_id)
    with db() as conn:
        cur = conn.cursor()
        cur.execute(q("DELETE FROM org_sites WHERE organisation_id=? AND device_id=?"), (org_id, site))
        cur.execute(q("DELETE FROM user_sites WHERE device_id=?"), (site,))
    return RedirectResponse("/organisations", status_code=303)


@app.get("/users", response_class=HTMLResponse)
def users_page(request: Request):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_manager(request):
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("users.html", users_page_ctx(request))


def replace_user_sites(cur, user_id: int, site_ids: list[str], known: list[str]) -> None:
    cur.execute(q("DELETE FROM user_sites WHERE user_id=?"), (user_id,))
    allowed = set(known)
    for site in site_ids:
        if site not in allowed:
            continue
        cur.execute(q("INSERT INTO user_sites (user_id, device_id) VALUES (?, ?)"), (user_id, site))


@app.post("/users", response_class=HTMLResponse)
def users_create(
    request: Request,
    name: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    role: str = Form("sub_operator"),
    organisation_id: str = Form(""),
    site_id: Any = Form(None),
):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_manager(request):
        raise HTTPException(403, "manager only")
    display = " ".join(str(name or "").split())
    mail = normalize_email(email)
    role_name = normalize_role(role)
    sites = form_id_list(site_id)
    org_id = as_int(organisation_id)
    if not display or len(display) > NAME_MAX_LEN:
        return templates.TemplateResponse(
            "users.html", users_page_ctx(request, error="Name must be 1 to 80 characters"), status_code=400
        )
    if "@" not in mail or "." not in mail.split("@")[-1]:
        return templates.TemplateResponse(
            "users.html", users_page_ctx(request, error="Enter a valid email address"), status_code=400
        )
    if len(password) < PASSWORD_MIN_LEN:
        return templates.TemplateResponse(
            "users.html",
            users_page_ctx(request, error=f"Password must be at least {PASSWORD_MIN_LEN} characters"),
            status_code=400,
        )
    orgs = load_organisations()
    root = next((row for row in orgs if not as_int(row.get("parent_id"))), None)
    sub_ids = {int(row["id"]) for row in orgs if as_int(row.get("parent_id"))}
    if role_name == "org_admin":
        org_id = as_int(root.get("id") if root else None)
        sites = []
    elif not org_id or org_id not in sub_ids:
        return templates.TemplateResponse(
            "users.html",
            users_page_ctx(request, error="Choose a sub-organisation for this operator"),
            status_code=400,
        )
    known = org_sites_for(org_id, include_children=False) if role_name == "sub_operator" else known_site_ids()
    try:
        with db() as conn:
            cur = conn.cursor()
            cur.execute(
                q(
                    """
                    INSERT INTO app_users (name, email, password_hash, role, organisation_id, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """
                ),
                (display, mail, hash_password(password), role_name, org_id, now_iso()),
            )
            if _is_pg():
                cur.execute(q("SELECT id FROM app_users WHERE email=?"), (mail,))
                row = cur.fetchone()
                user_id = int(_row_get(row, "id", 0))
            else:
                user_id = int(cur.lastrowid)
            if role_name == "sub_operator" and sites:
                replace_user_sites(cur, user_id, sites, known)
    except Exception as exc:
        text = str(exc).lower()
        if "unique" in text or "duplicate" in text:
            return templates.TemplateResponse(
                "users.html", users_page_ctx(request, error="That email is already in use"), status_code=400
            )
        raise
    return templates.TemplateResponse("users.html", users_page_ctx(request, notice="User added."))


@app.post("/users/{user_id}/access", response_class=HTMLResponse)
def users_access(
    request: Request,
    user_id: int,
    role: str = Form("sub_operator"),
    organisation_id: str = Form(""),
    site_id: Any = Form(None),
):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_manager(request):
        raise HTTPException(403, "manager only")
    role_name = normalize_role(role)
    sites = form_id_list(site_id)
    org_id = as_int(organisation_id)
    orgs = load_organisations()
    root = next((row for row in orgs if not as_int(row.get("parent_id"))), None)
    sub_ids = {int(row["id"]) for row in orgs if as_int(row.get("parent_id"))}
    if role_name == "org_admin":
        org_id = as_int(root.get("id") if root else None)
        sites = []
    elif not org_id or org_id not in sub_ids:
        return templates.TemplateResponse(
            "users.html",
            users_page_ctx(request, error="Choose a sub-organisation for this operator"),
            status_code=400,
        )
    known = org_sites_for(org_id, include_children=False) if role_name == "sub_operator" else known_site_ids()
    with db() as conn:
        cur = conn.cursor()
        cur.execute(q("SELECT id FROM app_users WHERE id=?"), (user_id,))
        if not cur.fetchone():
            raise HTTPException(404, "user not found")
        cur.execute(
            q("UPDATE app_users SET role=?, organisation_id=? WHERE id=?"),
            (role_name, org_id, user_id),
        )
        if role_name == "org_admin" or not sites:
            cur.execute(q("DELETE FROM user_sites WHERE user_id=?"), (user_id,))
        else:
            replace_user_sites(cur, user_id, sites, known)
    return templates.TemplateResponse("users.html", users_page_ctx(request, notice="Access updated."))


@app.post("/users/{user_id}/delete")
def users_delete(request: Request, user_id: int):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_manager(request):
        raise HTTPException(403, "manager only")
    with db() as conn:
        cur = conn.cursor()
        cur.execute(q("DELETE FROM user_sites WHERE user_id=?"), (user_id,))
        cur.execute(q("DELETE FROM app_users WHERE id=?"), (user_id,))
    return RedirectResponse("/users", status_code=303)


def valid_email(mail: str) -> bool:
    return bool(mail) and "@" in mail and "." in mail.split("@")[-1] and " " not in mail


def valid_device_id(value: str) -> bool:
    if not value or value == "unknown" or len(value) < 2 or len(value) > 64:
        return False
    return all(ch.isalnum() or ch in "_-" for ch in value)


def public_base(request: Request) -> str:
    configured = (os.environ.get("PUBLIC_BASE_URL") or "").strip().rstrip("/")
    if configured:
        return configured
    return str(request.base_url).rstrip("/")


def main_organisation_id(cur) -> int | None:
    cur.execute(q("SELECT id FROM organisations WHERE parent_id IS NULL ORDER BY id LIMIT 1"))
    row = cur.fetchone()
    if not row:
        return None
    return int(_row_get(row, "id", 0))


def holding_organisation_id(cur) -> int | None:
    root_id = main_organisation_id(cur)
    if not root_id:
        return None
    cur.execute(
        q("SELECT id FROM organisations WHERE parent_id=? AND name=? ORDER BY id LIMIT 1"),
        (root_id, "Main"),
    )
    row = cur.fetchone()
    if not row:
        return None
    return int(_row_get(row, "id", 0))


def claim_device_for_org(cur, org_id: int, device_id: str) -> str | None:
    site = normalize_device_id(device_id)
    if not valid_device_id(site):
        return "Enter the device's unique ID. Use letters, numbers, _ or -."
    cur.execute(q("SELECT organisation_id FROM org_sites WHERE device_id=?"), (site,))
    row = cur.fetchone()
    if row:
        existing = int(_row_get(row, "organisation_id", 0) or 0)
        if existing == org_id:
            return None
        holding = holding_organisation_id(cur)
        if existing != holding:
            return "That device ID is already registered to another organisation."
        cur.execute(q("DELETE FROM org_sites WHERE device_id=?"), (site,))
        cur.execute(q("DELETE FROM user_sites WHERE device_id=?"), (site,))
    ensure_site_record(cur, site)
    cur.execute(
        q("INSERT INTO org_sites (organisation_id, device_id) VALUES (?, ?)"),
        (org_id, site),
    )
    return None


def refresh_session_sites(request: Request) -> None:
    uid = as_int(request.session.get("uid"))
    if not uid:
        return
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q("SELECT id, name, email, password_hash, role, organisation_id FROM app_users WHERE id=?"),
            (uid,),
        )
        row = cur.fetchone()
    user = dict(row) if row and (isinstance(row, dict) or hasattr(row, "keys")) else None
    if user is None and row:
        user = {
            "id": row[0],
            "name": row[1],
            "email": row[2],
            "password_hash": row[3],
            "role": row[4],
            "organisation_id": row[5] if len(row) > 5 else None,
        }
    if not user:
        return
    portfolio = user_portfolio(user)
    request.session["sites"] = [] if portfolio is None else portfolio


def save_org_invite(cur, org_id: int, email: str, role: str) -> str:
    created = datetime.now(timezone.utc)
    expires = (created + timedelta(days=7)).isoformat()
    cur.execute(
        q(
            """
            SELECT id, token, expires_at FROM org_invites
            WHERE organisation_id=? AND email=? AND accepted_at IS NULL
            ORDER BY id DESC LIMIT 1
            """
        ),
        (org_id, email),
    )
    existing = cur.fetchone()
    if existing:
        invite_id = int(_row_get(existing, "id", 0))
        current = str(_row_get(existing, "token", 1) or "")
        exp = parse_ts(_row_get(existing, "expires_at", 2))
        if current and exp and exp > created:
            cur.execute(q("UPDATE org_invites SET role=? WHERE id=?"), (role, invite_id))
            return current
        token = secrets.token_urlsafe(32)
        cur.execute(
            q("UPDATE org_invites SET role=?, token=?, created_at=?, expires_at=? WHERE id=?"),
            (role, token, created.isoformat(), expires, invite_id),
        )
        return token
    token = secrets.token_urlsafe(32)
    cur.execute(
        q(
            """
            INSERT INTO org_invites (organisation_id, email, role, token, created_at, expires_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """
        ),
        (org_id, email, role, token, created.isoformat(), expires),
    )
    return token


def _reset_token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def save_password_reset(cur, user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    created = datetime.now(timezone.utc)
    expires = created + timedelta(hours=2)
    cur.execute(
        q("UPDATE password_resets SET used_at=? WHERE user_id=? AND used_at IS NULL"),
        (created.isoformat(), user_id),
    )
    cur.execute(
        q(
            """
            INSERT INTO password_resets (user_id, token_hash, created_at, expires_at)
            VALUES (?, ?, ?, ?)
            """
        ),
        (user_id, _reset_token_hash(token), created.isoformat(), expires.isoformat()),
    )
    return token


def load_password_reset(token: str) -> dict | None:
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q(
                """
                SELECT r.id, r.user_id, r.expires_at, r.used_at, u.email, u.name
                FROM password_resets r
                JOIN app_users u ON u.id = r.user_id
                WHERE r.token_hash=?
                """
            ),
            (_reset_token_hash(token),),
        )
        return _sql_row(cur.fetchone(), cur)


def reset_is_open(row: dict | None) -> str | None:
    if not row:
        return "This reset link is not valid."
    if row.get("used_at"):
        return "This reset link has already been used."
    expires = parse_ts(row.get("expires_at"))
    if expires and expires < datetime.now(timezone.utc):
        return "This reset link has expired. Ask for a new one."
    return None


def send_reset_email(email: str, link: str) -> bool:
    subject = "Reset your Keyview Cloud password"
    body = (
        "A password reset was requested for your Keyview Cloud account.\n\n"
        f"Open this link to choose a new password:\n{link}\n\n"
        "This link expires in 2 hours. If you did not ask for this, you can ignore this email.\n"
    )
    return _deliver_email([email], subject, body)


def send_invite_email(email: str, org_name: str, role: str, link: str) -> bool:
    subject = f"Join {org_name} on Keyview Cloud"
    body = (
        f"You have been invited to join {org_name} on Keyview Cloud.\n\n"
        f"Role: {role_label(role)}\n\n"
        f"Open this link to set your password and join:\n{link}\n\n"
        "This invite expires in 7 days.\n"
    )
    return _deliver_email([email], subject, body)


def signup_form_values(**values: str) -> dict:
    return {
        "first_name": values.get("first_name", ""),
        "surname": values.get("surname", ""),
        "email": values.get("email", ""),
        "organisation_name": values.get("organisation_name", ""),
        "device_id": values.get("device_id", ""),
        "invite_email": values.get("invite_email", ""),
        "invite_role": values.get("invite_role", "sub_operator"),
    }


@app.get("/signup", response_class=HTMLResponse)
def signup_page(request: Request):
    if logged_in(request):
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse(
        "signup.html",
        {"request": request, "error": None, "form": signup_form_values()},
    )


@app.post("/signup", response_class=HTMLResponse)
def signup_submit(
    request: Request,
    first_name: str = Form(""),
    surname: str = Form(""),
    email: str = Form(""),
    password: str = Form(""),
    organisation_name: str = Form(""),
    device_id: str = Form(""),
    invite_email: str = Form(""),
    invite_role: str = Form("sub_operator"),
):
    form = signup_form_values(
        first_name=first_name,
        surname=surname,
        email=email,
        organisation_name=organisation_name,
        device_id=device_id,
        invite_email=invite_email,
        invite_role=invite_role,
    )

    def reject(message: str, status: int = 400):
        return templates.TemplateResponse(
            "signup.html",
            {"request": request, "error": message, "form": form},
            status_code=status,
        )

    given = " ".join(str(first_name or "").split())
    family = " ".join(str(surname or "").split())
    display = " ".join(part for part in (given, family) if part)
    org_name = " ".join(str(organisation_name or "").split())
    mail = normalize_email(email)
    invite_mail = normalize_email(invite_email)
    role_name = normalize_role(invite_role) if invite_mail else "sub_operator"
    site = normalize_device_id(device_id) if str(device_id or "").strip() else ""
    if not given or not family or len(display) > NAME_MAX_LEN:
        return reject("Enter a name and surname, up to 80 characters together.")
    if not valid_email(mail):
        return reject("Enter a valid email address.")
    if len(password) < PASSWORD_MIN_LEN:
        return reject(f"Password must be at least {PASSWORD_MIN_LEN} characters.")
    if not org_name or len(org_name) > NAME_MAX_LEN:
        return reject("Enter an organisation name, up to 80 characters.")
    if site and not valid_device_id(site):
        return reject("Enter the device's unique ID. Use letters, numbers, _ or -.")
    if invite_mail:
        if not valid_email(invite_mail):
            return reject("Enter a valid email for the person you want to invite.")
        if invite_mail == mail:
            return reject("Invite a different email from your own.")
    if find_user_by_email(mail):
        return reject("That email is already in use. Sign in instead.")
    if invite_mail and find_user_by_email(invite_mail):
        return reject("That invited email already has an account.")

    invite_link = ""
    invite_sent = False
    try:
        with db() as conn:
            cur = conn.cursor()
            root_id = main_organisation_id(cur)
            if not root_id:
                return reject("The main organisation is not ready yet.", 500)
            if invite_mail:
                cur.execute(
                    q("SELECT id FROM org_invites WHERE email=? AND accepted_at IS NULL"),
                    (invite_mail,),
                )
                if cur.fetchone():
                    return reject("That email already has an invite waiting.")
            created = now_iso()
            cur.execute(
                q("INSERT INTO organisations (name, parent_id, locked, created_at) VALUES (?, ?, 0, ?) RETURNING id"),
                (org_name, root_id, created),
            )
            org_id = int(_row_get(cur.fetchone(), "id", 0))
            if site:
                claim_error = claim_device_for_org(cur, org_id, site)
                if claim_error:
                    raise ValueError(claim_error)
            cur.execute(
                q(
                    """
                    INSERT INTO app_users (name, email, password_hash, role, organisation_id, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """
                ),
                (display, mail, hash_password(password), "org_admin", org_id, now_iso()),
            )
            if invite_mail:
                token = save_org_invite(cur, org_id, invite_mail, role_name)
                invite_link = f"{public_base(request)}/invite/{token}"
    except ValueError as exc:
        return reject(str(exc))
    except Exception as exc:
        text = str(exc).lower()
        if "unique" in text or "duplicate" in text:
            return reject("That email is already in use. Sign in instead.")
        raise
    user = find_user_by_email(mail)
    if not user:
        return reject("Could not create the account.", 500)
    start_user_session(request, user)
    if invite_link:
        invite_sent = send_invite_email(invite_mail, org_name, role_name, invite_link)
    notice = "Account created."
    if invite_link and invite_sent:
        notice = f"Account created. An invite was emailed to {invite_mail}."
    elif invite_link:
        notice = f"Account created. Email is not sending yet. Share this invite link: {invite_link}"
    return RedirectResponse(f"/organisation?notice={quote(notice)}", status_code=303)


def load_invite(token: str) -> dict | None:
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q(
                """
                SELECT i.id, i.organisation_id, i.email, i.role, i.token, i.expires_at, i.accepted_at,
                       o.name AS organisation_name
                FROM org_invites i
                JOIN organisations o ON o.id = i.organisation_id
                WHERE i.token=?
                """
            ),
            (token,),
        )
        row = cur.fetchone()
        invite = _sql_row(row, cur)
        if invite:
            invite["role_label"] = role_label(invite.get("role"))
        return invite


def invite_is_open(invite: dict) -> str | None:
    if invite.get("accepted_at"):
        return "This invite has already been used."
    expires = parse_ts(invite.get("expires_at"))
    if expires and expires < datetime.now(timezone.utc):
        return "This invite has expired. Ask for a new one."
    return None


@app.get("/invite/{token}", response_class=HTMLResponse)
def invite_page(request: Request, token: str):
    invite = load_invite(token)
    if not invite:
        return templates.TemplateResponse(
            "invite.html",
            {"request": request, "error": "This invite link is not valid.", "invite": None, "token": token},
            status_code=404,
        )
    closed = invite_is_open(invite)
    return templates.TemplateResponse(
        "invite.html",
        {
            "request": request,
            "error": closed,
            "invite": None if closed else invite,
            "token": token,
        },
    )


@app.post("/invite/{token}", response_class=HTMLResponse)
def invite_accept(request: Request, token: str, name: str = Form(""), password: str = Form("")):
    invite = load_invite(token)
    if not invite:
        return templates.TemplateResponse(
            "invite.html",
            {"request": request, "error": "This invite link is not valid.", "invite": None, "token": token},
            status_code=404,
        )
    closed = invite_is_open(invite)
    if closed:
        return templates.TemplateResponse(
            "invite.html",
            {"request": request, "error": closed, "invite": None, "token": token},
            status_code=400,
        )
    display = " ".join(str(name or "").split())
    if not display or len(display) > NAME_MAX_LEN:
        return templates.TemplateResponse(
            "invite.html",
            {"request": request, "error": "Enter your name.", "invite": invite, "token": token},
            status_code=400,
        )
    if len(password) < PASSWORD_MIN_LEN:
        return templates.TemplateResponse(
            "invite.html",
            {
                "request": request,
                "error": f"Password must be at least {PASSWORD_MIN_LEN} characters.",
                "invite": invite,
                "token": token,
            },
            status_code=400,
        )
    if find_user_by_email(str(invite.get("email") or "")):
        return templates.TemplateResponse(
            "invite.html",
            {"request": request, "error": "That email already has an account. Sign in instead.", "invite": None, "token": token},
            status_code=400,
        )
    role_name = normalize_role(invite.get("role"))
    org_id = as_int(invite.get("organisation_id"))
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q(
                """
                INSERT INTO app_users (name, email, password_hash, role, organisation_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """
            ),
            (display, normalize_email(invite.get("email")), hash_password(password), role_name, org_id, now_iso()),
        )
        cur.execute(
            q("UPDATE org_invites SET accepted_at=? WHERE id=? AND accepted_at IS NULL"),
            (now_iso(), int(invite["id"])),
        )
    user = find_user_by_email(normalize_email(invite.get("email")))
    if not user:
        return templates.TemplateResponse(
            "invite.html",
            {"request": request, "error": "Could not join the organisation.", "invite": invite, "token": token},
            status_code=500,
        )
    start_user_session(request, user)
    return RedirectResponse("/", status_code=303)


def organisation_home_ctx(request: Request, error: str | None = None, notice: str | None = None) -> dict:
    org_id = as_int(request.session.get("org_id"))
    org = None
    team: list[dict] = []
    invites: list[dict] = []
    sites: list[str] = []
    if org_id:
        with db() as conn:
            cur = conn.cursor()
            cur.execute(q("SELECT id, name, parent_id, locked FROM organisations WHERE id=?"), (org_id,))
            row = cur.fetchone()
            org = _sql_row(row, cur)
            cur.execute(
                q("SELECT id, name, email, role FROM app_users WHERE organisation_id=? ORDER BY name, email"),
                (org_id,),
            )
            team = fetchall(cur)
            cur.execute(
                q(
                    """
                    SELECT email, role, expires_at, token FROM org_invites
                    WHERE organisation_id=? AND accepted_at IS NULL
                    ORDER BY created_at DESC
                    """
                ),
                (org_id,),
            )
            invites = fetchall(cur)
        sites = org_sites_for(org_id, include_children=False)
    names = {k: v["display_name"] for k, v in load_site_records(sites).items()} if sites else {}
    viewer_id = as_int(request.session.get("uid"))
    swatches = (
        ("#e3f0f7", "#1e6491"),
        ("#fff6e8", "#9a6700"),
        ("#e8f9ee", "#166534"),
        ("#eef2f6", "#334155"),
    )
    for person in team:
        person["role_key"] = normalize_role(person.get("role"))
        person["role_label"] = role_label(person.get("role"))
        person["is_self"] = int(person.get("id") or 0) == viewer_id
        parts = str(person.get("name") or "").split()
        letters = "".join(part[0] for part in parts[:2] if part)
        person["initials"] = letters.upper() or "?"
        bg, fg = swatches[int(person.get("id") or 0) % len(swatches)]
        person["avatar_bg"] = bg
        person["avatar_fg"] = fg
    base = public_base(request)
    for invite in invites:
        invite["role_label"] = role_label(invite.get("role"))
        token = str(invite.get("token") or "")
        invite["link"] = f"{base}/invite/{token}" if token else ""
    ctx = dashboard_shell(request)
    ctx.update(
        {
            "org": org,
            "team": team,
            "invites": invites,
            "sites": sites,
            "site_names": names,
            "error": error,
            "notice": notice,
        }
    )
    return attach_session(ctx, request, "organisation")


@app.get("/organisation", response_class=HTMLResponse)
def organisation_home(request: Request, notice: str = "", error: str = ""):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if is_manager(request):
        return RedirectResponse("/organisations", status_code=302)
    if not is_org_lead(request):
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse(
        "organisation.html",
        organisation_home_ctx(request, notice=notice or None, error=error or None),
    )


@app.post("/organisation/sites", response_class=HTMLResponse)
def organisation_add_site(request: Request, device_id: str = Form("")):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_org_lead(request):
        raise HTTPException(403, "organisation admin only")
    org_id = as_int(request.session.get("org_id"))
    if not org_id or org_is_locked(org_id):
        return templates.TemplateResponse(
            "organisation.html",
            organisation_home_ctx(request, error="This organisation is locked."),
            status_code=403,
        )
    with db() as conn:
        cur = conn.cursor()
        claim_error = claim_device_for_org(cur, org_id, device_id)
    if claim_error:
        return templates.TemplateResponse(
            "organisation.html",
            organisation_home_ctx(request, error=claim_error),
            status_code=400,
        )
    refresh_session_sites(request)
    site = normalize_device_id(device_id)
    return RedirectResponse(f"/organisation?notice={quote('Site ' + site + ' added.')}", status_code=303)


@app.post("/organisation/sites/delete")
def organisation_delete_site(request: Request, device_id: str = Form("")):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_org_lead(request):
        raise HTTPException(403, "organisation admin only")
    org_id = as_int(request.session.get("org_id"))
    site = normalize_device_id(device_id)
    if not org_id or not valid_device_id(site):
        return RedirectResponse("/organisation?error=" + quote("That site could not be removed."), status_code=303)
    with db() as conn:
        cur = conn.cursor()
        cur.execute(q("SELECT organisation_id FROM org_sites WHERE device_id=?"), (site,))
        row = cur.fetchone()
        owner = int(_row_get(row, "organisation_id", 0) or 0) if row else 0
        if owner != org_id:
            return RedirectResponse(
                "/organisation?error=" + quote("That site is not in this organisation."),
                status_code=303,
            )
        cur.execute(q("DELETE FROM org_sites WHERE organisation_id=? AND device_id=?"), (org_id, site))
        cur.execute(q("DELETE FROM user_sites WHERE device_id=?"), (site,))
    refresh_session_sites(request)
    label = load_site_records([site]).get(site, {}).get("display_name") or site
    return RedirectResponse(
        f"/organisation?notice={quote(str(label) + ' was removed from this organisation.')}",
        status_code=303,
    )


@app.post("/organisation/people/{user_id}/delete")
def organisation_delete_person(request: Request, user_id: int):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_org_lead(request):
        raise HTTPException(403, "organisation admin only")
    org_id = as_int(request.session.get("org_id"))
    if as_int(request.session.get("uid")) == user_id:
        return RedirectResponse(
            "/organisation?error=" + quote("You can't remove your own account."),
            status_code=303,
        )
    with db() as conn:
        cur = conn.cursor()
        cur.execute(q("SELECT id, name, organisation_id FROM app_users WHERE id=?"), (user_id,))
        row = cur.fetchone()
        person = _sql_row(row, cur)
        if not person or as_int(person.get("organisation_id")) != org_id:
            return RedirectResponse(
                "/organisation?error=" + quote("That person is not in this organisation."),
                status_code=303,
            )
        cur.execute(q("DELETE FROM user_sites WHERE user_id=?"), (user_id,))
        cur.execute(q("DELETE FROM password_resets WHERE user_id=?"), (user_id,))
        cur.execute(q("DELETE FROM app_users WHERE id=? AND organisation_id=?"), (user_id, org_id))
    name = str(person.get("name") or "User")
    return RedirectResponse(f"/organisation?notice={quote(name + ' was removed.')}", status_code=303)


@app.post("/organisation/invites", response_class=HTMLResponse)
def organisation_invite(request: Request, email: str = Form(""), role: str = Form("sub_operator")):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    if not is_org_lead(request):
        raise HTTPException(403, "organisation admin only")
    org_id = as_int(request.session.get("org_id"))
    mail = normalize_email(email)
    role_name = normalize_role(role)
    if not org_id or org_is_locked(org_id):
        return templates.TemplateResponse(
            "organisation.html",
            organisation_home_ctx(request, error="This organisation is locked."),
            status_code=403,
        )
    if not valid_email(mail):
        return templates.TemplateResponse(
            "organisation.html",
            organisation_home_ctx(request, error="Enter a valid email address."),
            status_code=400,
        )
    if find_user_by_email(mail):
        return templates.TemplateResponse(
            "organisation.html",
            organisation_home_ctx(request, error="That email already has an account."),
            status_code=400,
        )
    orgs = load_organisations()
    org = next((row for row in orgs if int(row["id"]) == org_id), None)
    org_name = str((org or {}).get("name") or "your organisation")
    with db() as conn:
        cur = conn.cursor()
        token = save_org_invite(cur, org_id, mail, role_name)
    link = f"{public_base(request)}/invite/{token}"
    sent = send_invite_email(mail, org_name, role_name, link)
    if sent:
        notice = f"Invite emailed to {mail}."
    else:
        notice = "Invite saved. Email is not sending yet. Copy the link under Invites waiting."
    return RedirectResponse(f"/organisation?notice={quote(notice)}", status_code=303)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    notice = None
    if request.query_params.get("reset") == "1":
        notice = "Password updated. Sign in with your new password."
    return templates.TemplateResponse("login.html", {"request": request, "error": None, "notice": notice})


@app.get("/forgot", response_class=HTMLResponse)
def forgot_page(request: Request):
    if logged_in(request):
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("forgot.html", {"request": request, "error": None, "notice": None})


@app.post("/forgot", response_class=HTMLResponse)
def forgot_submit(request: Request, email: str = Form("")):
    if logged_in(request):
        return RedirectResponse("/", status_code=302)
    mail = normalize_email(email)
    if not smtp_configured():
        return templates.TemplateResponse(
            "forgot.html",
            {
                "request": request,
                "error": "Password reset email is not set up yet. Ask your organisation administrator for a new password.",
                "notice": None,
            },
            status_code=503,
        )
    notice = "If that email has an account, a reset link is on its way. It expires in 2 hours."
    if not valid_email(mail):
        return templates.TemplateResponse(
            "forgot.html",
            {"request": request, "error": "Enter the email address on the account.", "notice": None},
            status_code=400,
        )
    user = find_user_by_email(mail)
    if user and user.get("id"):
        with db() as conn:
            cur = conn.cursor()
            token = save_password_reset(cur, int(user["id"]))
        send_reset_email(mail, f"{public_base(request)}/reset/{token}")
    return templates.TemplateResponse("forgot.html", {"request": request, "error": None, "notice": notice})


@app.get("/reset/{token}", response_class=HTMLResponse)
def reset_page(request: Request, token: str):
    closed = reset_is_open(load_password_reset(token))
    return templates.TemplateResponse(
        "reset.html",
        {"request": request, "error": closed, "token": token, "open": closed is None},
        status_code=200 if closed is None else 400,
    )


@app.post("/reset/{token}", response_class=HTMLResponse)
def reset_submit(request: Request, token: str, password: str = Form(""), confirm: str = Form("")):
    row = load_password_reset(token)
    closed = reset_is_open(row)
    if closed or not row:
        return templates.TemplateResponse(
            "reset.html",
            {"request": request, "error": closed or "This reset link is not valid.", "token": token, "open": False},
            status_code=400,
        )
    if len(password) < PASSWORD_MIN_LEN:
        return templates.TemplateResponse(
            "reset.html",
            {
                "request": request,
                "error": f"Password must be at least {PASSWORD_MIN_LEN} characters.",
                "token": token,
                "open": True,
            },
            status_code=400,
        )
    if password != confirm:
        return templates.TemplateResponse(
            "reset.html",
            {"request": request, "error": "The two passwords do not match.", "token": token, "open": True},
            status_code=400,
        )
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q("UPDATE app_users SET password_hash=? WHERE id=?"),
            (hash_password(password), int(row["user_id"])),
        )
        cur.execute(
            q("UPDATE password_resets SET used_at=? WHERE id=? AND used_at IS NULL"),
            (now_iso(), int(row["id"])),
        )
    return RedirectResponse("/login?reset=1", status_code=303)


@app.post("/login")
def login_submit(request: Request, password: str = Form(...), email: str = Form("")):
    mail = normalize_email(email)
    expected = os.environ.get("ADMIN_PASSWORD") or ""
    if (not mail or mail in {"admin", "administrator"}) and expected:
        if secrets.compare_digest(password, expected):
            start_admin_session(request)
            return RedirectResponse("/", status_code=302)
    user = find_user_by_email(mail) if mail else None
    if user and verify_password(password, str(user.get("password_hash") or "")):
        start_user_session(request, user)
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": "Invalid email or password", "notice": None},
        status_code=401,
    )


@app.post("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


def _wants_json(request: Request) -> bool:
    return "application/json" in (request.headers.get("accept") or "").lower()


def _redirect_next(next_url: str) -> RedirectResponse:
    nxt = (next_url or "/").strip()
    if not nxt.startswith("/") or nxt.startswith("//"):
        nxt = "/"
    return RedirectResponse(nxt, status_code=303)


def _queue_job(device_id: str, instrument_id: int, kind: str, register: int, raw: int, display_value: float) -> None:
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q(
                """
                INSERT INTO jobs (
                    device_id, instrument_id, kind, register, raw_value, display_value, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'pending', ?)
                """
            ),
            (device_id, instrument_id, kind, register, raw, display_value, now_iso()),
        )


@app.post("/setpoint")
def set_setpoint(
    request: Request,
    instrument_id: int = Form(...),
    device_id: str = Form(...),
    setpoint_c: str = Form(...),
    next: str = Form(""),
):
    require_device_writable(request, device_id)
    value = as_float(str(setpoint_c).replace(",", "."))
    if value is None or value < -50 or value > 40:
        raise HTTPException(400, "setpoint out of range")
    raw = int(round(value * 10))
    _queue_job(device_id, instrument_id, "setpoint", SETPOINT_REGISTER, raw, value)
    if _wants_json(request):
        return JSONResponse(
            {
                "ok": True,
                "queued": True,
                "instrument_id": instrument_id,
                "setpoint_c": value,
            }
        )
    return _redirect_next(next)


@app.post("/site/name")
def set_site_name(
    request: Request,
    device_id: str = Form(...),
    name: str = Form(""),
    next: str = Form(""),
):
    require_device_writable(request, device_id)
    store_name = " ".join(str(name or "").split())
    if len(store_name) > NAME_MAX_LEN:
        raise HTTPException(400, "name must be 1 to 80 characters")
    with db() as conn:
        cur = conn.cursor()
        ensure_site_record(cur, device_id)
        cur.execute(
            q("UPDATE sites SET store_name=? WHERE device_id=?"),
            (store_name, normalize_device_id(device_id)),
        )
    display = site_display_name(store_name, normalize_device_id(device_id))
    if _wants_json(request):
        return JSONResponse(
            {
                "ok": True,
                "device_id": normalize_device_id(device_id),
                "name": display,
                "store_name": store_name,
            }
        )
    return _redirect_next(next)


@app.post("/instrument/alarms")
def set_instrument_alarms(
    request: Request,
    instrument_id: int = Form(...),
    device_id: str = Form(...),
    high_alarm_temp: str = Form(""),
    high_alarm_delay_min: str = Form(""),
    alarm_reset_temp: str = Form(""),
    alarm_notify_delay_min: str = Form(""),
    next: str = Form(""),
):
    require_device_writable(request, device_id)
    high = as_float(high_alarm_temp)
    delay = as_float(high_alarm_delay_min)
    reset = as_float(alarm_reset_temp)
    notify = as_float(alarm_notify_delay_min)
    if high is not None and (high < -50 or high > 80):
        raise HTTPException(400, "high alarm temperature out of range")
    if reset is not None and (reset < -50 or reset > 80):
        raise HTTPException(400, "alarm reset temperature out of range")
    if high is not None and reset is not None and reset > high:
        raise HTTPException(400, "reset temperature must be at or below the high alarm")
    for label, value in (("high delay", delay), ("notification delay", notify)):
        if value is not None and (value < 0 or value > 10080):
            raise HTTPException(400, f"{label} must be 0 to 10080 minutes")
    if high is None:
        delay = None
        reset = None
        notify = None
    mail_out: list[dict] = []
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q("SELECT device_id FROM instruments WHERE device_id=? AND instrument_id=?"),
            (device_id, instrument_id),
        )
        if not cur.fetchone():
            raise HTTPException(404, "instrument not found")
        cur.execute(
            q(
                """
                UPDATE instruments SET
                    alarm_high_temp=?, alarm_high_delay_min=?, alarm_reset_temp=?,
                    alarm_notify_delay_min=?, alarm_active=0, alarm_high_since=NULL,
                    alarm_active_since=NULL, alarm_email_sent=0
                WHERE device_id=? AND instrument_id=?
                """
            ),
            (high, delay if delay is not None else 0, reset, notify if notify is not None else 0, device_id, instrument_id),
        )
        cur.execute(
            q(INSTRUMENT_SELECT + " WHERE device_id=? AND instrument_id=?"),
            (device_id, instrument_id),
        )
        found = cur.fetchone()
        inst = _sql_row(found, cur) if found else None
        if inst:
            apply_cloud_alarm(cur, inst, mail_out=mail_out)
    flush_alarm_mail(mail_out)
    if _wants_json(request):
        return JSONResponse(
            {
                "ok": True,
                "instrument_id": instrument_id,
                "high_alarm_temp": high,
                "high_alarm_delay_min": delay,
                "alarm_reset_temp": reset,
                "alarm_notify_delay_min": notify,
            }
        )
    return _redirect_next(next)


@app.post("/instrument/name")
def set_instrument_name(
    request: Request,
    instrument_id: int = Form(...),
    device_id: str = Form(...),
    name: str = Form(...),
    next: str = Form(""),
):
    require_device_writable(request, device_id)
    label = " ".join(str(name or "").split())
    if not label or len(label) > NAME_MAX_LEN:
        raise HTTPException(400, "name must be 1 to 80 characters")
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q("SELECT device_id FROM instruments WHERE device_id=? AND instrument_id=?"),
            (device_id, instrument_id),
        )
        if not cur.fetchone():
            raise HTTPException(404, "instrument not found")
        cur.execute(
            q("UPDATE instruments SET label=? WHERE device_id=? AND instrument_id=?"),
            (label, device_id, instrument_id),
        )
    if _wants_json(request):
        return JSONResponse(
            {"ok": True, "instrument_id": instrument_id, "name": label}
        )
    return _redirect_next(next)


@app.post("/power")
def set_power(
    request: Request,
    instrument_id: int = Form(...),
    device_id: str = Form(...),
    power: int = Form(...),
    next: str = Form(""),
):
    require_device_writable(request, device_id)
    want_on = bool(int(power))
    raw = POWER_ON_MASK if want_on else POWER_OFF_MASK
    _queue_job(device_id, instrument_id, "power", POWER_REGISTER, raw, 1 if want_on else 0)
    if _wants_json(request):
        return JSONResponse(
            {
                "ok": True,
                "queued": True,
                "instrument_id": instrument_id,
                "power": want_on,
            }
        )
    return _redirect_next(next)


@app.post("/defrost")
def set_defrost(
    request: Request,
    instrument_id: int = Form(...),
    device_id: str = Form(...),
    next: str = Form(""),
):
    require_device_writable(request, device_id)
    _queue_job(device_id, instrument_id, "defrost", POWER_REGISTER, DEFROST_MASK, 1)
    if _wants_json(request):
        return JSONResponse(
            {
                "ok": True,
                "queued": True,
                "instrument_id": instrument_id,
                "defrost": True,
            }
        )
    return _redirect_next(next)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), reload=True)

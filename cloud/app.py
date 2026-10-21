#!/usr/bin/env python3
"""EWNext cloud: ingest gateway telemetry, store history, queue setpoint jobs."""

from __future__ import annotations

import hashlib
import hmac
import json
import math
import os
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import quote

from fastapi import FastAPI, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

MAX_INSTRUMENTS_PER_SITE = 15
SETPOINT_REGISTER = 32769  # EWNext SEt, 0.1 C
POWER_REGISTER = 4123  # Ronon / RoFFoFF command word
POWER_ON_MASK = 4  # Ronon Device on
POWER_OFF_MASK = 8  # RoFFoFF Device off
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
        conn.commit()
        _ensure_org_tree(cur)
        conn.commit()


def _ensure_column(cur, table: str, column: str, typedef: str) -> None:
    if _is_pg():
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
    if request.session.get("admin"):
        return True
    return normalize_role(request.session.get("role")) == "org_admin"


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


def attach_session(ctx: dict, request: Request, page: str) -> dict:
    ctx.update(
        {
            "request": request,
            "page": page,
            "admin_name": session_name(request),
            "is_manager": is_manager(request),
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


def user_portfolio(user: dict) -> list[str] | None:
    role = normalize_role(user.get("role"))
    org_id = as_int(user.get("organisation_id"))
    if role == "org_admin":
        return None
    if org_is_locked(org_id):
        return []
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
    request.session["admin"] = role == "org_admin"
    request.session["org_id"] = as_int(user.get("organisation_id"))
    portfolio = user_portfolio(user)
    request.session["sites"] = [] if portfolio is None else portfolio


def start_admin_session(request: Request) -> None:
    request.session.clear()
    request.session["admin"] = True
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


def display_name(label: Any, server_name: Any, instrument_id: int) -> str:
    custom = str(label or "").strip()
    if custom:
        return custom
    raw = str(server_name or "").strip()
    if not raw:
        return f"Instrument {instrument_id}"
    return raw.replace("_", " ")


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


def instrument_online(row: dict) -> bool:
    if int(row.get("last_stale") or 0):
        return False
    seen = parse_ts(row.get("last_seen"))
    if seen is None:
        return False
    return (datetime.now(timezone.utc) - seen).total_seconds() < 180


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
        issue_label = issue_sites[0]["device_id"]
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
        site = widget.get("device_id") or "unknown"
        when = relative_ago(widget.get("last_seen"))
        href = widget.get("href") or "/"
        if widget.get("alarm"):
            items.append(
                {
                    "kind": "alarm",
                    "title": f"Alarm: {name}",
                    "detail": site,
                    "when": when,
                    "href": href,
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
    row: dict, pending: dict | None = None, pending_power_job: dict | None = None
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
    live_on = not bool(flags.get("standby"))
    pending_on = queued_power(pending_power_job, live_on)
    inst.update(
        {
            "name": display_name(inst.get("label"), inst.get("server_name"), inst.get("instrument_id") or 0),
            "online": online,
            "cooling": flags["cooling"],
            "fan": flags["fan"],
            "humidity": flags["humidity"],
            "defrost_active": flags["defrost_active"],
            "alarm": flags["alarm"],
            "standby": flags["standby"],
            "live_powered": live_on,
            "powered": live_on if pending_on is None else pending_on,
            "pending_power": pending_on,
            "cp1_label": "ON" if flags["cooling"] else "OFF",
            "def1_label": "ON" if flags["defrost_active"] else "OFF",
            "fan_label": "ON" if flags["fan"] else "OFF",
            "temperature_label": format_temp(probe1),
            "defrost_label": format_temp(inst.get("last_probe2")),
            "setpoint_label": format_temp(pending_value if pending_value is not None else setpoint),
            "pending_setpoint": pending_value,
            "href": f"/instrument/{inst.get('instrument_id') or 0}?device={quote(str(inst.get('device_id') or ''), safe='')}",
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
        "alarm": 1 if flags["alarm"] else 0,
    }


def load_history(device_id: str, instrument_id: int, range_key: str) -> list[dict]:
    span = HISTORY_RANGES.get(range_key, HISTORY_RANGES["24h"])
    cutoff = (datetime.now(timezone.utc) - timedelta(seconds=span)).isoformat()
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q(
                """
                SELECT probe1, probe2, setpoint, flags, values_json,
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
    init_db()


@app.get("/health")
def health() -> dict:
    return {"ok": True, "service": "ewnext-cloud"}


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
            q("SELECT last_setpoint FROM instruments WHERE device_id=? AND instrument_id=?"),
            (device_id, instrument_id),
        )
        prev = cur.fetchone()
        last_setpoint = live_setpoint
        if last_setpoint is None and prev:
            last_setpoint = prev[0] if not isinstance(prev, dict) else prev.get("last_setpoint")
        stale = 1 if body.get("stale") else 0
        if site_accepts_instrument(cur, device_id, instrument_id):
          if _is_pg():
            cur.execute(
                """
                INSERT INTO instruments (
                    device_id, instrument_id, server_name, last_probe1, last_probe2,
                    last_setpoint, last_flags, last_stale, last_seen
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (device_id, instrument_id) DO UPDATE SET
                    server_name = EXCLUDED.server_name,
                    last_probe1 = EXCLUDED.last_probe1,
                    last_probe2 = EXCLUDED.last_probe2,
                    last_setpoint = COALESCE(EXCLUDED.last_setpoint, instruments.last_setpoint),
                    last_flags = EXCLUDED.last_flags,
                    last_stale = EXCLUDED.last_stale,
                    last_seen = EXCLUDED.last_seen
                """,
                (
                    device_id,
                    instrument_id,
                    body.get("server_name"),
                    probe1,
                    probe2,
                    last_setpoint,
                    flags,
                    stale,
                    received,
                ),
            )
          else:
            cur.execute(
                """
                INSERT INTO instruments (
                    device_id, instrument_id, server_name, last_probe1, last_probe2,
                    last_setpoint, last_flags, last_stale, last_seen
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (device_id, instrument_id) DO UPDATE SET
                    server_name = excluded.server_name,
                    last_probe1 = excluded.last_probe1,
                    last_probe2 = excluded.last_probe2,
                    last_setpoint = COALESCE(excluded.last_setpoint, instruments.last_setpoint),
                    last_flags = excluded.last_flags,
                    last_stale = excluded.last_stale,
                    last_seen = excluded.last_seen
                """,
                (
                    device_id,
                    instrument_id,
                    body.get("server_name"),
                    probe1,
                    probe2,
                    last_setpoint,
                    flags,
                    stale,
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
    return JSONResponse({"ok": True, "jobs": payload_jobs})


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
    return {"ok": True}


def dashboard_shell(request: Request | None = None) -> dict:
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT device_id, instrument_id, server_name, label, last_probe1, last_probe2,
                   last_setpoint, last_flags, last_stale, last_seen
            FROM instruments
            ORDER BY device_id, instrument_id
            """
        )
        instruments = fetchall(cur)
    allowed = allowed_site_ids(request) if request is not None else None
    if allowed is not None:
        instruments = [row for row in instruments if str(row.get("device_id")) in allowed]
    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, device_id, instrument_id, kind, display_value, status, created_at
            FROM jobs
            WHERE status='pending' AND kind IN ('setpoint', 'power')
            ORDER BY id ASC
            """
        )
        pending_jobs = fetchall(cur)
    pending_setpoints = {}
    pending_power = {}
    for job in pending_jobs:
        key = (job.get("device_id"), job.get("instrument_id"))
        if job.get("kind") == "power":
            pending_power[key] = job
        else:
            pending_setpoints[key] = job
    widgets = [
        decorate_instrument(
            inst,
            pending_setpoints.get((inst.get("device_id"), inst.get("instrument_id"))),
            pending_power.get((inst.get("device_id"), inst.get("instrument_id"))),
        )
        for inst in instruments
    ]
    sites: list[dict[str, Any]] = []
    by_site: dict[str, list[dict]] = {}
    for widget in widgets:
        key = str(widget.get("device_id") or "unknown")
        by_site.setdefault(key, []).append(widget)
    for device_id, insts in by_site.items():
        meta = site_status_meta(insts)
        encoded = quote(device_id, safe="")
        sites.append(
            {
                "device_id": device_id,
                "instruments": insts,
                "count": len(insts),
                "max_n": MAX_INSTRUMENTS_PER_SITE,
                "href": f"/sites/{encoded}",
                **meta,
            }
        )
    online_n = sum(1 for w in widgets if w.get("online"))
    alarm_n = sum(1 for w in widgets if w.get("alarm"))
    offline_n = len(widgets) - online_n
    if not widgets:
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
    return {
        "instruments": widgets,
        "sites": sites,
        "health": health,
        "online_n": online_n,
        "alarm_n": alarm_n,
        "site_n": len(sites),
        "kpis": compute_kpis(widgets, sites),
        "alarm_feed": build_alarm_feed(widgets),
        "admin_name": session_name(request) if request is not None else "Admin",
        "is_manager": is_manager(request) if request is not None else True,
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
    ctx = dashboard_shell(request)
    site = next((row for row in ctx["sites"] if row.get("device_id") == device_id), None)
    if site is None:
        raise HTTPException(404, "site not found")
    scoped = list(site.get("instruments") or [])
    ctx["kpis"] = compute_kpis(scoped, [site])
    ctx["alarm_feed"] = build_alarm_feed(scoped)
    attach_session(ctx, request, "site")
    ctx.update({"site": site, "sites": [site]})
    return templates.TemplateResponse("site.html", ctx)


@app.get("/instrument/{instrument_id}", response_class=HTMLResponse)
def instrument_page(request: Request, instrument_id: int, device: str = "", range: str = "24h"):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    ctx = dashboard_shell(request)
    device_id = (device or "").strip()
    inst = None
    for widget in ctx["instruments"]:
        if int(widget.get("instrument_id") or 0) != instrument_id:
            continue
        if not device_id or str(widget.get("device_id")) == device_id:
            inst = widget
            break
    if inst is None:
        raise HTTPException(404, "instrument not found")
    if not can_access_site(request, str(inst.get("device_id") or "")):
        raise HTTPException(403, "no access to this site")
    range_key = range if range in HISTORY_RANGES else "24h"
    attach_session(ctx, request, "instrument")
    ctx.update(
        {
            "inst": inst,
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
    require_site_access(request, device_id)
    range_key = range if range in HISTORY_RANGES else "24h"
    return {
        "instrument_id": instrument_id,
        "device_id": device_id,
        "range": range_key,
        "points": load_history(device_id, instrument_id, range_key),
    }


@app.get("/jobs", response_class=HTMLResponse)
def jobs_page(request: Request):
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
    attach_session(ctx, request, "jobs")
    ctx.update({"jobs": jobs, "readings": readings})
    return templates.TemplateResponse("jobs.html", ctx)


def known_site_ids() -> list[str]:
    with db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT device_id FROM instruments ORDER BY device_id")
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
    ctx.update(
        {
            "app_users": load_app_users(),
            "known_sites": known_site_ids(),
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
    ctx.update(
        {
            "root_org": root,
            "sub_orgs": children,
            "unassigned_sites": unassigned,
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


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


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
        {"request": request, "error": "Invalid email or password"},
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
    setpoint_c: float = Form(...),
    next: str = Form(""),
):
    require_site_access(request, device_id)
    if setpoint_c < -50 or setpoint_c > 40:
        raise HTTPException(400, "setpoint out of range")
    raw = int(round(setpoint_c * 10))
    _queue_job(device_id, instrument_id, "setpoint", SETPOINT_REGISTER, raw, setpoint_c)
    if _wants_json(request):
        return JSONResponse(
            {
                "ok": True,
                "queued": True,
                "instrument_id": instrument_id,
                "setpoint_c": setpoint_c,
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
    require_site_access(request, device_id)
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
    require_site_access(request, device_id)
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), reload=True)

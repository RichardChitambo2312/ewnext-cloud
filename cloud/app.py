#!/usr/bin/env python3
"""EWNext cloud: ingest gateway telemetry, store history, queue setpoint jobs."""

from __future__ import annotations

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
PENDING_SETPOINT_TTL_SEC = 10
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


def logged_in(request: Request) -> bool:
    return bool(request.session.get("admin"))


def require_admin(request: Request) -> None:
    if not logged_in(request):
        raise HTTPException(401, "login required")


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


def dashboard_shell() -> dict:
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
        site_online = sum(1 for w in insts if w.get("online"))
        site_alarm = sum(1 for w in insts if w.get("alarm"))
        sites.append(
            {
                "device_id": device_id,
                "instruments": insts,
                "count": len(insts),
                "max_n": MAX_INSTRUMENTS_PER_SITE,
                "online_n": site_online,
                "alarm_n": site_alarm,
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
    }


@app.get("/api/instruments")
def instruments_api(request: Request):
    require_admin(request)
    ctx = dashboard_shell()
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
    ctx = dashboard_shell()
    ctx.update({"request": request, "page": "instruments"})
    return templates.TemplateResponse("index.html", ctx)


@app.get("/instrument/{instrument_id}", response_class=HTMLResponse)
def instrument_page(request: Request, instrument_id: int, device: str = "", range: str = "24h"):
    if not logged_in(request):
        return RedirectResponse("/login", status_code=302)
    ctx = dashboard_shell()
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
    range_key = range if range in HISTORY_RANGES else "24h"
    ctx.update(
        {
            "request": request,
            "page": "instrument",
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
        ctx = dashboard_shell()
        for widget in ctx["instruments"]:
            if int(widget.get("instrument_id") or 0) == instrument_id:
                device_id = str(widget.get("device_id") or "")
                break
    if not device_id:
        raise HTTPException(404, "instrument not found")
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
    ctx = dashboard_shell()
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
    ctx.update(
        {
            "request": request,
            "page": "jobs",
            "jobs": jobs,
            "readings": readings,
        }
    )
    return templates.TemplateResponse("jobs.html", ctx)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/login")
def login_submit(request: Request, password: str = Form(...)):
    expected = os.environ.get("ADMIN_PASSWORD") or ""
    if not expected or not secrets.compare_digest(password, expected):
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Invalid password"},
            status_code=401,
        )
    request.session["admin"] = True
    return RedirectResponse("/", status_code=302)


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
    require_admin(request)
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
    require_admin(request)
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
    require_admin(request)
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

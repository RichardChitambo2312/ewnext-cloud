"""Web Push for the home-screen app. Keys stay in the database and are never logged."""

from __future__ import annotations

import base64
import json
import queue
import threading
import time
from typing import Any

_events: queue.Queue[dict[str, str]] = queue.Queue()
_started = False
_lock = threading.Lock()
_offline_devices: set[str] = set()
_vapid_cache: tuple[str, str] | None = None


def claim_device_offline(device_id: str) -> bool:
    key = str(device_id or "")
    with _lock:
        if key in _offline_devices:
            return False
        _offline_devices.add(key)
        return True


def device_was_offline(device_id: str) -> bool:
    with _lock:
        return str(device_id or "") in _offline_devices


def release_device_offline(device_id: str) -> None:
    with _lock:
        _offline_devices.discard(str(device_id or ""))


def enqueue(device_id: str, title: str, body: str, url: str, tag: str) -> None:
    _events.put(
        {
            "device_id": str(device_id or ""),
            "title": title,
            "body": body,
            "url": url or "/",
            "tag": tag or "keyview",
        }
    )


def start() -> None:
    global _started
    with _lock:
        if _started:
            return
        _started = True
    threading.Thread(target=_send_loop, name="keyview-push", daemon=True).start()
    threading.Thread(target=_sweep_loop, name="keyview-push-sweep", daemon=True).start()


def public_application_key() -> str:
    public, _private = _keys()
    return public


def save_subscription(user_id: int | None, super_admin: bool, endpoint: str, p256dh: str, auth: str) -> None:
    from app import db, now_iso, q

    with db() as conn:
        cur = conn.cursor()
        cur.execute(
            q(
                """
                INSERT INTO push_subscriptions (user_id, super_admin, endpoint, p256dh, auth, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (endpoint) DO UPDATE SET
                    user_id=excluded.user_id,
                    super_admin=excluded.super_admin,
                    p256dh=excluded.p256dh,
                    auth=excluded.auth
                """
            ),
            (user_id, 1 if super_admin else 0, endpoint, p256dh, auth, now_iso()),
        )


def _keys() -> tuple[str, str]:
    global _vapid_cache
    if _vapid_cache:
        return _vapid_cache
    from app import db, q, _row_get

    with db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT public_key, private_key FROM push_vapid WHERE id=1")
        row = cur.fetchone()
        if row:
            _vapid_cache = (str(_row_get(row, "public_key", 0)), str(_row_get(row, "private_key", 1)))
            return _vapid_cache
        public, private = _generate_keys()
        cur.execute(
            q("INSERT INTO push_vapid (id, public_key, private_key) VALUES (1, ?, ?)"),
            (public, private),
        )
    _vapid_cache = (public, private)
    return _vapid_cache


def _generate_keys() -> tuple[str, str]:
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
    from py_vapid import Vapid

    vapid = Vapid()
    vapid.generate_keys()
    raw = vapid.public_key.public_bytes(Encoding.X962, PublicFormat.UncompressedPoint)
    public = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    private = vapid.private_pem().decode("ascii")
    return public, private


def _send_loop() -> None:
    while True:
        item = _events.get()
        time.sleep(0.4)
        try:
            _deliver(item)
        except Exception:
            continue


def _deliver(item: dict[str, str]) -> None:
    try:
        from py_vapid import Vapid
        from pywebpush import WebPushException, webpush
    except Exception:
        return
    public, private = _keys()
    vapid = Vapid.from_pem(private.encode("ascii"))
    payload = json.dumps(
        {"title": item["title"], "body": item["body"], "url": item["url"], "tag": item["tag"]},
        separators=(",", ":"),
    )
    for sub in _recipients(item["device_id"]):
        try:
            webpush(
                subscription_info={
                    "endpoint": sub["endpoint"],
                    "keys": {"p256dh": sub["p256dh"], "auth": sub["auth"]},
                },
                data=payload,
                vapid_private_key=vapid,
                vapid_claims={"sub": "mailto:alerts@ewnext-cloud.onrender.com"},
                ttl=86400,
                timeout=8,
            )
        except WebPushException as exc:
            response = getattr(exc, "response", None)
            status = getattr(response, "status_code", None)
            if status in (404, 410):
                _drop_endpoint(sub["endpoint"])
        except Exception:
            continue
    del public


def _drop_endpoint(endpoint: str) -> None:
    from app import db, q

    with db() as conn:
        cur = conn.cursor()
        cur.execute(q("DELETE FROM push_subscriptions WHERE endpoint=?"), (endpoint,))


def _recipients(device_id: str) -> list[dict[str, Any]]:
    from app import db, fetchall, load_app_users, user_portfolio

    with db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT user_id, super_admin, endpoint, p256dh, auth FROM push_subscriptions")
        rows = fetchall(cur)
    users = {int(user["id"]): user for user in load_app_users()}
    chosen: list[dict[str, Any]] = []
    for row in rows:
        if int(row.get("super_admin") or 0):
            chosen.append(row)
            continue
        uid = row.get("user_id")
        user = users.get(int(uid)) if uid is not None else None
        if not user:
            continue
        portfolio = user_portfolio(user)
        if portfolio is None or device_id in portfolio:
            chosen.append(row)
    return chosen


def _sweep_loop() -> None:
    time.sleep(20)
    while True:
        try:
            _sweep()
        except Exception:
            pass
        time.sleep(30)


def _sweep() -> None:
    from app import INSTRUMENT_SELECT, apply_cloud_alarm, db, fetchall

    with db() as conn:
        cur = conn.cursor()
        cur.execute(INSTRUMENT_SELECT)
        for row in fetchall(cur):
            apply_cloud_alarm(cur, row)

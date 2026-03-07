from __future__ import annotations

import asyncio
import sqlite3
from typing import Any, Optional

from sales_agent.sales_core.db import find_user_by_phone, resolve_preferred_thread_for_user
from sales_agent.sales_core.mango_client import MangoCallEvent, MangoClient, MangoClientError


def extract_mango_user_and_thread(
    conn: sqlite3.Connection,
    *,
    event: MangoCallEvent,
) -> tuple[Optional[int], Optional[str]]:
    payload = event.payload if isinstance(event.payload, dict) else {}
    user_id: Optional[int] = None
    thread_id: Optional[str] = None

    payload_user = payload.get("user_id")
    if isinstance(payload_user, int) and payload_user > 0:
        row = conn.execute("SELECT id FROM users WHERE id = ? LIMIT 1", (payload_user,)).fetchone()
        if row is not None:
            user_id = int(payload_user)

    payload_thread = str(payload.get("thread_id") or "").strip()
    if payload_thread:
        thread_id = payload_thread
        if thread_id.startswith("tg:"):
            token = thread_id[3:]
            if token.isdigit():
                user_id = int(token)
        elif thread_id.startswith("biz:") and user_id is None:
            row = conn.execute(
                "SELECT user_id FROM business_threads WHERE thread_key = ? LIMIT 1",
                (thread_id,),
            ).fetchone()
            if row and row["user_id"] is not None:
                user_id = int(row["user_id"])

    if user_id is None and event.phone:
        user_id = find_user_by_phone(conn, phone=event.phone)

    if user_id is not None and thread_id is None:
        thread_id = resolve_preferred_thread_for_user(conn, user_id=user_id)
    return user_id, thread_id


async def fetch_mango_poll_events_with_retries(
    *,
    client: MangoClient,
    since: str,
    limit: int,
    attempts: int,
    base_backoff_seconds: int,
) -> tuple[list[MangoCallEvent], int]:
    total_attempts = max(1, int(attempts))
    backoff = max(0, int(base_backoff_seconds))
    last_error: Optional[MangoClientError] = None
    for attempt in range(1, total_attempts + 1):
        try:
            events = client.list_recent_calls(since_iso=since, limit=limit)
            return events, attempt
        except MangoClientError as exc:
            last_error = exc
            if attempt < total_attempts and backoff > 0:
                await asyncio.sleep(float(backoff) * (2 ** (attempt - 1)))
    assert last_error is not None
    raise last_error


def event_from_mango_record(item: dict[str, Any]) -> Optional[MangoCallEvent]:
    payload = item.get("payload")
    payload_dict = payload if isinstance(payload, dict) else {}
    parser = MangoClient(base_url="https://offline.local", token="offline")
    parsed = parser.parse_call_event(payload_dict)
    if parsed is not None:
        return parsed
    event_id = str(item.get("event_id") or "").strip()
    if not event_id:
        return None
    call_external_id = str(item.get("call_external_id") or "").strip()
    return MangoCallEvent(
        event_id=event_id,
        call_id=call_external_id or event_id,
        phone="",
        recording_url="",
        transcript_hint="",
        occurred_at="",
        payload=payload_dict,
    )


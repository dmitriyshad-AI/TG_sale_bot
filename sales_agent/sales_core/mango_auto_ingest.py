from __future__ import annotations

import asyncio
import sqlite3
from typing import Any, Optional

from sales_agent.sales_core.db import find_user_by_phone, resolve_preferred_thread_for_user
from sales_agent.sales_core.mango_client import MangoCallEvent, MangoClient, MangoClientError


PHONE_KEYS = {
    "phone",
    "phone_number",
    "client_phone",
    "caller",
    "from",
    "from_number",
    "msisdn",
    "contact_phone",
}


def _extract_phone_candidates_from_payload(payload: dict[str, Any]) -> list[str]:
    if not isinstance(payload, dict):
        return []
    queue: list[Any] = [payload]
    visited = 0
    result: list[str] = []
    seen: set[str] = set()
    while queue and visited < 250:
        current = queue.pop(0)
        visited += 1
        if isinstance(current, dict):
            for key, value in current.items():
                key_norm = str(key or "").strip().lower()
                if key_norm in PHONE_KEYS and isinstance(value, (str, int, float)):
                    candidate = str(value).strip()
                    if candidate and candidate not in seen:
                        seen.add(candidate)
                        result.append(candidate)
                if isinstance(value, (dict, list, tuple)):
                    queue.append(value)
        elif isinstance(current, (list, tuple)):
            for item in current:
                if isinstance(item, (dict, list, tuple)):
                    queue.append(item)
    return result


def _extract_first_text_value(payload: dict[str, Any], keys: tuple[str, ...]) -> str:
    for key in keys:
        value = payload.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


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

    payload_chat_id = payload.get("chat_id")
    if payload_chat_id is None:
        payload_chat_id = payload.get("user_chat_id")
    chat_id_value: Optional[int] = None
    if isinstance(payload_chat_id, int) and payload_chat_id > 0:
        chat_id_value = payload_chat_id
    elif isinstance(payload_chat_id, str) and payload_chat_id.strip().isdigit():
        parsed_chat_id = int(payload_chat_id.strip())
        if parsed_chat_id > 0:
            chat_id_value = parsed_chat_id
    if user_id is None and chat_id_value is not None:
        row = conn.execute(
            """
            SELECT thread_key, user_id
            FROM business_threads
            WHERE chat_id = ?
            ORDER BY COALESCE(last_message_at, updated_at) DESC, id DESC
            LIMIT 1
            """,
            (chat_id_value,),
        ).fetchone()
        if row is not None:
            thread_id = str(row["thread_key"] or "").strip() or thread_id
            if row["user_id"] is not None:
                user_id = int(row["user_id"])

    if user_id is None:
        phone_candidates: list[str] = []
        if event.phone:
            phone_candidates.append(event.phone)
        phone_candidates.extend(_extract_phone_candidates_from_payload(payload))
        seen_phones: set[str] = set()
        for candidate in phone_candidates:
            normalized = str(candidate or "").strip()
            if not normalized or normalized in seen_phones:
                continue
            seen_phones.add(normalized)
            found = find_user_by_phone(conn, phone=normalized)
            if found is not None:
                user_id = found
                break

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
    call_external_id = str(item.get("call_external_id") or "").strip()
    if parsed is not None:
        if call_external_id and (not str(parsed.call_id or "").strip() or parsed.call_id == parsed.event_id):
            phone_candidates = _extract_phone_candidates_from_payload(payload_dict)
            resolved_phone = parsed.phone or (phone_candidates[0] if phone_candidates else "")
            return MangoCallEvent(
                event_id=parsed.event_id,
                call_id=call_external_id,
                phone=resolved_phone,
                recording_url=parsed.recording_url,
                transcript_hint=parsed.transcript_hint,
                occurred_at=parsed.occurred_at,
                payload=parsed.payload,
            )
        return parsed
    event_id = str(item.get("event_id") or "").strip()
    if not event_id:
        return None
    phone_candidates = _extract_phone_candidates_from_payload(payload_dict)
    phone = phone_candidates[0] if phone_candidates else ""
    recording_url = _extract_first_text_value(
        payload_dict,
        ("recording_url", "recordingUrl", "record_url", "recordUrl", "record_link", "recordLink", "audio_url"),
    )
    transcript_hint = _extract_first_text_value(payload_dict, ("transcript", "summary", "note", "comment"))
    occurred_at = _extract_first_text_value(payload_dict, ("occurred_at", "occurredAt", "created_at", "createdAt", "timestamp"))
    return MangoCallEvent(
        event_id=event_id,
        call_id=call_external_id or event_id,
        phone=phone,
        recording_url=recording_url,
        transcript_hint=transcript_hint,
        occurred_at=occurred_at,
        payload=payload_dict,
    )

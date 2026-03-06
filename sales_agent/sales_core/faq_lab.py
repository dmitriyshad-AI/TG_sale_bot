from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import re
import sqlite3
from typing import Any, Dict, Iterable, List

from sales_agent.sales_core import db


QUESTION_PREFIXES = (
    "как",
    "что",
    "почему",
    "зачем",
    "когда",
    "где",
    "сколько",
    "можно",
    "нужно",
    "стоит",
    "какой",
    "какая",
    "какие",
)


def normalize_question_key(text: str) -> str:
    if not isinstance(text, str):
        return ""
    normalized = text.lower().strip()
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"[^0-9a-zа-яё ]+", " ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if len(normalized) > 180:
        normalized = normalized[:180].rstrip()
    return normalized


def compact_text(text: str, *, max_len: int = 220) -> str:
    if not isinstance(text, str):
        return ""
    cleaned = " ".join(text.split()).strip()
    if len(cleaned) <= max_len:
        return cleaned
    return f"{cleaned[: max(0, max_len - 3)].rstrip()}..."


def looks_like_question(text: str) -> bool:
    compact = compact_text(text, max_len=500)
    if len(compact) < 8:
        return False
    lowered = compact.lower()
    if "?" in compact:
        return True
    return lowered.startswith(QUESTION_PREFIXES)


def _chunk(items: List[str], size: int = 250) -> Iterable[List[str]]:
    if size <= 0:
        size = 250
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _fetch_thread_counts(
    conn: sqlite3.Connection,
    *,
    thread_ids: List[str],
    action: str,
) -> Dict[str, int]:
    if not thread_ids:
        return {}
    counts: Dict[str, int] = {}
    for part in _chunk(thread_ids):
        placeholders = ",".join("?" for _ in part)
        params: List[Any] = [action, *part]
        cursor = conn.execute(
            f"""
            SELECT thread_id, COUNT(*) AS cnt
            FROM approval_actions
            WHERE action = ?
              AND thread_id IN ({placeholders})
            GROUP BY thread_id
            """,
            tuple(params),
        )
        for row in cursor.fetchall():
            counts[str(row["thread_id"])] = int(row["cnt"] or 0)
    return counts


def _fetch_outcome_threads(conn: sqlite3.Connection, *, thread_ids: List[str]) -> set[str]:
    if not thread_ids:
        return set()
    result: set[str] = set()
    for part in _chunk(thread_ids):
        placeholders = ",".join("?" for _ in part)
        cursor = conn.execute(
            f"""
            SELECT thread_id
            FROM conversation_outcomes
            WHERE thread_id IN ({placeholders})
              AND TRIM(COALESCE(outcome, '')) <> ''
            """,
            tuple(part),
        )
        for row in cursor.fetchall():
            result.add(str(row["thread_id"]))
    return result


def _fetch_source_messages(conn: sqlite3.Connection, *, window_days: int) -> list[Dict[str, Any]]:
    lookback = max(1, int(window_days))
    rows: list[Dict[str, Any]] = []

    tg_cursor = conn.execute(
        """
        SELECT user_id, text, created_at
        FROM messages
        WHERE direction = 'inbound'
          AND TRIM(COALESCE(text, '')) <> ''
          AND created_at >= datetime('now', ?)
        ORDER BY id DESC
        """,
        (f"-{lookback} days",),
    )
    for row in tg_cursor.fetchall():
        user_id = int(row["user_id"] or 0)
        if user_id <= 0:
            continue
        rows.append(
            {
                "thread_id": f"tg:{user_id}",
                "text": str(row["text"] or ""),
                "created_at": str(row["created_at"] or ""),
                "source": "telegram",
            }
        )

    biz_cursor = conn.execute(
        """
        SELECT thread_key, text, created_at
        FROM business_messages
        WHERE direction = 'inbound'
          AND is_deleted = 0
          AND TRIM(COALESCE(text, '')) <> ''
          AND created_at >= datetime('now', ?)
        ORDER BY id DESC
        """,
        (f"-{lookback} days",),
    )
    for row in biz_cursor.fetchall():
        thread_key = str(row["thread_key"] or "").strip()
        if not thread_key:
            continue
        rows.append(
            {
                "thread_id": thread_key,
                "text": str(row["text"] or ""),
                "created_at": str(row["created_at"] or ""),
                "source": "telegram_business",
            }
        )

    return rows


def build_faq_candidates(
    conn: sqlite3.Connection,
    *,
    window_days: int,
    min_question_count: int,
    limit: int,
) -> list[Dict[str, Any]]:
    source_rows = _fetch_source_messages(conn, window_days=window_days)
    grouped: dict[str, Dict[str, Any]] = {}

    for row in source_rows:
        raw_text = str(row.get("text") or "")
        if not looks_like_question(raw_text):
            continue

        question_text = compact_text(raw_text, max_len=240)
        key = normalize_question_key(question_text)
        if not key:
            continue

        bucket = grouped.get(key)
        if bucket is None:
            bucket = {
                "question_key": key,
                "question_text": question_text,
                "question_count": 0,
                "thread_ids": set(),
                "first_seen_at": None,
                "last_seen_at": None,
                "sources": defaultdict(int),
            }
            grouped[key] = bucket

        bucket["question_count"] += 1
        bucket["thread_ids"].add(str(row.get("thread_id") or ""))
        source = str(row.get("source") or "unknown")
        bucket["sources"][source] += 1

        created_at = str(row.get("created_at") or "").strip() or None
        if created_at:
            first_seen = bucket.get("first_seen_at")
            last_seen = bucket.get("last_seen_at")
            if first_seen is None or created_at < first_seen:
                bucket["first_seen_at"] = created_at
            if last_seen is None or created_at > last_seen:
                bucket["last_seen_at"] = created_at

    min_count = max(1, int(min_question_count))
    candidates = [
        item for item in grouped.values() if int(item.get("question_count") or 0) >= min_count and item.get("thread_ids")
    ]

    if not candidates:
        return []

    all_thread_ids = sorted({thread_id for item in candidates for thread_id in item["thread_ids"] if thread_id})
    approvals_by_thread = _fetch_thread_counts(conn, thread_ids=all_thread_ids, action="draft_approved")
    sends_by_thread = _fetch_thread_counts(conn, thread_ids=all_thread_ids, action="draft_sent")
    outcome_threads = _fetch_outcome_threads(conn, thread_ids=all_thread_ids)

    result: list[Dict[str, Any]] = []
    for item in candidates:
        thread_ids = sorted([thread for thread in item["thread_ids"] if thread])
        if not thread_ids:
            continue
        thread_count = len(thread_ids)
        approvals = sum(approvals_by_thread.get(thread_id, 0) for thread_id in thread_ids)
        sends = sum(sends_by_thread.get(thread_id, 0) for thread_id in thread_ids)
        next_steps = sum(1 for thread_id in thread_ids if thread_id in outcome_threads)

        approved_denominator = max(1, thread_count)
        next_step_denominator = max(1, sends if sends > 0 else thread_count)

        reply_approved_rate = approvals / approved_denominator
        next_step_rate = next_steps / next_step_denominator

        result.append(
            {
                "question_key": item["question_key"],
                "question_text": item["question_text"],
                "question_count": int(item["question_count"]),
                "thread_count": thread_count,
                "approvals_count": approvals,
                "sends_count": sends,
                "next_step_count": next_steps,
                "reply_approved_rate": round(reply_approved_rate, 4),
                "next_step_rate": round(next_step_rate, 4),
                "first_seen_at": item.get("first_seen_at"),
                "last_seen_at": item.get("last_seen_at"),
                "sample_thread_id": thread_ids[0],
                "thread_ids": thread_ids,
                "source": {"channels": dict(item["sources"])},
                "suggested_answer": (
                    "Уточнить цель, класс и формат, затем дать 2-3 релевантных варианта "
                    "и предложить следующий шаг с менеджером."
                ),
            }
        )

    result.sort(key=lambda x: (-int(x["question_count"]), -float(x["next_step_rate"]), str(x.get("last_seen_at") or "")), reverse=False)
    max_items = max(1, int(limit))
    return result[:max_items]


def refresh_faq_lab(
    conn: sqlite3.Connection,
    *,
    window_days: int,
    min_question_count: int,
    limit: int,
    trigger: str,
) -> Dict[str, Any]:
    candidates = build_faq_candidates(
        conn,
        window_days=window_days,
        min_question_count=min_question_count,
        limit=limit,
    )

    upserted = 0
    promoted_candidates = 0
    for item in candidates:
        candidate_id = db.upsert_faq_candidate(
            conn,
            question_key=str(item["question_key"]),
            question_text=str(item["question_text"]),
            question_count=int(item["question_count"]),
            thread_count=int(item["thread_count"]),
            approvals_count=int(item["approvals_count"]),
            sends_count=int(item["sends_count"]),
            next_step_count=int(item["next_step_count"]),
            reply_approved_rate=float(item["reply_approved_rate"]),
            next_step_rate=float(item["next_step_rate"]),
            first_seen_at=item.get("first_seen_at"),
            last_seen_at=item.get("last_seen_at"),
            sample_thread_id=item.get("sample_thread_id"),
            status="candidate",
            source={
                **(item.get("source") if isinstance(item.get("source"), dict) else {}),
                "trigger": trigger,
                "refreshed_at": datetime.utcnow().isoformat(),
            },
            suggested_answer=str(item.get("suggested_answer") or "").strip() or None,
        )
        if candidate_id <= 0:
            continue
        upserted += 1

        db.upsert_answer_performance(
            conn,
            answer_kind="candidate",
            answer_ref=f"candidate:{candidate_id}",
            question_key=str(item["question_key"]),
            question_text=str(item["question_text"]),
            question_count=int(item["question_count"]),
            approvals_count=int(item["approvals_count"]),
            sends_count=int(item["sends_count"]),
            next_step_count=int(item["next_step_count"]),
            reply_approved_rate=float(item["reply_approved_rate"]),
            next_step_rate=float(item["next_step_rate"]),
            source={"trigger": trigger},
        )

    canonical_answers = db.list_canonical_answers(conn, status="active", limit=500)
    for canonical in canonical_answers:
        candidate_id = int(canonical.get("candidate_id") or 0)
        if candidate_id <= 0:
            continue
        candidate = db.get_faq_candidate(conn, candidate_id=candidate_id)
        if not candidate:
            continue
        db.upsert_answer_performance(
            conn,
            answer_kind="canonical",
            answer_ref=f"canonical:{int(canonical.get('id') or 0)}",
            question_key=str(canonical.get("question_key") or ""),
            question_text=str(canonical.get("question_text") or ""),
            question_count=int(candidate.get("question_count") or 0),
            approvals_count=int(candidate.get("approvals_count") or 0),
            sends_count=int(candidate.get("sends_count") or 0),
            next_step_count=int(candidate.get("next_step_count") or 0),
            reply_approved_rate=float(candidate.get("reply_approved_rate") or 0.0),
            next_step_rate=float(candidate.get("next_step_rate") or 0.0),
            source={"trigger": trigger, "candidate_id": candidate_id},
        )
        promoted_candidates += 1

    return {
        "ok": True,
        "trigger": trigger,
        "window_days": max(1, int(window_days)),
        "min_question_count": max(1, int(min_question_count)),
        "limit": max(1, int(limit)),
        "candidates_scanned": len(candidates),
        "candidates_upserted": upserted,
        "canonical_synced": promoted_candidates,
    }

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
import re
import sqlite3
from typing import Any, Dict, Iterable, List, Optional

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


class FaqLabPromotionError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


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

    result.sort(
        key=lambda x: (
            int(x.get("question_count") or 0),
            float(x.get("next_step_rate") or 0.0),
            float(x.get("reply_approved_rate") or 0.0),
            str(x.get("last_seen_at") or ""),
        ),
        reverse=True,
    )
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
    started_at = datetime.utcnow()
    normalized_window = max(1, int(window_days))
    normalized_min_count = max(1, int(min_question_count))
    normalized_limit = max(1, int(limit))
    normalized_trigger = str(trigger or "").strip() or "unknown"
    run_id = db.create_faq_lab_run(
        conn,
        trigger=normalized_trigger,
        status="running",
        window_days=normalized_window,
        min_question_count=normalized_min_count,
        requested_limit=normalized_limit,
    )

    try:
        candidates = build_faq_candidates(
            conn,
            window_days=normalized_window,
            min_question_count=normalized_min_count,
            limit=normalized_limit,
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
                    "trigger": normalized_trigger,
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
                source={"trigger": normalized_trigger},
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
                source={"trigger": normalized_trigger, "candidate_id": candidate_id},
            )
            promoted_candidates += 1

        duration_ms = max(0, int((datetime.utcnow() - started_at).total_seconds() * 1000))
        summary = {
            "ok": True,
            "run_id": run_id,
            "trigger": normalized_trigger,
            "window_days": normalized_window,
            "min_question_count": normalized_min_count,
            "limit": normalized_limit,
            "candidates_scanned": len(candidates),
            "candidates_upserted": upserted,
            "canonical_synced": promoted_candidates,
            "duration_ms": duration_ms,
        }
        db.update_faq_lab_run(conn, run_id=run_id, status="success", summary=summary)
        db.create_faq_lab_event(
            conn,
            event_type="refresh_completed",
            actor="faq_lab",
            payload={
                "run_id": run_id,
                "trigger": normalized_trigger,
                "candidates_upserted": upserted,
                "canonical_synced": promoted_candidates,
                "duration_ms": duration_ms,
            },
        )
        return summary
    except Exception as exc:
        error_text = compact_text(str(exc), max_len=1000)
        db.update_faq_lab_run(conn, run_id=run_id, status="failed", error_text=error_text)
        db.create_faq_lab_event(
            conn,
            event_type="refresh_failed",
            actor="faq_lab",
            payload={
                "run_id": run_id,
                "trigger": normalized_trigger,
                "error": error_text,
            },
        )
        raise


def promote_candidate_to_canonical_safe(
    conn: sqlite3.Connection,
    *,
    candidate_id: int,
    answer_text: Optional[str],
    created_by: Optional[str],
    min_answer_len: int = 20,
) -> Dict[str, Any]:
    candidate = db.get_faq_candidate(conn, candidate_id=candidate_id)
    if not candidate:
        raise FaqLabPromotionError("not_found", "FAQ candidate not found.")

    candidate_status = str(candidate.get("status") or "").strip().lower()
    if candidate_status == "archived":
        raise FaqLabPromotionError("archived", "Archived FAQ candidate cannot be promoted.")

    normalized_answer = " ".join((answer_text or "").split()).strip()
    if normalized_answer and len(normalized_answer) < max(1, int(min_answer_len)):
        raise FaqLabPromotionError(
            "answer_too_short",
            f"Canonical answer is too short. Use at least {max(1, int(min_answer_len))} characters.",
        )

    warnings: list[str] = []
    question_count = int(candidate.get("question_count") or 0)
    if question_count < 2:
        warnings.append("Candidate has low signal (question_count < 2).")

    canonical = db.promote_faq_candidate_to_canonical(
        conn,
        candidate_id=candidate_id,
        answer_text=normalized_answer or None,
        created_by=created_by,
    )
    if not canonical:
        raise FaqLabPromotionError("promote_failed", "Unable to promote FAQ candidate.")

    event_id = db.create_faq_lab_event(
        conn,
        event_type="candidate_promoted",
        candidate_id=int(candidate_id),
        canonical_id=int(canonical.get("id") or 0),
        question_key=str(candidate.get("question_key") or ""),
        actor=(created_by or "").strip() or None,
        payload={
            "candidate_status_before": candidate_status or "new",
            "question_count": question_count,
            "next_step_rate": float(candidate.get("next_step_rate") or 0.0),
            "warnings": warnings,
        },
    )
    return {
        "candidate": db.get_faq_candidate(conn, candidate_id=candidate_id),
        "canonical": canonical,
        "event_id": event_id,
        "warnings": warnings,
    }

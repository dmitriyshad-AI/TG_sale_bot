from __future__ import annotations

from dataclasses import dataclass
import sqlite3
from typing import Any, Dict, List, Optional

from sales_agent.sales_core import db


DIRECTOR_MODEL_NAME = "director_agent_v1"

KEYWORD_TAGS = {
    "ege": {"егэ", "ege"},
    "oge": {"огэ", "oge"},
    "informatics": {"информат", "it", "ai", "программ"},
    "math": {"математ", "алгеб", "геометр"},
    "physics": {"физик"},
    "camp": {"лагер", "смен"},
    "olympiad": {"олимпиад"},
    "reactivation": {"реактив", "верн", "застыв", "stale", "warm", "тепл"},
}
ALLOWED_ACTION_TYPES = {"reactivation", "manual_review", "followup"}


@dataclass(frozen=True)
class ThreadCandidate:
    thread_id: str
    user_id: Optional[int]
    score: int
    reason: str
    last_message: str
    last_message_at: str
    source: str


def _normalize_text(value: object) -> str:
    if not isinstance(value, str):
        return ""
    cleaned = " ".join(value.split()).strip().lower()
    return cleaned


def _compact_text(value: object, max_len: int = 280) -> str:
    if not isinstance(value, str):
        return ""
    cleaned = " ".join(value.split()).strip()
    if len(cleaned) <= max_len:
        return cleaned
    return f"{cleaned[: max(0, max_len - 3)].rstrip()}..."


def extract_goal_tags(goal_text: str) -> list[str]:
    normalized = _normalize_text(goal_text)
    if not normalized:
        return []
    tags: list[str] = []
    for tag, keywords in KEYWORD_TAGS.items():
        if any(keyword in normalized for keyword in keywords):
            tags.append(tag)
    return tags


def _score_text_against_tags(text: str, tags: list[str]) -> int:
    if not text:
        return 0
    normalized = _normalize_text(text)
    if not normalized:
        return 0
    score = 1
    for tag in tags:
        for keyword in KEYWORD_TAGS.get(tag, set()):
            if keyword in normalized:
                score += 2
                break
    return score


def discover_thread_candidates(
    conn: sqlite3.Connection,
    *,
    goal_text: str,
    max_candidates: int = 200,
    lookback_days: int = 120,
) -> list[ThreadCandidate]:
    tags = extract_goal_tags(goal_text)
    lookback = max(1, int(lookback_days))
    per_thread: dict[str, ThreadCandidate] = {}

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
        thread_id = f"tg:{user_id}"
        score = _score_text_against_tags(str(row["text"] or ""), tags)
        candidate = ThreadCandidate(
            thread_id=thread_id,
            user_id=user_id,
            score=score,
            reason="keyword_match" if score > 1 else "recent_inbound",
            last_message=_compact_text(row["text"]),
            last_message_at=str(row["created_at"] or ""),
            source="telegram",
        )
        existing = per_thread.get(thread_id)
        if existing is None or candidate.score > existing.score:
            per_thread[thread_id] = candidate

    biz_cursor = conn.execute(
        """
        SELECT thread_key, user_id, text, created_at
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
        user_id_value = row["user_id"]
        user_id = int(user_id_value) if isinstance(user_id_value, int) and user_id_value > 0 else None
        score = _score_text_against_tags(str(row["text"] or ""), tags)
        candidate = ThreadCandidate(
            thread_id=thread_key,
            user_id=user_id,
            score=score,
            reason="keyword_match" if score > 1 else "recent_inbound",
            last_message=_compact_text(row["text"]),
            last_message_at=str(row["created_at"] or ""),
            source="telegram_business",
        )
        existing = per_thread.get(thread_key)
        if existing is None or candidate.score > existing.score:
            per_thread[thread_key] = candidate

    items = sorted(
        per_thread.values(),
        key=lambda item: (item.score, item.last_message_at),
        reverse=True,
    )

    max_items = max(1, int(max_candidates))
    return items[:max_items]


def build_campaign_plan(
    *,
    goal_text: str,
    candidates: list[ThreadCandidate],
    max_actions: int = 20,
) -> Dict[str, Any]:
    normalized_goal = _compact_text(goal_text, max_len=1200)
    if not normalized_goal:
        raise ValueError("goal_text is required")

    tags = extract_goal_tags(goal_text)
    action_limit = max(1, int(max_actions))

    assumptions: list[str] = [
        "Внешние отправки клиентам только после approve менеджером.",
        "Черновики формируются кратко и с акцентом на следующий шаг.",
    ]
    if "reactivation" in tags:
        assumptions.append("Фокус на реактивации warm/stale лидов.")
    if "ege" in tags or "oge" in tags:
        assumptions.append("Приоритет у лидов, где цель связана с экзаменационной подготовкой.")
    if "camp" in tags:
        assumptions.append("Уточнять сезон, формат и длительность смены перед оффером.")

    actions: list[Dict[str, Any]] = []
    for candidate in candidates[:action_limit]:
        priority = "hot" if candidate.score >= 5 else "warm"
        reason = f"{candidate.reason}; source={candidate.source}; score={candidate.score}"
        actions.append(
            {
                "action_type": "reactivation",
                "thread_id": candidate.thread_id,
                "user_id": candidate.user_id,
                "priority": priority,
                "reason": reason,
                "last_message": candidate.last_message,
                "last_message_at": candidate.last_message_at,
                "source": candidate.source,
            }
        )

    if not actions:
        actions.append(
            {
                "action_type": "manual_review",
                "thread_id": None,
                "user_id": None,
                "priority": "warm",
                "reason": "No matching candidates in history; manager review required.",
                "source": "director_agent",
            }
        )

    target_segment = {
        "tags": tags,
        "candidate_count": len(candidates),
        "selected_actions": len(actions),
    }

    success_metric = (
        f"approve_rate>=0.6 AND draft_sent_count>={max(1, min(10, len(actions)))} "
        "AND next_step_outcomes>0"
    )

    return {
        "objective": normalized_goal,
        "assumptions": assumptions,
        "target_segment": target_segment,
        "success_metric": success_metric,
        "actions": actions,
        "approvals_required": True,
        "model_name": DIRECTOR_MODEL_NAME,
    }


def _infer_user_id_from_thread_id(thread_id: Optional[str]) -> Optional[int]:
    if not isinstance(thread_id, str):
        return None
    normalized = thread_id.strip()
    if not normalized.startswith("tg:"):
        return None
    suffix = normalized[3:]
    if not suffix.isdigit():
        return None
    value = int(suffix)
    return value if value > 0 else None


def apply_campaign_plan(
    conn: sqlite3.Connection,
    *,
    goal_id: int,
    plan_id: int,
    plan: Dict[str, Any],
    actor: str,
) -> Dict[str, Any]:
    actions = plan.get("actions") if isinstance(plan, dict) else []
    if not isinstance(actions, list):
        actions = []

    created_followups = 0
    created_drafts = 0
    created_actions = 0
    skipped = 0
    skipped_by_reason = {
        "invalid_action_shape": 0,
        "unsupported_action_type": 0,
        "missing_thread_or_user": 0,
    }

    for index, action in enumerate(actions, start=1):
        if not isinstance(action, dict):
            skipped += 1
            skipped_by_reason["invalid_action_shape"] += 1
            continue

        action_type = str(action.get("action_type") or "manual").strip().lower() or "manual"
        if action_type not in ALLOWED_ACTION_TYPES:
            db.create_campaign_action(
                conn,
                goal_id=goal_id,
                plan_id=plan_id,
                action_type=action_type,
                status="skipped",
                user_id=None,
                thread_id=None,
                priority="warm",
                reason=f"unsupported_action_type:{action_type}",
                payload={"index": index, "action": action, "source": "director_agent", "actor": actor},
            )
            skipped += 1
            skipped_by_reason["unsupported_action_type"] += 1
            continue
        thread_id = str(action.get("thread_id") or "").strip() or None
        user_id_value = action.get("user_id")
        if isinstance(user_id_value, int) and user_id_value > 0:
            user_id = user_id_value
        else:
            user_id = _infer_user_id_from_thread_id(thread_id)

        priority = str(action.get("priority") or "warm").strip().lower() or "warm"
        reason = str(action.get("reason") or "director_agent_action").strip() or "director_agent_action"

        action_payload = {
            "index": index,
            "action": action,
            "source": "director_agent",
            "actor": actor,
        }

        if not thread_id or user_id is None:
            db.create_campaign_action(
                conn,
                goal_id=goal_id,
                plan_id=plan_id,
                action_type=action_type,
                status="skipped",
                user_id=user_id,
                thread_id=thread_id,
                priority=priority,
                reason=f"{reason} (missing_thread_or_user)",
                payload=action_payload,
            )
            skipped += 1
            skipped_by_reason["missing_thread_or_user"] += 1
            continue

        draft_text = (
            "Здравствуйте! Возвращаюсь к вашему запросу и подготовил следующий шаг.\n\n"
            f"Контекст: {reason}.\n"
            "Если удобно, задам 1-2 уточнения и сразу предложу оптимальный вариант."
        )
        idempotency_key = f"campaign:{plan_id}:{thread_id}"[:120]

        draft_id = db.create_reply_draft(
            conn,
            user_id=user_id,
            thread_id=thread_id,
            draft_text=draft_text,
            model_name=DIRECTOR_MODEL_NAME,
            quality={
                "source": "director_agent",
                "goal_id": goal_id,
                "plan_id": plan_id,
                "action_index": index,
            },
            created_by=actor,
            status="created",
            idempotency_key=idempotency_key,
        )
        created_drafts += 1

        followup_id = db.create_followup_task(
            conn,
            user_id=user_id,
            thread_id=thread_id,
            priority=priority,
            reason=f"campaign:{goal_id}:{reason}",
            status="pending",
            due_at=None,
            assigned_to=actor,
            related_draft_id=draft_id,
        )
        created_followups += 1

        db.create_campaign_action(
            conn,
            goal_id=goal_id,
            plan_id=plan_id,
            action_type=action_type,
            status="created",
            user_id=user_id,
            thread_id=thread_id,
            priority=priority,
            reason=reason,
            draft_id=draft_id,
            followup_task_id=followup_id,
            payload=action_payload,
        )
        created_actions += 1

        db.create_approval_action(
            conn,
            draft_id=draft_id,
            user_id=user_id,
            thread_id=thread_id,
            action="draft_created",
            actor=actor,
            payload={
                "source": "director_agent",
                "goal_id": goal_id,
                "plan_id": plan_id,
                "followup_task_id": followup_id,
            },
        )
        db.create_approval_action(
            conn,
            draft_id=draft_id,
            user_id=user_id,
            thread_id=thread_id,
            action="followup_created",
            actor=actor,
            payload={
                "source": "director_agent",
                "goal_id": goal_id,
                "plan_id": plan_id,
                "followup_task_id": followup_id,
            },
        )

    report = {
        "goal_id": goal_id,
        "plan_id": plan_id,
        "created_actions": created_actions,
        "created_drafts": created_drafts,
        "created_followups": created_followups,
        "skipped": skipped,
        "skipped_by_reason": skipped_by_reason,
    }
    db.create_campaign_report(
        conn,
        goal_id=goal_id,
        plan_id=plan_id,
        report=report,
        created_by=actor,
    )
    return report

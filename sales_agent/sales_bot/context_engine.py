from __future__ import annotations

from datetime import datetime
from typing import Callable, Dict, List, Optional


def parse_db_timestamp(raw_value: object) -> Optional[datetime]:
    if not isinstance(raw_value, str):
        return None
    value = raw_value.strip()
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def build_stitched_user_text(
    *,
    current_text: str,
    recent_messages: List[Dict[str, object]],
    normalize_text_fn: Callable[[str], str],
    is_structured_flow_input_fn: Callable[[str], bool],
    now_utc: Optional[datetime] = None,
    stitch_window_seconds: int = 210,
    stitch_max_parts: int = 4,
    stitch_max_chars: int = 900,
) -> str:
    base_text = current_text.strip()
    if not base_text:
        return current_text
    if is_structured_flow_input_fn(base_text):
        return current_text

    now = now_utc or datetime.utcnow()
    parts: List[str] = [base_text]
    seen = {normalize_text_fn(base_text)}
    current_size = len(base_text)

    for item in reversed(recent_messages):
        direction = str(item.get("direction") or "")
        if direction != "inbound":
            continue
        previous = str(item.get("text") or "").strip()
        if not previous or previous.startswith("/"):
            continue
        normalized_prev = normalize_text_fn(previous)
        if not normalized_prev or normalized_prev in seen:
            continue

        created_at = parse_db_timestamp(item.get("created_at"))
        if created_at is not None and (now - created_at).total_seconds() > stitch_window_seconds:
            continue

        if current_size + len(previous) + 1 > stitch_max_chars:
            break

        parts.insert(0, previous)
        seen.add(normalized_prev)
        current_size += len(previous) + 1
        if len(parts) >= stitch_max_parts:
            break

    stitched = " ".join(part for part in parts if part).strip()
    return stitched or current_text


def merge_unique_texts(
    items: List[str],
    *,
    normalize_text_fn: Callable[[str], str],
    limit: int = 8,
) -> List[str]:
    result: List[str] = []
    seen = set()
    for item in items:
        normalized = normalize_text_fn(item)
        if not normalized or normalized in seen:
            continue
        result.append(item.strip())
        seen.add(normalized)
    return result[-limit:]


def extract_intent_tags(text: str, *, normalize_text_fn: Callable[[str], str]) -> List[str]:
    normalized = normalize_text_fn(text)
    mapping = [
        ("поступление", ("поступить", "поступлен")),
        ("стратегия", ("стратег", "план", "маршрут")),
        ("егэ", ("егэ",)),
        ("огэ", ("огэ",)),
        ("олимпиады", ("олимп",)),
        ("успеваемость", ("успеваем", "база")),
        ("условия", ("условия", "договор", "документ")),
        ("оплата", ("оплата", "стоимость", "цена", "рассроч", "вычет")),
        ("расписание", ("расписан", "время", "график")),
    ]
    tags: List[str] = []
    for label, keywords in mapping:
        if any(keyword in normalized for keyword in keywords):
            tags.append(label)
    return tags


def build_context_summary_text(summary: Dict[str, object]) -> str:
    profile = summary.get("profile") if isinstance(summary.get("profile"), dict) else {}
    intents = summary.get("intents") if isinstance(summary.get("intents"), list) else []
    recent_requests = (
        summary.get("recent_user_requests")
        if isinstance(summary.get("recent_user_requests"), list)
        else []
    )

    chunks: List[str] = []
    grade = profile.get("grade")
    goal = profile.get("goal")
    subject = profile.get("subject")
    study_format = profile.get("format")
    target = profile.get("target")

    profile_parts: List[str] = []
    if grade:
        profile_parts.append(f"{grade} класс")
    if goal:
        profile_parts.append(f"цель: {goal}")
    if subject:
        profile_parts.append(f"предмет: {subject}")
    if study_format:
        profile_parts.append(f"формат: {study_format}")
    if target:
        profile_parts.append(f"вуз/цель: {target}")
    if profile_parts:
        chunks.append("Профиль: " + "; ".join(profile_parts) + ".")

    if intents:
        normalized_intents = [str(item).strip() for item in intents if str(item).strip()]
        if normalized_intents:
            chunks.append("Интересы: " + ", ".join(normalized_intents) + ".")

    if recent_requests:
        request_chunks = [str(item).strip() for item in recent_requests[-2:] if str(item).strip()]
        if request_chunks:
            chunks.append("Последние запросы: " + " | ".join(request_chunks) + ".")

    return " ".join(chunks).strip()

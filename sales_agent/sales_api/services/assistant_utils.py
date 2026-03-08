from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, Field

from sales_agent.sales_core.catalog import Product, SearchCriteria

ASSISTANT_RECENT_HISTORY_LIMIT = 12
ASSISTANT_RECENT_HISTORY_TEXT_LIMIT = 350
ASSISTANT_CONTEXT_RECENT_REQUESTS_LIMIT = 8
ASSISTANT_CONTEXT_INTENTS_LIMIT = 12
ASSISTANT_CONTEXT_SUMMARY_MAX = 1200

ASSISTANT_KNOWLEDGE_HINTS = {
    "договор",
    "документ",
    "документы",
    "оплата",
    "возврат",
    "маткапитал",
    "вычет",
    "проживание",
    "питание",
    "условия",
    "безопасность",
}
ASSISTANT_CONSULTATIVE_HINTS = {
    "поступить",
    "стратег",
    "план",
    "траект",
    "подготов",
    "егэ",
    "огэ",
    "олимпиад",
    "курс",
    "лагерь",
    "мфти",
}
ASSISTANT_CONTEXT_INTENT_KEYWORDS = {
    "поступление": {"поступить", "поступлен", "мфти", "вуз"},
    "стратегия": {"стратег", "траект", "план"},
    "егэ": {"егэ"},
    "огэ": {"огэ"},
    "олимпиады": {"олимп"},
    "лагерь": {"лагерь", "смена"},
    "успеваемость": {"успеваем", "база"},
    "условия": {"условия", "договор", "документ"},
    "оплата": {"оплата", "стоимость", "цена", "вычет", "рассроч"},
}

GOAL_LABELS = {
    "ege": "ЕГЭ",
    "oge": "ОГЭ",
    "olympiad": "олимпиады",
    "camp": "лагерь",
    "base": "успеваемость",
}

SUBJECT_LABELS = {
    "math": "математика",
    "physics": "физика",
    "informatics": "информатика",
}

FORMAT_LABELS = {
    "online": "онлайн",
    "offline": "очно",
    "hybrid": "гибрид",
}


class AssistantCriteriaPayload(BaseModel):
    brand: Optional[str] = None
    grade: Optional[int] = Field(default=None, ge=1, le=11)
    goal: Optional[str] = None
    subject: Optional[str] = None
    format: Optional[str] = None


class AssistantHistoryItem(BaseModel):
    role: Literal["user", "assistant"]
    text: str = Field(min_length=1, max_length=2000)


class AssistantAskPayload(BaseModel):
    question: str = Field(min_length=1, max_length=2000)
    criteria: Optional[AssistantCriteriaPayload] = None
    context_summary: Optional[str] = Field(default=None, max_length=1200)
    recent_history: Optional[list[AssistantHistoryItem]] = Field(default=None, max_length=30)


def normalize_lookup_token(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower()


def sanitize_recent_history(items: Optional[list[AssistantHistoryItem]]) -> list[Dict[str, str]]:
    if not items:
        return []

    sanitized: list[Dict[str, str]] = []
    for item in items[-ASSISTANT_RECENT_HISTORY_LIMIT:]:
        text = " ".join(item.text.split())
        if not text:
            continue
        if len(text) > ASSISTANT_RECENT_HISTORY_TEXT_LIMIT:
            text = f"{text[:ASSISTANT_RECENT_HISTORY_TEXT_LIMIT - 3].rstrip()}..."
        sanitized.append({"role": item.role, "text": text})
    return sanitized


def compact_text(value: object, *, limit: int = 350) -> str:
    if not isinstance(value, str):
        return ""
    normalized = " ".join(value.split()).strip()
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[: max(0, limit - 3)].rstrip()}..."


def merge_unique_tail(items: list[str], *, limit: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = compact_text(item, limit=ASSISTANT_RECENT_HISTORY_TEXT_LIMIT)
        if not text:
            continue
        key = normalize_lookup_token(text)
        if not key or key in seen:
            continue
        result.append(text)
        seen.add(key)
    if len(result) <= limit:
        return result
    return result[-limit:]


def extract_context_intents(text: str) -> list[str]:
    normalized = normalize_lookup_token(text)
    if not normalized:
        return []
    tags: list[str] = []
    for label, keywords in ASSISTANT_CONTEXT_INTENT_KEYWORDS.items():
        if any(keyword in normalized for keyword in keywords):
            tags.append(label)
    return tags


def format_context_summary(
    *,
    profile: dict[str, object],
    intents: list[str],
    recent_requests: list[str],
) -> str:
    chunks: list[str] = []

    profile_parts: list[str] = []
    grade = profile.get("grade")
    if isinstance(grade, int):
        profile_parts.append(f"{grade} класс")

    goal = compact_text(str(profile.get("goal") or ""), limit=80)
    if goal:
        profile_parts.append(f"цель: {goal}")

    subject = compact_text(str(profile.get("subject") or ""), limit=80)
    if subject:
        profile_parts.append(f"предмет: {subject}")

    learning_format = compact_text(str(profile.get("format") or ""), limit=80)
    if learning_format:
        profile_parts.append(f"формат: {learning_format}")

    target = compact_text(str(profile.get("target") or ""), limit=80)
    if target:
        profile_parts.append(f"цель: {target}")

    if profile_parts:
        chunks.append("Профиль: " + "; ".join(profile_parts) + ".")

    cleaned_intents = [item.strip() for item in intents if item.strip()]
    if cleaned_intents:
        chunks.append("Интересы: " + ", ".join(cleaned_intents) + ".")

    cleaned_requests = [item.strip() for item in recent_requests if item.strip()]
    if cleaned_requests:
        chunks.append("Последние запросы: " + " | ".join(cleaned_requests[-2:]) + ".")

    summary = " ".join(chunks).strip()
    if len(summary) <= ASSISTANT_CONTEXT_SUMMARY_MAX:
        return summary
    return f"{summary[: ASSISTANT_CONTEXT_SUMMARY_MAX - 3].rstrip()}..."


def merge_assistant_context(
    current: dict[str, object],
    *,
    question: str,
    criteria: SearchCriteria,
    recent_history: list[dict[str, str]],
    context_summary: str,
) -> dict[str, object]:
    existing = current if isinstance(current, dict) else {}
    profile = existing.get("profile") if isinstance(existing.get("profile"), dict) else {}
    profile = dict(profile)

    if criteria.grade is not None:
        profile["grade"] = int(criteria.grade)
    if criteria.goal:
        profile["goal"] = GOAL_LABELS.get(criteria.goal, criteria.goal)
    if criteria.subject:
        profile["subject"] = SUBJECT_LABELS.get(criteria.subject, criteria.subject)
    if criteria.format:
        profile["format"] = FORMAT_LABELS.get(criteria.format, criteria.format)

    normalized_question = normalize_lookup_token(question)
    if "мфти" in normalized_question:
        profile["target"] = "МФТИ"
    elif "мгу" in normalized_question:
        profile["target"] = "МГУ"

    previous_intents = existing.get("intents") if isinstance(existing.get("intents"), list) else []
    merged_intents = merge_unique_tail(
        [str(item) for item in previous_intents] + extract_context_intents(question),
        limit=ASSISTANT_CONTEXT_INTENTS_LIMIT,
    )

    history_user_requests = [
        compact_text(item.get("text"), limit=ASSISTANT_RECENT_HISTORY_TEXT_LIMIT)
        for item in recent_history
        if item.get("role") == "user"
    ]
    previous_requests = (
        existing.get("recent_user_requests")
        if isinstance(existing.get("recent_user_requests"), list)
        else []
    )
    merged_requests = merge_unique_tail(
        [str(item) for item in previous_requests]
        + history_user_requests
        + [compact_text(question, limit=ASSISTANT_RECENT_HISTORY_TEXT_LIMIT)],
        limit=ASSISTANT_CONTEXT_RECENT_REQUESTS_LIMIT,
    )

    summary_text = compact_text(context_summary, limit=ASSISTANT_CONTEXT_SUMMARY_MAX)
    if not summary_text:
        summary_text = format_context_summary(
            profile=profile,
            intents=merged_intents,
            recent_requests=merged_requests,
        )

    return {
        "profile": profile,
        "intents": merged_intents,
        "recent_user_requests": merged_requests,
        "summary_text": summary_text,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }


def is_format_compatible(criteria_format: Optional[str], product_format: Optional[str]) -> bool:
    if not criteria_format:
        return True
    if not product_format:
        return False
    if criteria_format == product_format:
        return True
    if product_format == "hybrid" or criteria_format == "hybrid":
        return True
    return False


def is_subject_compatible(criteria_subject: Optional[str], product_subjects: list[str]) -> bool:
    if not criteria_subject:
        return True
    lowered = {item.strip().lower() for item in product_subjects}
    if criteria_subject in lowered:
        return True
    return "general" in lowered


def evaluate_match_quality(criteria: SearchCriteria, products: list[Product]) -> str:
    if not products:
        return "none"

    first = products[0]
    checks = 0
    matched = 0

    if criteria.grade is not None:
        checks += 1
        if first.grade_min <= criteria.grade <= first.grade_max:
            matched += 1
    if criteria.goal:
        checks += 1
        if first.category == criteria.goal:
            matched += 1
    if criteria.subject:
        checks += 1
        if is_subject_compatible(criteria.subject, list(first.subjects)):
            matched += 1
    if criteria.format:
        checks += 1
        if is_format_compatible(criteria.format, first.format):
            matched += 1

    if checks == 0:
        return "strong" if len(products) >= 2 else "limited"
    if matched == checks and len(products) >= 1:
        return "strong"
    if matched >= max(1, checks - 1):
        return "limited"
    return "limited"


def build_manager_offer(match_quality: str, has_results: bool) -> Dict[str, object]:
    if match_quality == "strong":
        return {
            "recommended": False,
            "message": (
                "Мы уже видим хороший стартовый вариант под ваш запрос. "
                "Если хотите, менеджер может дополнительно сравнить расписание и нагрузку."
            ),
            "call_to_action": "Оставьте контакт, и менеджер уточнит детали в удобное время.",
        }

    if has_results:
        return {
            "recommended": True,
            "message": (
                "Под ваши параметры уже есть хорошие предложения. "
                "Чтобы выбрать максимально точный вариант под вашу цель, лучше подключить менеджера."
            ),
            "call_to_action": (
                "Оставьте контакт: у нас широкая линейка под разные уровни и задачи, "
                "менеджер подберет оптимальный путь именно для вас."
            ),
        }

    return {
        "recommended": True,
        "message": (
            "Идеального совпадения в автоматическом подборе не нашлось, "
            "но это нормальная ситуация для нестандартных запросов."
        ),
        "call_to_action": (
            "Оставьте контакт: подберем персонально, у нас есть решения для разных целей, "
            "классов и форматов обучения."
        ),
    }


def assistant_mode(question: str, criteria: SearchCriteria) -> str:
    normalized = normalize_lookup_token(question)
    if any(hint in normalized for hint in ASSISTANT_KNOWLEDGE_HINTS):
        return "knowledge"

    has_criteria = any(
        (
            criteria.grade is not None,
            bool(criteria.goal),
            bool(criteria.subject),
            bool(criteria.format),
        )
    )
    if has_criteria or any(hint in normalized for hint in ASSISTANT_CONSULTATIVE_HINTS):
        return "consultative"
    return "general"


def criteria_from_payload(payload: Optional[AssistantCriteriaPayload], brand_default: str) -> SearchCriteria:
    criteria = payload or AssistantCriteriaPayload()
    brand = normalize_lookup_token(criteria.brand) or brand_default
    goal = normalize_lookup_token(criteria.goal) or None
    subject = normalize_lookup_token(criteria.subject) or None
    learning_format = normalize_lookup_token(criteria.format) or None
    return SearchCriteria(
        brand=brand,
        grade=criteria.grade,
        goal=goal,
        subject=subject,
        format=learning_format,
    )


def missing_criteria_fields(criteria: SearchCriteria) -> list[str]:
    missing: list[str] = []
    if criteria.grade is None:
        missing.append("grade")
    if not criteria.goal:
        missing.append("goal")
    if not criteria.subject:
        missing.append("subject")
    if not criteria.format:
        missing.append("format")
    return missing


def format_price_text(product: object) -> str:
    sessions = getattr(product, "sessions", None)
    if not isinstance(sessions, list) or not sessions:
        return "Цена по запросу"

    prices = [int(item.price_rub) for item in sessions if getattr(item, "price_rub", None) is not None]
    if not prices:
        return "Цена по запросу"
    low = min(prices)
    high = max(prices)
    if low == high:
        return f"{low:,} ₽".replace(",", " ")
    return f"{low:,}-{high:,} ₽".replace(",", " ")


def format_next_start_text(product: object) -> str:
    sessions = getattr(product, "sessions", None)
    if not isinstance(sessions, list) or not sessions:
        return "Старт по мере набора группы"

    starts: list[date] = [item.start_date for item in sessions if isinstance(getattr(item, "start_date", None), date)]
    if not starts:
        return "Старт по мере набора группы"

    today = date.today()
    upcoming = [value for value in starts if value >= today]
    target = min(upcoming or starts)
    return target.strftime("%d.%m.%Y")


from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from sales_agent.sales_core.tallanto_client import TallantoClient, TallantoResult


@dataclass
class CopilotResult:
    source_format: str
    message_count: int
    summary: str
    customer_profile: Dict[str, Any]
    draft_reply: str


WHATSAPP_LINE_RE = re.compile(
    r"^(?P<date>\d{1,2}[./]\d{1,2}[./]\d{2,4}),?\s+(?P<time>\d{1,2}:\d{2})\s+-\s+(?P<sender>[^:]+):\s?(?P<text>.*)$"
)

MANAGER_HINTS = {
    "менеджер",
    "admin",
    "админ",
    "kmipt",
    "фотон",
    "foton",
    "sales",
    "support",
}

GOAL_KEYWORDS = {
    "егэ": "ege",
    "огэ": "oge",
    "олимп": "olympiad",
    "лагер": "camp",
    "успеваем": "base",
}

SUBJECT_KEYWORDS = {
    "мат": "math",
    "физ": "physics",
    "информ": "informatics",
}

OBJECTION_KEYWORDS = {
    "дорого": "price",
    "нет времени": "time",
    "не увер": "fit",
    "подума": "delay",
    "позже": "delay",
}


def _safe_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        chunks: List[str] = []
        for item in value:
            if isinstance(item, str):
                chunks.append(item)
            elif isinstance(item, dict) and isinstance(item.get("text"), str):
                chunks.append(item["text"])
        return "".join(chunks).strip()
    return ""


def _role_from_sender(sender: str) -> str:
    normalized = sender.strip().lower()
    if any(hint in normalized for hint in MANAGER_HINTS):
        return "manager"
    return "client"


def parse_whatsapp_export(raw_text: str) -> List[Dict[str, str]]:
    messages: List[Dict[str, str]] = []
    current: Optional[Dict[str, str]] = None

    for line in raw_text.splitlines():
        matched = WHATSAPP_LINE_RE.match(line)
        if matched:
            if current:
                messages.append(current)

            sender = matched.group("sender").strip()
            msg_text = matched.group("text").strip()
            date_part = matched.group("date")
            time_part = matched.group("time")
            current = {
                "source": "whatsapp",
                "created_at": f"{date_part} {time_part}",
                "sender": sender,
                "role": _role_from_sender(sender),
                "text": msg_text,
            }
            continue

        if current:
            continuation = line.strip()
            if continuation:
                current["text"] = (current["text"] + "\n" + continuation).strip()

    if current:
        messages.append(current)

    return [msg for msg in messages if msg.get("text")]


def parse_telegram_export(raw_payload: Dict[str, Any]) -> List[Dict[str, str]]:
    raw_messages = raw_payload.get("messages")
    if not isinstance(raw_messages, list):
        return []

    normalized: List[Dict[str, str]] = []
    for item in raw_messages:
        if not isinstance(item, dict):
            continue
        text = _safe_text(item.get("text"))
        if not text:
            continue

        sender = _safe_text(item.get("from")) or "Unknown"
        date = _safe_text(item.get("date"))
        normalized.append(
            {
                "source": "telegram",
                "created_at": date,
                "sender": sender,
                "role": _role_from_sender(sender),
                "text": text,
            }
        )

    return normalized


def detect_source_format(filename: str, content: str) -> str:
    lower = filename.lower()
    if lower.endswith(".json"):
        return "telegram_json"
    if lower.endswith(".txt"):
        return "whatsapp_txt"

    stripped = content.lstrip()
    if stripped.startswith("{") and '"messages"' in stripped:
        return "telegram_json"
    return "whatsapp_txt"


def import_dialogue(filename: str, content: bytes) -> Tuple[str, List[Dict[str, str]]]:
    text = content.decode("utf-8", errors="ignore")
    source_format = detect_source_format(filename=filename, content=text)

    if source_format == "telegram_json":
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError("Invalid Telegram JSON export file.") from exc
        if not isinstance(payload, dict):
            raise ValueError("Telegram export must be a JSON object.")
        return source_format, parse_telegram_export(payload)

    return source_format, parse_whatsapp_export(text)


def _extract_grade(text: str) -> Optional[int]:
    match = re.search(r"\b([1-9]|10|11)\s*класс", text.lower())
    if not match:
        return None
    return int(match.group(1))


def _extract_goal(text: str) -> Optional[str]:
    lowered = text.lower()
    for keyword, goal in GOAL_KEYWORDS.items():
        if keyword in lowered:
            return goal
    return None


def _extract_subject(text: str) -> Optional[str]:
    lowered = text.lower()
    for keyword, subject in SUBJECT_KEYWORDS.items():
        if keyword in lowered:
            return subject
    return None


def _extract_objections(text: str) -> List[str]:
    lowered = text.lower()
    found: List[str] = []
    for keyword, code in OBJECTION_KEYWORDS.items():
        if keyword in lowered and code not in found:
            found.append(code)
    return found


def summarize_dialogue(messages: List[Dict[str, str]]) -> Tuple[str, Dict[str, Any]]:
    if not messages:
        return (
            "Диалог пустой или не распознан.",
            {
                "grade": None,
                "goal": None,
                "subject": None,
                "objections": [],
                "last_client_message": None,
            },
        )

    client_messages = [msg for msg in messages if msg.get("role") == "client"]
    manager_messages = [msg for msg in messages if msg.get("role") == "manager"]

    combined_client_text = "\n".join(msg.get("text", "") for msg in client_messages)
    grade = _extract_grade(combined_client_text)
    goal = _extract_goal(combined_client_text)
    subject = _extract_subject(combined_client_text)
    objections = _extract_objections(combined_client_text)

    last_client_message = client_messages[-1]["text"] if client_messages else None
    first_client_message = client_messages[0]["text"] if client_messages else ""

    summary_lines = [
        f"Сообщений клиента: {len(client_messages)}",
        f"Сообщений менеджера: {len(manager_messages)}",
    ]
    if grade is not None:
        summary_lines.append(f"Класс: {grade}")
    if goal:
        summary_lines.append(f"Цель: {goal}")
    if subject:
        summary_lines.append(f"Предмет: {subject}")
    if objections:
        summary_lines.append(f"Возражения: {', '.join(objections)}")
    if first_client_message:
        summary_lines.append(f"Первый запрос клиента: {first_client_message[:180]}")
    if last_client_message:
        summary_lines.append(f"Последнее сообщение клиента: {last_client_message[:180]}")

    profile = {
        "grade": grade,
        "goal": goal,
        "subject": subject,
        "objections": objections,
        "last_client_message": last_client_message,
    }
    return "\n".join(summary_lines), profile


def propose_reply(
    summary: str,
    customer_profile: Dict[str, Any],
    catalog_context: Optional[List[Dict[str, str]]] = None,
) -> str:
    grade = customer_profile.get("grade")
    goal = customer_profile.get("goal")
    subject = customer_profile.get("subject")

    intro = "Здравствуйте! Возвращаюсь к вашему запросу."
    if grade or goal or subject:
        context_parts = []
        if grade:
            context_parts.append(f"{grade} класс")
        if goal:
            context_parts.append(f"цель: {goal}")
        if subject:
            context_parts.append(f"предмет: {subject}")
        intro += " Вижу, что для вас актуально: " + ", ".join(context_parts) + "."

    offers = ""
    if catalog_context:
        names = [item.get("title") for item in catalog_context if item.get("title")]
        if names:
            offers = " Могу предложить 2-3 подходящие программы и коротко объяснить разницу."

    objections = customer_profile.get("objections") or []
    objection_line = ""
    if "price" in objections:
        objection_line = " Также подберу вариант в комфортном бюджете."

    closing = "Если удобно, отправьте номер телефона — менеджер согласует оптимальный формат и ближайший старт."
    return f"{intro}{offers}{objection_line} {closing}".strip()


def create_tallanto_copilot_task(
    tallanto: TallantoClient,
    summary: str,
    draft_reply: str,
    contact: Optional[str] = None,
) -> TallantoResult:
    payload = {
        "title": "Copilot: реактивация диалога",
        "summary": summary,
        "draft_reply": draft_reply,
        "contact": contact or "",
    }
    return tallanto.set_entry(module="tasks", fields_values=payload)


def run_copilot_from_file(
    filename: str,
    content: bytes,
    catalog_context: Optional[List[Dict[str, str]]] = None,
) -> CopilotResult:
    source_format, messages = import_dialogue(filename=filename, content=content)
    summary, profile = summarize_dialogue(messages)
    draft = propose_reply(summary=summary, customer_profile=profile, catalog_context=catalog_context)
    return CopilotResult(
        source_format=source_format,
        message_count=len(messages),
        summary=summary,
        customer_profile=profile,
        draft_reply=draft,
    )

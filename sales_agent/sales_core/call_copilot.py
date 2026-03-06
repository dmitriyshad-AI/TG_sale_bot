from __future__ import annotations

from dataclasses import dataclass
import re
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse


MAX_TRANSCRIPT_CHARS = 16000
MAX_SUMMARY_CHARS = 2000

HOT_HINTS = {
    "готовы записаться",
    "готов оплатить",
    "готова оплатить",
    "когда оплатить",
    "где оплатить",
    "хочу записаться",
    "когда старт",
    "нужен договор",
}
WARM_HINTS = {
    "интересно",
    "хотим",
    "план подготовки",
    "консультац",
    "стоимость",
    "цена",
    "расписание",
    "формат",
}
COLD_HINTS = {
    "пока не актуально",
    "не сейчас",
    "подумаем",
    "дорого",
    "не уверен",
    "не уверена",
}

INTEREST_RULES: dict[str, set[str]] = {
    "ЕГЭ": {"егэ"},
    "ОГЭ": {"огэ"},
    "олимпиадная подготовка": {"олимпиад"},
    "лагерь": {"лагер", "смен"},
    "математика": {"математ"},
    "физика": {"физик"},
    "информатика": {"информат", "программ"},
    "поступление в МФТИ": {"мфти", "поступить"},
}

OBJECTION_RULES: dict[str, set[str]] = {
    "цена": {"дорого", "стоим", "цена", "бюджет"},
    "время": {"времени нет", "график", "расписание неудобно"},
    "онлайн/очно": {"онлайн", "очно", "формат"},
    "перегрузка": {"перегруз", "сложно", "не потянет", "устает"},
}


@dataclass(frozen=True)
class CallInsights:
    summary_text: str
    interests: list[str]
    objections: list[str]
    next_best_action: str
    warmth: str
    confidence: float
    score: float


def normalize_transcript(text: str) -> str:
    cleaned = " ".join((text or "").replace("\u00a0", " ").split())
    if len(cleaned) <= MAX_TRANSCRIPT_CHARS:
        return cleaned
    return cleaned[:MAX_TRANSCRIPT_CHARS].rstrip()


def _collect_tags(text: str, rules: dict[str, set[str]]) -> list[str]:
    lowered = text.lower()
    found: list[str] = []
    for tag, tokens in rules.items():
        if any(token in lowered for token in tokens):
            found.append(tag)
    return found


def _extract_sentences(text: str, limit: int = 3) -> list[str]:
    parts = re.split(r"(?<=[.!?])\s+|\n+", text.strip())
    sentences = [part.strip(" \t") for part in parts if part.strip()]
    if not sentences:
        return []
    return sentences[: max(1, int(limit))]


def _infer_warmth(text: str) -> tuple[str, float, float]:
    lowered = text.lower()
    hot_hits = sum(1 for token in HOT_HINTS if token in lowered)
    warm_hits = sum(1 for token in WARM_HINTS if token in lowered)
    cold_hits = sum(1 for token in COLD_HINTS if token in lowered)

    if hot_hits > 0:
        score = min(97.0, 82.0 + hot_hits * 6.0 - cold_hits * 2.0)
        confidence = min(0.96, 0.68 + hot_hits * 0.08)
        return ("hot", confidence, score)
    if cold_hits > max(0, warm_hits):
        score = max(12.0, 36.0 - cold_hits * 6.0)
        confidence = min(0.9, 0.62 + cold_hits * 0.08)
        return ("cold", confidence, score)

    score = min(78.0, 54.0 + warm_hits * 5.0 - cold_hits * 2.0)
    confidence = min(0.92, 0.6 + warm_hits * 0.07)
    return ("warm", confidence, score)


def _build_next_best_action(warmth: str, interests: list[str], objections: list[str]) -> str:
    interest_hint = f"по теме: {', '.join(interests[:2])}" if interests else "по запросу клиента"
    if warmth == "hot":
        return (
            "Связаться в течение 15 минут, подтвердить ближайший старт и отправить ссылку на запись "
            f"{interest_hint}."
        )
    if warmth == "cold":
        return (
            "Отправить мягкий follow-up с пользой (мини-план подготовки) и предложить короткую консультацию "
            "через 3-5 дней."
        )
    objection_hint = f" Отработать возражения: {', '.join(objections[:2])}." if objections else ""
    return (
        "Согласовать 10-15 минут консультации, уточнить цель и критерии выбора программы, "
        f"затем предложить 2-3 релевантных варианта {interest_hint}."
        f"{objection_hint}"
    )


def build_call_insights(transcript_text: str) -> CallInsights:
    normalized = normalize_transcript(transcript_text)
    if not normalized:
        normalized = "Транскрипт звонка отсутствует."

    interests = _collect_tags(normalized, INTEREST_RULES)
    objections = _collect_tags(normalized, OBJECTION_RULES)
    warmth, confidence, score = _infer_warmth(normalized)

    sentences = _extract_sentences(normalized, limit=3)
    if sentences:
        summary_text = " ".join(sentences)
    else:
        summary_text = normalized
    if len(summary_text) > MAX_SUMMARY_CHARS:
        summary_text = summary_text[: MAX_SUMMARY_CHARS - 3].rstrip() + "..."

    next_best_action = _build_next_best_action(warmth, interests, objections)
    return CallInsights(
        summary_text=summary_text,
        interests=interests,
        objections=objections,
        next_best_action=next_best_action,
        warmth=warmth,
        confidence=round(confidence, 2),
        score=round(score, 1),
    )


def extract_transcript_from_file(file_path: Path) -> str:
    suffix = file_path.suffix.lower()
    if suffix not in {".txt", ".md", ".log"}:
        return ""

    content = file_path.read_bytes()
    for encoding in ("utf-8", "utf-8-sig", "cp1251", "latin-1"):
        try:
            decoded = content.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        return ""
    return normalize_transcript(decoded)


def build_transcript_fallback(
    *,
    source_type: str,
    source_ref: Optional[str] = None,
    transcript_hint: Optional[str] = None,
) -> str:
    hint = normalize_transcript(transcript_hint or "")
    if hint:
        return hint
    if source_type == "url" and source_ref:
        parsed = urlparse(source_ref)
        host = parsed.netloc or source_ref
        return normalize_transcript(
            "Запись звонка получена по ссылке "
            f"{host}. Автотранскрипт недоступен в текущей конфигурации, нужен ручной конспект."
        )
    return "Запись звонка загружена. Автотранскрипт недоступен в текущей конфигурации."

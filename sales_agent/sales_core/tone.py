from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from sales_agent.sales_core.config import project_root


@dataclass(frozen=True)
class ToneProfile:
    persona: str
    principles: List[str]
    polite_markers: List[str]
    pressure_markers: List[str]
    substitutions: Dict[str, str]


DEFAULT_TONE_PROFILE = ToneProfile(
    persona=(
        "Уважительный и дружелюбный консультант отдела продаж, "
        "который сначала приносит пользу клиенту и только затем мягко продает."
    ),
    principles=[
        "Обращайся на 'вы'.",
        "Сначала проясни задачу и помоги с планом действий.",
        "Предлагай следующий шаг мягко, без давления.",
        "Избегай категоричных формулировок и рекламных клише.",
    ],
    polite_markers=[
        "пожалуйста",
        "если удобно",
        "спасибо",
        "понимаю",
        "подскажите",
        "если хотите",
    ],
    pressure_markers=[
        "срочно",
        "только сегодня",
        "последний шанс",
        "обязательно оставьте",
        "нужно прямо сейчас",
        "иначе",
    ],
    substitutions={
        "Оставьте телефон": "Если вам удобно, оставьте телефон",
        "Оставьте контакт": "Если вам удобно, оставьте контакт",
        "Срочно": "Когда вам будет удобно",
        "Вы обязаны": "Рекомендую",
        "Привет!": "Здравствуйте!",
    },
)


def tone_profile_path(path: Optional[Path] = None) -> Path:
    if path:
        return path
    env_path = os.getenv("SALES_TONE_PATH", "").strip()
    if env_path:
        return Path(env_path)
    return project_root() / "config" / "sales_tone.yaml"


def _as_list(raw: object) -> List[str]:
    if not isinstance(raw, list):
        return []
    values: List[str] = []
    for item in raw:
        if isinstance(item, str):
            cleaned = item.strip()
            if cleaned:
                values.append(cleaned)
    return values


def _as_mapping(raw: object) -> Dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    values: Dict[str, str] = {}
    for key, value in raw.items():
        if isinstance(key, str) and isinstance(value, str):
            from_text = key.strip()
            to_text = value.strip()
            if from_text and to_text:
                values[from_text] = to_text
    return values


def load_tone_profile(path: Optional[Path] = None) -> ToneProfile:
    profile_path = tone_profile_path(path)
    if not profile_path.exists():
        return DEFAULT_TONE_PROFILE

    try:
        with profile_path.open("r", encoding="utf-8") as fh:
            payload = yaml.safe_load(fh) or {}
    except Exception:
        return DEFAULT_TONE_PROFILE

    if not isinstance(payload, dict):
        return DEFAULT_TONE_PROFILE

    persona = payload.get("persona")
    principles = _as_list(payload.get("principles"))
    polite_markers = _as_list(payload.get("polite_markers"))
    pressure_markers = _as_list(payload.get("pressure_markers"))
    substitutions = _as_mapping(payload.get("substitutions"))

    return ToneProfile(
        persona=persona.strip() if isinstance(persona, str) and persona.strip() else DEFAULT_TONE_PROFILE.persona,
        principles=principles or DEFAULT_TONE_PROFILE.principles,
        polite_markers=polite_markers or DEFAULT_TONE_PROFILE.polite_markers,
        pressure_markers=pressure_markers or DEFAULT_TONE_PROFILE.pressure_markers,
        substitutions=substitutions or DEFAULT_TONE_PROFILE.substitutions,
    )


def tone_as_prompt_block(profile: ToneProfile) -> str:
    principles = "\n".join(f"- {line}" for line in profile.principles)
    return (
        "Профиль тона:\n"
        f"Роль: {profile.persona}\n"
        "Правила:\n"
        f"{principles}"
    )


def _replace_insensitive(text: str, source: str, target: str) -> str:
    pattern = re.compile(re.escape(source), flags=re.IGNORECASE)
    return pattern.sub(target, text)


def apply_tone_guardrails(text: str, profile: Optional[ToneProfile] = None) -> str:
    profile = profile or DEFAULT_TONE_PROFILE
    normalized = text.strip()
    if not normalized:
        return normalized

    for source, target in profile.substitutions.items():
        normalized = _replace_insensitive(normalized, source, target)

    normalized = re.sub(r"!{2,}", "!", normalized)
    normalized = re.sub(r"[ \t]+", " ", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


def assess_response_quality(text: str, profile: Optional[ToneProfile] = None) -> Dict[str, int]:
    profile = profile or DEFAULT_TONE_PROFILE
    normalized = text.lower().strip()
    if not normalized:
        return {
            "helpfulness_score": 1,
            "friendliness_score": 1,
            "pressure_score": 1,
        }

    polite_hits = sum(1 for marker in profile.polite_markers if marker in normalized)
    pressure_hits = sum(1 for marker in profile.pressure_markers if marker in normalized)

    helpfulness_score = 2
    if len(normalized) >= 120:
        helpfulness_score += 1
    if "?" in normalized:
        helpfulness_score += 1
    if any(token in normalized for token in ("план", "шаг", "дальше", "подскажу", "помогу")):
        helpfulness_score += 1

    friendliness_score = 2 + min(2, polite_hits)
    if pressure_hits > 0:
        friendliness_score -= 1

    pressure_score = 1 + min(4, pressure_hits)

    def _clip(value: int) -> int:
        return max(1, min(5, value))

    return {
        "helpfulness_score": _clip(helpfulness_score),
        "friendliness_score": _clip(friendliness_score),
        "pressure_score": _clip(pressure_score),
    }

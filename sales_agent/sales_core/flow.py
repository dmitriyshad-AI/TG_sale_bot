from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from sales_agent.sales_core.catalog import normalize_format, normalize_goal, normalize_subject


STATE_START = "start"
STATE_ASK_GRADE = "ask_grade"
STATE_ASK_GOAL = "ask_goal"
STATE_ASK_SUBJECT = "ask_subject"
STATE_ASK_FORMAT = "ask_format"
STATE_SUGGEST_PRODUCTS = "suggest_products"
STATE_ASK_CONTACT = "ask_contact"
STATE_DONE = "done"

GOAL_OPTIONS = [
    ("ЕГЭ", "ege"),
    ("ОГЭ", "oge"),
    ("Олимпиада", "olympiad"),
    ("Лагерь", "camp"),
    ("Успеваемость", "base"),
]

SUBJECT_OPTIONS = [
    ("Математика", "math"),
    ("Физика", "physics"),
    ("Информатика", "informatics"),
    ("Не важно", "any"),
]

FORMAT_OPTIONS = [
    ("Онлайн", "online"),
    ("Очно", "offline"),
    ("Смешанный", "hybrid"),
]


@dataclass
class FlowStep:
    message: str
    next_state: str
    state_data: Dict[str, Any]
    keyboard: List[List[Tuple[str, str]]]
    should_suggest_products: bool = False
    ask_contact_now: bool = False
    completed: bool = False


def _with_brand_default(brand_default: str) -> Dict[str, Any]:
    return {
        "state": STATE_ASK_GRADE,
        "criteria": {
            "brand": brand_default,
            "grade": None,
            "goal": None,
            "subject": None,
            "format": None,
        },
        "contact": None,
    }


def ensure_state(state_data: Optional[Dict[str, Any]], brand_default: str) -> Dict[str, Any]:
    if not isinstance(state_data, dict):
        return _with_brand_default(brand_default)
    state_data = dict(state_data)
    criteria = state_data.get("criteria") if isinstance(state_data.get("criteria"), dict) else {}

    state_data.setdefault("state", STATE_ASK_GRADE)
    state_data["criteria"] = {
        "brand": criteria.get("brand") or brand_default,
        "grade": criteria.get("grade"),
        "goal": criteria.get("goal"),
        "subject": criteria.get("subject"),
        "format": criteria.get("format"),
    }
    state_data.setdefault("contact", state_data.get("contact"))
    return state_data


def _grade_keyboard() -> List[List[Tuple[str, str]]]:
    rows: List[List[Tuple[str, str]]] = []
    row: List[Tuple[str, str]] = []
    for grade in range(1, 12):
        row.append((str(grade), f"grade:{grade}"))
        if len(row) == 4:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    return rows


def _goal_keyboard() -> List[List[Tuple[str, str]]]:
    return [[(title, f"goal:{value}")] for title, value in GOAL_OPTIONS]


def _subject_keyboard() -> List[List[Tuple[str, str]]]:
    return [[(title, f"subject:{value}")] for title, value in SUBJECT_OPTIONS]


def _format_keyboard() -> List[List[Tuple[str, str]]]:
    return [[(title, f"format:{value}")] for title, value in FORMAT_OPTIONS]


def _suggest_keyboard() -> List[List[Tuple[str, str]]]:
    return [
        [("Оставить контакт", "contact:start")],
        [("Подобрать заново", "flow:restart")],
    ]


def _restart_keyboard() -> List[List[Tuple[str, str]]]:
    return [[("Подобрать заново", "flow:restart")]]


def build_prompt(state_data: Dict[str, Any]) -> FlowStep:
    state = state_data.get("state", STATE_ASK_GRADE)

    if state == STATE_ASK_GRADE:
        return FlowStep(
            message="Укажите класс ученика (1-11):",
            next_state=STATE_ASK_GRADE,
            state_data=state_data,
            keyboard=_grade_keyboard(),
        )
    if state == STATE_ASK_GOAL:
        return FlowStep(
            message="Какая цель подготовки?",
            next_state=STATE_ASK_GOAL,
            state_data=state_data,
            keyboard=_goal_keyboard(),
        )
    if state == STATE_ASK_SUBJECT:
        return FlowStep(
            message="Какой предмет приоритетный?",
            next_state=STATE_ASK_SUBJECT,
            state_data=state_data,
            keyboard=_subject_keyboard(),
        )
    if state == STATE_ASK_FORMAT:
        return FlowStep(
            message="Какой формат удобнее?",
            next_state=STATE_ASK_FORMAT,
            state_data=state_data,
            keyboard=_format_keyboard(),
        )
    if state == STATE_SUGGEST_PRODUCTS:
        return FlowStep(
            message="Вот лучшие варианты. Нажмите кнопку, чтобы оставить контакт.",
            next_state=STATE_SUGGEST_PRODUCTS,
            state_data=state_data,
            keyboard=_suggest_keyboard(),
            should_suggest_products=True,
        )
    if state == STATE_ASK_CONTACT:
        return FlowStep(
            message="Отправьте номер телефона, и менеджер свяжется с вами.",
            next_state=STATE_ASK_CONTACT,
            state_data=state_data,
            keyboard=_restart_keyboard(),
            ask_contact_now=True,
        )

    return FlowStep(
        message="Спасибо! Заявка сохранена.",
        next_state=STATE_DONE,
        state_data=state_data,
        keyboard=_restart_keyboard(),
        completed=True,
    )


def _parse_grade(text: str) -> Optional[int]:
    cleaned = "".join(ch for ch in text if ch.isdigit())
    if not cleaned:
        return None
    grade = int(cleaned)
    if 1 <= grade <= 11:
        return grade
    return None


def _parse_contact(text: str) -> Optional[str]:
    stripped = text.strip()
    if not stripped:
        return None
    digits = "".join(ch for ch in stripped if ch.isdigit())
    if len(digits) >= 10:
        return stripped
    return None


def _parse_callback(callback_data: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not callback_data or ":" not in callback_data:
        return None, None
    prefix, value = callback_data.split(":", 1)
    return prefix, value


def advance_flow(
    state_data: Optional[Dict[str, Any]],
    brand_default: str,
    message_text: Optional[str] = None,
    callback_data: Optional[str] = None,
) -> FlowStep:
    state = ensure_state(state_data, brand_default=brand_default)
    callback_prefix, callback_value = _parse_callback(callback_data)

    if callback_data == "flow:restart":
        restarted = _with_brand_default(brand_default)
        return build_prompt(restarted)

    current_state = state.get("state", STATE_ASK_GRADE)
    criteria = state["criteria"]
    text = (message_text or "").strip()

    if current_state in {STATE_START, STATE_ASK_GRADE}:
        grade_value: Optional[int] = None
        if callback_prefix == "grade":
            grade_value = _parse_grade(callback_value or "")
        elif text:
            grade_value = _parse_grade(text)

        if grade_value is None:
            state["state"] = STATE_ASK_GRADE
            prompt = build_prompt(state)
            prompt.message = "Нужен класс от 1 до 11. Выберите класс:"
            return prompt

        criteria["grade"] = grade_value
        state["state"] = STATE_ASK_GOAL
        return build_prompt(state)

    if current_state == STATE_ASK_GOAL:
        goal_value: Optional[str] = None
        if callback_prefix == "goal":
            goal_value = normalize_goal(callback_value)
        elif text:
            goal_value = normalize_goal(text)

        if goal_value not in {"ege", "oge", "olympiad", "camp", "base", "intensive"}:
            prompt = build_prompt(state)
            prompt.message = "Не понял цель. Выберите вариант из кнопок:"
            return prompt

        criteria["goal"] = goal_value
        state["state"] = STATE_ASK_SUBJECT
        return build_prompt(state)

    if current_state == STATE_ASK_SUBJECT:
        subject_value: Optional[str] = None
        if callback_prefix == "subject":
            subject_value = callback_value
        elif text:
            subject_value = normalize_subject(text)

        if subject_value == "any":
            criteria["subject"] = None
        elif normalize_subject(subject_value) in {"math", "physics", "informatics"}:
            criteria["subject"] = normalize_subject(subject_value)
        else:
            prompt = build_prompt(state)
            prompt.message = "Выберите предмет из кнопок или вариант 'Не важно'."
            return prompt

        state["state"] = STATE_ASK_FORMAT
        return build_prompt(state)

    if current_state == STATE_ASK_FORMAT:
        format_value: Optional[str] = None
        if callback_prefix == "format":
            format_value = normalize_format(callback_value)
        elif text:
            format_value = normalize_format(text)

        if format_value not in {"online", "offline", "hybrid"}:
            prompt = build_prompt(state)
            prompt.message = "Нужен формат: онлайн, очно или смешанный."
            return prompt

        criteria["format"] = format_value
        state["state"] = STATE_SUGGEST_PRODUCTS
        prompt = build_prompt(state)
        prompt.message = "Подбираю лучшие варианты..."
        return prompt

    if current_state == STATE_SUGGEST_PRODUCTS:
        if callback_data == "contact:start":
            state["state"] = STATE_ASK_CONTACT
            return build_prompt(state)

        if text:
            return FlowStep(
                message=(
                    "Понял. Если хотите, отвечу на ваш вопрос и затем продолжим подбор. "
                    "Чтобы оставить контакт, используйте кнопку ниже."
                ),
                next_state=STATE_SUGGEST_PRODUCTS,
                state_data=state,
                keyboard=_suggest_keyboard(),
                should_suggest_products=False,
            )

        prompt = build_prompt(state)
        prompt.message = "Чтобы продолжить, нажмите 'Оставить контакт' или 'Подобрать заново'."
        return prompt

    if current_state == STATE_ASK_CONTACT:
        if callback_data == "flow:restart":
            restarted = _with_brand_default(brand_default)
            return build_prompt(restarted)

        contact_value = _parse_contact(text)
        if not contact_value:
            prompt = build_prompt(state)
            prompt.message = "Отправьте номер телефона для связи, например +79991234567."
            return prompt

        state["contact"] = contact_value
        state["state"] = STATE_DONE
        return build_prompt(state)

    return build_prompt(state)

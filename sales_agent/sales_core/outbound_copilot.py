from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

OUTBOUND_MODEL_NAME = "outbound_copilot_v1"

_SEGMENT_TAGS: dict[str, set[str]] = {
    "school": {"школ", "лицей", "гимназ", "school", "lyceum", "gymnasium"},
    "college": {"колледж", "техникум", "college"},
    "university": {"вуз", "университет", "university", "institute"},
    "corporate": {"компан", "корпорат", "hr", "business", "b2b"},
    "edtech": {"edtech", "онлайн-школ", "образовательн", "допобраз"},
    "moscow": {"москв", "moscow"},
    "ege": {"егэ", "oge", "огэ", "экзам"},
    "olympiad": {"олимпиад", "olymp"},
}


@dataclass(frozen=True)
class OutboundProposalGuard:
    allowed: bool
    reason_code: str
    reason_text: str


def _normalize(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return " ".join(value.split()).strip().lower()


def _compact(value: Any, max_len: int = 200) -> str:
    if not isinstance(value, str):
        return ""
    text = " ".join(value.split()).strip()
    if len(text) <= max_len:
        return text
    return f"{text[: max(0, max_len - 3)].rstrip()}..."


def score_company_fit(company: Dict[str, Any], *, campaign_tags: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    normalized_text = " ".join(
        part
        for part in (
            _normalize(company.get("company_name")),
            _normalize(company.get("segment")),
            _normalize(company.get("city")),
            _normalize(company.get("note")),
        )
        if part
    )

    score = 15.0
    matched_tags: list[str] = []
    reasons: list[str] = []

    for tag, keywords in _SEGMENT_TAGS.items():
        if any(keyword in normalized_text for keyword in keywords):
            matched_tags.append(tag)
            if tag in {"school", "college", "university"}:
                score += 22
            elif tag in {"corporate", "edtech"}:
                score += 16
            elif tag == "moscow":
                score += 10
            else:
                score += 8

    normalized_campaign_tags = {_normalize(item) for item in (campaign_tags or []) if _normalize(item)}
    for tag in normalized_campaign_tags:
        if tag in matched_tags:
            score += 6
            reasons.append(f"campaign tag matched: {tag}")

    website = _normalize(company.get("website"))
    if website:
        score += 4
    if website and any(token in website for token in ("school", "edu", "academy", "lyceum")):
        score += 6
        reasons.append("website indicates education profile")

    score = max(0.0, min(100.0, score))
    if not reasons:
        if matched_tags:
            reasons.append(f"matched tags: {', '.join(sorted(set(matched_tags)))}")
        else:
            reasons.append("insufficient explicit education markers; requires manual qualification")

    return {
        "score": round(score, 1),
        "tags": sorted(set(matched_tags)),
        "reason": "; ".join(reasons),
    }


def build_outbound_proposal(
    company: Dict[str, Any],
    *,
    fit: Dict[str, Any],
    offer_focus: str = "подготовка к ОГЭ/ЕГЭ, олимпиадам и профильным сменам",
) -> Dict[str, str]:
    company_name = _compact(company.get("company_name"), max_len=120) or "коллеги"
    city = _compact(company.get("city"), max_len=80)
    segment = _compact(company.get("segment"), max_len=120)
    score = float(fit.get("score") or 0)
    tags = [str(item) for item in (fit.get("tags") or []) if str(item)]

    geo_suffix = f" в {city}" if city else ""
    segment_suffix = f" ({segment})" if segment else ""

    short_message = (
        f"Здравствуйте! Видим потенциальный fit{geo_suffix}: можем предложить формат партнёрства по линии {offer_focus}. "
        "Если актуально, отправим краткий план и пилотный сценарий на 2–4 недели."
    )

    bullets: list[str] = [
        "цели и профиль учеников/группы;",
        "формат: очно/онлайн/смешанный;",
        "модель пилота: 2–4 недели с метриками;",
        "ответственные, SLA и календарь запуска.",
    ]
    if score >= 70:
        bullets.insert(0, "предварительно высокий fit по сегменту и образовательному профилю;")
    elif score >= 45:
        bullets.insert(0, "предварительно средний fit, рекомендуем короткий discovery-звонок;")
    else:
        bullets.insert(0, "нужна дополнительная квалификация запроса перед коммерческим предложением;")

    tags_line = ", ".join(tags) if tags else "manual_review"
    proposal_text = (
        f"Черновик КП для {company_name}{segment_suffix}{geo_suffix}.\n\n"
        f"Фокус предложения: {offer_focus}.\n"
        f"Оценка fit: {score:.1f}/100. Теги: {tags_line}.\n\n"
        "Предлагаем следующий процесс:\n"
        + "\n".join(f"- {line}" for line in bullets)
        + "\n\nГотовы адаптировать оффер под ваш учебный план и ограничения по расписанию/бюджету."
    )

    return {
        "short_message": short_message,
        "proposal_text": proposal_text,
        "model_name": OUTBOUND_MODEL_NAME,
    }


def parse_outbound_companies_csv(content: str, *, source: str = "csv_import") -> List[Dict[str, str]]:
    reader = csv.DictReader(io.StringIO(content))
    items: list[dict[str, str]] = []
    for row in reader:
        name = _compact(row.get("company_name") or row.get("name") or row.get("company"), max_len=180)
        if not name:
            continue
        items.append(
            {
                "company_name": name,
                "website": _compact(row.get("website"), max_len=180),
                "city": _compact(row.get("city"), max_len=120),
                "segment": _compact(row.get("segment"), max_len=140),
                "note": _compact(row.get("note"), max_len=500),
                "owner": _compact(row.get("owner"), max_len=120),
                "source": source,
            }
        )
    return items


def evaluate_outbound_proposal_guard(
    *,
    company_status: str,
    open_proposals: int,
    recent_touches: int,
    max_open_proposals: int = 1,
    max_recent_touches: int = 2,
) -> OutboundProposalGuard:
    normalized_status = _normalize(company_status) or "new"
    if normalized_status in {"archived", "lost"}:
        return OutboundProposalGuard(
            allowed=False,
            reason_code="company_inactive",
            reason_text="Компания в архиве/проиграна. Сначала переведите в активный статус.",
        )
    if normalized_status == "won":
        return OutboundProposalGuard(
            allowed=False,
            reason_code="company_already_won",
            reason_text="По компании уже отмечена победа. Новый драфт КП не требуется.",
        )
    if int(open_proposals) >= max(1, int(max_open_proposals)):
        return OutboundProposalGuard(
            allowed=False,
            reason_code="open_proposal_exists",
            reason_text="У компании уже есть открытый драфт/approved КП. Завершите его перед новым.",
        )
    if int(recent_touches) >= max(1, int(max_recent_touches)):
        return OutboundProposalGuard(
            allowed=False,
            reason_code="touch_limit_reached",
            reason_text="Сработал анти-спам лимит касаний за последние сутки. Повторите позже.",
        )
    return OutboundProposalGuard(
        allowed=True,
        reason_code="ok",
        reason_text="ok",
    )

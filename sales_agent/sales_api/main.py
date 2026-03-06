from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
import html
import json
import logging
import secrets
from pathlib import Path
from typing import Any, Dict, Literal, Optional
from uuid import uuid4
from urllib.parse import quote_plus

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile, status
from pydantic import BaseModel, ConfigDict, Field
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from telegram import Update

from sales_agent.sales_bot import bot as bot_runtime
from sales_agent.sales_core.catalog import CatalogValidationError, Product, SearchCriteria, explain_match, select_top_products
from sales_agent.sales_core.call_copilot import (
    build_call_insights,
    build_transcript_fallback,
    extract_transcript_from_file,
)
from sales_agent.sales_core import faq_lab as faq_lab_service
from sales_agent.sales_core.config import Settings, get_settings, project_root
from sales_agent.sales_core.copilot import run_copilot_from_file
from sales_agent.sales_core.crm import build_crm_client
from sales_agent.sales_core.db import (
    clear_call_record_file_path,
    claim_webhook_update,
    count_mango_events,
    count_webhook_updates_by_status,
    create_approval_action,
    create_call_record,
    create_or_get_mango_event,
    create_followup_task,
    create_lead_score,
    create_reply_draft,
    enqueue_webhook_update,
    find_user_by_phone,
    get_business_connection,
    get_latest_mango_event_created_at,
    get_oldest_mango_event_created_at,
    get_conversation_outcome,
    get_conversation_context,
    get_connection,
    get_call_record,
    get_or_create_user,
    get_crm_cache,
    get_inbox_thread_detail,
    get_latest_call_summary_for_thread,
    get_revenue_metrics_snapshot,
    get_reply_draft,
    get_latest_lead_score,
    init_db,
    list_approval_actions_for_thread,
    list_business_messages,
    list_call_records,
    list_call_records_with_files_for_cleanup,
    list_followup_tasks,
    list_inbox_threads,
    list_mango_events,
    list_recent_business_threads,
    list_conversation_messages,
    list_recent_conversations,
    list_recent_leads,
    list_reply_drafts_for_thread,
    mark_webhook_update_done,
    mark_webhook_update_retry,
    requeue_stuck_webhook_updates,
    resolve_preferred_thread_for_user,
    update_call_record_status,
    update_mango_event_status,
    update_reply_draft_status,
    update_reply_draft_text,
    upsert_call_summary,
    upsert_call_transcript,
    upsert_conversation_outcome,
    upsert_conversation_context,
    upsert_crm_cache,
)
from sales_agent.sales_core.runtime_diagnostics import build_runtime_diagnostics
from sales_agent.sales_core.rate_limit import InMemoryRateLimiter
from sales_agent.sales_core.tallanto_readonly import (
    TallantoReadOnlyClient,
    normalize_tallanto_fields,
    normalize_tallanto_modules,
    sanitize_tallanto_lookup_context,
)
from sales_agent.sales_core.mango_client import MangoCallEvent, MangoClient, MangoClientError
from sales_agent.sales_core.telegram_webapp import verify_telegram_webapp_init_data
from sales_agent.sales_core.llm_client import LLMClient
from sales_agent.sales_core.vector_store import load_vector_store_id
from sales_agent.sales_api.routers.faq_lab import build_faq_lab_router
from sales_agent.sales_api.routers.director import build_director_router


security = HTTPBasic()
logger = logging.getLogger(__name__)
WEBHOOK_MAX_ATTEMPTS = 5
WEBHOOK_STALE_PROCESSING_SECONDS = 180
WEBHOOK_RETRY_BASE_SECONDS = 2
CRM_CACHE_TTL_SECONDS = 3 * 3600
ASSISTANT_TIMEOUT_SECONDS = 36.0
REQUEST_ID_HEADER = "X-Request-ID"
ASSISTANT_API_TOKEN_HEADER = "X-Assistant-Token"
FORWARDED_FOR_HEADER = "X-Forwarded-For"
ASSISTANT_RECENT_HISTORY_LIMIT = 12
ASSISTANT_RECENT_HISTORY_TEXT_LIMIT = 350
ASSISTANT_CONTEXT_RECENT_REQUESTS_LIMIT = 8
ASSISTANT_CONTEXT_INTENTS_LIMIT = 12
ASSISTANT_CONTEXT_SUMMARY_MAX = 1200
LEAD_RADAR_RULE_NO_REPLY = "radar:no_reply"
LEAD_RADAR_RULE_CALL_NO_NEXT_STEP = "radar:call_no_next_step"
LEAD_RADAR_RULE_STALE_WARM = "radar:stale_warm"
LEAD_RADAR_MODEL_NAME = "lead_radar_v1"
FAQ_LAB_MODEL_NAME = "faq_lab_v1"
CALL_COPILOT_MODEL_NAME = "call_copilot_v1"
MANGO_CLEANUP_BATCH_SIZE = 200
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


class ReplyDraftPayload(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    draft_text: str = Field(min_length=1, max_length=8000)
    source_message_id: Optional[int] = None
    model_name: Optional[str] = Field(default=None, max_length=120)
    quality: Optional[Dict[str, Any]] = None
    idempotency_key: Optional[str] = Field(default=None, max_length=120)


class ConversationOutcomePayload(BaseModel):
    outcome: str = Field(min_length=1, max_length=120)
    note: Optional[str] = Field(default=None, max_length=2000)


class FollowupTaskPayload(BaseModel):
    priority: Literal["hot", "warm", "cold"] = "warm"
    reason: str = Field(min_length=1, max_length=2000)
    due_at: Optional[str] = Field(default=None, max_length=120)
    assigned_to: Optional[str] = Field(default=None, max_length=120)


class LeadScorePayload(BaseModel):
    score: float = Field(ge=0.0, le=100.0)
    temperature: Literal["hot", "warm", "cold"]
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    factors: Optional[Dict[str, Any]] = None


class ReplyDraftUpdatePayload(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    draft_text: str = Field(min_length=1, max_length=8000)
    model_name: Optional[str] = Field(default=None, max_length=120)
    quality: Optional[Dict[str, Any]] = None


class DraftSendPayload(BaseModel):
    sent_message_id: Optional[str] = Field(default=None, max_length=255)


class RevenueEventPayload(BaseModel):
    action: Literal[
        "draft_created",
        "draft_edited",
        "draft_approved",
        "draft_rejected",
        "draft_sent",
        "followup_created",
        "lead_scored",
        "conversation_outcome_set",
        "manual_action",
    ]
    payload: Optional[Dict[str, Any]] = None
    draft_id: Optional[int] = None


class LeadRadarRunPayload(BaseModel):
    dry_run: bool = False
    limit: Optional[int] = Field(default=None, ge=1, le=500)


def _normalize_lookup_token(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower()


def _sanitize_recent_history(items: Optional[list[AssistantHistoryItem]]) -> list[Dict[str, str]]:
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


def _compact_text(value: object, *, limit: int = 350) -> str:
    if not isinstance(value, str):
        return ""
    normalized = " ".join(value.split()).strip()
    if len(normalized) <= limit:
        return normalized
    return f"{normalized[: max(0, limit - 3)].rstrip()}..."


def _merge_unique_tail(items: list[str], *, limit: int) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = _compact_text(item, limit=ASSISTANT_RECENT_HISTORY_TEXT_LIMIT)
        if not text:
            continue
        key = _normalize_lookup_token(text)
        if not key or key in seen:
            continue
        result.append(text)
        seen.add(key)
    if len(result) <= limit:
        return result
    return result[-limit:]


def _extract_context_intents(text: str) -> list[str]:
    normalized = _normalize_lookup_token(text)
    if not normalized:
        return []
    tags: list[str] = []
    for label, keywords in ASSISTANT_CONTEXT_INTENT_KEYWORDS.items():
        if any(keyword in normalized for keyword in keywords):
            tags.append(label)
    return tags


def _format_context_summary(
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

    goal = _compact_text(str(profile.get("goal") or ""), limit=80)
    if goal:
        profile_parts.append(f"цель: {goal}")

    subject = _compact_text(str(profile.get("subject") or ""), limit=80)
    if subject:
        profile_parts.append(f"предмет: {subject}")

    learning_format = _compact_text(str(profile.get("format") or ""), limit=80)
    if learning_format:
        profile_parts.append(f"формат: {learning_format}")

    target = _compact_text(str(profile.get("target") or ""), limit=80)
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


def _merge_assistant_context(
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

    normalized_question = _normalize_lookup_token(question)
    if "мфти" in normalized_question:
        profile["target"] = "МФТИ"
    elif "мгу" in normalized_question:
        profile["target"] = "МГУ"

    previous_intents = existing.get("intents") if isinstance(existing.get("intents"), list) else []
    merged_intents = _merge_unique_tail(
        [str(item) for item in previous_intents] + _extract_context_intents(question),
        limit=ASSISTANT_CONTEXT_INTENTS_LIMIT,
    )

    history_user_requests = [
        _compact_text(item.get("text"), limit=ASSISTANT_RECENT_HISTORY_TEXT_LIMIT)
        for item in recent_history
        if item.get("role") == "user"
    ]
    previous_requests = (
        existing.get("recent_user_requests")
        if isinstance(existing.get("recent_user_requests"), list)
        else []
    )
    merged_requests = _merge_unique_tail(
        [str(item) for item in previous_requests]
        + history_user_requests
        + [_compact_text(question, limit=ASSISTANT_RECENT_HISTORY_TEXT_LIMIT)],
        limit=ASSISTANT_CONTEXT_RECENT_REQUESTS_LIMIT,
    )

    summary_text = _compact_text(context_summary, limit=ASSISTANT_CONTEXT_SUMMARY_MAX)
    if not summary_text:
        summary_text = _format_context_summary(
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


def _is_format_compatible(criteria_format: Optional[str], product_format: Optional[str]) -> bool:
    if not criteria_format:
        return True
    if not product_format:
        return False
    if criteria_format == product_format:
        return True
    # Hybrid is treated as compatible with online/offline requests.
    if product_format == "hybrid" or criteria_format == "hybrid":
        return True
    return False


def _is_subject_compatible(criteria_subject: Optional[str], product_subjects: list[str]) -> bool:
    if not criteria_subject:
        return True
    lowered = {item.strip().lower() for item in product_subjects}
    if criteria_subject in lowered:
        return True
    return "general" in lowered


def _evaluate_match_quality(criteria: SearchCriteria, products: list[Product]) -> str:
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
        if _is_subject_compatible(criteria.subject, list(first.subjects)):
            matched += 1
    if criteria.format:
        checks += 1
        if _is_format_compatible(criteria.format, first.format):
            matched += 1

    if checks == 0:
        return "strong" if len(products) >= 2 else "limited"
    if matched == checks and len(products) >= 1:
        return "strong"
    if matched >= max(1, checks - 1):
        return "limited"
    return "limited"


def _build_manager_offer(match_quality: str, has_results: bool) -> Dict[str, object]:
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


def _assistant_mode(question: str, criteria: SearchCriteria) -> str:
    normalized = _normalize_lookup_token(question)
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


def _criteria_from_payload(payload: Optional[AssistantCriteriaPayload], brand_default: str) -> SearchCriteria:
    criteria = payload or AssistantCriteriaPayload()
    brand = _normalize_lookup_token(criteria.brand) or brand_default
    goal = _normalize_lookup_token(criteria.goal) or None
    subject = _normalize_lookup_token(criteria.subject) or None
    learning_format = _normalize_lookup_token(criteria.format) or None
    return SearchCriteria(
        brand=brand,
        grade=criteria.grade,
        goal=goal,
        subject=subject,
        format=learning_format,
    )


def _missing_criteria_fields(criteria: SearchCriteria) -> list[str]:
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


def _extract_tg_init_data(request: Request) -> str:
    direct = request.headers.get("X-Tg-Init-Data", "").strip()
    if direct:
        return direct

    legacy = request.headers.get("X-Telegram-Init-Data", "").strip()
    if legacy:
        return legacy

    auth = request.headers.get("Authorization", "").strip()
    if auth.lower().startswith("tma "):
        token = auth[4:].strip()
        if token:
            return token
    return ""


def _safe_user_payload(payload: dict | None) -> dict:
    if not isinstance(payload, dict):
        return {}
    user_id_raw = payload.get("id")
    user_id = int(user_id_raw) if isinstance(user_id_raw, int) else user_id_raw
    return {
        "id": user_id,
        "first_name": payload.get("first_name"),
        "last_name": payload.get("last_name"),
        "username": payload.get("username"),
        "language_code": payload.get("language_code"),
    }


def _request_id_from_request(request: Request) -> str:
    request_id = getattr(request.state, "request_id", "")
    if isinstance(request_id, str) and request_id.strip():
        return request_id.strip()
    return "unknown"


def _extract_bearer_token(request: Request) -> str:
    auth = request.headers.get("Authorization", "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ""


def _request_client_ip(request: Request) -> str:
    forwarded = request.headers.get(FORWARDED_FOR_HEADER, "").strip()
    if forwarded:
        first = forwarded.split(",", 1)[0].strip()
        if first:
            return first
    if request.client and request.client.host:
        return str(request.client.host)
    return "unknown"


def _format_price_text(product: object) -> str:
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


def _format_next_start_text(product: object) -> str:
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


def create_app(settings: Settings | None = None) -> FastAPI:
    cfg = settings or get_settings()
    init_db(cfg.database_path)
    webhook_path = cfg.telegram_webhook_path if cfg.telegram_webhook_path.startswith("/") else f"/{cfg.telegram_webhook_path}"
    mango_webhook_path = cfg.mango_webhook_path if cfg.mango_webhook_path.startswith("/") else f"/{cfg.mango_webhook_path}"
    telegram_application = None
    assistant_rate_limiter = InMemoryRateLimiter(
        window_seconds=cfg.assistant_rate_limit_window_seconds,
    )
    crm_rate_limiter = InMemoryRateLimiter(
        window_seconds=cfg.crm_rate_limit_window_seconds,
    )
    user_webapp_dist = Path(cfg.webapp_dist_path)
    user_webapp_index = user_webapp_dist / "index.html"
    user_webapp_ready = user_webapp_index.exists()

    if cfg.telegram_mode == "webhook":
        if not cfg.telegram_bot_token:
            logger.warning("TELEGRAM_MODE=webhook but TELEGRAM_BOT_TOKEN is empty. Webhook endpoint will return 503.")
        else:
            telegram_application = bot_runtime.build_application(cfg.telegram_bot_token)

    async def process_next_webhook_queue_item(app_instance: FastAPI) -> bool:
        telegram_app = getattr(app_instance.state, "telegram_application", None)
        if telegram_app is None:
            return False

        conn = get_connection(cfg.database_path)
        try:
            claimed = claim_webhook_update(conn)
        finally:
            conn.close()
        if not claimed:
            return False

        queue_id = int(claimed["id"])
        payload = claimed.get("payload") if isinstance(claimed.get("payload"), dict) else {}
        attempts = int(claimed.get("attempts") or 1)

        try:
            update = Update.de_json(payload, telegram_app.bot)
            if update is None:
                raise ValueError("Could not parse Telegram update payload.")
            await telegram_app.process_update(update)
        except Exception as exc:
            delay = min(60, WEBHOOK_RETRY_BASE_SECONDS ** max(1, min(attempts, 5)))
            conn_retry = get_connection(cfg.database_path)
            try:
                final_state = mark_webhook_update_retry(
                    conn_retry,
                    queue_id=queue_id,
                    error=str(exc),
                    retry_delay_seconds=delay,
                    max_attempts=WEBHOOK_MAX_ATTEMPTS,
                )
            finally:
                conn_retry.close()
            if final_state == "failed":
                logger.exception("Webhook update failed permanently (queue_id=%s)", queue_id)
            else:
                logger.exception("Webhook update failed; queued for retry (queue_id=%s)", queue_id)
            return True

        conn_done = get_connection(cfg.database_path)
        try:
            mark_webhook_update_done(conn_done, queue_id=queue_id)
        finally:
            conn_done.close()
        return True

    async def webhook_worker_loop(app_instance: FastAPI) -> None:
        event = getattr(app_instance.state, "webhook_worker_event", None)
        if event is None:
            return
        while True:
            processed = False
            try:
                while await process_next_webhook_queue_item(app_instance):
                    processed = True
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Webhook worker loop iteration failed")

            if processed:
                continue

            try:
                await asyncio.wait_for(event.wait(), timeout=1.5)
            except asyncio.TimeoutError:
                continue
            finally:
                event.clear()

    @asynccontextmanager
    async def lifespan(app_instance: FastAPI):
        if telegram_application is not None:
            app_instance.state.telegram_application = telegram_application
            await telegram_application.initialize()
            await telegram_application.start()
            app_instance.state.webhook_worker_event = asyncio.Event()
            conn = get_connection(cfg.database_path)
            try:
                restored = requeue_stuck_webhook_updates(
                    conn,
                    stale_after_seconds=WEBHOOK_STALE_PROCESSING_SECONDS,
                )
            finally:
                conn.close()
            if restored:
                logger.warning("Requeued %s stale webhook updates after restart", restored)
            app_instance.state.webhook_worker_task = asyncio.create_task(webhook_worker_loop(app_instance))
            app_instance.state.webhook_worker_event.set()
            logger.info("Telegram webhook application initialized at path: %s", webhook_path)
        if cfg.enable_lead_radar and cfg.lead_radar_scheduler_enabled:
            app_instance.state.lead_radar_event = asyncio.Event()
            app_instance.state.lead_radar_task = asyncio.create_task(lead_radar_loop(app_instance))
            app_instance.state.lead_radar_event.set()
            logger.info(
                "Lead radar scheduler started (interval=%ss, no_reply=%sh, call_no_next_step=%sh, stale_warm=%sd)",
                cfg.lead_radar_interval_seconds,
                cfg.lead_radar_no_reply_hours,
                cfg.lead_radar_call_no_next_step_hours,
                cfg.lead_radar_stale_warm_days,
            )
        if cfg.enable_faq_lab and cfg.faq_lab_scheduler_enabled:
            app_instance.state.faq_lab_event = asyncio.Event()
            app_instance.state.faq_lab_task = asyncio.create_task(faq_lab_loop(app_instance))
            app_instance.state.faq_lab_event.set()
            logger.info(
                "FAQ lab scheduler started (interval=%ss, window=%sd, min_count=%s, max_items=%s)",
                cfg.faq_lab_interval_seconds,
                cfg.faq_lab_window_days,
                cfg.faq_lab_min_question_count,
                cfg.faq_lab_max_items_per_run,
            )
        if _mango_ingest_enabled() and cfg.mango_polling_enabled:
            app_instance.state.mango_poll_event = asyncio.Event()
            app_instance.state.mango_poll_task = asyncio.create_task(mango_poll_loop(app_instance))
            app_instance.state.mango_poll_event.set()
            logger.info(
                "Mango poll scheduler started (interval=%ss, limit=%s, ttl=%sh)",
                cfg.mango_poll_interval_seconds,
                cfg.mango_poll_limit_per_run,
                cfg.mango_call_recording_ttl_hours,
            )
        yield
        if _mango_ingest_enabled() and cfg.mango_polling_enabled:
            mango_task = getattr(app_instance.state, "mango_poll_task", None)
            if mango_task is not None:
                mango_task.cancel()
                try:
                    await mango_task
                except asyncio.CancelledError:
                    pass
            logger.info("Mango poll scheduler stopped")
        if cfg.enable_faq_lab and cfg.faq_lab_scheduler_enabled:
            faq_task = getattr(app_instance.state, "faq_lab_task", None)
            if faq_task is not None:
                faq_task.cancel()
                try:
                    await faq_task
                except asyncio.CancelledError:
                    pass
            logger.info("FAQ lab scheduler stopped")
        if cfg.enable_lead_radar and cfg.lead_radar_scheduler_enabled:
            radar_task = getattr(app_instance.state, "lead_radar_task", None)
            if radar_task is not None:
                radar_task.cancel()
                try:
                    await radar_task
                except asyncio.CancelledError:
                    pass
            logger.info("Lead radar scheduler stopped")
        if telegram_application is not None:
            worker_task = getattr(app_instance.state, "webhook_worker_task", None)
            if worker_task is not None:
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass
            await telegram_application.stop()
            await telegram_application.shutdown()
            logger.info("Telegram webhook application stopped")

    app = FastAPI(title="sales-agent", lifespan=lifespan)

    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        incoming = request.headers.get(REQUEST_ID_HEADER, "").strip()
        request_id = incoming if incoming else uuid4().hex[:12]
        request.state.request_id = request_id
        try:
            response = await call_next(request)
        except Exception:
            logger.exception("Unhandled API error (request_id=%s, path=%s)", request_id, request.url.path)
            response = JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={
                    "ok": False,
                    "detail": {
                        "code": "internal_error",
                        "message": "Unexpected server error.",
                        "user_message": "Сервис временно недоступен. Попробуйте еще раз через минуту.",
                        "request_id": request_id,
                    },
                },
            )
        response.headers[REQUEST_ID_HEADER] = request_id
        return response

    miniapp_dir = project_root() / "sales_agent" / "sales_api" / "static" / "admin_miniapp"
    if miniapp_dir.exists():
        app.mount(
            "/admin/miniapp/static",
            StaticFiles(directory=str(miniapp_dir)),
            name="admin-miniapp-static",
        )
    if user_webapp_ready:
        app.mount(
            "/app",
            StaticFiles(directory=str(user_webapp_dist), html=True),
            name="sales-user-miniapp",
        )

    def require_miniapp_user(request: Request) -> dict:
        if not cfg.admin_miniapp_enabled:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Admin miniapp is disabled.")
        if not cfg.telegram_bot_token:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="TELEGRAM_BOT_TOKEN is not configured.",
            )
        if not cfg.admin_telegram_ids:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="ADMIN_TELEGRAM_IDS is not configured.",
            )

        init_data = _extract_tg_init_data(request)

        auth = verify_telegram_webapp_init_data(
            init_data=init_data,
            bot_token=cfg.telegram_bot_token,
        )
        if not auth.ok:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid Telegram miniapp auth: {auth.reason}",
            )
        if auth.user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Telegram user id is missing in init data.",
            )
        if auth.user_id not in cfg.admin_telegram_ids:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="This Telegram account is not allowed.",
            )
        return {
            "user_id": auth.user_id,
            "user": auth.user or {},
        }

    def require_admin(credentials: HTTPBasicCredentials = Depends(security)) -> str:
        if not cfg.admin_user or not cfg.admin_pass:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Admin auth is not configured. Set ADMIN_USER and ADMIN_PASS.",
            )

        user_ok = secrets.compare_digest(credentials.username, cfg.admin_user)
        pass_ok = secrets.compare_digest(credentials.password, cfg.admin_pass)
        if not (user_ok and pass_ok):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid admin credentials",
                headers={"WWW-Authenticate": "Basic"},
            )
        return credentials.username

    def _enforce_rate_limit(
        *,
        request: Request,
        limiter: InMemoryRateLimiter,
        key: str,
        limit: int,
        scope: str,
    ) -> None:
        decision = limiter.check(key, limit=max(1, int(limit)))
        if decision.allowed:
            return

        request_id = _request_id_from_request(request)
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "code": "rate_limited",
                "scope": scope,
                "message": "Rate limit exceeded.",
                "user_message": "Слишком много запросов подряд. Подождите немного и повторите.",
                "retry_after_seconds": decision.retry_after_seconds,
                "request_id": request_id,
            },
            headers={"Retry-After": str(decision.retry_after_seconds)},
        )

    def _require_assistant_access(request: Request) -> Dict[str, Any]:
        init_data = _extract_tg_init_data(request)
        if init_data:
            if not cfg.telegram_bot_token:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="Assistant auth via Telegram is unavailable: TELEGRAM_BOT_TOKEN is not configured.",
                )
            auth = verify_telegram_webapp_init_data(
                init_data=init_data,
                bot_token=cfg.telegram_bot_token,
            )
            if not auth.ok:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=f"Invalid Telegram miniapp auth: {auth.reason}",
                )
            return {
                "kind": "telegram",
                "user_id": auth.user_id,
            }

        provided_token = request.headers.get(ASSISTANT_API_TOKEN_HEADER, "").strip() or _extract_bearer_token(request)
        expected_token = cfg.assistant_api_token.strip()
        if expected_token and secrets.compare_digest(provided_token, expected_token):
            return {"kind": "service_token", "user_id": None}

        if expected_token:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=(
                    "Assistant auth is required. Provide Telegram initData or "
                    f"{ASSISTANT_API_TOKEN_HEADER}."
                ),
            )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Assistant endpoint is available from Telegram Mini App only.",
        )

    def _require_crm_api_access(
        request: Request,
        credentials: HTTPBasicCredentials = Depends(security),
    ) -> str:
        if not cfg.crm_api_exposed:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="CRM API is disabled.")
        admin_username = require_admin(credentials)
        client_ip = _request_client_ip(request)
        _enforce_rate_limit(
            request=request,
            limiter=crm_rate_limiter,
            key=f"crm:ip:{client_ip}",
            limit=cfg.crm_rate_limit_ip_requests,
            scope="crm_ip",
        )
        return admin_username

    def _require_tallanto_readonly_client() -> TallantoReadOnlyClient:
        if not cfg.tallanto_read_only:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Tallanto read-only mode is disabled. Set TALLANTO_READ_ONLY=1.",
            )
        token = cfg.tallanto_api_token or cfg.tallanto_api_key
        if not cfg.tallanto_api_url or not token:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Tallanto read-only config is incomplete. Fill TALLANTO_API_URL and TALLANTO_API_TOKEN.",
            )
        return TallantoReadOnlyClient(base_url=cfg.tallanto_api_url, token=token)

    def _crm_cache_key(prefix: str, params: Dict[str, Any]) -> str:
        serialized = json.dumps(params, ensure_ascii=False, sort_keys=True)
        return f"crm:{prefix}:{quote_plus(serialized)}"

    def _read_crm_cache(key: str) -> Optional[Dict[str, Any]]:
        conn = get_connection(cfg.database_path)
        try:
            return get_crm_cache(conn, key=key, max_age_seconds=CRM_CACHE_TTL_SECONDS)
        finally:
            conn.close()

    def _write_crm_cache(key: str, payload: Dict[str, Any]) -> None:
        conn = get_connection(cfg.database_path)
        try:
            upsert_crm_cache(conn, key=key, value=payload)
        finally:
            conn.close()

    def _map_tallanto_error(exc: RuntimeError) -> HTTPException:
        message = str(exc)
        lowered = message.lower()
        if "401" in message or "unauthorized" in lowered:
            code = status.HTTP_401_UNAUTHORIZED
        elif "400" in message:
            code = status.HTTP_400_BAD_REQUEST
        elif "403" in message:
            code = status.HTTP_403_FORBIDDEN
        else:
            code = status.HTTP_502_BAD_GATEWAY
        return HTTPException(status_code=code, detail=message)

    def _thread_id_from_user_id(user_id: int) -> str:
        return f"tg:{int(user_id)}"

    def _require_user_exists(conn: Any, user_id: int) -> None:
        row = conn.execute("SELECT id FROM users WHERE id = ? LIMIT 1", (int(user_id),)).fetchone()
        if row is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found.")

    def _calls_storage_root() -> Path:
        if cfg.persistent_data_root != Path():
            return cfg.persistent_data_root / "calls_uploads"
        return project_root() / "data" / "calls_uploads"

    def _resolve_call_thread_and_user(
        conn: Any,
        *,
        user_id_input: Optional[int],
        thread_id_input: Optional[str],
    ) -> tuple[int, str]:
        normalized_thread = (thread_id_input or "").strip()
        resolved_user_id: Optional[int] = None

        if isinstance(user_id_input, int):
            _require_user_exists(conn, user_id_input)
            resolved_user_id = int(user_id_input)
            if not normalized_thread:
                normalized_thread = _thread_id_from_user_id(resolved_user_id)

        if resolved_user_id is None and normalized_thread.startswith("tg:"):
            user_token = normalized_thread[3:]
            if user_token.isdigit():
                candidate_user_id = int(user_token)
                _require_user_exists(conn, candidate_user_id)
                resolved_user_id = candidate_user_id

        if resolved_user_id is None and normalized_thread.startswith("biz:"):
            row = conn.execute(
                """
                SELECT user_id
                FROM business_threads
                WHERE thread_key = ?
                LIMIT 1
                """,
                (normalized_thread,),
            ).fetchone()
            if row and row["user_id"] is not None:
                resolved_user_id = int(row["user_id"])

        if resolved_user_id is None:
            external_id = f"call-import-{uuid4().hex[:12]}"
            resolved_user_id = get_or_create_user(
                conn,
                channel="call_import",
                external_id=external_id,
            )
            if not normalized_thread:
                normalized_thread = _thread_id_from_user_id(resolved_user_id)

        if not normalized_thread:
            normalized_thread = _thread_id_from_user_id(resolved_user_id)
        return resolved_user_id, normalized_thread

    async def _store_call_upload_file(upload: UploadFile) -> Optional[Path]:
        filename = (upload.filename or "").strip()
        if not filename:
            return None
        suffix = Path(filename).suffix.lower()
        if not suffix or len(suffix) > 12:
            suffix = ".bin"
        target_dir = _calls_storage_root()
        target_dir.mkdir(parents=True, exist_ok=True)
        file_path = target_dir / f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid4().hex}{suffix}"
        content = await upload.read()
        if not content:
            return None
        file_path.write_bytes(content)
        return file_path

    def _priority_from_warmth(value: str) -> str:
        normalized = (value or "").strip().lower()
        if normalized in {"hot", "warm", "cold"}:
            return normalized
        return "warm"

    def _mango_ingest_enabled() -> bool:
        return bool(cfg.enable_mango_auto_ingest and cfg.enable_call_copilot)

    def _build_mango_client() -> MangoClient:
        if not cfg.mango_api_base_url or not cfg.mango_api_token:
            raise MangoClientError(
                "Mango API is not configured. Fill MANGO_API_BASE_URL and MANGO_API_TOKEN."
            )
        return MangoClient(
            base_url=cfg.mango_api_base_url,
            token=cfg.mango_api_token,
            calls_path=cfg.mango_calls_path,
            timeout_seconds=12.0,
            webhook_secret=cfg.mango_webhook_secret,
        )

    def _cleanup_old_call_files() -> Dict[str, Any]:
        conn = get_connection(cfg.database_path)
        cleaned = 0
        missing = 0
        errors = 0
        try:
            rows = list_call_records_with_files_for_cleanup(
                conn,
                older_than_hours=max(1, int(cfg.mango_call_recording_ttl_hours)),
                limit=MANGO_CLEANUP_BATCH_SIZE,
            )
            for row in rows:
                call_id = int(row.get("id") or 0)
                raw_path = str(row.get("file_path") or "").strip()
                if not call_id or not raw_path:
                    continue
                file_path = Path(raw_path)
                try:
                    if file_path.exists():
                        file_path.unlink()
                        cleaned += 1
                    else:
                        missing += 1
                    clear_call_record_file_path(conn, call_id=call_id)
                except Exception:
                    errors += 1
            return {"ok": True, "checked": len(rows), "cleaned": cleaned, "missing": missing, "errors": errors}
        finally:
            conn.close()

    def _format_thread_display_name(item: Dict[str, Any]) -> str:
        first_name = str(item.get("first_name") or "").strip()
        last_name = str(item.get("last_name") or "").strip()
        username = str(item.get("username") or "").strip()
        if first_name or last_name:
            return f"{first_name} {last_name}".strip()
        if username:
            return f"@{username}"
        external_id = str(item.get("external_id") or "").strip()
        if external_id:
            return external_id
        return f"user #{item.get('user_id')}"

    def _is_radar_reason(reason: object) -> bool:
        if not isinstance(reason, str):
            return False
        return reason.startswith("radar:")

    def _pending_radar_followup_exists(conn: Any, *, thread_id: str, rule_key: str) -> bool:
        row = conn.execute(
            """
            SELECT id
            FROM followup_tasks
            WHERE thread_id = ?
              AND status = 'pending'
              AND reason LIKE ?
            LIMIT 1
            """,
            (thread_id, f"{rule_key}:%"),
        ).fetchone()
        return row is not None

    def _collect_no_reply_candidates(conn: Any, *, no_reply_hours: int, limit: int) -> list[Dict[str, Any]]:
        cursor = conn.execute(
            """
            WITH last_inbound AS (
                SELECT m.user_id, m.id AS inbound_message_id, m.created_at AS inbound_at, m.text AS inbound_text
                FROM messages m
                JOIN (
                    SELECT user_id, MAX(id) AS max_id
                    FROM messages
                    WHERE direction = 'inbound'
                    GROUP BY user_id
                ) latest ON latest.max_id = m.id
            ),
            last_outbound AS (
                SELECT user_id, MAX(id) AS outbound_message_id
                FROM messages
                WHERE direction = 'outbound'
                GROUP BY user_id
            )
            SELECT li.user_id, li.inbound_message_id, li.inbound_at, li.inbound_text
            FROM last_inbound li
            LEFT JOIN last_outbound lo ON lo.user_id = li.user_id
            WHERE (lo.outbound_message_id IS NULL OR lo.outbound_message_id < li.inbound_message_id)
              AND julianday(li.inbound_at) <= julianday('now') - (? / 24.0)
            ORDER BY li.inbound_message_id DESC
            LIMIT ?
            """,
            (max(1, int(no_reply_hours)), max(1, int(limit))),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _collect_call_no_next_step_candidates(
        conn: Any,
        *,
        call_no_next_step_hours: int,
        limit: int,
    ) -> list[Dict[str, Any]]:
        cursor = conn.execute(
            """
            WITH call_actions AS (
                SELECT thread_id, MAX(id) AS max_id
                FROM approval_actions
                WHERE action = 'manual_action'
                  AND (
                    LOWER(COALESCE(payload_json, '')) LIKE '%call%'
                    OR LOWER(COALESCE(payload_json, '')) LIKE '%звон%'
                  )
                GROUP BY thread_id
            )
            SELECT aa.id AS action_id, aa.user_id, aa.thread_id, aa.created_at, aa.payload_json
            FROM approval_actions aa
            JOIN call_actions ca ON ca.max_id = aa.id
            WHERE julianday(aa.created_at) <= julianday('now') - (? / 24.0)
            ORDER BY aa.id DESC
            LIMIT ?
            """,
            (max(1, int(call_no_next_step_hours)), max(1, int(limit))),
        )
        rows: list[Dict[str, Any]] = []
        for row in cursor.fetchall():
            item = dict(row)
            raw_payload = item.pop("payload_json", None)
            try:
                payload = json.loads(raw_payload) if isinstance(raw_payload, str) else {}
            except json.JSONDecodeError:
                payload = {}
            item["payload"] = payload if isinstance(payload, dict) else {}
            rows.append(item)
        return rows

    def _collect_stale_warm_candidates(conn: Any, *, stale_warm_days: int, limit: int) -> list[Dict[str, Any]]:
        cursor = conn.execute(
            """
            WITH latest_scores AS (
                SELECT thread_id, MAX(id) AS max_id
                FROM lead_scores
                GROUP BY thread_id
            )
            SELECT ls.id AS score_id, ls.user_id, ls.thread_id, ls.score, ls.temperature, ls.created_at
            FROM lead_scores ls
            JOIN latest_scores latest ON latest.max_id = ls.id
            WHERE LOWER(ls.temperature) = 'warm'
              AND julianday(ls.created_at) <= julianday('now') - ?
            ORDER BY ls.id DESC
            LIMIT ?
            """,
            (max(1, int(stale_warm_days)), max(1, int(limit))),
        )
        return [dict(row) for row in cursor.fetchall()]

    def _safe_thread_id(value: object) -> str:
        if not isinstance(value, str):
            return ""
        return value.strip()

    def _build_radar_idempotency_key(*, rule_key: str, thread_id: str, source_token: str) -> str:
        cleaned_rule = rule_key.replace(":", "_").strip("_")
        cleaned_thread = _safe_thread_id(thread_id).replace(":", "_").replace("/", "_")
        cleaned_token = (source_token or "").strip().replace(":", "_").replace("/", "_")
        key = f"lr:{cleaned_rule}:{cleaned_thread}:{cleaned_token}"
        return key[:120]

    def _create_radar_artifacts(
        conn: Any,
        *,
        user_id: int,
        thread_id: str,
        rule_key: str,
        priority: str,
        reason_human: str,
        draft_text: str,
        source_token: str,
        trigger: str,
    ) -> Dict[str, Any]:
        task_id = create_followup_task(
            conn,
            user_id=user_id,
            thread_id=thread_id,
            priority=priority,
            reason=f"{rule_key}: {reason_human}",
            status="pending",
            due_at=None,
            assigned_to="lead_radar:auto",
            related_draft_id=None,
        )
        create_approval_action(
            conn,
            draft_id=None,
            user_id=user_id,
            thread_id=thread_id,
            action="followup_created",
            actor="lead_radar:auto",
            payload={
                "source": "lead_radar",
                "rule": rule_key,
                "trigger": trigger,
                "followup_task_id": task_id,
            },
        )

        draft_id = create_reply_draft(
            conn,
            user_id=user_id,
            thread_id=thread_id,
            source_message_id=None,
            draft_text=draft_text.strip(),
            model_name=LEAD_RADAR_MODEL_NAME,
            quality={"source": "lead_radar", "rule": rule_key, "trigger": trigger},
            created_by="lead_radar:auto",
            status="created",
            idempotency_key=_build_radar_idempotency_key(
                rule_key=rule_key,
                thread_id=thread_id,
                source_token=source_token,
            ),
        )
        create_approval_action(
            conn,
            draft_id=draft_id,
            user_id=user_id,
            thread_id=thread_id,
            action="draft_created",
            actor="lead_radar:auto",
            payload={
                "source": "lead_radar",
                "rule": rule_key,
                "trigger": trigger,
                "followup_task_id": task_id,
            },
        )
        return {"task_id": int(task_id), "draft_id": int(draft_id)}

    def _resolve_radar_limit(limit_override: Optional[int]) -> int:
        max_cfg = max(1, int(cfg.lead_radar_max_items_per_run))
        if limit_override is None:
            return max_cfg
        return max(1, min(int(limit_override), max_cfg))

    lead_radar_lock: Optional[asyncio.Lock] = None

    async def run_lead_radar_once(
        *,
        trigger: str,
        dry_run: bool = False,
        limit_override: Optional[int] = None,
    ) -> Dict[str, Any]:
        effective_limit = _resolve_radar_limit(limit_override)
        if not cfg.enable_lead_radar:
            return {
                "ok": False,
                "enabled": False,
                "trigger": trigger,
                "dry_run": bool(dry_run),
                "limit": effective_limit,
                "created_followups": 0,
                "created_drafts": 0,
                "scanned": 0,
                "rules": {},
            }

        nonlocal lead_radar_lock
        if lead_radar_lock is None:
            lead_radar_lock = asyncio.Lock()

        async with lead_radar_lock:
            conn = get_connection(cfg.database_path)
            try:
                no_reply_candidates = _collect_no_reply_candidates(
                    conn,
                    no_reply_hours=cfg.lead_radar_no_reply_hours,
                    limit=effective_limit,
                )
                call_candidates = _collect_call_no_next_step_candidates(
                    conn,
                    call_no_next_step_hours=cfg.lead_radar_call_no_next_step_hours,
                    limit=effective_limit,
                )
                stale_warm_candidates = _collect_stale_warm_candidates(
                    conn,
                    stale_warm_days=cfg.lead_radar_stale_warm_days,
                    limit=effective_limit,
                )

                result: Dict[str, Any] = {
                    "ok": True,
                    "enabled": True,
                    "trigger": trigger,
                    "dry_run": bool(dry_run),
                    "limit": effective_limit,
                    "created_followups": 0,
                    "created_drafts": 0,
                    "scanned": len(no_reply_candidates) + len(call_candidates) + len(stale_warm_candidates),
                    "rules": {
                        LEAD_RADAR_RULE_NO_REPLY: {"candidates": len(no_reply_candidates), "created": 0},
                        LEAD_RADAR_RULE_CALL_NO_NEXT_STEP: {"candidates": len(call_candidates), "created": 0},
                        LEAD_RADAR_RULE_STALE_WARM: {"candidates": len(stale_warm_candidates), "created": 0},
                    },
                }

                remaining = effective_limit
                for candidate in no_reply_candidates:
                    if remaining <= 0:
                        break
                    user_id = int(candidate.get("user_id") or 0)
                    if user_id <= 0:
                        continue
                    thread_id = _thread_id_from_user_id(user_id)
                    if _pending_radar_followup_exists(conn, thread_id=thread_id, rule_key=LEAD_RADAR_RULE_NO_REPLY):
                        continue
                    if get_conversation_outcome(conn, thread_id=thread_id) is not None:
                        continue
                    inbound_text = str(candidate.get("inbound_text") or "").strip()
                    reason_human = f"Нет ответа менеджера после входящего сообщения более {cfg.lead_radar_no_reply_hours} ч."
                    draft_text = (
                        "Здравствуйте! Спасибо за ожидание. Возвращаюсь к вашему запросу.\n\n"
                        "Хочу уточнить 1-2 детали и сразу предложить конкретный следующий шаг под вашу цель.\n\n"
                        f"Последний запрос клиента: {inbound_text or 'без текста'}"
                    )
                    if dry_run:
                        result["rules"][LEAD_RADAR_RULE_NO_REPLY]["created"] += 1
                        remaining -= 1
                        continue
                    created = _create_radar_artifacts(
                        conn,
                        user_id=user_id,
                        thread_id=thread_id,
                        rule_key=LEAD_RADAR_RULE_NO_REPLY,
                        priority="hot",
                        reason_human=reason_human,
                        draft_text=draft_text,
                        source_token=str(candidate.get("inbound_message_id") or "na"),
                        trigger=trigger,
                    )
                    result["rules"][LEAD_RADAR_RULE_NO_REPLY]["created"] += 1
                    result["created_followups"] += 1
                    result["created_drafts"] += 1 if created.get("draft_id") else 0
                    remaining -= 1

                for candidate in call_candidates:
                    if remaining <= 0:
                        break
                    user_id = int(candidate.get("user_id") or 0)
                    thread_id = _safe_thread_id(candidate.get("thread_id"))
                    if user_id <= 0 or not thread_id:
                        continue
                    if _pending_radar_followup_exists(
                        conn,
                        thread_id=thread_id,
                        rule_key=LEAD_RADAR_RULE_CALL_NO_NEXT_STEP,
                    ):
                        continue
                    if get_conversation_outcome(conn, thread_id=thread_id) is not None:
                        continue
                    action_time = str(candidate.get("created_at") or "").strip()
                    reason_human = (
                        "После звонка не зафиксирован следующий шаг "
                        f"более {cfg.lead_radar_call_no_next_step_hours} ч."
                    )
                    draft_text = (
                        "Спасибо за разговор. Подтверждаю, что зафиксировал ваш запрос.\n\n"
                        "Предлагаю согласовать удобное время короткого follow-up, "
                        "чтобы закрыть оставшиеся вопросы и выбрать следующий шаг.\n\n"
                        f"Контекст звонка (время события): {action_time or 'не указано'}"
                    )
                    if dry_run:
                        result["rules"][LEAD_RADAR_RULE_CALL_NO_NEXT_STEP]["created"] += 1
                        remaining -= 1
                        continue
                    created = _create_radar_artifacts(
                        conn,
                        user_id=user_id,
                        thread_id=thread_id,
                        rule_key=LEAD_RADAR_RULE_CALL_NO_NEXT_STEP,
                        priority="hot",
                        reason_human=reason_human,
                        draft_text=draft_text,
                        source_token=str(candidate.get("action_id") or "na"),
                        trigger=trigger,
                    )
                    result["rules"][LEAD_RADAR_RULE_CALL_NO_NEXT_STEP]["created"] += 1
                    result["created_followups"] += 1
                    result["created_drafts"] += 1 if created.get("draft_id") else 0
                    remaining -= 1

                for candidate in stale_warm_candidates:
                    if remaining <= 0:
                        break
                    user_id = int(candidate.get("user_id") or 0)
                    thread_id = _safe_thread_id(candidate.get("thread_id"))
                    if user_id <= 0 or not thread_id:
                        continue
                    if _pending_radar_followup_exists(conn, thread_id=thread_id, rule_key=LEAD_RADAR_RULE_STALE_WARM):
                        continue
                    if get_conversation_outcome(conn, thread_id=thread_id) is not None:
                        continue
                    score = float(candidate.get("score") or 0.0)
                    score_created_at = str(candidate.get("created_at") or "").strip()
                    reason_human = (
                        f"Теплый лид без активности более {cfg.lead_radar_stale_warm_days} дней "
                        "(нужна реактивация)."
                    )
                    draft_text = (
                        "Возвращаюсь к вашему запросу и подготовил обновленный следующий шаг.\n\n"
                        "Если вам удобно, можем быстро сверить текущую цель и выбрать оптимальную программу.\n\n"
                        f"Текущий warm-score: {score:.1f}; дата оценки: {score_created_at or 'не указана'}."
                    )
                    if dry_run:
                        result["rules"][LEAD_RADAR_RULE_STALE_WARM]["created"] += 1
                        remaining -= 1
                        continue
                    created = _create_radar_artifacts(
                        conn,
                        user_id=user_id,
                        thread_id=thread_id,
                        rule_key=LEAD_RADAR_RULE_STALE_WARM,
                        priority="warm",
                        reason_human=reason_human,
                        draft_text=draft_text,
                        source_token=str(candidate.get("score_id") or "na"),
                        trigger=trigger,
                    )
                    result["rules"][LEAD_RADAR_RULE_STALE_WARM]["created"] += 1
                    result["created_followups"] += 1
                    result["created_drafts"] += 1 if created.get("draft_id") else 0
                    remaining -= 1
            finally:
                conn.close()
        return result

    async def lead_radar_loop(app_instance: FastAPI) -> None:
        event = getattr(app_instance.state, "lead_radar_event", None)
        if event is None:
            return
        interval = max(60, int(cfg.lead_radar_interval_seconds))
        while True:
            try:
                summary = await run_lead_radar_once(trigger="scheduler")
                if int(summary.get("created_followups") or 0) > 0:
                    logger.info(
                        "Lead radar created followups=%s drafts=%s",
                        summary.get("created_followups"),
                        summary.get("created_drafts"),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Lead radar scheduler iteration failed")

            try:
                await asyncio.wait_for(event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue
            finally:
                event.clear()

    faq_lab_lock: Optional[asyncio.Lock] = None

    def _resolve_faq_lab_limit(limit_override: Optional[int]) -> int:
        max_cfg = max(1, int(cfg.faq_lab_max_items_per_run))
        if limit_override is None:
            return max_cfg
        return max(1, min(int(limit_override), max_cfg))

    async def run_faq_lab_once(
        *,
        trigger: str,
        limit_override: Optional[int] = None,
    ) -> Dict[str, Any]:
        effective_limit = _resolve_faq_lab_limit(limit_override)
        if not cfg.enable_faq_lab:
            return {
                "ok": False,
                "enabled": False,
                "trigger": trigger,
                "window_days": cfg.faq_lab_window_days,
                "min_question_count": cfg.faq_lab_min_question_count,
                "limit": effective_limit,
                "model_name": FAQ_LAB_MODEL_NAME,
            }

        nonlocal faq_lab_lock
        if faq_lab_lock is None:
            faq_lab_lock = asyncio.Lock()

        async with faq_lab_lock:
            conn = get_connection(cfg.database_path)
            try:
                summary = faq_lab_service.refresh_faq_lab(
                    conn,
                    window_days=cfg.faq_lab_window_days,
                    min_question_count=cfg.faq_lab_min_question_count,
                    limit=effective_limit,
                    trigger=trigger,
                )
            finally:
                conn.close()

        summary["enabled"] = True
        summary["limit"] = effective_limit
        summary["model_name"] = FAQ_LAB_MODEL_NAME
        return summary

    async def faq_lab_loop(app_instance: FastAPI) -> None:
        event = getattr(app_instance.state, "faq_lab_event", None)
        if event is None:
            return
        interval = max(300, int(cfg.faq_lab_interval_seconds))
        while True:
            try:
                summary = await run_faq_lab_once(trigger="scheduler")
                if int(summary.get("candidates_upserted") or 0) > 0:
                    logger.info(
                        "FAQ lab refreshed: candidates=%s canonical_synced=%s",
                        summary.get("candidates_upserted"),
                        summary.get("canonical_synced"),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("FAQ lab scheduler iteration failed")

            try:
                await asyncio.wait_for(event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue
            finally:
                event.clear()

    def _extract_mango_user_and_thread(
        conn: Any,
        *,
        event: MangoCallEvent,
    ) -> tuple[Optional[int], Optional[str]]:
        payload = event.payload if isinstance(event.payload, dict) else {}
        user_id: Optional[int] = None
        thread_id: Optional[str] = None

        payload_user = payload.get("user_id")
        if isinstance(payload_user, int) and payload_user > 0:
            row = conn.execute("SELECT id FROM users WHERE id = ? LIMIT 1", (payload_user,)).fetchone()
            if row is not None:
                user_id = int(payload_user)

        payload_thread = str(payload.get("thread_id") or "").strip()
        if payload_thread:
            thread_id = payload_thread
            if thread_id.startswith("tg:"):
                token = thread_id[3:]
                if token.isdigit():
                    user_id = int(token)
            elif thread_id.startswith("biz:") and user_id is None:
                row = conn.execute(
                    "SELECT user_id FROM business_threads WHERE thread_key = ? LIMIT 1",
                    (thread_id,),
                ).fetchone()
                if row and row["user_id"] is not None:
                    user_id = int(row["user_id"])

        if user_id is None and event.phone:
            user_id = find_user_by_phone(conn, phone=event.phone)

        if user_id is not None and thread_id is None:
            thread_id = resolve_preferred_thread_for_user(conn, user_id=user_id)
        return user_id, thread_id

    mango_ingest_lock: Optional[asyncio.Lock] = None

    async def ingest_mango_event(
        *,
        event: MangoCallEvent,
        source: str,
        existing_event_row_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        nonlocal mango_ingest_lock
        if mango_ingest_lock is None:
            mango_ingest_lock = asyncio.Lock()

        async with mango_ingest_lock:
            conn = get_connection(cfg.database_path)
            try:
                if isinstance(existing_event_row_id, int) and existing_event_row_id > 0:
                    event_row_id = int(existing_event_row_id)
                    updated = update_mango_event_status(
                        conn,
                        event_row_id=event_row_id,
                        status="processing",
                        error_text=None,
                    )
                    if not updated:
                        raise ValueError(f"Mango event row {event_row_id} not found.")
                else:
                    state = create_or_get_mango_event(
                        conn,
                        event_id=event.event_id,
                        call_external_id=event.call_id,
                        source=source,
                        payload=event.payload,
                    )
                    event_row_id = int(state.get("id") or 0)
                    if not bool(state.get("is_new")):
                        return {
                            "ok": True,
                            "duplicate": True,
                            "event_row_id": event_row_id,
                            "event_id": event.event_id,
                            "call_external_id": event.call_id,
                        }
                    update_mango_event_status(conn, event_row_id=event_row_id, status="processing")
                user_id, thread_id = _extract_mango_user_and_thread(conn, event=event)
            finally:
                conn.close()

            try:
                recording_url = event.recording_url or ""
                transcript_hint = event.transcript_hint or ""
                if not recording_url and not transcript_hint:
                    transcript_hint = (
                        "Звонок импортирован из Mango. Нет ссылки на запись, нужен ручной конспект менеджера."
                    )
                process_result = await _process_manual_call_upload(
                    user_id=user_id,
                    thread_id=thread_id,
                    recording_url=recording_url,
                    transcript_hint=transcript_hint,
                    audio_file=None,
                    source_type_override="mango",
                    source_ref_override=event.call_id or event.event_id,
                    created_by="mango:auto",
                    action_source="mango_auto_ingest",
                    assigned_to="mango:auto",
                )
                conn_done = get_connection(cfg.database_path)
                try:
                    update_mango_event_status(conn_done, event_row_id=event_row_id, status="done")
                finally:
                    conn_done.close()
                return {
                    "ok": True,
                    "duplicate": False,
                    "retried": isinstance(existing_event_row_id, int) and existing_event_row_id > 0,
                    "event_row_id": event_row_id,
                    "event_id": event.event_id,
                    "call_external_id": event.call_id,
                    "call": process_result.get("item"),
                }
            except Exception as exc:
                conn_failed = get_connection(cfg.database_path)
                try:
                    update_mango_event_status(
                        conn_failed,
                        event_row_id=event_row_id,
                        status="failed",
                        error_text=str(exc)[:700],
                    )
                finally:
                    conn_failed.close()
                raise

    async def _fetch_mango_poll_events_with_retries(
        *,
        client: MangoClient,
        since: str,
        limit: int,
    ) -> tuple[list[MangoCallEvent], int]:
        attempts = max(1, int(cfg.mango_poll_retry_attempts))
        base_backoff = max(0, int(cfg.mango_poll_retry_backoff_seconds))
        last_error: Optional[MangoClientError] = None
        for attempt in range(1, attempts + 1):
            try:
                events = client.list_recent_calls(since_iso=since, limit=limit)
                return events, attempt
            except MangoClientError as exc:
                last_error = exc
                if attempt < attempts and base_backoff > 0:
                    await asyncio.sleep(float(base_backoff) * (2 ** (attempt - 1)))
        assert last_error is not None
        raise last_error

    def _event_from_mango_record(item: Dict[str, Any]) -> Optional[MangoCallEvent]:
        payload = item.get("payload")
        payload_dict = payload if isinstance(payload, dict) else {}
        parser = MangoClient(base_url="https://offline.local", token="offline")
        parsed = parser.parse_call_event(payload_dict)
        if parsed is not None:
            return parsed
        event_id = str(item.get("event_id") or "").strip()
        if not event_id:
            return None
        call_external_id = str(item.get("call_external_id") or "").strip()
        return MangoCallEvent(
            event_id=event_id,
            call_id=call_external_id or event_id,
            phone="",
            recording_url="",
            transcript_hint="",
            occurred_at="",
            payload=payload_dict,
        )

    async def run_mango_poll_once(*, trigger: str, limit_override: Optional[int] = None) -> Dict[str, Any]:
        if not _mango_ingest_enabled():
            return {
                "ok": False,
                "enabled": False,
                "trigger": trigger,
                "processed": 0,
                "created": 0,
                "duplicates": 0,
                "failed": 0,
                "attempts": 0,
                "cleanup": _cleanup_old_call_files(),
            }

        try:
            client = _build_mango_client()
        except MangoClientError as exc:
            return {
                "ok": False,
                "enabled": True,
                "trigger": trigger,
                "error": str(exc),
                "processed": 0,
                "created": 0,
                "duplicates": 0,
                "failed": 0,
                "attempts": 0,
                "cleanup": _cleanup_old_call_files(),
            }

        conn = get_connection(cfg.database_path)
        try:
            since = get_latest_mango_event_created_at(conn)
        finally:
            conn.close()

        effective_limit = max(1, min(int(limit_override or cfg.mango_poll_limit_per_run), 500))
        try:
            events, attempts_used = await _fetch_mango_poll_events_with_retries(
                client=client,
                since=since,
                limit=effective_limit,
            )
        except MangoClientError as exc:
            return {
                "ok": False,
                "enabled": True,
                "trigger": trigger,
                "error": str(exc),
                "processed": 0,
                "created": 0,
                "duplicates": 0,
                "failed": 0,
                "attempts": max(1, int(cfg.mango_poll_retry_attempts)),
                "cleanup": _cleanup_old_call_files(),
            }

        processed = 0
        created = 0
        duplicates = 0
        failed = 0
        for event in events:
            processed += 1
            try:
                result = await ingest_mango_event(event=event, source=f"poll:{trigger}")
                if result.get("duplicate"):
                    duplicates += 1
                else:
                    created += 1
            except Exception:
                failed += 1
                logger.exception("Mango poll event processing failed (event_id=%s)", event.event_id)

        cleanup_result = _cleanup_old_call_files()
        return {
            "ok": failed == 0,
            "enabled": True,
            "trigger": trigger,
            "since": since,
            "limit": effective_limit,
            "attempts": attempts_used,
            "fetched": len(events),
            "processed": processed,
            "created": created,
            "duplicates": duplicates,
            "failed": failed,
            "cleanup": cleanup_result,
        }

    async def run_mango_retry_failed_once(*, trigger: str, limit_override: Optional[int] = None) -> Dict[str, Any]:
        if not _mango_ingest_enabled():
            return {
                "ok": False,
                "enabled": False,
                "trigger": trigger,
                "processed": 0,
                "retried": 0,
                "failed": 0,
                "duplicates": 0,
                "cleanup": _cleanup_old_call_files(),
            }

        effective_limit = max(1, min(int(limit_override or cfg.mango_retry_failed_limit_per_run), 500))
        conn = get_connection(cfg.database_path)
        try:
            items = list_mango_events(conn, status="failed", limit=effective_limit, newest_first=False)
        finally:
            conn.close()

        processed = 0
        retried = 0
        failed = 0
        duplicates = 0
        for item in items:
            processed += 1
            event_row_id = int(item.get("id") or 0)
            event = _event_from_mango_record(item)
            if event is None:
                failed += 1
                conn_failed = get_connection(cfg.database_path)
                try:
                    update_mango_event_status(
                        conn_failed,
                        event_row_id=event_row_id,
                        status="failed",
                        error_text="retry_parse_failed",
                    )
                finally:
                    conn_failed.close()
                continue
            try:
                result = await ingest_mango_event(
                    event=event,
                    source=f"retry:{trigger}",
                    existing_event_row_id=event_row_id,
                )
                if result.get("duplicate"):
                    duplicates += 1
                else:
                    retried += 1
            except Exception:
                failed += 1
                logger.exception("Mango retry-failed event processing failed (event_id=%s)", event.event_id)

        cleanup_result = _cleanup_old_call_files()
        return {
            "ok": failed == 0,
            "enabled": True,
            "trigger": trigger,
            "limit": effective_limit,
            "fetched": len(items),
            "processed": processed,
            "retried": retried,
            "duplicates": duplicates,
            "failed": failed,
            "cleanup": cleanup_result,
        }

    async def mango_poll_loop(app_instance: FastAPI) -> None:
        event = getattr(app_instance.state, "mango_poll_event", None)
        if event is None:
            return
        interval = max(30, int(cfg.mango_poll_interval_seconds))
        while True:
            try:
                summary = await run_mango_poll_once(trigger="scheduler")
                if summary.get("processed"):
                    logger.info(
                        "Mango poll processed=%s created=%s duplicates=%s failed=%s",
                        summary.get("processed"),
                        summary.get("created"),
                        summary.get("duplicates"),
                        summary.get("failed"),
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Mango poll scheduler iteration failed")

            try:
                await asyncio.wait_for(event.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue
            finally:
                event.clear()

    def render_page(title: str, body_html: str) -> HTMLResponse:
        page = f"""
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #1f2937; }}
    h1, h2 {{ margin: 0 0 12px; }}
    .muted {{ color: #6b7280; }}
    nav {{ margin-bottom: 16px; }}
    nav a {{ margin-right: 12px; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 12px; }}
    th, td {{ border: 1px solid #d1d5db; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f3f4f6; }}
    .card {{ border: 1px solid #d1d5db; border-radius: 8px; padding: 12px; margin-bottom: 12px; }}
    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 999px; background: #eef2ff; }}
    pre {{ white-space: pre-wrap; word-break: break-word; background: #f9fafb; padding: 10px; border-radius: 6px; }}
    input, button {{ font-size: 14px; }}
    button {{ padding: 8px 12px; cursor: pointer; }}
  </style>
</head>
<body>
  <nav>
    <a href="/admin">Dashboard</a>
    <a href="/admin/ui/inbox">Inbox</a>
    <a href="/admin/ui/business-inbox">Business Inbox</a>
    <a href="/admin/ui/followups">Followups</a>
    <a href="/admin/ui/director">Director</a>
    <a href="/admin/ui/calls">Calls</a>
    <a href="/admin/ui/faq-lab">FAQ Lab</a>
    <a href="/admin/ui/revenue-metrics">Revenue Metrics</a>
    <a href="/admin/ui/leads">Leads</a>
    <a href="/admin/ui/conversations">Conversations</a>
    <a href="/admin/ui/copilot">Copilot</a>
  </nav>
  {body_html}
</body>
</html>
"""
        return HTMLResponse(page)

    app.include_router(
        build_faq_lab_router(
            db_path=cfg.database_path,
            require_admin_dependency=require_admin,
            render_page=render_page,
            enabled=cfg.enable_faq_lab,
            scheduler_enabled=cfg.faq_lab_scheduler_enabled,
            interval_seconds=cfg.faq_lab_interval_seconds,
            window_days=cfg.faq_lab_window_days,
            min_question_count=cfg.faq_lab_min_question_count,
            default_limit=cfg.faq_lab_max_items_per_run,
        )
    )
    app.include_router(
        build_director_router(
            db_path=cfg.database_path,
            require_admin_dependency=require_admin,
            render_page=render_page,
            enabled=cfg.enable_director_agent,
        )
    )

    @app.get("/admin", response_class=HTMLResponse)
    async def admin_home(_: str = Depends(require_admin)):
        conn = get_connection(cfg.database_path)
        try:
            leads_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM leads").fetchone()["cnt"])
            users_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM users").fetchone()["cnt"])
            messages_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM messages").fetchone()["cnt"])
            recent_conversations = list_recent_conversations(conn, limit=5)
        finally:
            conn.close()

        rows: list[str] = []
        for item in recent_conversations:
            user_id = int(item["user_id"])
            rows.append(
                "<tr>"
                f"<td>{user_id}</td>"
                f"<td>{html.escape(str(item.get('channel', '')))}</td>"
                f"<td>{html.escape(str(item.get('external_id', '')))}</td>"
                f"<td>{int(item.get('messages_count') or 0)}</td>"
                f"<td>{html.escape(str(item.get('last_message_at') or '-'))}</td>"
                f"<td><a href='/admin/ui/conversations/{user_id}'>Открыть</a></td>"
                "</tr>"
            )

        body = (
            "<h1>Sales Agent Admin</h1>"
            "<p class='muted'>Оперативный контроль лидов и диалогов</p>"
            "<div class='card'>"
            f"<b>Users:</b> {users_count} &nbsp; | &nbsp; "
            f"<b>Messages:</b> {messages_count} &nbsp; | &nbsp; "
            f"<b>Leads:</b> {leads_count}"
            "</div>"
            "<h2>Последние диалоги</h2>"
            "<table>"
            "<thead><tr><th>User ID</th><th>Channel</th><th>External ID</th><th>Messages</th><th>Last Message</th><th></th></tr></thead>"
            f"<tbody>{''.join(rows) if rows else '<tr><td colspan=6>Нет данных</td></tr>'}</tbody>"
            "</table>"
        )
        return render_page("Sales Agent Admin", body)

    @app.get("/admin/revenue-metrics")
    async def admin_revenue_metrics(_: str = Depends(require_admin)):
        conn = get_connection(cfg.database_path)
        try:
            metrics = get_revenue_metrics_snapshot(conn)
        finally:
            conn.close()
        return {
            "ok": True,
            "metrics": metrics,
            "feature_flags": {
                "enable_business_inbox": cfg.enable_business_inbox,
                "enable_call_copilot": cfg.enable_call_copilot,
                "enable_tallanto_enrichment": cfg.enable_tallanto_enrichment,
                "enable_director_agent": cfg.enable_director_agent,
                "enable_lead_radar": cfg.enable_lead_radar,
                "enable_faq_lab": cfg.enable_faq_lab,
                "enable_mango_auto_ingest": cfg.enable_mango_auto_ingest,
                "lead_radar_scheduler_enabled": cfg.lead_radar_scheduler_enabled,
                "faq_lab_scheduler_enabled": cfg.faq_lab_scheduler_enabled,
            },
            "lead_radar": {
                "interval_seconds": cfg.lead_radar_interval_seconds,
                "no_reply_hours": cfg.lead_radar_no_reply_hours,
                "call_no_next_step_hours": cfg.lead_radar_call_no_next_step_hours,
                "stale_warm_days": cfg.lead_radar_stale_warm_days,
                "max_items_per_run": cfg.lead_radar_max_items_per_run,
            },
            "faq_lab": {
                "interval_seconds": cfg.faq_lab_interval_seconds,
                "window_days": cfg.faq_lab_window_days,
                "min_question_count": cfg.faq_lab_min_question_count,
                "max_items_per_run": cfg.faq_lab_max_items_per_run,
            },
            "mango": {
                "webhook_path": mango_webhook_path,
                "polling_enabled": cfg.mango_polling_enabled,
                "poll_interval_seconds": cfg.mango_poll_interval_seconds,
                "poll_limit_per_run": cfg.mango_poll_limit_per_run,
                "poll_retry_attempts": cfg.mango_poll_retry_attempts,
                "poll_retry_backoff_seconds": cfg.mango_poll_retry_backoff_seconds,
                "retry_failed_limit_per_run": cfg.mango_retry_failed_limit_per_run,
                "recording_ttl_hours": cfg.mango_call_recording_ttl_hours,
                "calls_path": cfg.mango_calls_path,
            },
        }

    @app.get("/admin/ui/revenue-metrics", response_class=HTMLResponse)
    async def admin_revenue_metrics_ui(_: str = Depends(require_admin)):
        conn = get_connection(cfg.database_path)
        try:
            metrics = get_revenue_metrics_snapshot(conn)
        finally:
            conn.close()

        lead_temperature = metrics.get("lead_temperature") if isinstance(metrics, dict) else {}
        hot = int((lead_temperature or {}).get("hot") or 0)
        warm = int((lead_temperature or {}).get("warm") or 0)
        cold = int((lead_temperature or {}).get("cold") or 0)
        body = (
            "<h1>Revenue Metrics</h1>"
            "<div class='card'>"
            f"<b>Drafts Created Today:</b> {int(metrics.get('drafts_created_today') or 0)}<br/>"
            f"<b>Drafts Approved Today:</b> {int(metrics.get('drafts_approved_today') or 0)}<br/>"
            f"<b>Drafts Sent Today:</b> {int(metrics.get('drafts_sent_today') or 0)}<br/>"
            f"<b>Followups Pending:</b> {int(metrics.get('followups_pending') or 0)}<br/>"
            f"<b>Lead Temperature:</b> hot={hot}, warm={warm}, cold={cold}"
            "</div>"
            "<div class='card'>"
            "<b>Feature Flags</b><br/>"
            f"ENABLE_BUSINESS_INBOX={cfg.enable_business_inbox}<br/>"
            f"ENABLE_CALL_COPILOT={cfg.enable_call_copilot}<br/>"
            f"ENABLE_TALLANTO_ENRICHMENT={cfg.enable_tallanto_enrichment}<br/>"
            f"ENABLE_DIRECTOR_AGENT={cfg.enable_director_agent}<br/>"
            f"ENABLE_LEAD_RADAR={cfg.enable_lead_radar}<br/>"
            f"ENABLE_FAQ_LAB={cfg.enable_faq_lab}<br/>"
            f"ENABLE_MANGO_AUTO_INGEST={cfg.enable_mango_auto_ingest}<br/>"
            f"LEAD_RADAR_SCHEDULER_ENABLED={cfg.lead_radar_scheduler_enabled}<br/>"
            f"LEAD_RADAR_INTERVAL_SECONDS={cfg.lead_radar_interval_seconds}<br/>"
            f"LEAD_RADAR_NO_REPLY_HOURS={cfg.lead_radar_no_reply_hours}<br/>"
            f"LEAD_RADAR_CALL_NO_NEXT_STEP_HOURS={cfg.lead_radar_call_no_next_step_hours}<br/>"
            f"LEAD_RADAR_STALE_WARM_DAYS={cfg.lead_radar_stale_warm_days}<br/>"
            f"LEAD_RADAR_MAX_ITEMS_PER_RUN={cfg.lead_radar_max_items_per_run}<br/>"
            f"FAQ_LAB_SCHEDULER_ENABLED={cfg.faq_lab_scheduler_enabled}<br/>"
            f"FAQ_LAB_INTERVAL_SECONDS={cfg.faq_lab_interval_seconds}<br/>"
            f"FAQ_LAB_WINDOW_DAYS={cfg.faq_lab_window_days}<br/>"
            f"FAQ_LAB_MIN_QUESTION_COUNT={cfg.faq_lab_min_question_count}<br/>"
            f"FAQ_LAB_MAX_ITEMS_PER_RUN={cfg.faq_lab_max_items_per_run}<br/>"
            f"MANGO_WEBHOOK_PATH={html.escape(mango_webhook_path)}<br/>"
            f"MANGO_POLLING_ENABLED={cfg.mango_polling_enabled}<br/>"
            f"MANGO_POLL_INTERVAL_SECONDS={cfg.mango_poll_interval_seconds}<br/>"
            f"MANGO_POLL_LIMIT_PER_RUN={cfg.mango_poll_limit_per_run}<br/>"
            f"MANGO_POLL_RETRY_ATTEMPTS={cfg.mango_poll_retry_attempts}<br/>"
            f"MANGO_POLL_RETRY_BACKOFF_SECONDS={cfg.mango_poll_retry_backoff_seconds}<br/>"
            f"MANGO_RETRY_FAILED_LIMIT_PER_RUN={cfg.mango_retry_failed_limit_per_run}<br/>"
            f"MANGO_CALL_RECORDING_TTL_HOURS={cfg.mango_call_recording_ttl_hours}<br/>"
            f"MANGO_CALLS_PATH={html.escape(cfg.mango_calls_path)}"
            "</div>"
        )
        return render_page("Revenue Metrics", body)

    @app.get("/admin/inbox")
    async def admin_inbox(_: str = Depends(require_admin), limit: int = 100):
        conn = get_connection(cfg.database_path)
        try:
            items = list_inbox_threads(conn, limit=max(1, min(limit, 500)))
        finally:
            conn.close()
        return {"ok": True, "items": items}

    @app.get("/admin/followups")
    async def admin_followups(
        _: str = Depends(require_admin),
        status_filter: Optional[str] = Query(default=None, alias="status"),
        priority: Optional[str] = Query(default=None),
        radar_only: bool = False,
        limit: int = 200,
    ):
        normalized_status = (status_filter or "").strip() or None
        normalized_priority = (priority or "").strip().lower() or None
        conn = get_connection(cfg.database_path)
        try:
            items = list_followup_tasks(
                conn,
                status=normalized_status,
                limit=max(1, min(limit, 500)),
            )
        finally:
            conn.close()
        if normalized_priority:
            items = [
                item
                for item in items
                if str(item.get("priority") or "").strip().lower() == normalized_priority
            ]
        if radar_only:
            items = [item for item in items if _is_radar_reason(item.get("reason"))]
        return {"ok": True, "items": items}

    @app.post("/admin/followups/run")
    async def admin_followups_run(
        payload: Optional[LeadRadarRunPayload] = None,
        admin_username: str = Depends(require_admin),
    ):
        if not cfg.enable_lead_radar:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Lead radar is disabled. Set ENABLE_LEAD_RADAR=true.",
            )
        run_payload = payload or LeadRadarRunPayload()
        result = await run_lead_radar_once(
            trigger=f"manual:{admin_username}",
            dry_run=run_payload.dry_run,
            limit_override=run_payload.limit,
        )
        return result

    async def _process_manual_call_upload(
        *,
        user_id: Optional[int],
        thread_id: Optional[str],
        recording_url: Optional[str],
        transcript_hint: Optional[str],
        audio_file: Optional[UploadFile],
        source_type_override: Optional[str] = None,
        source_ref_override: Optional[str] = None,
        created_by: str = "admin:manual",
        action_source: str = "call_copilot",
        assigned_to: str = "sales:manual",
    ) -> Dict[str, Any]:
        has_file = audio_file is not None and bool((audio_file.filename or "").strip())
        has_url = bool((recording_url or "").strip())
        has_hint = bool((transcript_hint or "").strip())
        if not has_file and not has_url and not has_hint:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Provide audio_file, recording_url or transcript_hint.",
            )

        stored_file_path: Optional[Path] = None
        transcript_text = ""
        source_type = (source_type_override or "").strip() or ("url" if has_url and not has_file else "upload")
        source_ref = (source_ref_override or "").strip() or (recording_url or "").strip() or None
        if has_file and audio_file is not None:
            stored_file_path = await _store_call_upload_file(audio_file)
            if stored_file_path is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Uploaded file is empty.",
                )
            transcript_text = extract_transcript_from_file(stored_file_path)
            if not source_ref:
                source_ref = (audio_file.filename or "").strip() or None

        conn = get_connection(cfg.database_path)
        try:
            actor_name = f"{(action_source or 'call_copilot').strip()}:auto"
            resolved_user_id, resolved_thread_id = _resolve_call_thread_and_user(
                conn,
                user_id_input=user_id,
                thread_id_input=thread_id,
            )
            call_id = create_call_record(
                conn,
                user_id=resolved_user_id,
                thread_id=resolved_thread_id,
                source_type=source_type,
                source_ref=source_ref,
                file_path=str(stored_file_path) if stored_file_path else None,
                status="queued",
                created_by=created_by,
            )
            update_call_record_status(conn, call_id=call_id, status="processing")

            effective_transcript = transcript_text or build_transcript_fallback(
                source_type=source_type,
                source_ref=source_ref,
                transcript_hint=transcript_hint,
            )
            upsert_call_transcript(
                conn,
                call_id=call_id,
                provider="heuristic",
                transcript_text=effective_transcript,
                language="ru",
                confidence=0.55 if transcript_text else 0.4,
            )

            insights = build_call_insights(effective_transcript)
            upsert_call_summary(
                conn,
                call_id=call_id,
                summary_text=insights.summary_text,
                interests=insights.interests,
                objections=insights.objections,
                next_best_action=insights.next_best_action,
                warmth=insights.warmth,
                confidence=insights.confidence,
                model_name=CALL_COPILOT_MODEL_NAME,
            )

            lead_score_id = create_lead_score(
                conn,
                user_id=resolved_user_id,
                thread_id=resolved_thread_id,
                score=insights.score,
                temperature=insights.warmth,
                confidence=insights.confidence,
                factors={
                    "source": action_source,
                    "interests": insights.interests,
                    "objections": insights.objections,
                },
            )
            followup_id = create_followup_task(
                conn,
                user_id=resolved_user_id,
                thread_id=resolved_thread_id,
                priority=_priority_from_warmth(insights.warmth),
                reason=f"{action_source}: {insights.next_best_action}",
                status="pending",
                due_at=None,
                assigned_to=assigned_to,
                related_draft_id=None,
            )
            create_approval_action(
                conn,
                draft_id=None,
                user_id=resolved_user_id,
                thread_id=resolved_thread_id,
                action="manual_action",
                actor=actor_name,
                payload={
                    "source": action_source,
                    "call_id": call_id,
                    "followup_task_id": followup_id,
                    "lead_score_id": lead_score_id,
                    "next_best_action": insights.next_best_action,
                },
            )
            create_approval_action(
                conn,
                draft_id=None,
                user_id=resolved_user_id,
                thread_id=resolved_thread_id,
                action="followup_created",
                actor=actor_name,
                payload={
                    "source": action_source,
                    "call_id": call_id,
                    "followup_task_id": followup_id,
                },
            )
            create_approval_action(
                conn,
                draft_id=None,
                user_id=resolved_user_id,
                thread_id=resolved_thread_id,
                action="lead_scored",
                actor=actor_name,
                payload={
                    "source": action_source,
                    "call_id": call_id,
                    "lead_score_id": lead_score_id,
                    "temperature": insights.warmth,
                    "score": insights.score,
                },
            )
            update_call_record_status(conn, call_id=call_id, status="done")
            item = get_call_record(conn, call_id=call_id)
        except HTTPException:
            raise
        except Exception as exc:
            if "call_id" in locals():
                update_call_record_status(
                    conn,
                    call_id=int(call_id),
                    status="failed",
                    error_text=str(exc),
                )
            logger.exception("Call copilot processing failed")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Call processing failed: {exc}",
            ) from exc
        finally:
            conn.close()
            if audio_file is not None:
                await audio_file.close()

        return {"ok": True, "item": item}

    @app.get("/admin/calls")
    async def admin_calls(
        _: str = Depends(require_admin),
        status_filter: Optional[str] = Query(default=None, alias="status"),
        limit: int = 100,
    ):
        if not cfg.enable_call_copilot:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Call Copilot is disabled. Set ENABLE_CALL_COPILOT=true.",
            )
        conn = get_connection(cfg.database_path)
        try:
            items = list_call_records(
                conn,
                status=(status_filter or "").strip() or None,
                limit=max(1, min(limit, 500)),
            )
        finally:
            conn.close()
        return {"ok": True, "items": items}

    @app.get("/admin/calls/{call_id}")
    async def admin_call_detail(call_id: int, _: str = Depends(require_admin)):
        if not cfg.enable_call_copilot:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Call Copilot is disabled. Set ENABLE_CALL_COPILOT=true.",
            )
        conn = get_connection(cfg.database_path)
        try:
            item = get_call_record(conn, call_id=call_id)
        finally:
            conn.close()
        if item is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Call {call_id} not found.")
        return {"ok": True, "item": item}

    @app.post("/admin/calls/upload")
    async def admin_calls_upload(
        _: str = Depends(require_admin),
        user_id: Optional[int] = Form(default=None),
        thread_id: Optional[str] = Form(default=None),
        recording_url: Optional[str] = Form(default=None),
        transcript_hint: Optional[str] = Form(default=None),
        audio_file: Optional[UploadFile] = File(default=None),
    ):
        if not cfg.enable_call_copilot:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Call Copilot is disabled. Set ENABLE_CALL_COPILOT=true.",
            )
        return await _process_manual_call_upload(
            user_id=user_id,
            thread_id=thread_id,
            recording_url=recording_url,
            transcript_hint=transcript_hint,
            audio_file=audio_file,
        )

    @app.post("/admin/calls/mango/poll")
    async def admin_calls_mango_poll(
        _: str = Depends(require_admin),
        limit: Optional[int] = Query(default=None, ge=1, le=500),
    ):
        if not _mango_ingest_enabled():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Mango auto-ingest is disabled. Set ENABLE_MANGO_AUTO_INGEST=true and ENABLE_CALL_COPILOT=true.",
            )
        result = await run_mango_poll_once(trigger="manual", limit_override=limit)
        return result

    @app.post("/admin/calls/mango/retry-failed")
    async def admin_calls_mango_retry_failed(
        _: str = Depends(require_admin),
        limit: Optional[int] = Query(default=None, ge=1, le=500),
    ):
        if not _mango_ingest_enabled():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Mango auto-ingest is disabled. Set ENABLE_MANGO_AUTO_INGEST=true and ENABLE_CALL_COPILOT=true.",
            )
        result = await run_mango_retry_failed_once(trigger="manual", limit_override=limit)
        return result

    @app.get("/admin/calls/mango/events")
    async def admin_calls_mango_events(
        _: str = Depends(require_admin),
        status_filter: Optional[str] = Query(default=None, alias="status"),
        limit: int = 100,
    ):
        conn = get_connection(cfg.database_path)
        try:
            items = list_mango_events(
                conn,
                status=(status_filter or "").strip() or None,
                limit=max(1, min(limit, 500)),
            )
        finally:
            conn.close()
        return {"ok": True, "items": items}

    @app.get("/admin/business/inbox")
    async def admin_business_inbox(_: str = Depends(require_admin), limit: int = 100):
        conn = get_connection(cfg.database_path)
        try:
            items = list_recent_business_threads(conn, limit=max(1, min(limit, 500)))
        finally:
            conn.close()
        return {"ok": True, "items": items}

    @app.get("/admin/business/inbox/thread")
    async def admin_business_inbox_thread(
        thread_key: str = Query(..., min_length=5),
        _: str = Depends(require_admin),
    ):
        normalized_thread_key = thread_key.strip()
        conn = get_connection(cfg.database_path)
        try:
            thread_row = conn.execute(
                """
                SELECT
                    thread_key,
                    business_connection_id,
                    chat_id,
                    user_id,
                    last_message_at,
                    last_inbound_at,
                    last_outbound_at,
                    updated_at
                FROM business_threads
                WHERE thread_key = ?
                LIMIT 1
                """,
                (normalized_thread_key,),
            ).fetchone()
            if not thread_row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Business thread not found: {normalized_thread_key}",
                )

            thread = dict(thread_row)
            business_connection = get_business_connection(
                conn,
                business_connection_id=str(thread.get("business_connection_id") or ""),
            )
            messages = list_business_messages(conn, thread_key=normalized_thread_key, limit=500)
            drafts = list_reply_drafts_for_thread(conn, thread_id=normalized_thread_key, limit=100)
            approval_actions = list_approval_actions_for_thread(conn, thread_id=normalized_thread_key, limit=200)

            user = None
            user_id_value = thread.get("user_id")
            if isinstance(user_id_value, int):
                user_row = conn.execute(
                    """
                    SELECT id, channel, external_id, username, first_name, last_name, created_at
                    FROM users
                    WHERE id = ?
                    LIMIT 1
                    """,
                    (user_id_value,),
                ).fetchone()
                if user_row:
                    user = dict(user_row)
        finally:
            conn.close()

        return {
            "ok": True,
            "thread": thread,
            "business_connection": business_connection,
            "user": user,
            "messages": messages,
            "drafts": drafts,
            "approval_actions": approval_actions,
        }

    @app.get("/admin/inbox/{user_id}")
    async def admin_inbox_detail(user_id: int, _: str = Depends(require_admin)):
        conn = get_connection(cfg.database_path)
        try:
            detail = get_inbox_thread_detail(conn, user_id=user_id, limit_messages=500)
        finally:
            conn.close()
        if detail.get("user") is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found.")
        return {"ok": True, **detail}

    @app.post("/admin/inbox/{user_id}/drafts")
    async def admin_inbox_create_draft(
        user_id: int,
        payload: ReplyDraftPayload,
        admin_username: str = Depends(require_admin),
    ):
        thread_id = _thread_id_from_user_id(user_id)
        conn = get_connection(cfg.database_path)
        try:
            _require_user_exists(conn, user_id)
            draft_id = create_reply_draft(
                conn,
                user_id=user_id,
                thread_id=thread_id,
                draft_text=payload.draft_text.strip(),
                source_message_id=payload.source_message_id,
                model_name=payload.model_name,
                quality=payload.quality or {},
                created_by=admin_username,
                status="created",
                idempotency_key=payload.idempotency_key,
            )
            create_approval_action(
                conn,
                draft_id=draft_id,
                user_id=user_id,
                thread_id=thread_id,
                action="draft_created",
                actor=admin_username,
                payload={
                    "source_message_id": payload.source_message_id,
                    "model_name": payload.model_name,
                },
            )
            draft = get_reply_draft(conn, draft_id)
        finally:
            conn.close()
        return {"ok": True, "draft": draft}

    @app.patch("/admin/inbox/drafts/{draft_id}")
    async def admin_inbox_update_draft(
        draft_id: int,
        payload: ReplyDraftUpdatePayload,
        admin_username: str = Depends(require_admin),
    ):
        conn = get_connection(cfg.database_path)
        try:
            draft = get_reply_draft(conn, draft_id)
            if draft is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Draft {draft_id} not found.")
            updated = update_reply_draft_text(
                conn,
                draft_id=draft_id,
                draft_text=payload.draft_text.strip(),
                model_name=payload.model_name,
                quality=payload.quality or {},
                actor=admin_username,
            )
            if not updated:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Draft {draft_id} not found.")
            create_approval_action(
                conn,
                draft_id=draft_id,
                user_id=int(draft["user_id"]),
                thread_id=str(draft["thread_id"]),
                action="draft_edited",
                actor=admin_username,
                payload={"model_name": payload.model_name},
            )
            updated_draft = get_reply_draft(conn, draft_id)
        finally:
            conn.close()
        return {"ok": True, "draft": updated_draft}

    @app.post("/admin/inbox/drafts/{draft_id}/approve")
    async def admin_inbox_approve_draft(draft_id: int, admin_username: str = Depends(require_admin)):
        conn = get_connection(cfg.database_path)
        try:
            draft = get_reply_draft(conn, draft_id)
            if draft is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Draft {draft_id} not found.")
            current_status = str(draft.get("status") or "")
            if current_status == "sent":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Draft {draft_id} is already sent and cannot be approved.",
                )
            if current_status == "approved":
                return {"ok": True, "already_approved": True, "draft": draft}
            update_reply_draft_status(
                conn,
                draft_id=draft_id,
                status="approved",
                actor=admin_username,
            )
            create_approval_action(
                conn,
                draft_id=draft_id,
                user_id=int(draft["user_id"]),
                thread_id=str(draft["thread_id"]),
                action="draft_approved",
                actor=admin_username,
                payload={},
            )
            updated_draft = get_reply_draft(conn, draft_id)
        finally:
            conn.close()
        return {"ok": True, "draft": updated_draft}

    @app.post("/admin/inbox/drafts/{draft_id}/reject")
    async def admin_inbox_reject_draft(draft_id: int, admin_username: str = Depends(require_admin)):
        conn = get_connection(cfg.database_path)
        try:
            draft = get_reply_draft(conn, draft_id)
            if draft is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Draft {draft_id} not found.")
            current_status = str(draft.get("status") or "")
            if current_status == "sent":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Draft {draft_id} is already sent and cannot be rejected.",
                )
            update_reply_draft_status(
                conn,
                draft_id=draft_id,
                status="rejected",
                actor=admin_username,
            )
            create_approval_action(
                conn,
                draft_id=draft_id,
                user_id=int(draft["user_id"]),
                thread_id=str(draft["thread_id"]),
                action="draft_rejected",
                actor=admin_username,
                payload={},
            )
            updated_draft = get_reply_draft(conn, draft_id)
        finally:
            conn.close()
        return {"ok": True, "draft": updated_draft}

    @app.post("/admin/inbox/drafts/{draft_id}/send")
    async def admin_inbox_send_draft(
        draft_id: int,
        payload: DraftSendPayload,
        admin_username: str = Depends(require_admin),
    ):
        conn = get_connection(cfg.database_path)
        try:
            draft = get_reply_draft(conn, draft_id)
            if draft is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Draft {draft_id} not found.")

            current_status = str(draft.get("status") or "")
            if current_status == "sent":
                return {"ok": True, "already_sent": True, "draft": draft}
            if current_status != "approved":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Draft {draft_id} must be approved before send.",
                )

            update_reply_draft_status(
                conn,
                draft_id=draft_id,
                status="sent",
                actor=admin_username,
                sent_message_id=payload.sent_message_id,
            )
            create_approval_action(
                conn,
                draft_id=draft_id,
                user_id=int(draft["user_id"]),
                thread_id=str(draft["thread_id"]),
                action="draft_sent",
                actor=admin_username,
                payload={"sent_message_id": payload.sent_message_id},
            )
            updated_draft = get_reply_draft(conn, draft_id)
        finally:
            conn.close()
        return {"ok": True, "draft": updated_draft}

    @app.post("/admin/inbox/{user_id}/outcome")
    async def admin_inbox_set_outcome(
        user_id: int,
        payload: ConversationOutcomePayload,
        admin_username: str = Depends(require_admin),
    ):
        thread_id = _thread_id_from_user_id(user_id)
        conn = get_connection(cfg.database_path)
        try:
            _require_user_exists(conn, user_id)
            upsert_conversation_outcome(
                conn,
                user_id=user_id,
                thread_id=thread_id,
                outcome=payload.outcome.strip(),
                note=(payload.note or "").strip() or None,
                created_by=admin_username,
            )
            create_approval_action(
                conn,
                draft_id=None,
                user_id=user_id,
                thread_id=thread_id,
                action="conversation_outcome_set",
                actor=admin_username,
                payload={"outcome": payload.outcome.strip()},
            )
            outcome = get_conversation_outcome(conn, thread_id=thread_id)
        finally:
            conn.close()
        return {"ok": True, "outcome": outcome}

    @app.post("/admin/inbox/{user_id}/followups")
    async def admin_inbox_create_followup(
        user_id: int,
        payload: FollowupTaskPayload,
        admin_username: str = Depends(require_admin),
    ):
        thread_id = _thread_id_from_user_id(user_id)
        conn = get_connection(cfg.database_path)
        try:
            _require_user_exists(conn, user_id)
            task_id = create_followup_task(
                conn,
                user_id=user_id,
                thread_id=thread_id,
                priority=payload.priority,
                reason=payload.reason.strip(),
                status="pending",
                due_at=(payload.due_at or "").strip() or None,
                assigned_to=(payload.assigned_to or "").strip() or None,
            )
            create_approval_action(
                conn,
                draft_id=None,
                user_id=user_id,
                thread_id=thread_id,
                action="followup_created",
                actor=admin_username,
                payload={"task_id": task_id, "priority": payload.priority},
            )
            tasks = [item for item in list_followup_tasks(conn, status=None, limit=500) if int(item["id"]) == task_id]
        finally:
            conn.close()
        return {"ok": True, "task": tasks[0] if tasks else None}

    @app.post("/admin/inbox/{user_id}/lead-score")
    async def admin_inbox_create_lead_score(
        user_id: int,
        payload: LeadScorePayload,
        admin_username: str = Depends(require_admin),
    ):
        thread_id = _thread_id_from_user_id(user_id)
        conn = get_connection(cfg.database_path)
        try:
            _require_user_exists(conn, user_id)
            score_id = create_lead_score(
                conn,
                user_id=user_id,
                thread_id=thread_id,
                score=payload.score,
                temperature=payload.temperature,
                confidence=payload.confidence,
                factors=payload.factors or {},
            )
            create_approval_action(
                conn,
                draft_id=None,
                user_id=user_id,
                thread_id=thread_id,
                action="lead_scored",
                actor=admin_username,
                payload={
                    "score_id": score_id,
                    "score": payload.score,
                    "temperature": payload.temperature,
                    "confidence": payload.confidence,
                },
            )
            lead_score = get_latest_lead_score(conn, thread_id=thread_id)
        finally:
            conn.close()
        return {"ok": True, "lead_score": lead_score}

    @app.post("/admin/inbox/{user_id}/events")
    async def admin_inbox_log_event(
        user_id: int,
        payload: RevenueEventPayload,
        admin_username: str = Depends(require_admin),
    ):
        thread_id = _thread_id_from_user_id(user_id)
        conn = get_connection(cfg.database_path)
        try:
            _require_user_exists(conn, user_id)
            action_id = create_approval_action(
                conn,
                draft_id=payload.draft_id,
                user_id=user_id,
                thread_id=thread_id,
                action=payload.action,
                actor=admin_username,
                payload=payload.payload or {},
            )
        finally:
            conn.close()
        return {"ok": True, "action_id": action_id}

    @app.get("/admin/ui/inbox", response_class=HTMLResponse)
    async def admin_inbox_ui(_: str = Depends(require_admin), limit: int = 100):
        conn = get_connection(cfg.database_path)
        try:
            items = list_inbox_threads(conn, limit=max(1, min(limit, 500)))
        finally:
            conn.close()

        rows: list[str] = []
        for item in items:
            user_id = int(item["user_id"])
            status_value = html.escape(str(item.get("status") or "new"))
            display_name = html.escape(_format_thread_display_name(item))
            last_message = html.escape(str(item.get("last_message_at") or "-"))
            messages_count = int(item.get("messages_count") or 0)
            pending_followups = int(item.get("pending_followups") or 0)
            rows.append(
                "<tr>"
                f"<td>{user_id}</td>"
                f"<td>{display_name}</td>"
                f"<td><span class='badge'>{status_value}</span></td>"
                f"<td>{messages_count}</td>"
                f"<td>{pending_followups}</td>"
                f"<td>{last_message}</td>"
                f"<td><a href='/admin/ui/inbox/{user_id}'>Открыть тред</a></td>"
                "</tr>"
            )

        body = (
            "<h1>Inbox</h1>"
            "<p class='muted'>Треды, драфты, follow-up и статусы продаж.</p>"
            "<table>"
            "<thead><tr><th>User ID</th><th>Клиент</th><th>Статус</th><th>Messages</th><th>Followups</th><th>Last Message</th><th></th></tr></thead>"
            f"<tbody>{''.join(rows) if rows else '<tr><td colspan=7>Нет тредов</td></tr>'}</tbody>"
            "</table>"
        )
        return render_page("Inbox", body)

    @app.get("/admin/ui/followups", response_class=HTMLResponse)
    async def admin_followups_ui(
        _: str = Depends(require_admin),
        status_filter: Optional[str] = Query(default=None, alias="status"),
        priority: Optional[str] = Query(default=None),
        radar_only: bool = False,
        limit: int = 200,
    ):
        normalized_status = (status_filter or "").strip() or None
        normalized_priority = (priority or "").strip().lower() or None
        conn = get_connection(cfg.database_path)
        try:
            items = list_followup_tasks(
                conn,
                status=normalized_status,
                limit=max(1, min(limit, 500)),
            )
        finally:
            conn.close()
        if normalized_priority:
            items = [
                item
                for item in items
                if str(item.get("priority") or "").strip().lower() == normalized_priority
            ]
        if radar_only:
            items = [item for item in items if _is_radar_reason(item.get("reason"))]

        rows: list[str] = []
        for item in items:
            thread_id = html.escape(str(item.get("thread_id") or ""))
            thread_link = thread_id
            raw_thread = str(item.get("thread_id") or "")
            if raw_thread.startswith("tg:") and raw_thread[3:].isdigit():
                thread_link = (
                    f"<a href='/admin/ui/inbox/{int(raw_thread[3:])}'>"
                    f"{thread_id}</a>"
                )
            elif raw_thread.startswith("biz:"):
                thread_link = (
                    f"<a href='/admin/ui/business-inbox/thread?thread_key={quote_plus(raw_thread)}'>"
                    f"{thread_id}</a>"
                )

            rows.append(
                "<tr>"
                f"<td>{int(item.get('id') or 0)}</td>"
                f"<td>{thread_link}</td>"
                f"<td><span class='badge'>{html.escape(str(item.get('priority') or ''))}</span></td>"
                f"<td><span class='badge'>{html.escape(str(item.get('status') or ''))}</span></td>"
                f"<td>{html.escape(str(item.get('assigned_to') or '-'))}</td>"
                f"<td>{html.escape(str(item.get('created_at') or '-'))}</td>"
                f"<td><pre>{html.escape(str(item.get('reason') or ''))}</pre></td>"
                "</tr>"
            )

        body = (
            "<h1>Followups</h1>"
            "<p class='muted'>Очередь задач для менеджеров, включая Lead Radar сигналы.</p>"
            "<div class='card'>"
            f"<b>Lead Radar:</b> enabled={cfg.enable_lead_radar} | "
            f"interval={cfg.lead_radar_interval_seconds}s | "
            f"no-reply={cfg.lead_radar_no_reply_hours}h | "
            f"call-no-next={cfg.lead_radar_call_no_next_step_hours}h | "
            f"stale-warm={cfg.lead_radar_stale_warm_days}d"
            "</div>"
            "<table>"
            "<thead><tr><th>ID</th><th>Thread</th><th>Priority</th><th>Status</th><th>Assigned</th><th>Created</th><th>Reason</th></tr></thead>"
            f"<tbody>{''.join(rows) if rows else '<tr><td colspan=7>Нет задач</td></tr>'}</tbody>"
            "</table>"
        )
        return render_page("Followups", body)

    @app.get("/admin/ui/calls", response_class=HTMLResponse)
    async def admin_calls_ui(
        _: str = Depends(require_admin),
        status_filter: Optional[str] = Query(default=None, alias="status"),
        limit: int = 100,
    ):
        if not cfg.enable_call_copilot:
            body = (
                "<h1>Calls</h1>"
                "<div class='card'>Call Copilot disabled. Set <code>ENABLE_CALL_COPILOT=true</code>.</div>"
            )
            return render_page("Calls", body)

        conn = get_connection(cfg.database_path)
        try:
            items = list_call_records(
                conn,
                status=(status_filter or "").strip() or None,
                limit=max(1, min(limit, 500)),
            )
        finally:
            conn.close()

        rows: list[str] = []
        for item in items:
            call_id = int(item.get("id") or 0)
            thread_id = html.escape(str(item.get("thread_id") or ""))
            warm = html.escape(str(item.get("warmth") or "-"))
            summary = html.escape(str(item.get("summary_text") or ""))
            if len(summary) > 200:
                summary = f"{summary[:197]}..."
            rows.append(
                "<tr>"
                f"<td><a href='/admin/ui/calls/{call_id}'>{call_id}</a></td>"
                f"<td>{thread_id}</td>"
                f"<td><span class='badge'>{html.escape(str(item.get('status') or ''))}</span></td>"
                f"<td><span class='badge'>{warm}</span></td>"
                f"<td>{html.escape(str(item.get('created_at') or '-'))}</td>"
                f"<td>{summary or '-'}</td>"
                "</tr>"
            )

        body = (
            "<h1>Calls</h1>"
            "<p class='muted'>Ручная загрузка звонков, автоконспект и next best action.</p>"
            "<div class='card'>"
            f"<b>Mango Auto-Ingest:</b> enabled={_mango_ingest_enabled()} | "
            f"polling={cfg.mango_polling_enabled} | interval={cfg.mango_poll_interval_seconds}s | "
            f"ttl={cfg.mango_call_recording_ttl_hours}h | "
            f"retries={cfg.mango_poll_retry_attempts} (backoff={cfg.mango_poll_retry_backoff_seconds}s)<br/>"
            "<form method='post' action='/admin/ui/calls/mango/poll'>"
            f"<p><label>Manual poll limit: <input type='number' name='limit' min='1' max='500' value='{cfg.mango_poll_limit_per_run}' /></label></p>"
            "<p><button type='submit'>Запустить Mango poll сейчас</button></p>"
            "</form>"
            "<form method='post' action='/admin/ui/calls/mango/retry-failed'>"
            f"<p><label>Retry failed limit: <input type='number' name='limit' min='1' max='500' value='{cfg.mango_retry_failed_limit_per_run}' /></label></p>"
            "<p><button type='submit'>Повторно обработать failed events</button></p>"
            "</form>"
            "<p><a href='/admin/calls/mango/events'>Открыть Mango events (JSON)</a></p>"
            "</div>"
            "<div class='card'>"
            "<form method='post' action='/admin/ui/calls/upload' enctype='multipart/form-data'>"
            "<p><label>User ID (optional): <input type='number' name='user_id' min='1' /></label></p>"
            "<p><label>Thread ID (optional): <input name='thread_id' placeholder='tg:123 or biz:...' style='width:320px;' /></label></p>"
            "<p><label>Recording URL (optional): <input name='recording_url' placeholder='https://...' style='width:420px;' /></label></p>"
            "<p><label>Audio file (optional): <input type='file' name='audio_file' /></label></p>"
            "<p><label>Transcript hint (optional):<br/><textarea name='transcript_hint' rows='3' style='width:100%;' placeholder='Краткий конспект звонка'></textarea></label></p>"
            "<p><button type='submit'>Загрузить и обработать звонок</button></p>"
            "</form>"
            "</div>"
            "<table>"
            "<thead><tr><th>ID</th><th>Thread</th><th>Status</th><th>Warmth</th><th>Created</th><th>Summary</th></tr></thead>"
            f"<tbody>{''.join(rows) if rows else '<tr><td colspan=6>Нет звонков</td></tr>'}</tbody>"
            "</table>"
        )
        return render_page("Calls", body)

    @app.post("/admin/ui/calls/mango/poll")
    async def admin_calls_ui_mango_poll(
        _: str = Depends(require_admin),
        limit: int = Form(50),
    ):
        if not _mango_ingest_enabled():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Mango auto-ingest is disabled. Set ENABLE_MANGO_AUTO_INGEST=true and ENABLE_CALL_COPILOT=true.",
            )
        await run_mango_poll_once(trigger="manual_ui", limit_override=limit)
        return RedirectResponse(url="/admin/ui/calls", status_code=status.HTTP_303_SEE_OTHER)

    @app.post("/admin/ui/calls/mango/retry-failed")
    async def admin_calls_ui_mango_retry_failed(
        _: str = Depends(require_admin),
        limit: int = Form(25),
    ):
        if not _mango_ingest_enabled():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Mango auto-ingest is disabled. Set ENABLE_MANGO_AUTO_INGEST=true and ENABLE_CALL_COPILOT=true.",
            )
        await run_mango_retry_failed_once(trigger="manual_ui", limit_override=limit)
        return RedirectResponse(url="/admin/ui/calls", status_code=status.HTTP_303_SEE_OTHER)

    @app.post("/admin/ui/calls/upload")
    async def admin_calls_ui_upload(
        _: str = Depends(require_admin),
        user_id: Optional[int] = Form(default=None),
        thread_id: Optional[str] = Form(default=None),
        recording_url: Optional[str] = Form(default=None),
        transcript_hint: Optional[str] = Form(default=None),
        audio_file: Optional[UploadFile] = File(default=None),
    ):
        if not cfg.enable_call_copilot:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Call Copilot is disabled. Set ENABLE_CALL_COPILOT=true.",
            )
        result = await _process_manual_call_upload(
            user_id=user_id,
            thread_id=thread_id,
            recording_url=recording_url,
            transcript_hint=transcript_hint,
            audio_file=audio_file,
        )
        item = result.get("item") if isinstance(result, dict) else None
        if not isinstance(item, dict) or "id" not in item:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Call created but detail is missing.",
            )
        return RedirectResponse(
            url=f"/admin/ui/calls/{int(item['id'])}",
            status_code=status.HTTP_303_SEE_OTHER,
        )

    @app.get("/admin/ui/calls/{call_id}", response_class=HTMLResponse)
    async def admin_call_detail_ui(call_id: int, _: str = Depends(require_admin)):
        if not cfg.enable_call_copilot:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Call Copilot is disabled. Set ENABLE_CALL_COPILOT=true.",
            )
        conn = get_connection(cfg.database_path)
        try:
            item = get_call_record(conn, call_id=call_id)
        finally:
            conn.close()
        if item is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Call {call_id} not found.")

        body = (
            "<h1>Call Detail</h1>"
            "<div class='card'>"
            f"<b>Call ID:</b> {int(item.get('id') or 0)}<br/>"
            f"<b>Thread:</b> {html.escape(str(item.get('thread_id') or '-'))}<br/>"
            f"<b>Status:</b> <span class='badge'>{html.escape(str(item.get('status') or ''))}</span><br/>"
            f"<b>Warmth:</b> <span class='badge'>{html.escape(str(item.get('warmth') or '-'))}</span><br/>"
            f"<b>Created:</b> {html.escape(str(item.get('created_at') or '-'))}<br/>"
            f"<b>Source:</b> {html.escape(str(item.get('source_type') or '-'))} "
            f"{html.escape(str(item.get('source_ref') or ''))}"
            "</div>"
            "<div class='card'>"
            f"<b>Summary</b><pre>{html.escape(str(item.get('summary_text') or ''))}</pre>"
            f"<b>Next Best Action</b><pre>{html.escape(str(item.get('next_best_action') or ''))}</pre>"
            f"<b>Interests</b><pre>{html.escape(', '.join(item.get('interests') or []))}</pre>"
            f"<b>Objections</b><pre>{html.escape(', '.join(item.get('objections') or []))}</pre>"
            "</div>"
            "<div class='card'>"
            f"<b>Transcript</b><pre>{html.escape(str(item.get('transcript_text') or ''))}</pre>"
            "</div>"
            "<p><a href='/admin/ui/calls'>← Назад к звонкам</a></p>"
        )
        return render_page("Call Detail", body)

    @app.get("/admin/ui/business-inbox", response_class=HTMLResponse)
    async def admin_business_inbox_ui(_: str = Depends(require_admin), limit: int = 100):
        conn = get_connection(cfg.database_path)
        try:
            items = list_recent_business_threads(conn, limit=max(1, min(limit, 500)))
        finally:
            conn.close()

        rows: list[str] = []
        for item in items:
            thread_key = html.escape(str(item.get("thread_key") or ""))
            display_name = html.escape(_format_thread_display_name(item))
            last_message = html.escape(str(item.get("last_message_at") or "-"))
            messages_count = int(item.get("messages_count") or 0)
            status_value = "active"
            rows.append(
                "<tr>"
                f"<td>{thread_key}</td>"
                f"<td>{display_name}</td>"
                f"<td><span class='badge'>{status_value}</span></td>"
                f"<td>{messages_count}</td>"
                f"<td>{last_message}</td>"
                f"<td><a href='/admin/ui/business-inbox/thread?thread_key={quote_plus(str(item.get('thread_key') or ''))}'>Открыть тред</a></td>"
                "</tr>"
            )

        body = (
            "<h1>Business Inbox</h1>"
            "<p class='muted'>Telegram Business диалоги и события.</p>"
            "<table>"
            "<thead><tr><th>Thread Key</th><th>Клиент</th><th>Статус</th><th>Messages</th><th>Last Message</th><th></th></tr></thead>"
            f"<tbody>{''.join(rows) if rows else '<tr><td colspan=6>Нет business тредов</td></tr>'}</tbody>"
            "</table>"
        )
        return render_page("Business Inbox", body)

    @app.get("/admin/ui/business-inbox/thread", response_class=HTMLResponse)
    async def admin_business_inbox_thread_ui(
        thread_key: str = Query(..., min_length=5),
        _: str = Depends(require_admin),
    ):
        normalized_thread_key = thread_key.strip()
        conn = get_connection(cfg.database_path)
        try:
            thread_row = conn.execute(
                """
                SELECT
                    thread_key,
                    business_connection_id,
                    chat_id,
                    user_id,
                    last_message_at,
                    last_inbound_at,
                    last_outbound_at,
                    updated_at
                FROM business_threads
                WHERE thread_key = ?
                LIMIT 1
                """,
                (normalized_thread_key,),
            ).fetchone()
            if not thread_row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Business thread not found: {normalized_thread_key}",
                )
            thread = dict(thread_row)
            business_connection = get_business_connection(
                conn,
                business_connection_id=str(thread.get("business_connection_id") or ""),
            )
            messages = list_business_messages(conn, thread_key=normalized_thread_key, limit=300)
            drafts = list_reply_drafts_for_thread(conn, thread_id=normalized_thread_key, limit=80)
            actions = list_approval_actions_for_thread(conn, thread_id=normalized_thread_key, limit=120)
        finally:
            conn.close()

        message_rows: list[str] = []
        for item in messages[-120:]:
            message_rows.append(
                "<tr>"
                f"<td><span class='badge'>{html.escape(str(item.get('direction') or ''))}</span></td>"
                f"<td>{html.escape(str(item.get('created_at') or '-'))}</td>"
                f"<td>{html.escape(str(item.get('telegram_message_id') or '-'))}</td>"
                f"<td>{'yes' if bool(item.get('is_deleted')) else 'no'}</td>"
                f"<td><pre>{html.escape(str(item.get('text') or ''))}</pre></td>"
                "</tr>"
            )

        draft_rows: list[str] = []
        for draft in drafts:
            draft_rows.append(
                "<tr>"
                f"<td>{int(draft.get('id') or 0)}</td>"
                f"<td><span class='badge'>{html.escape(str(draft.get('status') or ''))}</span></td>"
                f"<td>{html.escape(str(draft.get('created_at') or '-'))}</td>"
                f"<td><pre>{html.escape(str(draft.get('draft_text') or ''))}</pre></td>"
                "</tr>"
            )

        action_rows: list[str] = []
        for item in actions[:100]:
            action_rows.append(
                "<tr>"
                f"<td>{int(item.get('id') or 0)}</td>"
                f"<td>{html.escape(str(item.get('action') or ''))}</td>"
                f"<td>{html.escape(str(item.get('actor') or ''))}</td>"
                f"<td>{html.escape(str(item.get('created_at') or '-'))}</td>"
                "</tr>"
            )

        business_card = (
            "<div class='card'>"
            f"<b>Thread:</b> {html.escape(str(thread.get('thread_key') or ''))}<br/>"
            f"<b>Business Connection:</b> {html.escape(str(thread.get('business_connection_id') or ''))}<br/>"
            f"<b>Chat ID:</b> {html.escape(str(thread.get('chat_id') or '-'))}<br/>"
            f"<b>Last Message:</b> {html.escape(str(thread.get('last_message_at') or '-'))}"
            "</div>"
        )
        if isinstance(business_connection, dict):
            business_card += (
                "<div class='card'>"
                "<b>Business Connection Meta</b><br/>"
                f"owner_telegram_user_id={html.escape(str(business_connection.get('telegram_user_id') or '-'))}<br/>"
                f"user_chat_id={html.escape(str(business_connection.get('user_chat_id') or '-'))}<br/>"
                f"can_reply={html.escape(str(bool(business_connection.get('can_reply'))))}<br/>"
                f"is_enabled={html.escape(str(bool(business_connection.get('is_enabled'))))}"
                "</div>"
            )

        body = (
            "<h1>Business Thread</h1>"
            "<p class='muted'>Сообщения, драфты и approval actions по Telegram Business треду.</p>"
            f"{business_card}"
            "<h2>Messages</h2>"
            "<table>"
            "<thead><tr><th>Direction</th><th>Created At</th><th>Message ID</th><th>Deleted</th><th>Text</th></tr></thead>"
            f"<tbody>{''.join(message_rows) if message_rows else '<tr><td colspan=5>Нет сообщений</td></tr>'}</tbody>"
            "</table>"
            "<h2>Drafts</h2>"
            "<table>"
            "<thead><tr><th>ID</th><th>Status</th><th>Created At</th><th>Text</th></tr></thead>"
            f"<tbody>{''.join(draft_rows) if draft_rows else '<tr><td colspan=4>Нет драфтов</td></tr>'}</tbody>"
            "</table>"
            "<h2>Approval Actions</h2>"
            "<table>"
            "<thead><tr><th>ID</th><th>Action</th><th>Actor</th><th>Created At</th></tr></thead>"
            f"<tbody>{''.join(action_rows) if action_rows else '<tr><td colspan=4>Нет действий</td></tr>'}</tbody>"
            "</table>"
            "<p><a href='/admin/ui/business-inbox'>← Назад к business inbox</a></p>"
        )
        return render_page("Business Thread", body)

    @app.get("/admin/ui/inbox/{user_id}", response_class=HTMLResponse)
    async def admin_inbox_thread_ui(user_id: int, _: str = Depends(require_admin)):
        conn = get_connection(cfg.database_path)
        try:
            detail = get_inbox_thread_detail(conn, user_id=user_id, limit_messages=500)
        finally:
            conn.close()
        if detail.get("user") is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found.")

        messages = detail.get("messages") if isinstance(detail.get("messages"), list) else []
        drafts = detail.get("drafts") if isinstance(detail.get("drafts"), list) else []
        followups = detail.get("followups") if isinstance(detail.get("followups"), list) else []
        actions = detail.get("approval_actions") if isinstance(detail.get("approval_actions"), list) else []
        lead_score = detail.get("lead_score") if isinstance(detail.get("lead_score"), dict) else None
        outcome = detail.get("outcome") if isinstance(detail.get("outcome"), dict) else None
        latest_call_insights = (
            detail.get("latest_call_insights") if isinstance(detail.get("latest_call_insights"), dict) else None
        )
        user_item = detail.get("user") if isinstance(detail.get("user"), dict) else {}

        message_rows: list[str] = []
        for item in messages[-80:]:
            message_rows.append(
                "<tr>"
                f"<td><span class='badge'>{html.escape(str(item.get('direction') or ''))}</span></td>"
                f"<td>{html.escape(str(item.get('created_at') or '-'))}</td>"
                f"<td><pre>{html.escape(str(item.get('text') or ''))}</pre></td>"
                "</tr>"
            )

        draft_cards: list[str] = []
        for draft in drafts:
            draft_id = int(draft["id"])
            status_value = html.escape(str(draft.get("status") or "created"))
            draft_text = html.escape(str(draft.get("draft_text") or ""))
            draft_cards.append(
                "<div class='card'>"
                f"<b>Draft #{draft_id}</b> <span class='badge'>{status_value}</span><br/>"
                f"<small class='muted'>created_at={html.escape(str(draft.get('created_at') or '-'))}</small>"
                f"<pre>{draft_text}</pre>"
                f"<form method='post' action='/admin/ui/inbox/drafts/{draft_id}/edit'>"
                "<p><textarea name='draft_text' rows='4' style='width:100%;' required>"
                f"{draft_text}</textarea></p>"
                "<p><input name='model_name' placeholder='model name (optional)' /></p>"
                "<p><button type='submit'>Сохранить правки</button></p>"
                "</form>"
                f"<form method='post' action='/admin/ui/inbox/drafts/{draft_id}/approve' style='display:inline-block;margin-right:8px;'>"
                "<button type='submit'>Approve</button>"
                "</form>"
                f"<form method='post' action='/admin/ui/inbox/drafts/{draft_id}/send' style='display:inline-block;margin-right:8px;'>"
                "<button type='submit'>Send</button>"
                "</form>"
                f"<form method='post' action='/admin/ui/inbox/drafts/{draft_id}/reject' style='display:inline-block;'>"
                "<button type='submit'>Reject</button>"
                "</form>"
                "</div>"
            )

        followup_rows: list[str] = []
        for item in followups:
            followup_rows.append(
                "<tr>"
                f"<td>{int(item.get('id') or 0)}</td>"
                f"<td>{html.escape(str(item.get('priority') or ''))}</td>"
                f"<td>{html.escape(str(item.get('status') or ''))}</td>"
                f"<td>{html.escape(str(item.get('reason') or ''))}</td>"
                f"<td>{html.escape(str(item.get('due_at') or '-'))}</td>"
                "</tr>"
            )

        action_rows: list[str] = []
        for item in actions[:30]:
            action_rows.append(
                "<tr>"
                f"<td>{int(item.get('id') or 0)}</td>"
                f"<td>{html.escape(str(item.get('action') or ''))}</td>"
                f"<td>{html.escape(str(item.get('actor') or '-'))}</td>"
                f"<td>{html.escape(str(item.get('created_at') or '-'))}</td>"
                "</tr>"
            )

        customer_name = html.escape(_format_thread_display_name(user_item))
        lead_score_html = "<p class='muted'>Не выставлен</p>"
        if lead_score:
            lead_score_html = (
                f"<p><b>{float(lead_score.get('score') or 0):.1f}</b> / 100 "
                f"({html.escape(str(lead_score.get('temperature') or '-'))}, "
                f"confidence={html.escape(str(lead_score.get('confidence') or '-'))})</p>"
            )
        outcome_html = "<p class='muted'>Не задан</p>"
        if outcome:
            outcome_html = (
                f"<p><b>{html.escape(str(outcome.get('outcome') or '-'))}</b><br/>"
                f"{html.escape(str(outcome.get('note') or ''))}</p>"
            )
        call_insights_html = "<p class='muted'>Пока нет обработанных звонков.</p>"
        if latest_call_insights:
            interests = ", ".join(latest_call_insights.get("interests") or [])
            objections = ", ".join(latest_call_insights.get("objections") or [])
            call_insights_html = (
                "<div class='card'>"
                f"<b>Warmth:</b> {html.escape(str(latest_call_insights.get('warmth') or '-'))}<br/>"
                f"<b>Summary:</b><pre>{html.escape(str(latest_call_insights.get('summary_text') or ''))}</pre>"
                f"<b>Next Action:</b><pre>{html.escape(str(latest_call_insights.get('next_best_action') or ''))}</pre>"
                f"<b>Interests:</b> {html.escape(interests or '-')}<br/>"
                f"<b>Objections:</b> {html.escape(objections or '-')}"
                "</div>"
            )

        body = (
            f"<h1>Inbox Thread #{user_id}</h1>"
            f"<p class='muted'>Клиент: {customer_name}</p>"
            "<p><a href='/admin/ui/inbox'>← Назад к списку</a></p>"
            "<h2>Latest Call Insights</h2>"
            f"{call_insights_html}"
            "<h2>Lead Score</h2>"
            f"{lead_score_html}"
            f"<form method='post' action='/admin/ui/inbox/{user_id}/lead-score'>"
            "<p><input name='score' type='number' step='0.1' min='0' max='100' placeholder='Score 0..100' required></p>"
            "<p><select name='temperature'>"
            "<option value='hot'>hot</option><option value='warm' selected>warm</option><option value='cold'>cold</option>"
            "</select></p>"
            "<p><input name='confidence' type='number' step='0.01' min='0' max='1' placeholder='Confidence 0..1 (optional)'></p>"
            "<p><button type='submit'>Сохранить score</button></p>"
            "</form>"
            "<h2>Outcome</h2>"
            f"{outcome_html}"
            f"<form method='post' action='/admin/ui/inbox/{user_id}/outcome'>"
            "<p><input name='outcome' placeholder='consultation_booked / no_action / won ...' required /></p>"
            "<p><textarea name='note' rows='2' style='width:100%;' placeholder='Комментарий (optional)'></textarea></p>"
            "<p><button type='submit'>Сохранить outcome</button></p>"
            "</form>"
            "<h2>Create Draft</h2>"
            f"<form method='post' action='/admin/ui/inbox/{user_id}/drafts'>"
            "<p><textarea name='draft_text' rows='4' style='width:100%;' required></textarea></p>"
            "<p><input name='model_name' placeholder='model name (optional)' /></p>"
            "<p><button type='submit'>Создать draft</button></p>"
            "</form>"
            "<h2>Drafts</h2>"
            f"{''.join(draft_cards) if draft_cards else '<p class=muted>Нет драфтов.</p>'}"
            "<h2>Messages</h2>"
            "<table><thead><tr><th>Direction</th><th>Created At</th><th>Text</th></tr></thead>"
            f"<tbody>{''.join(message_rows) if message_rows else '<tr><td colspan=3>Нет сообщений</td></tr>'}</tbody></table>"
            "<h2>Followups</h2>"
            f"<form method='post' action='/admin/ui/inbox/{user_id}/followups'>"
            "<p><select name='priority'>"
            "<option value='hot'>hot</option><option value='warm' selected>warm</option><option value='cold'>cold</option>"
            "</select></p>"
            "<p><textarea name='reason' rows='2' style='width:100%;' placeholder='Причина follow-up' required></textarea></p>"
            "<p><input name='due_at' placeholder='YYYY-MM-DD HH:MM (optional)' /></p>"
            "<p><input name='assigned_to' placeholder='manager id/name (optional)' /></p>"
            "<p><button type='submit'>Создать follow-up</button></p>"
            "</form>"
            "<table><thead><tr><th>ID</th><th>Priority</th><th>Status</th><th>Reason</th><th>Due At</th></tr></thead>"
            f"<tbody>{''.join(followup_rows) if followup_rows else '<tr><td colspan=5>Нет задач</td></tr>'}</tbody></table>"
            "<h2>Approval Actions</h2>"
            "<table><thead><tr><th>ID</th><th>Action</th><th>Actor</th><th>Created At</th></tr></thead>"
            f"<tbody>{''.join(action_rows) if action_rows else '<tr><td colspan=4>Нет действий</td></tr>'}</tbody></table>"
        )
        return render_page(f"Inbox Thread {user_id}", body)

    @app.post("/admin/ui/inbox/{user_id}/drafts")
    async def admin_inbox_create_draft_ui(
        user_id: int,
        draft_text: str = Form(...),
        draft_model: str = Form("", alias="model_name"),
        admin_username: str = Depends(require_admin),
    ):
        conn = get_connection(cfg.database_path)
        try:
            _require_user_exists(conn, user_id)
            thread_id = _thread_id_from_user_id(user_id)
            draft_id = create_reply_draft(
                conn,
                user_id=user_id,
                thread_id=thread_id,
                draft_text=draft_text.strip(),
                model_name=draft_model.strip() or None,
                quality={},
                created_by=admin_username,
                status="created",
            )
            create_approval_action(
                conn,
                draft_id=draft_id,
                user_id=user_id,
                thread_id=thread_id,
                action="draft_created",
                actor=admin_username,
                payload={},
            )
        finally:
            conn.close()
        return RedirectResponse(url=f"/admin/ui/inbox/{user_id}", status_code=status.HTTP_303_SEE_OTHER)

    @app.post("/admin/ui/inbox/drafts/{draft_id}/edit")
    async def admin_inbox_edit_draft_ui(
        draft_id: int,
        draft_text: str = Form(...),
        draft_model: str = Form("", alias="model_name"),
        admin_username: str = Depends(require_admin),
    ):
        conn = get_connection(cfg.database_path)
        try:
            draft = get_reply_draft(conn, draft_id)
            if draft is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Draft {draft_id} not found.")
            update_reply_draft_text(
                conn,
                draft_id=draft_id,
                draft_text=draft_text.strip(),
                model_name=draft_model.strip() or None,
                quality={},
                actor=admin_username,
            )
            create_approval_action(
                conn,
                draft_id=draft_id,
                user_id=int(draft["user_id"]),
                thread_id=str(draft["thread_id"]),
                action="draft_edited",
                actor=admin_username,
                payload={},
            )
            user_id = int(draft["user_id"])
        finally:
            conn.close()
        return RedirectResponse(url=f"/admin/ui/inbox/{user_id}", status_code=status.HTTP_303_SEE_OTHER)

    @app.post("/admin/ui/inbox/drafts/{draft_id}/approve")
    async def admin_inbox_approve_draft_ui(draft_id: int, admin_username: str = Depends(require_admin)):
        conn = get_connection(cfg.database_path)
        try:
            draft = get_reply_draft(conn, draft_id)
            if draft is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Draft {draft_id} not found.")
            if str(draft.get("status") or "") != "sent":
                update_reply_draft_status(conn, draft_id=draft_id, status="approved", actor=admin_username)
                create_approval_action(
                    conn,
                    draft_id=draft_id,
                    user_id=int(draft["user_id"]),
                    thread_id=str(draft["thread_id"]),
                    action="draft_approved",
                    actor=admin_username,
                    payload={},
                )
            user_id = int(draft["user_id"])
        finally:
            conn.close()
        return RedirectResponse(url=f"/admin/ui/inbox/{user_id}", status_code=status.HTTP_303_SEE_OTHER)

    @app.post("/admin/ui/inbox/drafts/{draft_id}/send")
    async def admin_inbox_send_draft_ui(draft_id: int, admin_username: str = Depends(require_admin)):
        conn = get_connection(cfg.database_path)
        try:
            draft = get_reply_draft(conn, draft_id)
            if draft is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Draft {draft_id} not found.")
            current_status = str(draft.get("status") or "")
            if current_status != "sent":
                if current_status != "approved":
                    update_reply_draft_status(conn, draft_id=draft_id, status="approved", actor=admin_username)
                    create_approval_action(
                        conn,
                        draft_id=draft_id,
                        user_id=int(draft["user_id"]),
                        thread_id=str(draft["thread_id"]),
                        action="draft_approved",
                        actor=admin_username,
                        payload={},
                    )
                update_reply_draft_status(conn, draft_id=draft_id, status="sent", actor=admin_username)
                create_approval_action(
                    conn,
                    draft_id=draft_id,
                    user_id=int(draft["user_id"]),
                    thread_id=str(draft["thread_id"]),
                    action="draft_sent",
                    actor=admin_username,
                    payload={},
                )
            user_id = int(draft["user_id"])
        finally:
            conn.close()
        return RedirectResponse(url=f"/admin/ui/inbox/{user_id}", status_code=status.HTTP_303_SEE_OTHER)

    @app.post("/admin/ui/inbox/drafts/{draft_id}/reject")
    async def admin_inbox_reject_draft_ui(draft_id: int, admin_username: str = Depends(require_admin)):
        conn = get_connection(cfg.database_path)
        try:
            draft = get_reply_draft(conn, draft_id)
            if draft is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Draft {draft_id} not found.")
            if str(draft.get("status") or "") != "sent":
                update_reply_draft_status(conn, draft_id=draft_id, status="rejected", actor=admin_username)
                create_approval_action(
                    conn,
                    draft_id=draft_id,
                    user_id=int(draft["user_id"]),
                    thread_id=str(draft["thread_id"]),
                    action="draft_rejected",
                    actor=admin_username,
                    payload={},
                )
            user_id = int(draft["user_id"])
        finally:
            conn.close()
        return RedirectResponse(url=f"/admin/ui/inbox/{user_id}", status_code=status.HTTP_303_SEE_OTHER)

    @app.post("/admin/ui/inbox/{user_id}/outcome")
    async def admin_inbox_set_outcome_ui(
        user_id: int,
        outcome: str = Form(...),
        note: str = Form(""),
        admin_username: str = Depends(require_admin),
    ):
        conn = get_connection(cfg.database_path)
        try:
            _require_user_exists(conn, user_id)
            thread_id = _thread_id_from_user_id(user_id)
            upsert_conversation_outcome(
                conn,
                user_id=user_id,
                thread_id=thread_id,
                outcome=outcome.strip(),
                note=note.strip() or None,
                created_by=admin_username,
            )
            create_approval_action(
                conn,
                draft_id=None,
                user_id=user_id,
                thread_id=thread_id,
                action="conversation_outcome_set",
                actor=admin_username,
                payload={"outcome": outcome.strip()},
            )
        finally:
            conn.close()
        return RedirectResponse(url=f"/admin/ui/inbox/{user_id}", status_code=status.HTTP_303_SEE_OTHER)

    @app.post("/admin/ui/inbox/{user_id}/followups")
    async def admin_inbox_followup_ui(
        user_id: int,
        priority: str = Form("warm"),
        reason: str = Form(...),
        due_at: str = Form(""),
        assigned_to: str = Form(""),
        admin_username: str = Depends(require_admin),
    ):
        normalized_priority = priority.strip().lower()
        if normalized_priority not in {"hot", "warm", "cold"}:
            normalized_priority = "warm"
        conn = get_connection(cfg.database_path)
        try:
            _require_user_exists(conn, user_id)
            thread_id = _thread_id_from_user_id(user_id)
            task_id = create_followup_task(
                conn,
                user_id=user_id,
                thread_id=thread_id,
                priority=normalized_priority,
                reason=reason.strip(),
                status="pending",
                due_at=due_at.strip() or None,
                assigned_to=assigned_to.strip() or None,
            )
            create_approval_action(
                conn,
                draft_id=None,
                user_id=user_id,
                thread_id=thread_id,
                action="followup_created",
                actor=admin_username,
                payload={"task_id": task_id, "priority": normalized_priority},
            )
        finally:
            conn.close()
        return RedirectResponse(url=f"/admin/ui/inbox/{user_id}", status_code=status.HTTP_303_SEE_OTHER)

    @app.post("/admin/ui/inbox/{user_id}/lead-score")
    async def admin_inbox_lead_score_ui(
        user_id: int,
        score: float = Form(...),
        temperature: str = Form("warm"),
        confidence: str = Form(""),
        admin_username: str = Depends(require_admin),
    ):
        normalized_temperature = temperature.strip().lower()
        if normalized_temperature not in {"hot", "warm", "cold"}:
            normalized_temperature = "warm"
        confidence_value: Optional[float] = None
        if confidence.strip():
            try:
                confidence_value = float(confidence.strip())
            except ValueError:
                confidence_value = None
        conn = get_connection(cfg.database_path)
        try:
            _require_user_exists(conn, user_id)
            thread_id = _thread_id_from_user_id(user_id)
            score_id = create_lead_score(
                conn,
                user_id=user_id,
                thread_id=thread_id,
                score=score,
                temperature=normalized_temperature,
                confidence=confidence_value,
                factors={},
            )
            create_approval_action(
                conn,
                draft_id=None,
                user_id=user_id,
                thread_id=thread_id,
                action="lead_scored",
                actor=admin_username,
                payload={"score_id": score_id, "score": score, "temperature": normalized_temperature},
            )
        finally:
            conn.close()
        return RedirectResponse(url=f"/admin/ui/inbox/{user_id}", status_code=status.HTTP_303_SEE_OTHER)

    @app.get("/api/health")
    async def health():
        return {"status": "ok", "service": "sales-agent"}

    @app.get("/api/auth/whoami")
    async def auth_whoami(request: Request):
        init_data = _extract_tg_init_data(request)
        if not init_data:
            return {"ok": False, "reason": "not_in_telegram", "user": None}
        if not cfg.telegram_bot_token:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="TELEGRAM_BOT_TOKEN is not configured.",
            )

        auth = verify_telegram_webapp_init_data(
            init_data=init_data,
            bot_token=cfg.telegram_bot_token,
        )
        if not auth.ok:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid Telegram miniapp auth: {auth.reason}",
            )

        user = _safe_user_payload(auth.user)
        if user.get("id") is None and auth.user_id is not None:
            user["id"] = auth.user_id
        return {"ok": True, "user": user}

    @app.get("/api/catalog/search")
    async def catalog_search(
        brand: Optional[str] = None,
        grade: Optional[int] = Query(default=None, ge=1, le=11),
        goal: Optional[str] = None,
        subject: Optional[str] = None,
        format: Optional[str] = None,
    ):
        criteria = SearchCriteria(
            brand=brand,
            grade=grade,
            goal=goal,
            subject=subject,
            format=format,
        )
        try:
            products = select_top_products(
                criteria,
                path=cfg.catalog_path,
                top_k=3,
                brand_default=cfg.brand_default,
            )
        except CatalogValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Catalog validation error: {exc}",
            ) from exc
        except FileNotFoundError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Catalog file not found: {cfg.catalog_path}",
            ) from exc

        match_quality = _evaluate_match_quality(criteria, products)
        manager_offer = _build_manager_offer(match_quality, has_results=bool(products))
        items = []
        for product in products:
            items.append(
                {
                    "id": product.id,
                    "title": product.title,
                    "url": str(product.url),
                    "usp": list(product.usp[:3]),
                    "price_text": _format_price_text(product),
                    "next_start_text": _format_next_start_text(product),
                    "why_match": explain_match(product, criteria),
                }
            )

        return {
            "ok": True,
            "criteria": {
                "brand": brand or cfg.brand_default,
                "grade": grade,
                "goal": goal,
                "subject": subject,
                "format": format,
            },
            "count": len(items),
            "items": items,
            "match_quality": match_quality,
            "manager_recommended": bool(manager_offer.get("recommended")),
            "manager_message": str(manager_offer.get("message") or ""),
            "manager_call_to_action": str(manager_offer.get("call_to_action") or ""),
        }

    @app.post("/api/assistant/ask")
    async def assistant_ask(payload: AssistantAskPayload, request: Request):
        request_id = _request_id_from_request(request)
        assistant_access = _require_assistant_access(request)
        client_ip = _request_client_ip(request)

        telegram_user_id = assistant_access.get("user_id")
        telegram_user_id_int = telegram_user_id if isinstance(telegram_user_id, int) else None
        if telegram_user_id_int is not None:
            _enforce_rate_limit(
                request=request,
                limiter=assistant_rate_limiter,
                key=f"assistant:user:{telegram_user_id_int}",
                limit=cfg.assistant_rate_limit_user_requests,
                scope="assistant_user",
            )

        _enforce_rate_limit(
            request=request,
            limiter=assistant_rate_limiter,
            key=f"assistant:ip:{client_ip}",
            limit=cfg.assistant_rate_limit_ip_requests,
            scope="assistant_ip",
        )

        question = payload.question.strip()
        if not question:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "code": "empty_question",
                    "message": "Question is empty.",
                    "user_message": "Сообщение получилось пустым. Напишите вопрос текстом.",
                    "request_id": request_id,
                },
            )

        try:
            criteria = _criteria_from_payload(payload.criteria, brand_default=cfg.brand_default)
            try:
                top_products = select_top_products(
                    criteria,
                    path=cfg.catalog_path,
                    top_k=3,
                    brand_default=cfg.brand_default,
                )
            except (CatalogValidationError, FileNotFoundError, OSError):
                top_products = []

            match_quality = _evaluate_match_quality(criteria, top_products)
            manager_offer = _build_manager_offer(match_quality, has_results=bool(top_products))
            mode = _assistant_mode(question, criteria)

            llm_client = LLMClient(
                api_key=cfg.openai_api_key,
                model=cfg.openai_model,
                timeout_seconds=ASSISTANT_TIMEOUT_SECONDS,
            )
            persisted_context: Dict[str, Any] = {}
            context_user_id: Optional[int] = None
            if telegram_user_id_int is not None:
                conn_context = get_connection(cfg.database_path)
                try:
                    context_user_id = get_or_create_user(
                        conn_context,
                        channel="telegram",
                        external_id=str(telegram_user_id_int),
                    )
                    persisted_context = get_conversation_context(conn_context, user_id=context_user_id)
                finally:
                    conn_context.close()

            user_context: Dict[str, Any] = {}
            recent_history = _sanitize_recent_history(payload.recent_history)
            summary_text = ""
            if isinstance(persisted_context, dict):
                summary_text = _compact_text(persisted_context.get("summary_text"), limit=ASSISTANT_CONTEXT_SUMMARY_MAX)

            if isinstance(payload.context_summary, str) and payload.context_summary.strip():
                summary_text = _compact_text(payload.context_summary, limit=ASSISTANT_CONTEXT_SUMMARY_MAX)
            elif not summary_text and recent_history:
                history_excerpt = " | ".join(
                    f"{item['role']}: {item['text']}" for item in recent_history[-4:]
                )
                if history_excerpt:
                    summary_text = _compact_text(history_excerpt, limit=ASSISTANT_CONTEXT_SUMMARY_MAX)

            if summary_text:
                user_context["summary_text"] = summary_text

            answer_text = ""
            sources: list[str] = []
            used_fallback = False
            recommended_ids: list[str] = []

            if mode == "knowledge":
                vector_store_id = cfg.openai_vector_store_id or load_vector_store_id(cfg.vector_store_meta_path)
                knowledge_reply = await llm_client.answer_knowledge_question_async(
                    question=question,
                    vector_store_id=vector_store_id,
                    user_context=user_context,
                    allow_web_fallback=cfg.openai_web_fallback_enabled,
                    site_domain=cfg.openai_web_fallback_domain,
                )
                answer_text = knowledge_reply.answer_text
                sources = list(knowledge_reply.sources)
                used_fallback = knowledge_reply.used_fallback
            elif mode == "consultative":
                consult_reply = await llm_client.build_consultative_reply_async(
                    user_message=question,
                    criteria=criteria,
                    top_products=top_products,
                    missing_fields=_missing_criteria_fields(criteria),
                    repeat_count=0,
                    product_offer_allowed=match_quality != "none",
                    recent_history=recent_history,
                    user_context=user_context,
                )
                answer_text = consult_reply.answer_text
                used_fallback = consult_reply.used_fallback
                recommended_ids = list(consult_reply.recommended_product_ids)
            else:
                general_reply = await llm_client.build_general_help_reply_async(
                    user_message=question,
                    dialogue_state=None,
                    recent_history=recent_history,
                    user_context=user_context,
                )
                answer_text = general_reply.answer_text
                used_fallback = general_reply.used_fallback

            recommended_products: list[Dict[str, str]] = []
            if top_products:
                allowed_ids = {item.id for item in top_products}
                filtered_ids = [item_id for item_id in recommended_ids if item_id in allowed_ids]
                if mode == "consultative" and not filtered_ids:
                    filtered_ids = [top_products[0].id]
                for product in top_products:
                    if product.id not in filtered_ids:
                        continue
                    recommended_products.append(
                        {
                            "id": product.id,
                            "title": product.title,
                            "url": str(product.url),
                            "why_match": explain_match(product, criteria),
                        }
                    )

            if context_user_id is not None:
                merged_context = _merge_assistant_context(
                    persisted_context if isinstance(persisted_context, dict) else {},
                    question=question,
                    criteria=criteria,
                    recent_history=recent_history,
                    context_summary=summary_text,
                )
                conn_update = get_connection(cfg.database_path)
                try:
                    upsert_conversation_context(conn_update, user_id=context_user_id, summary=merged_context)
                finally:
                    conn_update.close()

            return {
                "ok": True,
                "request_id": request_id,
                "mode": mode,
                "answer_text": answer_text,
                "sources": sources,
                "used_fallback": used_fallback,
                "criteria": {
                    "brand": criteria.brand or cfg.brand_default,
                    "grade": criteria.grade,
                    "goal": criteria.goal,
                    "subject": criteria.subject,
                    "format": criteria.format,
                },
                "match_quality": match_quality,
                "recommended_products": recommended_products,
                "manager_offer": manager_offer,
                "processing_note": "Спасибо за ожидание. Запрос проработан подробно, можно продолжать диалог.",
            }
        except HTTPException:
            raise
        except Exception:
            logger.exception("Assistant ask failed (request_id=%s)", request_id)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "code": "assistant_unavailable",
                    "message": "Assistant request failed.",
                    "user_message": "Сервис ответа временно недоступен. Повторите вопрос через минуту.",
                    "request_id": request_id,
                },
            )

    @app.get("/api/crm/meta/modules")
    async def crm_meta_modules(_: str = Depends(_require_crm_api_access)):
        client = _require_tallanto_readonly_client()
        cache_key = _crm_cache_key("modules", {})
        cached = _read_crm_cache(cache_key)
        if cached is not None:
            return {"ok": True, "cached": True, **cached}

        try:
            payload = client.call("list_possible_modules", {})
        except RuntimeError as exc:
            raise _map_tallanto_error(exc) from exc

        response_payload = {
            "items": normalize_tallanto_modules(payload),
        }
        _write_crm_cache(cache_key, response_payload)
        return {"ok": True, "cached": False, **response_payload}

    @app.get("/api/crm/meta/fields")
    async def crm_meta_fields(
        module: str = Query(..., min_length=1, max_length=128),
        _: str = Depends(_require_crm_api_access),
    ):
        client = _require_tallanto_readonly_client()
        params = {"module": module.strip()}
        cache_key = _crm_cache_key("fields", params)
        cached = _read_crm_cache(cache_key)
        if cached is not None:
            return {"ok": True, "cached": True, **cached}

        try:
            payload = client.call("list_possible_fields", params)
        except RuntimeError as exc:
            raise _map_tallanto_error(exc) from exc

        response_payload = {
            "module": params["module"],
            "items": normalize_tallanto_fields(payload),
        }
        _write_crm_cache(cache_key, response_payload)
        return {"ok": True, "cached": False, **response_payload}

    @app.get("/api/crm/lookup")
    async def crm_lookup(
        module: str = Query(..., min_length=1, max_length=128),
        field: str = Query(..., min_length=1, max_length=128),
        value: str = Query(..., min_length=1, max_length=512),
        _: str = Depends(_require_crm_api_access),
    ):
        client = _require_tallanto_readonly_client()
        normalized_module = module.strip()
        normalized_field = field.strip()
        normalized_value = value.strip()
        params = {
            "module": normalized_module,
            "field": normalized_field,
            "value": normalized_value,
        }
        cache_key = _crm_cache_key("lookup", params)
        cached = _read_crm_cache(cache_key)
        if cached is not None:
            return {"ok": True, "cached": True, **cached}

        try:
            primary = client.call(
                "entry_by_fields",
                {
                    "module": normalized_module,
                    "fields_values": {normalized_field: normalized_value},
                },
            )
        except RuntimeError as exc:
            raise _map_tallanto_error(exc) from exc

        context = sanitize_tallanto_lookup_context(primary)
        fallback_used = False
        if not context.get("found"):
            fallback_used = True
            try:
                fallback = client.call(
                    "get_entry_list",
                    {
                        "module": normalized_module,
                        "fields_values": {normalized_field: normalized_value},
                    },
                )
            except RuntimeError as exc:
                raise _map_tallanto_error(exc) from exc
            context = sanitize_tallanto_lookup_context(fallback)

        response_payload = {
            "module": normalized_module,
            "lookup_field": normalized_field,
            "found": bool(context.get("found")),
            "tags": list(context.get("tags") or []),
            "interests": list(context.get("interests") or []),
            "last_touch_days": context.get("last_touch_days"),
            "fallback_used": fallback_used,
        }
        _write_crm_cache(cache_key, response_payload)
        return {"ok": True, "cached": False, **response_payload}

    @app.get("/api/runtime/diagnostics")
    async def runtime_diagnostics():
        payload = build_runtime_diagnostics(cfg)
        conn = get_connection(cfg.database_path)
        try:
            payload.setdefault("runtime", {})
            payload["runtime"]["webhook_queue"] = {
                "pending": count_webhook_updates_by_status(conn, "pending"),
                "retry": count_webhook_updates_by_status(conn, "retry"),
                "processing": count_webhook_updates_by_status(conn, "processing"),
                "failed": count_webhook_updates_by_status(conn, "failed"),
            }
            payload["runtime"]["mango"] = {
                "enabled": _mango_ingest_enabled(),
                "webhook_path": mango_webhook_path,
                "polling_enabled": cfg.mango_polling_enabled,
                "poll_interval_seconds": cfg.mango_poll_interval_seconds,
                "poll_limit_per_run": cfg.mango_poll_limit_per_run,
                "poll_retry_attempts": cfg.mango_poll_retry_attempts,
                "poll_retry_backoff_seconds": cfg.mango_poll_retry_backoff_seconds,
                "retry_failed_limit_per_run": cfg.mango_retry_failed_limit_per_run,
                "recording_ttl_hours": cfg.mango_call_recording_ttl_hours,
                "calls_path": cfg.mango_calls_path,
                "events_total": count_mango_events(conn),
                "events_queued": count_mango_events(conn, status="queued"),
                "events_processing": count_mango_events(conn, status="processing"),
                "events_failed": count_mango_events(conn, status="failed"),
                "oldest_failed_created_at": get_oldest_mango_event_created_at(conn, status="failed"),
            }
            payload["runtime"]["faq_lab"] = {
                "enabled": cfg.enable_faq_lab,
                "scheduler_enabled": cfg.faq_lab_scheduler_enabled,
                "interval_seconds": cfg.faq_lab_interval_seconds,
                "window_days": cfg.faq_lab_window_days,
                "min_question_count": cfg.faq_lab_min_question_count,
                "max_items_per_run": cfg.faq_lab_max_items_per_run,
            }
        finally:
            conn.close()
        return payload

    @app.get("/api/miniapp/meta")
    async def miniapp_meta():
        manager_chat_url = cfg.sales_manager_chat_url.strip()
        user_miniapp_url = cfg.user_webapp_url.strip() or "/app"
        if user_miniapp_url and not (
            user_miniapp_url.startswith("http://")
            or user_miniapp_url.startswith("https://")
            or user_miniapp_url.startswith("/")
        ):
            user_miniapp_url = f"/{user_miniapp_url}"

        return {
            "ok": True,
            "brand_name": cfg.miniapp_brand_name,
            "advisor_name": cfg.miniapp_advisor_name,
            "manager_label": cfg.sales_manager_label,
            "manager_chat_url": manager_chat_url,
            "user_miniapp_url": user_miniapp_url,
        }

    @app.get("/")
    async def root():
        app_status = "ready" if user_webapp_ready else "build-required"
        return {
            "status": "ok",
            "service": "sales-agent",
            "user_miniapp": {
                "status": app_status,
                "entrypoint": "/app",
            },
        }

    if not user_webapp_ready:

        @app.get("/app", response_class=HTMLResponse)
        async def user_miniapp_placeholder():
            body = (
                "<h1>User Mini App is not built yet</h1>"
                "<p>Run:</p>"
                "<pre>cd webapp\nnpm install\nnpm run build</pre>"
                f"<p>Expected dist path: <code>{html.escape(str(user_webapp_dist))}</code></p>"
            )
            return render_page("Mini App Build Required", body)

    @app.get("/admin/miniapp")
    async def admin_miniapp_page():
        if not cfg.admin_miniapp_enabled:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Admin miniapp is disabled.")
        index_path = miniapp_dir / "index.html"
        if not index_path.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Miniapp UI is not available.")
        return FileResponse(index_path)

    @app.get("/admin/miniapp/api/me")
    async def admin_miniapp_me(auth_user: dict = Depends(require_miniapp_user)):
        return {"ok": True, "user": auth_user.get("user", {}), "user_id": auth_user["user_id"]}

    @app.get("/admin/miniapp/api/leads")
    async def admin_miniapp_leads(
        limit: int = 100,
        auth_user: dict = Depends(require_miniapp_user),
    ):
        conn = get_connection(cfg.database_path)
        try:
            items = list_recent_leads(conn, limit=max(1, min(limit, 500)))
        finally:
            conn.close()
        return {"ok": True, "requested_by": auth_user["user_id"], "items": items}

    @app.get("/admin/miniapp/api/conversations")
    async def admin_miniapp_conversations(
        limit: int = 100,
        auth_user: dict = Depends(require_miniapp_user),
    ):
        conn = get_connection(cfg.database_path)
        try:
            items = list_recent_conversations(conn, limit=max(1, min(limit, 500)))
        finally:
            conn.close()
        return {"ok": True, "requested_by": auth_user["user_id"], "items": items}

    @app.get("/admin/miniapp/api/conversations/{user_id}")
    async def admin_miniapp_conversation_history(
        user_id: int,
        limit: int = 500,
        auth_user: dict = Depends(require_miniapp_user),
    ):
        conn = get_connection(cfg.database_path)
        try:
            messages = list_conversation_messages(conn, user_id=user_id, limit=max(1, min(limit, 2000)))
        finally:
            conn.close()
        return {
            "ok": True,
            "requested_by": auth_user["user_id"],
            "user_id": user_id,
            "messages": messages,
        }

    @app.post(mango_webhook_path)
    async def mango_webhook(request: Request):
        if not _mango_ingest_enabled():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Mango auto-ingest is disabled.",
            )

        raw_body = await request.body()
        try:
            payload = json.loads(raw_body.decode("utf-8")) if raw_body else {}
        except Exception as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Mango payload.") from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Mango payload.")

        try:
            client = _build_mango_client()
        except MangoClientError as exc:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

        signature = request.headers.get("X-Mango-Signature", "").strip()
        if not client.verify_webhook_signature(raw_body=raw_body, signature=signature):
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid Mango webhook signature.")

        event = client.parse_call_event(payload)
        if event is None:
            return {"ok": True, "ignored": True, "reason": "not_call_event"}

        try:
            result = await ingest_mango_event(event=event, source="webhook")
        except Exception as exc:
            logger.exception("Mango webhook event processing failed")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Mango ingest failed: {exc}",
            ) from exc

        cleanup_result = _cleanup_old_call_files()
        return {
            "ok": True,
            "result": result,
            "cleanup": cleanup_result,
        }

    @app.post(webhook_path)
    async def telegram_webhook(request: Request):
        if cfg.telegram_mode != "webhook":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Webhook endpoint is disabled. Set TELEGRAM_MODE=webhook.",
            )
        if not cfg.telegram_bot_token:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="TELEGRAM_BOT_TOKEN is not configured.",
            )
        if cfg.telegram_webhook_secret:
            header_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if not secrets.compare_digest(header_secret, cfg.telegram_webhook_secret):
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid webhook secret token.")

        try:
            payload = await request.json()
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid Telegram payload.",
            ) from exc
        if not isinstance(payload, dict):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Telegram payload.")

        telegram_application = getattr(app.state, "telegram_application", None)
        if telegram_application is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Telegram webhook application is not initialized.",
            )

        update_id = payload.get("update_id") if isinstance(payload.get("update_id"), int) else None
        conn = get_connection(cfg.database_path)
        try:
            enqueue_result = enqueue_webhook_update(conn, payload=payload, update_id=update_id)
        finally:
            conn.close()

        event = getattr(app.state, "webhook_worker_event", None)
        if event is not None:
            event.set()

        if not enqueue_result.get("is_new", False):
            logger.info("Ignoring duplicate Telegram update_id=%s", update_id)
        return {"ok": True, "queued": True}

    @app.get("/admin/leads")
    async def admin_leads(_: str = Depends(require_admin), limit: int = 100):
        conn = get_connection(cfg.database_path)
        try:
            return {"items": list_recent_leads(conn, limit=max(1, min(limit, 500)))}
        finally:
            conn.close()

    @app.get("/admin/ui/leads", response_class=HTMLResponse)
    async def admin_leads_ui(_: str = Depends(require_admin), limit: int = 100):
        conn = get_connection(cfg.database_path)
        try:
            items = list_recent_leads(conn, limit=max(1, min(limit, 500)))
        finally:
            conn.close()

        rows: list[str] = []
        for item in items:
            contact = item.get("contact") if isinstance(item.get("contact"), dict) else {}
            rows.append(
                "<tr>"
                f"<td>{int(item['lead_id'])}</td>"
                f"<td>{int(item['user_id'])}</td>"
                f"<td>{html.escape(str(item.get('status') or ''))}</td>"
                f"<td>{html.escape(str(item.get('tallanto_entry_id') or '-'))}</td>"
                f"<td>{html.escape(str(contact.get('phone') or '-'))}</td>"
                f"<td>{html.escape(str(contact.get('source') or '-'))}</td>"
                f"<td>{html.escape(str(item.get('created_at') or '-'))}</td>"
                "</tr>"
            )

        body = (
            "<h1>Leads</h1>"
            "<table>"
            "<thead><tr><th>Lead ID</th><th>User ID</th><th>Status</th><th>CRM ID</th><th>Phone</th><th>Source</th><th>Created At</th></tr></thead>"
            f"<tbody>{''.join(rows) if rows else '<tr><td colspan=7>Нет лидов</td></tr>'}</tbody>"
            "</table>"
        )
        return render_page("Leads", body)

    @app.get("/admin/conversations")
    async def admin_conversations(_: str = Depends(require_admin), limit: int = 100):
        conn = get_connection(cfg.database_path)
        try:
            return {"items": list_recent_conversations(conn, limit=max(1, min(limit, 500)))}
        finally:
            conn.close()

    @app.get("/admin/ui/conversations", response_class=HTMLResponse)
    async def admin_conversations_ui(_: str = Depends(require_admin), limit: int = 100):
        conn = get_connection(cfg.database_path)
        try:
            items = list_recent_conversations(conn, limit=max(1, min(limit, 500)))
        finally:
            conn.close()

        rows: list[str] = []
        for item in items:
            user_id = int(item["user_id"])
            rows.append(
                "<tr>"
                f"<td>{user_id}</td>"
                f"<td>{html.escape(str(item.get('channel') or ''))}</td>"
                f"<td>{html.escape(str(item.get('external_id') or ''))}</td>"
                f"<td>{int(item.get('messages_count') or 0)}</td>"
                f"<td>{html.escape(str(item.get('last_message_at') or '-'))}</td>"
                f"<td><a href='/admin/ui/conversations/{user_id}'>История</a></td>"
                "</tr>"
            )

        body = (
            "<h1>Conversations</h1>"
            "<table>"
            "<thead><tr><th>User ID</th><th>Channel</th><th>External ID</th><th>Messages</th><th>Last Message</th><th></th></tr></thead>"
            f"<tbody>{''.join(rows) if rows else '<tr><td colspan=6>Нет диалогов</td></tr>'}</tbody>"
            "</table>"
        )
        return render_page("Conversations", body)

    @app.get("/admin/conversations/{user_id}")
    async def admin_conversation_history(user_id: int, _: str = Depends(require_admin), limit: int = 500):
        conn = get_connection(cfg.database_path)
        try:
            messages = list_conversation_messages(conn, user_id=user_id, limit=max(1, min(limit, 2000)))
            return {"user_id": user_id, "messages": messages}
        finally:
            conn.close()

    @app.get("/admin/ui/conversations/{user_id}", response_class=HTMLResponse)
    async def admin_conversation_history_ui(user_id: int, _: str = Depends(require_admin), limit: int = 500):
        conn = get_connection(cfg.database_path)
        try:
            messages = list_conversation_messages(conn, user_id=user_id, limit=max(1, min(limit, 2000)))
        finally:
            conn.close()

        rows: list[str] = []
        for item in messages:
            direction = str(item.get("direction") or "")
            text = str(item.get("text") or "")
            created_at = str(item.get("created_at") or "-")
            meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
            rows.append(
                "<tr>"
                f"<td><span class='badge'>{html.escape(direction)}</span></td>"
                f"<td>{html.escape(created_at)}</td>"
                f"<td><pre>{html.escape(text)}</pre></td>"
                f"<td><pre>{html.escape(json.dumps(meta, ensure_ascii=False, indent=2))}</pre></td>"
                "</tr>"
            )

        body = (
            f"<h1>Conversation #{user_id}</h1>"
            "<table>"
            "<thead><tr><th>Direction</th><th>Created At</th><th>Text</th><th>Meta</th></tr></thead>"
            f"<tbody>{''.join(rows) if rows else '<tr><td colspan=4>Нет сообщений</td></tr>'}</tbody>"
            "</table>"
        )
        return render_page(f"Conversation {user_id}", body)

    @app.get("/admin/ui/copilot", response_class=HTMLResponse)
    async def admin_copilot_ui(_: str = Depends(require_admin)):
        body = (
            "<h1>Copilot Import</h1>"
            "<p class='muted'>Загрузите WhatsApp .txt или Telegram export .json</p>"
            "<form method='post' action='/admin/ui/copilot/import' enctype='multipart/form-data'>"
            "<p><input type='file' name='file' required></p>"
            "<p><label><input type='checkbox' name='create_task' value='true'> Создать задачу в CRM (если настроено)</label></p>"
            "<p><button type='submit'>Импортировать</button></p>"
            "</form>"
        )
        return render_page("Copilot", body)

    @app.post("/admin/ui/copilot/import", response_class=HTMLResponse)
    async def admin_copilot_import_ui(
        _: str = Depends(require_admin),
        file: UploadFile = File(...),
        create_task: bool = Form(False),
    ):
        content = await file.read()
        if not content:
            return render_page("Copilot Error", "<h1>Ошибка</h1><p>Файл пустой.</p>")

        try:
            result = run_copilot_from_file(filename=file.filename or "dialog.txt", content=content)
        except ValueError as exc:
            return render_page("Copilot Error", f"<h1>Ошибка</h1><p>{html.escape(str(exc))}</p>")

        task_html = ""
        if create_task:
            crm = build_crm_client(cfg)
            task_result = crm.create_copilot_task(
                summary=result.summary,
                draft_reply=result.draft_reply,
            )
            task_html = (
                "<h2>CRM Task</h2>"
                f"<p><b>Success:</b> {html.escape(str(task_result.success))}</p>"
                f"<p><b>Entry ID:</b> {html.escape(str(task_result.entry_id or '-'))}</p>"
                f"<p><b>Error:</b> {html.escape(str(task_result.error or '-'))}</p>"
            )

        profile_json = html.escape(json.dumps(result.customer_profile, ensure_ascii=False, indent=2))
        draft_text = html.escape(result.draft_reply)
        body = (
            "<h1>Copilot Result</h1>"
            f"<p><b>Source format:</b> {html.escape(result.source_format)}</p>"
            f"<p><b>Message count:</b> {int(result.message_count)}</p>"
            "<h2>Summary</h2>"
            f"<pre>{html.escape(result.summary)}</pre>"
            "<h2>Customer profile</h2>"
            f"<pre>{profile_json}</pre>"
            "<h2>Draft reply</h2>"
            f"<pre id='draft_reply'>{draft_text}</pre>"
            "<button type='button' onclick='navigator.clipboard.writeText(document.getElementById(\"draft_reply\").innerText)'>Скопировать черновик</button>"
            f"{task_html}"
        )
        return render_page("Copilot Result", body)

    @app.post("/admin/copilot/import")
    async def admin_copilot_import(
        _: str = Depends(require_admin),
        file: UploadFile = File(...),
        create_task: bool = False,
    ):
        content = await file.read()
        if not content:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Uploaded file is empty.",
            )
        try:
            result = run_copilot_from_file(filename=file.filename or "dialog.txt", content=content)
        except ValueError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

        response = {
            "source_format": result.source_format,
            "message_count": result.message_count,
            "summary": result.summary,
            "customer_profile": result.customer_profile,
            "draft_reply": result.draft_reply,
            "auto_send": False,
        }

        if create_task:
            crm = build_crm_client(cfg)
            task_result = crm.create_copilot_task(
                summary=result.summary,
                draft_reply=result.draft_reply,
            )
            response["task"] = {
                "success": task_result.success,
                "entry_id": task_result.entry_id,
                "error": task_result.error,
            }

        return response

    return app


app = create_app()

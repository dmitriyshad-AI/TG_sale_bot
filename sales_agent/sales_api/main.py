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
from urllib.parse import quote_plus, urlparse

from fastapi import Depends, FastAPI, HTTPException, Query, Request, UploadFile, status
from pydantic import BaseModel, Field
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from telegram import Update

from sales_agent.sales_bot import bot as bot_runtime
from sales_agent.sales_core.catalog import CatalogValidationError, Product, SearchCriteria, explain_match, select_top_products
from sales_agent.sales_core import faq_lab as faq_lab_service
from sales_agent.sales_core.config import Settings, get_settings, project_root
from sales_agent.sales_core.copilot import run_copilot_from_file
from sales_agent.sales_core.crm import build_crm_client
from sales_agent.sales_core.db import (
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
    get_business_connection,
    get_latest_mango_event_created_at,
    get_oldest_mango_event_created_at,
    get_conversation_outcome,
    get_conversation_context,
    get_connection,
    get_or_create_user,
    get_crm_cache,
    get_inbox_thread_detail,
    get_latest_call_summary_for_thread,
    get_revenue_metrics_snapshot,
    get_reply_draft,
    get_latest_lead_score,
    init_db,
    log_business_message,
    list_approval_actions_for_thread,
    list_business_messages,
    list_call_records,
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
    update_mango_event_status,
    set_reply_draft_last_error,
    update_reply_draft_status,
    update_reply_draft_text,
    upsert_conversation_outcome,
    upsert_conversation_context,
    upsert_crm_cache,
)
from sales_agent.sales_core.runtime_diagnostics import build_runtime_diagnostics
from sales_agent.sales_core.rate_limit import InMemoryRateLimiter, RateLimiter, RedisRateLimiter
from sales_agent.sales_core.tallanto_readonly import (
    TallantoReadOnlyClient,
    normalize_tallanto_fields,
    normalize_tallanto_modules,
    sanitize_tallanto_lookup_context,
)
from sales_agent.sales_core.mango_client import MangoCallEvent, MangoClient, MangoClientError
from sales_agent.sales_core.telegram_webapp import verify_telegram_webapp_init_data
from sales_agent.sales_core.llm_client import LLMClient
from sales_agent.sales_core.telegram_business_sender import (
    TelegramBusinessSendError,
    send_business_message,
)
from sales_agent.sales_core.vector_store import load_vector_store_id
from sales_agent.sales_api.routers.admin_core import build_admin_core_router
from sales_agent.sales_api.routers.admin_calls import build_admin_calls_router
from sales_agent.sales_api.routers.admin_inbox import build_admin_inbox_router
from sales_agent.sales_api.routers.faq_lab import build_faq_lab_router
from sales_agent.sales_api.routers.director import build_director_router
from sales_agent.sales_api.services.business_sender import send_business_draft_and_log as send_business_draft_and_log_service
from sales_agent.sales_api.services.revenue_ops import RevenueOpsService


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
TELEGRAM_MAX_TEXT_CHARS = 4000
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


def _build_rate_limiter(
    *,
    backend: str,
    window_seconds: int,
    redis_url: str,
    key_prefix: str,
) -> RateLimiter:
    normalized_backend = (backend or "").strip().lower()
    if normalized_backend == "redis":
        try:
            return RedisRateLimiter(
                redis_url=redis_url,
                window_seconds=window_seconds,
                key_prefix=key_prefix,
            )
        except Exception as exc:
            logger.warning("Redis rate limiter fallback to in-memory (%s)", exc)
    return InMemoryRateLimiter(window_seconds=window_seconds)


def create_app(settings: Settings | None = None) -> FastAPI:
    cfg = settings or get_settings()
    init_db(cfg.database_path)
    if cfg.app_env == "production" and cfg.telegram_mode == "webhook" and not cfg.telegram_webhook_secret:
        raise RuntimeError("TELEGRAM_WEBHOOK_SECRET is required in production when TELEGRAM_MODE=webhook.")
    webhook_path = cfg.telegram_webhook_path if cfg.telegram_webhook_path.startswith("/") else f"/{cfg.telegram_webhook_path}"
    mango_webhook_path = cfg.mango_webhook_path if cfg.mango_webhook_path.startswith("/") else f"/{cfg.mango_webhook_path}"
    telegram_application = None
    assistant_rate_limiter = _build_rate_limiter(
        backend=cfg.rate_limit_backend,
        window_seconds=cfg.assistant_rate_limit_window_seconds,
        redis_url=cfg.redis_url,
        key_prefix="assistant_rate_limit",
    )
    crm_rate_limiter = _build_rate_limiter(
        backend=cfg.rate_limit_backend,
        window_seconds=cfg.crm_rate_limit_window_seconds,
        redis_url=cfg.redis_url,
        key_prefix="crm_rate_limit",
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

    def _enforce_admin_ui_csrf(request: Request) -> None:
        if not cfg.admin_ui_csrf_enabled:
            return
        origin = request.headers.get("Origin", "").strip()
        referer = request.headers.get("Referer", "").strip()
        source = origin or referer
        if not source:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Missing Origin/Referer for admin UI POST request.",
            )

        parsed = urlparse(source)
        source_host = (parsed.hostname or "").strip().lower()
        if not source_host:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Invalid Origin/Referer host for admin UI POST request.",
            )

        allowed_hosts: set[str] = set()
        request_host = (request.url.hostname or "").strip().lower()
        if request_host:
            allowed_hosts.add(request_host)
        app_host = (urlparse(str(request.base_url)).hostname or "").strip().lower()
        if app_host:
            allowed_hosts.add(app_host)
        if cfg.admin_webapp_url:
            allowed_hosts.add((urlparse(cfg.admin_webapp_url).hostname or "").strip().lower())

        if source_host not in {host for host in allowed_hosts if host}:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="CSRF check failed for admin UI POST request.",
            )

    def _enforce_rate_limit(
        *,
        request: Request,
        limiter: RateLimiter,
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

    def _build_thread_crm_context(user_item: Dict[str, Any]) -> Dict[str, Any]:
        context: Dict[str, Any] = {
            "enabled": bool(cfg.enable_tallanto_enrichment and cfg.crm_provider == "tallanto"),
            "found": False,
            "tags": [],
            "interests": [],
            "last_touch_days": None,
        }
        if not context["enabled"]:
            return context

        if not cfg.tallanto_read_only:
            context["error"] = "tallanto_read_only_disabled"
            return context
        token = cfg.tallanto_api_token or cfg.tallanto_api_key
        if not cfg.tallanto_api_url or not token:
            context["error"] = "tallanto_not_configured"
            return context

        module = (cfg.tallanto_default_contact_module or "contacts").strip() or "contacts"
        external_id = str(user_item.get("external_id") or "").strip()
        username = str(user_item.get("username") or "").strip()
        lookup_candidates: list[tuple[str, str]] = []
        if external_id:
            lookup_candidates.extend(
                [
                    ("telegram_id", external_id),
                    ("telegram", external_id),
                    ("external_id", external_id),
                ]
            )
        if username:
            lookup_candidates.extend(
                [
                    ("username", username),
                    ("telegram_username", username),
                    ("telegram_username", f"@{username}"),
                ]
            )

        deduped: list[tuple[str, str]] = []
        seen_pairs: set[tuple[str, str]] = set()
        for field_name, field_value in lookup_candidates:
            key = (field_name.strip(), field_value.strip())
            if not key[0] or not key[1] or key in seen_pairs:
                continue
            seen_pairs.add(key)
            deduped.append(key)

        if not deduped:
            context["error"] = "lookup_candidates_empty"
            return context

        client = TallantoReadOnlyClient(base_url=cfg.tallanto_api_url, token=token)
        for field_name, field_value in deduped:
            cache_key = _crm_cache_key(
                "thread_context",
                {"module": module, "field": field_name, "value": field_value},
            )
            cached = _read_crm_cache(cache_key)
            if isinstance(cached, dict):
                cached_context = {
                    "enabled": True,
                    "found": bool(cached.get("found")),
                    "tags": list(cached.get("tags") or []),
                    "interests": list(cached.get("interests") or []),
                    "last_touch_days": cached.get("last_touch_days"),
                    "lookup_field": field_name,
                }
                if cached_context["found"]:
                    return cached_context

            try:
                primary = client.call(
                    "entry_by_fields",
                    {"module": module, "fields_values": {field_name: field_value}},
                )
                payload = sanitize_tallanto_lookup_context(primary)
                if not payload.get("found"):
                    fallback = client.call(
                        "get_entry_list",
                        {"module": module, "fields_values": {field_name: field_value}},
                    )
                    payload = sanitize_tallanto_lookup_context(fallback)
            except RuntimeError:
                continue

            response_payload = {
                "found": bool(payload.get("found")),
                "tags": list(payload.get("tags") or []),
                "interests": list(payload.get("interests") or []),
                "last_touch_days": payload.get("last_touch_days"),
            }
            _write_crm_cache(cache_key, response_payload)
            if response_payload["found"]:
                return {
                    "enabled": True,
                    **response_payload,
                    "lookup_field": field_name,
                }

        return context

    def _thread_id_from_user_id(user_id: int) -> str:
        return f"tg:{int(user_id)}"

    def _parse_business_thread_key(thread_id: str) -> Optional[tuple[str, int]]:
        raw = (thread_id or "").strip()
        if not raw.startswith("biz:"):
            return None
        parts = raw.split(":", 2)
        if len(parts) != 3:
            return None
        business_connection_id = parts[1].strip()
        chat_token = parts[2].strip()
        if not business_connection_id:
            return None
        if not chat_token or not chat_token.lstrip("-").isdigit():
            return None
        return business_connection_id, int(chat_token)

    def _require_user_exists(conn: Any, user_id: int) -> None:
        row = conn.execute("SELECT id FROM users WHERE id = ? LIMIT 1", (int(user_id),)).fetchone()
        if row is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found.")

    revenue_ops = RevenueOpsService(
        settings=cfg,
        db_path=cfg.database_path,
        require_user_exists=_require_user_exists,
        thread_id_from_user_id=_thread_id_from_user_id,
        lead_radar_rule_no_reply=LEAD_RADAR_RULE_NO_REPLY,
        lead_radar_rule_call_no_next_step=LEAD_RADAR_RULE_CALL_NO_NEXT_STEP,
        lead_radar_rule_stale_warm=LEAD_RADAR_RULE_STALE_WARM,
        lead_radar_model_name=LEAD_RADAR_MODEL_NAME,
        call_copilot_model_name=CALL_COPILOT_MODEL_NAME,
        mango_cleanup_batch_size=MANGO_CLEANUP_BATCH_SIZE,
        logger=logger,
    )

    async def _send_business_draft_and_log(
        conn: Any,
        *,
        draft: Dict[str, Any],
        actor: str,
    ) -> Dict[str, Any]:
        return await send_business_draft_and_log_service(
            conn=conn,
            draft=draft,
            actor=actor,
            telegram_bot_token=cfg.telegram_bot_token,
            parse_business_thread_key=_parse_business_thread_key,
            get_business_connection=get_business_connection,
            send_business_message=send_business_message,
            send_error_type=TelegramBusinessSendError,
            set_reply_draft_last_error=set_reply_draft_last_error,
            create_approval_action=create_approval_action,
            log_business_message=log_business_message,
            max_text_chars=TELEGRAM_MAX_TEXT_CHARS,
        )

    def _mango_ingest_enabled() -> bool:
        return revenue_ops.mango_ingest_enabled()

    def _build_mango_client() -> MangoClient:
        return revenue_ops.build_mango_client()

    def _cleanup_old_call_files() -> Dict[str, Any]:
        return revenue_ops.cleanup_old_call_files()

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

    def _inbox_workflow_status_label(status_value: str) -> str:
        normalized = (status_value or "").strip().lower()
        labels = {
            "new": "Новый",
            "needs_approval": "Нужен approve",
            "ready_to_send": "Готов к отправке",
            "sending": "Отправляется",
            "failed": "Ошибка отправки",
            "sent": "Отправлен",
            "rejected": "Отклонён",
            "manual_required": "Нужен ручной шаг",
        }
        return labels.get(normalized, status_value or "new")

    def _inbox_workflow_badge(status_value: str) -> str:
        normalized = (status_value or "").strip().lower()
        colors = {
            "new": "#e5e7eb",
            "needs_approval": "#fef3c7",
            "ready_to_send": "#dbeafe",
            "sending": "#bfdbfe",
            "failed": "#fecaca",
            "sent": "#dcfce7",
            "rejected": "#f3f4f6",
            "manual_required": "#ede9fe",
        }
        bg = colors.get(normalized, "#e5e7eb")
        label = html.escape(_inbox_workflow_status_label(normalized))
        return f"<span class='badge' style='background:{bg};'>{label}</span>"

    async def run_lead_radar_once(
        *,
        trigger: str,
        dry_run: bool = False,
        limit_override: Optional[int] = None,
    ) -> Dict[str, Any]:
        return await revenue_ops.run_lead_radar_once(
            trigger=trigger,
            dry_run=dry_run,
            limit_override=limit_override,
        )

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

    async def ingest_mango_event(
        *,
        event: MangoCallEvent,
        source: str,
        existing_event_row_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        return await revenue_ops.ingest_mango_event(
            event=event,
            source=source,
            existing_event_row_id=existing_event_row_id,
        )

    async def run_mango_poll_once(*, trigger: str, limit_override: Optional[int] = None) -> Dict[str, Any]:
        return await revenue_ops.run_mango_poll_once(trigger=trigger, limit_override=limit_override)

    async def run_mango_retry_failed_once(*, trigger: str, limit_override: Optional[int] = None) -> Dict[str, Any]:
        return await revenue_ops.run_mango_retry_failed_once(trigger=trigger, limit_override=limit_override)

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
    :root {{
      --fg: #0f172a;
      --muted: #475569;
      --line: #cbd5e1;
      --card: #f8fafc;
      --bg: #eef2ff;
      --nav: #0b1d35;
      --nav-link: #e2e8f0;
      --nav-link-active: #bfdbfe;
      --btn: #1d4ed8;
      --btn-text: #ffffff;
    }}
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; color: var(--fg); background: var(--bg); }}
    .shell {{ max-width: 1420px; margin: 0 auto; padding: 16px 20px 28px; }}
    h1, h2 {{ margin: 0 0 12px; line-height: 1.25; }}
    .muted {{ color: var(--muted); }}
    nav {{
      position: sticky;
      top: 0;
      z-index: 10;
      background: var(--nav);
      border-bottom: 1px solid #0f2a49;
      padding: 10px 14px;
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      margin: -16px -20px 16px;
    }}
    nav a {{
      color: var(--nav-link);
      text-decoration: none;
      padding: 7px 10px;
      border-radius: 10px;
      font-weight: 600;
      font-size: 13px;
      line-height: 1;
      white-space: nowrap;
    }}
    nav a:hover {{ background: #16355a; color: #ffffff; }}
    nav a:focus {{ outline: 2px solid #93c5fd; outline-offset: 1px; }}
    .current {{ color: var(--nav-link-active); background: #16355a; }}
    .toolbar {{
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin: 0 0 12px;
    }}
    .toolbar a {{
      text-decoration: none;
      border: 1px solid #bfdbfe;
      color: #1e3a8a;
      background: #eff6ff;
      border-radius: 999px;
      padding: 6px 10px;
      font-size: 12px;
      font-weight: 600;
    }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 12px; background: #fff; }}
    th, td {{ border: 1px solid var(--line); padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f1f5f9; }}
    .card {{ border: 1px solid var(--line); border-radius: 10px; padding: 12px; margin-bottom: 12px; background: var(--card); }}
    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 999px; background: #dbeafe; color: #1e3a8a; font-weight: 600; }}
    pre {{ white-space: pre-wrap; word-break: break-word; background: #f8fafc; border: 1px solid #e2e8f0; padding: 10px; border-radius: 6px; }}
    input, textarea, select, button {{ font-size: 14px; }}
    input, textarea, select {{ border: 1px solid #94a3b8; border-radius: 8px; padding: 7px 9px; }}
    button {{ padding: 8px 12px; cursor: pointer; background: var(--btn); color: var(--btn-text); border: 1px solid #1e40af; border-radius: 8px; font-weight: 600; }}
    button:hover {{ background: #1e40af; }}
    @media (max-width: 900px) {{
      .shell {{ padding: 12px; }}
      nav {{ margin: -12px -12px 12px; }}
    }}
  </style>
</head>
<body>
  <div class="shell">
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
    <div class="toolbar">
      <a href="/admin/ui/inbox?status=new">Новые треды</a>
      <a href="/admin/ui/inbox?status=failed">Ошибки отправки</a>
      <a href="/admin/ui/followups?priority=hot&status=pending">Hot followups</a>
      <a href="/admin/ui/calls">Последние звонки</a>
      <a href="/admin/ui/director">Активные кампании</a>
    </div>
    {body_html}
  </div>
</body>
</html>
"""
        return HTMLResponse(page)

    app.include_router(
        build_faq_lab_router(
            db_path=cfg.database_path,
            require_admin_dependency=require_admin,
            enforce_ui_csrf=_enforce_admin_ui_csrf,
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
            enforce_ui_csrf=_enforce_admin_ui_csrf,
            render_page=render_page,
            enabled=cfg.enable_director_agent,
        )
    )
    app.include_router(
        build_admin_core_router(
            db_path=cfg.database_path,
            require_admin_dependency=require_admin,
            enforce_ui_csrf=_enforce_admin_ui_csrf,
            render_page=render_page,
            run_copilot_from_file=run_copilot_from_file,
            build_crm_client=build_crm_client,
            settings=cfg,
            mango_webhook_path=mango_webhook_path,
        )
    )

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
        return await revenue_ops.process_manual_call_upload(
            user_id=user_id,
            thread_id=thread_id,
            recording_url=recording_url,
            transcript_hint=transcript_hint,
            audio_file=audio_file,
            source_type_override=source_type_override,
            source_ref_override=source_ref_override,
            created_by=created_by,
            action_source=action_source,
            assigned_to=assigned_to,
        )

    app.include_router(
        build_admin_inbox_router(
            db_path=cfg.database_path,
            settings=cfg,
            require_admin_dependency=require_admin,
            enforce_ui_csrf=_enforce_admin_ui_csrf,
            render_page=render_page,
            run_lead_radar_once=run_lead_radar_once,
            thread_id_from_user_id=_thread_id_from_user_id,
            require_user_exists=_require_user_exists,
            build_thread_crm_context=_build_thread_crm_context,
            parse_business_thread_key=_parse_business_thread_key,
            send_business_draft_and_log=lambda conn, draft, actor: _send_business_draft_and_log(
                conn,
                draft=draft,
                actor=actor,
            ),
            format_thread_display_name=_format_thread_display_name,
            inbox_workflow_badge=_inbox_workflow_badge,
            inbox_workflow_status_label=_inbox_workflow_status_label,
            is_radar_reason=_is_radar_reason,
        )
    )
    app.include_router(
        build_admin_calls_router(
            db_path=cfg.database_path,
            settings=cfg,
            require_admin_dependency=require_admin,
            enforce_ui_csrf=_enforce_admin_ui_csrf,
            render_page=render_page,
            process_manual_call_upload=_process_manual_call_upload,
            mango_ingest_enabled=_mango_ingest_enabled,
            run_mango_poll_once=run_mango_poll_once,
            run_mango_retry_failed_once=run_mango_retry_failed_once,
            cleanup_old_call_files=_cleanup_old_call_files,
        )
    )

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

    return app


app = create_app()

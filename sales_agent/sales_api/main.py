from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date
import html
import json
import logging
import secrets
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import quote_plus

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile, status
from pydantic import BaseModel, Field
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from telegram import Update

from sales_agent.sales_bot import bot as bot_runtime
from sales_agent.sales_core.catalog import CatalogValidationError, Product, SearchCriteria, explain_match, select_top_products
from sales_agent.sales_core.config import Settings, get_settings, project_root
from sales_agent.sales_core.copilot import run_copilot_from_file
from sales_agent.sales_core.crm import build_crm_client
from sales_agent.sales_core.db import (
    claim_webhook_update,
    count_webhook_updates_by_status,
    enqueue_webhook_update,
    get_connection,
    get_crm_cache,
    init_db,
    list_conversation_messages,
    list_recent_conversations,
    list_recent_leads,
    mark_webhook_update_done,
    mark_webhook_update_retry,
    requeue_stuck_webhook_updates,
    upsert_crm_cache,
)
from sales_agent.sales_core.runtime_diagnostics import build_runtime_diagnostics
from sales_agent.sales_core.tallanto_readonly import (
    TallantoReadOnlyClient,
    normalize_tallanto_fields,
    normalize_tallanto_modules,
    sanitize_tallanto_lookup_context,
)
from sales_agent.sales_core.telegram_webapp import verify_telegram_webapp_init_data
from sales_agent.sales_core.llm_client import LLMClient
from sales_agent.sales_core.vector_store import load_vector_store_id


security = HTTPBasic()
logger = logging.getLogger(__name__)
WEBHOOK_MAX_ATTEMPTS = 5
WEBHOOK_STALE_PROCESSING_SECONDS = 180
WEBHOOK_RETRY_BASE_SECONDS = 2
CRM_CACHE_TTL_SECONDS = 3 * 3600
ASSISTANT_TIMEOUT_SECONDS = 36.0
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


class AssistantCriteriaPayload(BaseModel):
    brand: Optional[str] = None
    grade: Optional[int] = Field(default=None, ge=1, le=11)
    goal: Optional[str] = None
    subject: Optional[str] = None
    format: Optional[str] = None


class AssistantAskPayload(BaseModel):
    question: str = Field(min_length=1, max_length=2000)
    criteria: Optional[AssistantCriteriaPayload] = None
    context_summary: Optional[str] = Field(default=None, max_length=1200)


def _normalize_lookup_token(value: object) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower()


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
    telegram_application = None
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
        yield
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
    <a href="/admin/ui/leads">Leads</a>
    <a href="/admin/ui/conversations">Conversations</a>
    <a href="/admin/ui/copilot">Copilot</a>
  </nav>
  {body_html}
</body>
</html>
"""
        return HTMLResponse(page)

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
    async def assistant_ask(payload: AssistantAskPayload):
        question = payload.question.strip()
        if not question:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Question is empty.")

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
        user_context: Dict[str, Any] = {}
        if isinstance(payload.context_summary, str) and payload.context_summary.strip():
            user_context["summary_text"] = payload.context_summary.strip()

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
                recent_history=[],
                user_context=user_context,
            )
            answer_text = consult_reply.answer_text
            used_fallback = consult_reply.used_fallback
            recommended_ids = list(consult_reply.recommended_product_ids)
        else:
            general_reply = await llm_client.build_general_help_reply_async(
                user_message=question,
                dialogue_state=None,
                recent_history=[],
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

        return {
            "ok": True,
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

    @app.get("/api/crm/meta/modules")
    async def crm_meta_modules():
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
    async def crm_meta_fields(module: str = Query(..., min_length=1, max_length=128)):
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

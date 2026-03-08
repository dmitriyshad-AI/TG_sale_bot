from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import logging
import secrets
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from telegram import Update

from sales_agent.sales_bot import bot as bot_runtime
from sales_agent.sales_core import faq_lab as faq_lab_service
from sales_agent.sales_core.config import Settings, get_settings, project_root
from sales_agent.sales_core.copilot import run_copilot_from_file
from sales_agent.sales_core.crm import build_crm_client
from sales_agent.sales_core.db import (
    claim_webhook_update,
    create_approval_action,
    create_call_record,
    create_or_get_mango_event,
    create_followup_task,
    create_lead_score,
    create_reply_draft,
    enqueue_webhook_update,
    get_business_connection,
    get_latest_mango_event_created_at,
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
from sales_agent.sales_core.rate_limit import InMemoryRateLimiter, RateLimiter, RedisRateLimiter
from sales_agent.sales_core.mango_client import MangoCallEvent, MangoClient, MangoClientError
from sales_agent.sales_core.telegram_webapp import verify_telegram_webapp_init_data
from sales_agent.sales_core.llm_client import LLMClient
from sales_agent.sales_core.telegram_business_sender import (
    TelegramBusinessSendError,
    send_business_message,
)
from sales_agent.sales_core.vector_store import load_vector_store_id
from sales_agent.sales_core.tallanto_readonly import TallantoReadOnlyClient
from sales_agent.sales_api.routers.admin_core import build_admin_core_router
from sales_agent.sales_api.routers.admin_calls import build_admin_calls_router
from sales_agent.sales_api.routers.admin_inbox import build_admin_inbox_router
from sales_agent.sales_api.routers.admin_miniapp import build_admin_miniapp_router
from sales_agent.sales_api.routers.faq_lab import build_faq_lab_router
from sales_agent.sales_api.routers.director import build_director_router
from sales_agent.sales_api.routers.outbound import build_outbound_router
from sales_agent.sales_api.routers.public_api import build_public_api_router
from sales_agent.sales_api.routers.assistant_api import build_assistant_api_router
from sales_agent.sales_api.routers.crm_api import build_crm_api_router
from sales_agent.sales_api.routers.webhooks import build_webhooks_router
from sales_agent.sales_api.services.business_sender import send_business_draft_and_log as send_business_draft_and_log_service
from sales_agent.sales_api.services.auth_helpers import (
    enforce_admin_ui_csrf as enforce_admin_ui_csrf_service,
    require_admin_credentials as require_admin_credentials_service,
    require_assistant_access as require_assistant_access_service,
)
from sales_agent.sales_api.services.assistant_utils import (
    ASSISTANT_CONTEXT_SUMMARY_MAX,
    AssistantAskPayload,
    AssistantCriteriaPayload,
    AssistantHistoryItem,
    assistant_mode as _assistant_mode,
    build_manager_offer as _build_manager_offer,
    compact_text as _compact_text,
    criteria_from_payload as _criteria_from_payload,
    evaluate_match_quality as _evaluate_match_quality,
    format_next_start_text as _format_next_start_text,
    format_price_text as _format_price_text,
    merge_assistant_context as _merge_assistant_context,
    missing_criteria_fields as _missing_criteria_fields,
    sanitize_recent_history as _sanitize_recent_history,
)
from sales_agent.sales_api.services.crm_dependencies import CrmDependencyService
from sales_agent.sales_api.services.request_access import (
    extract_bearer_token as _extract_bearer_token,
    extract_tg_init_data as _extract_tg_init_data,
    request_client_ip as _request_client_ip,
    request_id_from_request as _request_id_from_request,
    safe_user_payload as _safe_user_payload,
    build_rate_limiter as build_rate_limiter_service,
    enforce_rate_limit as enforce_rate_limit_service,
)
from sales_agent.sales_api.services.admin_layout import (
    inbox_workflow_badge as inbox_workflow_badge_service,
    inbox_workflow_status_label as inbox_workflow_status_label_service,
    render_admin_page as render_admin_page_service,
)
from sales_agent.sales_api.services.runtime_orchestration import (
    faq_lab_loop as faq_lab_loop_service,
    lead_radar_loop as lead_radar_loop_service,
    mango_poll_loop as mango_poll_loop_service,
    process_next_webhook_queue_item as process_next_webhook_queue_item_service,
    webhook_worker_loop as webhook_worker_loop_service,
)
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
TELEGRAM_MAX_TEXT_CHARS = 4000
LEAD_RADAR_RULE_NO_REPLY = "radar:no_reply"
LEAD_RADAR_RULE_CALL_NO_NEXT_STEP = "radar:call_no_next_step"
LEAD_RADAR_RULE_STALE_WARM = "radar:stale_warm"
LEAD_RADAR_MODEL_NAME = "lead_radar_v1"
FAQ_LAB_MODEL_NAME = "faq_lab_v1"
CALL_COPILOT_MODEL_NAME = "call_copilot_v1"
MANGO_CLEANUP_BATCH_SIZE = 200


def _build_rate_limiter(
    *,
    backend: str,
    window_seconds: int,
    redis_url: str,
    key_prefix: str,
) -> RateLimiter:
    return build_rate_limiter_service(
        backend=backend,
        window_seconds=window_seconds,
        redis_url=redis_url,
        key_prefix=key_prefix,
        redis_rate_limiter_cls=RedisRateLimiter,
        in_memory_rate_limiter_cls=InMemoryRateLimiter,
        logger=logger,
    )


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
        return await process_next_webhook_queue_item_service(
            app_instance=app_instance,
            database_path=cfg.database_path,
            get_connection=get_connection,
            claim_webhook_update=claim_webhook_update,
            mark_webhook_update_retry=mark_webhook_update_retry,
            mark_webhook_update_done=mark_webhook_update_done,
            update_parser=Update.de_json,
            retry_base_seconds=WEBHOOK_RETRY_BASE_SECONDS,
            max_attempts=WEBHOOK_MAX_ATTEMPTS,
            logger=logger,
        )

    async def webhook_worker_loop(app_instance: FastAPI) -> None:
        await webhook_worker_loop_service(
            app_instance=app_instance,
            process_next_item=process_next_webhook_queue_item,
            logger=logger,
        )

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
        return require_admin_credentials_service(
            username=credentials.username,
            password=credentials.password,
            expected_username=cfg.admin_user,
            expected_password=cfg.admin_pass,
        )

    def _enforce_admin_ui_csrf(request: Request) -> None:
        enforce_admin_ui_csrf_service(
            request,
            csrf_enabled=cfg.admin_ui_csrf_enabled,
            admin_webapp_url=cfg.admin_webapp_url,
        )

    def _enforce_rate_limit(
        *,
        request: Request,
        limiter: RateLimiter,
        key: str,
        limit: int,
        scope: str,
    ) -> None:
        enforce_rate_limit_service(
            request=request,
            limiter=limiter,
            key=key,
            limit=limit,
            scope=scope,
            request_id_getter=_request_id_from_request,
        )

    def _require_assistant_access(request: Request) -> Dict[str, Any]:
        return require_assistant_access_service(
            request,
            telegram_bot_token=cfg.telegram_bot_token,
            assistant_api_token=cfg.assistant_api_token,
            assistant_api_token_header=ASSISTANT_API_TOKEN_HEADER,
            extract_tg_init_data=_extract_tg_init_data,
            extract_bearer_token=_extract_bearer_token,
            verify_telegram_auth=lambda init_data, bot_token: verify_telegram_webapp_init_data(
                init_data=init_data,
                bot_token=bot_token,
            ),
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

    crm_dependencies = CrmDependencyService(
        settings=cfg,
        database_path=cfg.database_path,
        cache_ttl_seconds=CRM_CACHE_TTL_SECONDS,
        get_connection=get_connection,
        get_crm_cache=get_crm_cache,
        upsert_crm_cache=upsert_crm_cache,
        client_cls=TallantoReadOnlyClient,
    )
    _require_tallanto_readonly_client = crm_dependencies.require_tallanto_readonly_client
    _crm_cache_key = crm_dependencies.crm_cache_key
    _read_crm_cache = crm_dependencies.read_crm_cache
    _write_crm_cache = crm_dependencies.write_crm_cache
    _map_tallanto_error = crm_dependencies.map_tallanto_error
    _build_thread_crm_context = crm_dependencies.build_thread_crm_context

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

    _inbox_workflow_status_label = inbox_workflow_status_label_service
    _inbox_workflow_badge = inbox_workflow_badge_service

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
        await lead_radar_loop_service(
            app_instance=app_instance,
            interval_seconds=cfg.lead_radar_interval_seconds,
            run_once=lambda: run_lead_radar_once(trigger="scheduler"),
            logger=logger,
        )

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
        await faq_lab_loop_service(
            app_instance=app_instance,
            interval_seconds=cfg.faq_lab_interval_seconds,
            run_once=lambda: run_faq_lab_once(trigger="scheduler"),
            logger=logger,
        )

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

    async def run_call_retry_failed_once(*, trigger: str, limit_override: Optional[int] = None) -> Dict[str, Any]:
        return await revenue_ops.run_call_retry_failed_once(trigger=trigger, limit_override=limit_override)

    async def mango_poll_loop(app_instance: FastAPI) -> None:
        await mango_poll_loop_service(
            app_instance=app_instance,
            interval_seconds=cfg.mango_poll_interval_seconds,
            run_once=lambda: run_mango_poll_once(trigger="scheduler"),
            logger=logger,
        )

    def render_page(title: str, body_html: str) -> HTMLResponse:
        return render_admin_page_service(title=title, body_html=body_html)

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
            run_faq_lab_once=run_faq_lab_once,
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
        build_outbound_router(
            db_path=cfg.database_path,
            require_admin_dependency=require_admin,
            enforce_ui_csrf=_enforce_admin_ui_csrf,
            render_page=render_page,
            enabled=cfg.enable_outbound_copilot,
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
            run_call_retry_failed_once=run_call_retry_failed_once,
            cleanup_old_call_files=_cleanup_old_call_files,
        )
    )
    app.include_router(
        build_admin_miniapp_router(
            settings=cfg,
            miniapp_dir=miniapp_dir,
            db_path=cfg.database_path,
            require_miniapp_user=require_miniapp_user,
        )
    )
    app.include_router(
        build_public_api_router(
            settings=cfg,
            extract_tg_init_data=_extract_tg_init_data,
            safe_user_payload=_safe_user_payload,
            evaluate_match_quality=_evaluate_match_quality,
            build_manager_offer=_build_manager_offer,
            format_price_text=_format_price_text,
            format_next_start_text=_format_next_start_text,
            render_page=render_page,
            user_webapp_ready=user_webapp_ready,
            user_webapp_dist=user_webapp_dist,
            mango_webhook_path=mango_webhook_path,
            mango_ingest_enabled=_mango_ingest_enabled,
        )
    )
    app.include_router(
        build_assistant_api_router(
            settings=cfg,
            assistant_payload_model=AssistantAskPayload,
            request_id_from_request=_request_id_from_request,
            require_assistant_access=_require_assistant_access,
            request_client_ip=_request_client_ip,
            enforce_rate_limit=_enforce_rate_limit,
            assistant_rate_limiter=assistant_rate_limiter,
            criteria_from_payload=_criteria_from_payload,
            evaluate_match_quality=_evaluate_match_quality,
            build_manager_offer=_build_manager_offer,
            assistant_mode=_assistant_mode,
            missing_criteria_fields=_missing_criteria_fields,
            sanitize_recent_history=_sanitize_recent_history,
            compact_text=_compact_text,
            merge_assistant_context=_merge_assistant_context,
            llm_client_factory=lambda: LLMClient(
                api_key=cfg.openai_api_key,
                model=cfg.openai_model,
                timeout_seconds=ASSISTANT_TIMEOUT_SECONDS,
            ),
            load_vector_store_id=load_vector_store_id,
            get_connection=get_connection,
            get_or_create_user=get_or_create_user,
            get_conversation_context=get_conversation_context,
            upsert_conversation_context=upsert_conversation_context,
        )
    )
    app.include_router(
        build_crm_api_router(
            require_crm_api_access=_require_crm_api_access,
            require_tallanto_readonly_client=_require_tallanto_readonly_client,
            crm_cache_key=_crm_cache_key,
            read_crm_cache=_read_crm_cache,
            write_crm_cache=_write_crm_cache,
            map_tallanto_error=_map_tallanto_error,
        )
    )
    app.include_router(
        build_webhooks_router(
            settings=cfg,
            mango_webhook_path=mango_webhook_path,
            telegram_webhook_path=webhook_path,
            mango_ingest_enabled=_mango_ingest_enabled,
            build_mango_client=_build_mango_client,
            ingest_mango_event=ingest_mango_event,
            cleanup_old_call_files=_cleanup_old_call_files,
            get_connection=get_connection,
            enqueue_webhook_update=enqueue_webhook_update,
            mango_client_error_type=MangoClientError,
        )
    )

    return app


app = create_app()

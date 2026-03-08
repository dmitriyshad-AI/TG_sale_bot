from __future__ import annotations

import json
import logging
import secrets
from typing import Any, Awaitable, Callable, Dict

from fastapi import APIRouter, HTTPException, Request, status

logger = logging.getLogger(__name__)


def build_webhooks_router(
    *,
    settings: Any,
    mango_webhook_path: str,
    telegram_webhook_path: str,
    mango_ingest_enabled: Callable[[], bool],
    build_mango_client: Callable[[], Any],
    ingest_mango_event: Callable[..., Awaitable[Dict[str, Any]]],
    cleanup_old_call_files: Callable[[], Dict[str, Any]],
    get_connection: Callable[[Any], Any],
    enqueue_webhook_update: Callable[..., Dict[str, Any]],
    mango_client_error_type: type[Exception],
) -> APIRouter:
    router = APIRouter()

    @router.post(mango_webhook_path)
    async def mango_webhook(request: Request):
        if not mango_ingest_enabled():
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
            client = build_mango_client()
        except mango_client_error_type as exc:
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

        cleanup_result = cleanup_old_call_files()
        return {
            "ok": True,
            "result": result,
            "cleanup": cleanup_result,
        }

    @router.post(telegram_webhook_path)
    async def telegram_webhook(request: Request):
        if settings.telegram_mode != "webhook":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Webhook endpoint is disabled. Set TELEGRAM_MODE=webhook.",
            )
        if not settings.telegram_bot_token:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="TELEGRAM_BOT_TOKEN is not configured.",
            )
        if settings.telegram_webhook_secret:
            header_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if not secrets.compare_digest(header_secret, settings.telegram_webhook_secret):
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

        telegram_application = getattr(request.app.state, "telegram_application", None)
        if telegram_application is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Telegram webhook application is not initialized.",
            )

        update_id = payload.get("update_id") if isinstance(payload.get("update_id"), int) else None
        conn = get_connection(settings.database_path)
        try:
            enqueue_result = enqueue_webhook_update(conn, payload=payload, update_id=update_id)
        finally:
            conn.close()

        event = getattr(request.app.state, "webhook_worker_event", None)
        if event is not None:
            event.set()

        if not enqueue_result.get("is_new", False):
            logger.info("Ignoring duplicate Telegram update_id=%s", update_id)
        return {"ok": True, "queued": True}

    return router

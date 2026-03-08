from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict

from fastapi import FastAPI


async def process_next_webhook_queue_item(
    *,
    app_instance: FastAPI,
    database_path: Any,
    get_connection: Callable[[Any], Any],
    claim_webhook_update: Callable[..., Any],
    mark_webhook_update_retry: Callable[..., str],
    mark_webhook_update_done: Callable[..., Any],
    update_parser: Callable[[Dict[str, Any], Any], Any],
    retry_base_seconds: int,
    max_attempts: int,
    logger: Any,
) -> bool:
    telegram_app = getattr(app_instance.state, "telegram_application", None)
    if telegram_app is None:
        return False

    conn = get_connection(database_path)
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
        update = update_parser(payload, telegram_app.bot)
        if update is None:
            raise ValueError("Could not parse Telegram update payload.")
        await telegram_app.process_update(update)
    except Exception as exc:
        delay = min(60, retry_base_seconds ** max(1, min(attempts, 5)))
        conn_retry = get_connection(database_path)
        try:
            final_state = mark_webhook_update_retry(
                conn_retry,
                queue_id=queue_id,
                error=str(exc),
                retry_delay_seconds=delay,
                max_attempts=max_attempts,
            )
        finally:
            conn_retry.close()
        if final_state == "failed":
            logger.exception("Webhook update failed permanently (queue_id=%s)", queue_id)
        else:
            logger.exception("Webhook update failed; queued for retry (queue_id=%s)", queue_id)
        return True

    conn_done = get_connection(database_path)
    try:
        mark_webhook_update_done(conn_done, queue_id=queue_id)
    finally:
        conn_done.close()
    return True


async def webhook_worker_loop(
    *,
    app_instance: FastAPI,
    process_next_item: Callable[[FastAPI], Awaitable[bool]],
    logger: Any,
) -> None:
    event = getattr(app_instance.state, "webhook_worker_event", None)
    if event is None:
        return
    while True:
        processed = False
        try:
            while await process_next_item(app_instance):
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


async def lead_radar_loop(
    *,
    app_instance: FastAPI,
    interval_seconds: int,
    run_once: Callable[[], Awaitable[Dict[str, Any]]],
    logger: Any,
) -> None:
    event = getattr(app_instance.state, "lead_radar_event", None)
    if event is None:
        return
    interval = max(60, int(interval_seconds))
    while True:
        try:
            summary = await run_once()
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


async def faq_lab_loop(
    *,
    app_instance: FastAPI,
    interval_seconds: int,
    run_once: Callable[[], Awaitable[Dict[str, Any]]],
    logger: Any,
) -> None:
    event = getattr(app_instance.state, "faq_lab_event", None)
    if event is None:
        return
    interval = max(300, int(interval_seconds))
    while True:
        try:
            summary = await run_once()
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


async def mango_poll_loop(
    *,
    app_instance: FastAPI,
    interval_seconds: int,
    run_once: Callable[[], Awaitable[Dict[str, Any]]],
    logger: Any,
) -> None:
    event = getattr(app_instance.state, "mango_poll_event", None)
    if event is None:
        return
    interval = max(30, int(interval_seconds))
    while True:
        try:
            summary = await run_once()
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


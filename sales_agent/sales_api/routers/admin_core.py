from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any, Callable, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse

from sales_agent.sales_core import db


def build_admin_core_router(
    *,
    db_path: Path,
    require_admin_dependency: Callable[..., str],
    enforce_ui_csrf: Callable[[Request], None],
    render_page: Callable[[str, str], HTMLResponse],
    run_copilot_from_file: Callable[..., Any],
    build_crm_client: Callable[..., Any],
    settings: Any,
    mango_webhook_path: str,
) -> APIRouter:
    router = APIRouter()

    def _build_revenue_metrics_payload() -> dict[str, Any]:
        conn = db.get_connection(db_path)
        try:
            metrics = db.get_revenue_metrics_snapshot(conn)
        finally:
            conn.close()
        return {
            "ok": True,
            "metrics": metrics,
            "feature_flags": {
                "enable_business_inbox": settings.enable_business_inbox,
                "enable_call_copilot": settings.enable_call_copilot,
                "enable_tallanto_enrichment": settings.enable_tallanto_enrichment,
                "enable_director_agent": settings.enable_director_agent,
                "enable_lead_radar": settings.enable_lead_radar,
                "enable_faq_lab": settings.enable_faq_lab,
                "enable_mango_auto_ingest": settings.enable_mango_auto_ingest,
                "enable_outbound_copilot": settings.enable_outbound_copilot,
                "lead_radar_scheduler_enabled": settings.lead_radar_scheduler_enabled,
                "faq_lab_scheduler_enabled": settings.faq_lab_scheduler_enabled,
            },
            "lead_radar": {
                "interval_seconds": settings.lead_radar_interval_seconds,
                "no_reply_hours": settings.lead_radar_no_reply_hours,
                "call_no_next_step_hours": settings.lead_radar_call_no_next_step_hours,
                "stale_warm_days": settings.lead_radar_stale_warm_days,
                "max_items_per_run": settings.lead_radar_max_items_per_run,
                "thread_cooldown_hours": settings.lead_radar_thread_cooldown_hours,
                "daily_cap_per_thread": settings.lead_radar_daily_cap_per_thread,
            },
            "faq_lab": {
                "interval_seconds": settings.faq_lab_interval_seconds,
                "window_days": settings.faq_lab_window_days,
                "min_question_count": settings.faq_lab_min_question_count,
                "max_items_per_run": settings.faq_lab_max_items_per_run,
            },
            "mango": {
                "webhook_path": mango_webhook_path,
                "polling_enabled": settings.mango_polling_enabled,
                "poll_interval_seconds": settings.mango_poll_interval_seconds,
                "poll_limit_per_run": settings.mango_poll_limit_per_run,
                "poll_retry_attempts": settings.mango_poll_retry_attempts,
                "poll_retry_backoff_seconds": settings.mango_poll_retry_backoff_seconds,
                "retry_failed_limit_per_run": settings.mango_retry_failed_limit_per_run,
                "recording_ttl_hours": settings.mango_call_recording_ttl_hours,
                "calls_path": settings.mango_calls_path,
            },
        }

    @router.get("/admin", response_class=HTMLResponse)
    async def admin_home(_: str = Depends(require_admin_dependency)):
        conn = db.get_connection(db_path)
        try:
            leads_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM leads").fetchone()["cnt"])
            users_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM users").fetchone()["cnt"])
            messages_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM messages").fetchone()["cnt"])
            recent_conversations = db.list_recent_conversations(conn, limit=5)
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

    @router.get("/admin/revenue-metrics")
    async def admin_revenue_metrics(_: str = Depends(require_admin_dependency)):
        return _build_revenue_metrics_payload()

    @router.get("/admin/ui/revenue-metrics", response_class=HTMLResponse)
    async def admin_revenue_metrics_ui(_: str = Depends(require_admin_dependency)):
        payload = _build_revenue_metrics_payload()
        metrics = payload["metrics"]
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
            f"ENABLE_BUSINESS_INBOX={settings.enable_business_inbox}<br/>"
            f"ENABLE_CALL_COPILOT={settings.enable_call_copilot}<br/>"
            f"ENABLE_TALLANTO_ENRICHMENT={settings.enable_tallanto_enrichment}<br/>"
            f"ENABLE_DIRECTOR_AGENT={settings.enable_director_agent}<br/>"
            f"ENABLE_LEAD_RADAR={settings.enable_lead_radar}<br/>"
            f"ENABLE_FAQ_LAB={settings.enable_faq_lab}<br/>"
            f"ENABLE_MANGO_AUTO_INGEST={settings.enable_mango_auto_ingest}<br/>"
            f"ENABLE_OUTBOUND_COPILOT={settings.enable_outbound_copilot}<br/>"
            f"LEAD_RADAR_SCHEDULER_ENABLED={settings.lead_radar_scheduler_enabled}<br/>"
            f"LEAD_RADAR_INTERVAL_SECONDS={settings.lead_radar_interval_seconds}<br/>"
            f"LEAD_RADAR_NO_REPLY_HOURS={settings.lead_radar_no_reply_hours}<br/>"
            f"LEAD_RADAR_CALL_NO_NEXT_STEP_HOURS={settings.lead_radar_call_no_next_step_hours}<br/>"
            f"LEAD_RADAR_STALE_WARM_DAYS={settings.lead_radar_stale_warm_days}<br/>"
            f"LEAD_RADAR_MAX_ITEMS_PER_RUN={settings.lead_radar_max_items_per_run}<br/>"
            f"LEAD_RADAR_THREAD_COOLDOWN_HOURS={settings.lead_radar_thread_cooldown_hours}<br/>"
            f"LEAD_RADAR_DAILY_CAP_PER_THREAD={settings.lead_radar_daily_cap_per_thread}<br/>"
            f"FAQ_LAB_SCHEDULER_ENABLED={settings.faq_lab_scheduler_enabled}<br/>"
            f"FAQ_LAB_INTERVAL_SECONDS={settings.faq_lab_interval_seconds}<br/>"
            f"FAQ_LAB_WINDOW_DAYS={settings.faq_lab_window_days}<br/>"
            f"FAQ_LAB_MIN_QUESTION_COUNT={settings.faq_lab_min_question_count}<br/>"
            f"FAQ_LAB_MAX_ITEMS_PER_RUN={settings.faq_lab_max_items_per_run}<br/>"
            f"MANGO_WEBHOOK_PATH={html.escape(mango_webhook_path)}<br/>"
            f"MANGO_POLLING_ENABLED={settings.mango_polling_enabled}<br/>"
            f"MANGO_POLL_INTERVAL_SECONDS={settings.mango_poll_interval_seconds}<br/>"
            f"MANGO_POLL_LIMIT_PER_RUN={settings.mango_poll_limit_per_run}<br/>"
            f"MANGO_POLL_RETRY_ATTEMPTS={settings.mango_poll_retry_attempts}<br/>"
            f"MANGO_POLL_RETRY_BACKOFF_SECONDS={settings.mango_poll_retry_backoff_seconds}<br/>"
            f"MANGO_RETRY_FAILED_LIMIT_PER_RUN={settings.mango_retry_failed_limit_per_run}<br/>"
            f"MANGO_CALL_RECORDING_TTL_HOURS={settings.mango_call_recording_ttl_hours}<br/>"
            f"MANGO_CALLS_PATH={html.escape(settings.mango_calls_path)}"
            "</div>"
        )
        return render_page("Revenue Metrics", body)

    @router.get("/admin/leads")
    async def admin_leads(_: str = Depends(require_admin_dependency), limit: int = 100):
        conn = db.get_connection(db_path)
        try:
            return {"items": db.list_recent_leads(conn, limit=max(1, min(limit, 500)))}
        finally:
            conn.close()

    @router.get("/admin/ui/leads", response_class=HTMLResponse)
    async def admin_leads_ui(_: str = Depends(require_admin_dependency), limit: int = 100):
        conn = db.get_connection(db_path)
        try:
            items = db.list_recent_leads(conn, limit=max(1, min(limit, 500)))
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

    @router.get("/admin/conversations")
    async def admin_conversations(_: str = Depends(require_admin_dependency), limit: int = 100):
        conn = db.get_connection(db_path)
        try:
            return {"items": db.list_recent_conversations(conn, limit=max(1, min(limit, 500)))}
        finally:
            conn.close()

    @router.get("/admin/ui/conversations", response_class=HTMLResponse)
    async def admin_conversations_ui(_: str = Depends(require_admin_dependency), limit: int = 100):
        conn = db.get_connection(db_path)
        try:
            items = db.list_recent_conversations(conn, limit=max(1, min(limit, 500)))
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

    @router.get("/admin/conversations/{user_id}")
    async def admin_conversation_history(user_id: int, _: str = Depends(require_admin_dependency), limit: int = 500):
        conn = db.get_connection(db_path)
        try:
            messages = db.list_conversation_messages(conn, user_id=user_id, limit=max(1, min(limit, 2000)))
            return {"user_id": user_id, "messages": messages}
        finally:
            conn.close()

    @router.get("/admin/ui/conversations/{user_id}", response_class=HTMLResponse)
    async def admin_conversation_history_ui(user_id: int, _: str = Depends(require_admin_dependency), limit: int = 500):
        conn = db.get_connection(db_path)
        try:
            messages = db.list_conversation_messages(conn, user_id=user_id, limit=max(1, min(limit, 2000)))
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

    @router.get("/admin/ui/copilot", response_class=HTMLResponse)
    async def admin_copilot_ui(_: str = Depends(require_admin_dependency)):
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

    @router.post("/admin/ui/copilot/import", response_class=HTMLResponse)
    async def admin_copilot_import_ui(
        request: Request,
        _: str = Depends(require_admin_dependency),
        file: UploadFile = File(...),
        create_task: bool = Form(False),
    ):
        enforce_ui_csrf(request)
        content = await file.read()
        if not content:
            return render_page("Copilot Error", "<h1>Ошибка</h1><p>Файл пустой.</p>")

        try:
            result = run_copilot_from_file(filename=file.filename or "dialog.txt", content=content)
        except ValueError as exc:
            return render_page("Copilot Error", f"<h1>Ошибка</h1><p>{html.escape(str(exc))}</p>")

        task_html = ""
        if create_task:
            crm = build_crm_client(settings)
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

    @router.post("/admin/copilot/import")
    async def admin_copilot_import(
        _: str = Depends(require_admin_dependency),
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
            crm = build_crm_client(settings)
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

    return router

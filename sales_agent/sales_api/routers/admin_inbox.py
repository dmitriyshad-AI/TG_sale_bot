from __future__ import annotations

import html
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Literal, Optional
from urllib.parse import quote_plus

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, ConfigDict, Field

from sales_agent.sales_api.services.draft_send import send_approved_draft
from sales_agent.sales_core.db import (
    create_approval_action,
    create_followup_task,
    create_lead_score,
    create_reply_draft,
    get_business_connection,
    get_connection,
    get_conversation_outcome,
    get_inbox_thread_detail,
    get_reply_draft,
    get_latest_lead_score,
    list_approval_actions_for_thread,
    list_business_messages,
    list_followup_tasks,
    list_inbox_threads,
    list_recent_business_threads,
    list_reply_drafts_for_thread,
    update_reply_draft_status,
    update_reply_draft_text,
    upsert_conversation_outcome,
)


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


def build_admin_inbox_router(
    *,
    db_path: Path,
    settings: Any,
    require_admin_dependency: Callable[..., str],
    enforce_ui_csrf: Callable[[Request], None],
    render_page: Callable[[str, str], HTMLResponse],
    run_lead_radar_once: Callable[..., Awaitable[Dict[str, Any]]],
    thread_id_from_user_id: Callable[[int], str],
    require_user_exists: Callable[[Any, int], None],
    build_thread_crm_context: Callable[[Dict[str, Any]], Dict[str, Any]],
    parse_business_thread_key: Callable[[str], Optional[tuple[str, int]]],
    send_business_draft_and_log: Callable[[Any, Dict[str, Any], str], Awaitable[Dict[str, Any]]],
    format_thread_display_name: Callable[[Dict[str, Any]], str],
    inbox_workflow_badge: Callable[[str], str],
    inbox_workflow_status_label: Callable[[str], str],
    is_radar_reason: Callable[[object], bool],
) -> APIRouter:
    router = APIRouter()

    @router.get("/admin/inbox")
    async def admin_inbox(
        _: str = Depends(require_admin_dependency),
        status_filter: Optional[str] = Query(default=None, alias="status"),
        search: Optional[str] = Query(default=None),
        limit: int = 100,
    ):
        conn = get_connection(db_path)
        try:
            items = list_inbox_threads(
                conn,
                workflow_status=(status_filter or "").strip() or None,
                search=(search or "").strip() or None,
                limit=max(1, min(limit, 500)),
            )
        finally:
            conn.close()
        return {"ok": True, "items": items}

    @router.get("/admin/followups")
    async def admin_followups(
        _: str = Depends(require_admin_dependency),
        status_filter: Optional[str] = Query(default=None, alias="status"),
        priority: Optional[str] = Query(default=None),
        search: Optional[str] = Query(default=None),
        radar_only: bool = False,
        limit: int = 200,
    ):
        normalized_status = (status_filter or "").strip() or None
        normalized_priority = (priority or "").strip().lower() or None
        conn = get_connection(db_path)
        try:
            items = list_followup_tasks(
                conn,
                status=normalized_status,
                search=(search or "").strip() or None,
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
            items = [item for item in items if is_radar_reason(item.get("reason"))]
        return {"ok": True, "items": items}

    @router.post("/admin/followups/run")
    async def admin_followups_run(
        payload: Optional[LeadRadarRunPayload] = None,
        admin_username: str = Depends(require_admin_dependency),
    ):
        if not settings.enable_lead_radar:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Lead radar is disabled. Set ENABLE_LEAD_RADAR=true.",
            )
        run_payload = payload or LeadRadarRunPayload()
        return await run_lead_radar_once(
            trigger=f"manual:{admin_username}",
            dry_run=run_payload.dry_run,
            limit_override=run_payload.limit,
        )

    @router.get("/admin/business/inbox")
    async def admin_business_inbox(_: str = Depends(require_admin_dependency), limit: int = 100):
        conn = get_connection(db_path)
        try:
            items = list_recent_business_threads(conn, limit=max(1, min(limit, 500)))
        finally:
            conn.close()
        return {"ok": True, "items": items}

    @router.get("/admin/business/inbox/thread")
    async def admin_business_inbox_thread(
        thread_key: str = Query(..., min_length=5),
        _: str = Depends(require_admin_dependency),
    ):
        normalized_thread_key = thread_key.strip()
        conn = get_connection(db_path)
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

    @router.get("/admin/inbox/{user_id}")
    async def admin_inbox_detail(user_id: int, _: str = Depends(require_admin_dependency)):
        conn = get_connection(db_path)
        try:
            detail = get_inbox_thread_detail(conn, user_id=user_id, limit_messages=500)
        finally:
            conn.close()
        if detail.get("user") is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"User {user_id} not found.")
        user_item = detail.get("user") if isinstance(detail.get("user"), dict) else {}
        crm_context = build_thread_crm_context(user_item)
        return {"ok": True, **detail, "crm_context": crm_context}

    @router.post("/admin/inbox/{user_id}/drafts")
    async def admin_inbox_create_draft(
        user_id: int,
        payload: ReplyDraftPayload,
        admin_username: str = Depends(require_admin_dependency),
    ):
        thread_id = thread_id_from_user_id(user_id)
        conn = get_connection(db_path)
        try:
            require_user_exists(conn, user_id)
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

    @router.patch("/admin/inbox/drafts/{draft_id}")
    async def admin_inbox_update_draft(
        draft_id: int,
        payload: ReplyDraftUpdatePayload,
        admin_username: str = Depends(require_admin_dependency),
    ):
        conn = get_connection(db_path)
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

    @router.post("/admin/inbox/drafts/{draft_id}/approve")
    async def admin_inbox_approve_draft(draft_id: int, admin_username: str = Depends(require_admin_dependency)):
        conn = get_connection(db_path)
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

    @router.post("/admin/inbox/drafts/{draft_id}/reject")
    async def admin_inbox_reject_draft(draft_id: int, admin_username: str = Depends(require_admin_dependency)):
        conn = get_connection(db_path)
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

    @router.post("/admin/inbox/drafts/{draft_id}/send")
    async def admin_inbox_send_draft(
        draft_id: int,
        payload: DraftSendPayload,
        admin_username: str = Depends(require_admin_dependency),
    ):
        conn = get_connection(db_path)
        try:
            result = await send_approved_draft(
                conn,
                draft_id=draft_id,
                actor=admin_username,
                manual_sent_message_id=(payload.sent_message_id or "").strip(),
                is_business_thread=lambda thread_id: parse_business_thread_key(thread_id) is not None,
                business_sender=lambda draft_item, actor_name: send_business_draft_and_log(
                    conn,
                    draft_item,
                    actor_name,
                ),
            )
        finally:
            conn.close()
        return {
            "ok": True,
            "draft": result.draft,
            "delivery": result.delivery,
            "already_sent": result.already_sent,
        }

    @router.post("/admin/inbox/{user_id}/outcome")
    async def admin_inbox_set_outcome(
        user_id: int,
        payload: ConversationOutcomePayload,
        admin_username: str = Depends(require_admin_dependency),
    ):
        thread_id = thread_id_from_user_id(user_id)
        conn = get_connection(db_path)
        try:
            require_user_exists(conn, user_id)
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

    @router.post("/admin/inbox/{user_id}/followups")
    async def admin_inbox_create_followup(
        user_id: int,
        payload: FollowupTaskPayload,
        admin_username: str = Depends(require_admin_dependency),
    ):
        thread_id = thread_id_from_user_id(user_id)
        conn = get_connection(db_path)
        try:
            require_user_exists(conn, user_id)
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

    @router.post("/admin/inbox/{user_id}/lead-score")
    async def admin_inbox_create_lead_score(
        user_id: int,
        payload: LeadScorePayload,
        admin_username: str = Depends(require_admin_dependency),
    ):
        thread_id = thread_id_from_user_id(user_id)
        conn = get_connection(db_path)
        try:
            require_user_exists(conn, user_id)
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

    @router.post("/admin/inbox/{user_id}/events")
    async def admin_inbox_log_event(
        user_id: int,
        payload: RevenueEventPayload,
        admin_username: str = Depends(require_admin_dependency),
    ):
        thread_id = thread_id_from_user_id(user_id)
        conn = get_connection(db_path)
        try:
            require_user_exists(conn, user_id)
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

    @router.get("/admin/ui/inbox", response_class=HTMLResponse)
    async def admin_inbox_ui(
        _: str = Depends(require_admin_dependency),
        status_filter: Optional[str] = Query(default=None, alias="status"),
        search: Optional[str] = Query(default=None),
        limit: int = 100,
    ):
        normalized_status_filter = (status_filter or "").strip() or None
        normalized_search = (search or "").strip() or None
        conn = get_connection(db_path)
        try:
            items = list_inbox_threads(
                conn,
                workflow_status=normalized_status_filter,
                search=normalized_search,
                limit=max(1, min(limit, 500)),
            )
        finally:
            conn.close()

        counters: Dict[str, int] = {}
        for item in items:
            key = str(item.get("workflow_status") or "new").strip().lower() or "new"
            counters[key] = counters.get(key, 0) + 1

        rows: list[str] = []
        for item in items:
            user_id = int(item["user_id"])
            workflow_status = str(item.get("workflow_status") or "new")
            status_raw = html.escape(str(item.get("status") or "new"))
            display_name = html.escape(format_thread_display_name(item))
            last_message = html.escape(str(item.get("last_message_at") or "-"))
            messages_count = int(item.get("messages_count") or 0)
            pending_followups = int(item.get("pending_followups") or 0)
            last_error = str(
                ((item.get("latest_draft") or {}) if isinstance(item.get("latest_draft"), dict) else {}).get("last_error")
                or ""
            ).strip()
            last_error_html = html.escape(last_error) if last_error else "-"
            rows.append(
                "<tr>"
                f"<td>{user_id}</td>"
                f"<td>{display_name}</td>"
                f"<td>{inbox_workflow_badge(workflow_status)}</td>"
                f"<td><span class='badge'>{status_raw}</span></td>"
                f"<td>{messages_count}</td>"
                f"<td>{pending_followups}</td>"
                f"<td>{last_message}</td>"
                f"<td>{last_error_html}</td>"
                f"<td><a href='/admin/ui/inbox/{user_id}'>Открыть тред</a></td>"
                "</tr>"
            )

        filter_search_value = html.escape(normalized_search or "")
        summary_blocks: list[str] = []
        for key in (
            "new",
            "needs_approval",
            "ready_to_send",
            "failed",
            "sending",
            "sent",
            "rejected",
            "manual_required",
        ):
            count = counters.get(key, 0)
            if count <= 0:
                continue
            summary_blocks.append(
                f"<span class='badge' style='margin-right:6px;'>{html.escape(inbox_workflow_status_label(key))}: {count}</span>"
            )

        body = (
            "<h1>Inbox</h1>"
            "<p class='muted'>Треды, драфты, follow-up и статусы продаж.</p>"
            "<div class='card'>"
            "<form method='get' action='/admin/ui/inbox'>"
            "<p><label>Статус (workflow): "
            "<select name='status'>"
            f"<option value='' {'selected' if not normalized_status_filter else ''}>Все</option>"
            f"<option value='new' {'selected' if normalized_status_filter == 'new' else ''}>Новый</option>"
            f"<option value='needs_approval' {'selected' if normalized_status_filter == 'needs_approval' else ''}>Нужен approve</option>"
            f"<option value='ready_to_send' {'selected' if normalized_status_filter == 'ready_to_send' else ''}>Готов к отправке</option>"
            f"<option value='failed' {'selected' if normalized_status_filter == 'failed' else ''}>Ошибка отправки</option>"
            f"<option value='sending' {'selected' if normalized_status_filter == 'sending' else ''}>Отправляется</option>"
            f"<option value='sent' {'selected' if normalized_status_filter == 'sent' else ''}>Отправлен</option>"
            f"<option value='rejected' {'selected' if normalized_status_filter == 'rejected' else ''}>Отклонён</option>"
            f"<option value='manual_required' {'selected' if normalized_status_filter == 'manual_required' else ''}>Нужен ручной шаг</option>"
            "</select></label></p>"
            f"<p><input name='search' placeholder='Поиск: id, username, имя, thread' value='{filter_search_value}' style='width:320px;' /></p>"
            f"<p><input type='number' name='limit' min='1' max='500' value='{int(limit)}' /></p>"
            "<p><button type='submit'>Применить фильтры</button></p>"
            "</form>"
            f"<p>{''.join(summary_blocks) if summary_blocks else '<span class=muted>Нет данных</span>'}</p>"
            "</div>"
            "<table>"
            "<thead><tr><th>User ID</th><th>Клиент</th><th>Workflow</th><th>Draft Status</th><th>Messages</th><th>Followups</th><th>Last Message</th><th>Last Error</th><th></th></tr></thead>"
            f"<tbody>{''.join(rows) if rows else '<tr><td colspan=9>Нет тредов</td></tr>'}</tbody>"
            "</table>"
        )
        return render_page("Inbox", body)

    @router.get("/admin/ui/followups", response_class=HTMLResponse)
    async def admin_followups_ui(
        _: str = Depends(require_admin_dependency),
        status_filter: Optional[str] = Query(default=None, alias="status"),
        priority: Optional[str] = Query(default=None),
        search: Optional[str] = Query(default=None),
        radar_only: bool = False,
        limit: int = 200,
    ):
        normalized_status = (status_filter or "").strip() or None
        normalized_priority = (priority or "").strip().lower() or None
        normalized_search = (search or "").strip() or None
        conn = get_connection(db_path)
        try:
            items = list_followup_tasks(
                conn,
                status=normalized_status,
                search=normalized_search,
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
            items = [item for item in items if is_radar_reason(item.get("reason"))]

        counters: Dict[str, int] = {}
        for item in items:
            key = str(item.get("priority") or "warm").strip().lower() or "warm"
            counters[key] = counters.get(key, 0) + 1

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
            "<form method='get' action='/admin/ui/followups'>"
            "<p><label>Status: "
            "<select name='status'>"
            f"<option value='' {'selected' if not normalized_status else ''}>Все</option>"
            f"<option value='pending' {'selected' if normalized_status == 'pending' else ''}>pending</option>"
            f"<option value='done' {'selected' if normalized_status == 'done' else ''}>done</option>"
            f"<option value='canceled' {'selected' if normalized_status == 'canceled' else ''}>canceled</option>"
            "</select></label></p>"
            "<p><label>Priority: "
            "<select name='priority'>"
            f"<option value='' {'selected' if not normalized_priority else ''}>Все</option>"
            f"<option value='hot' {'selected' if normalized_priority == 'hot' else ''}>hot</option>"
            f"<option value='warm' {'selected' if normalized_priority == 'warm' else ''}>warm</option>"
            f"<option value='cold' {'selected' if normalized_priority == 'cold' else ''}>cold</option>"
            "</select></label></p>"
            f"<p><label><input type='checkbox' name='radar_only' value='true' {'checked' if radar_only else ''}/> Только Lead Radar</label></p>"
            f"<p><input name='search' placeholder='Поиск: thread, причина, клиент' value='{html.escape(normalized_search or '')}' style='width:320px;' /></p>"
            f"<p><input type='number' name='limit' min='1' max='500' value='{int(limit)}' /></p>"
            "<p><button type='submit'>Применить фильтры</button></p>"
            "</form>"
            f"<p><span class='badge'>hot: {counters.get('hot', 0)}</span> "
            f"<span class='badge'>warm: {counters.get('warm', 0)}</span> "
            f"<span class='badge'>cold: {counters.get('cold', 0)}</span></p>"
            "</div>"
            "<div class='card'>"
            f"<b>Lead Radar:</b> enabled={settings.enable_lead_radar} | "
            f"interval={settings.lead_radar_interval_seconds}s | "
            f"no-reply={settings.lead_radar_no_reply_hours}h | "
            f"call-no-next={settings.lead_radar_call_no_next_step_hours}h | "
            f"stale-warm={settings.lead_radar_stale_warm_days}d | "
            f"cooldown={settings.lead_radar_thread_cooldown_hours}h | "
            f"daily-cap/thread={settings.lead_radar_daily_cap_per_thread}"
            "</div>"
            "<table>"
            "<thead><tr><th>ID</th><th>Thread</th><th>Priority</th><th>Status</th><th>Assigned</th><th>Created</th><th>Reason</th></tr></thead>"
            f"<tbody>{''.join(rows) if rows else '<tr><td colspan=7>Нет задач</td></tr>'}</tbody>"
            "</table>"
        )
        return render_page("Followups", body)

    @router.get("/admin/ui/business-inbox", response_class=HTMLResponse)
    async def admin_business_inbox_ui(_: str = Depends(require_admin_dependency), limit: int = 100):
        conn = get_connection(db_path)
        try:
            items = list_recent_business_threads(conn, limit=max(1, min(limit, 500)))
        finally:
            conn.close()

        rows: list[str] = []
        for item in items:
            thread_key = html.escape(str(item.get("thread_key") or ""))
            display_name = html.escape(format_thread_display_name(item))
            last_message = html.escape(str(item.get("last_message_at") or "-"))
            messages_count = int(item.get("messages_count") or 0)
            rows.append(
                "<tr>"
                f"<td>{thread_key}</td>"
                f"<td>{display_name}</td>"
                "<td><span class='badge'>active</span></td>"
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

    @router.get("/admin/ui/business-inbox/thread", response_class=HTMLResponse)
    async def admin_business_inbox_thread_ui(
        thread_key: str = Query(..., min_length=5),
        _: str = Depends(require_admin_dependency),
    ):
        normalized_thread_key = thread_key.strip()
        conn = get_connection(db_path)
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

    @router.get("/admin/ui/inbox/{user_id}", response_class=HTMLResponse)
    async def admin_inbox_thread_ui(user_id: int, _: str = Depends(require_admin_dependency)):
        conn = get_connection(db_path)
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
        crm_context = build_thread_crm_context(user_item)

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
            draft_status_raw = str(draft.get("status") or "created")
            status_value = html.escape(draft_status_raw)
            draft_text = html.escape(str(draft.get("draft_text") or ""))
            draft_last_error = str(draft.get("last_error") or "").strip()
            send_label = "Retry Send" if draft_status_raw == "approved" and draft_last_error else "Send"
            last_error_block = ""
            if draft_last_error:
                last_error_block = (
                    "<div class='card' style='background:#fef2f2;border-color:#fecaca;'>"
                    f"<b>Последняя ошибка отправки:</b><pre>{html.escape(draft_last_error)}</pre>"
                    "</div>"
                )
            draft_cards.append(
                "<div class='card'>"
                f"<b>Draft #{draft_id}</b> <span class='badge'>{status_value}</span><br/>"
                f"<small class='muted'>created_at={html.escape(str(draft.get('created_at') or '-'))}</small>"
                f"{last_error_block}"
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
                "<input name='sent_message_id' placeholder='sent_message_id (manual non-business)' style='margin-bottom:6px;' />"
                "<br/>"
                f"<button type='submit'>{send_label}</button>"
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

        customer_name = html.escape(format_thread_display_name(user_item))
        latest_draft = drafts[0] if drafts else None
        thread_workflow_status = "new"
        if isinstance(latest_draft, dict):
            latest_status_raw = str(latest_draft.get("status") or "").strip().lower()
            if latest_status_raw == "sent":
                thread_workflow_status = "sent"
            elif latest_status_raw == "sending":
                thread_workflow_status = "sending"
            elif latest_status_raw == "approved":
                thread_workflow_status = (
                    "failed" if str(latest_draft.get("last_error") or "").strip() else "ready_to_send"
                )
            elif latest_status_raw == "created":
                thread_workflow_status = "needs_approval"
            elif latest_status_raw == "rejected":
                thread_workflow_status = "rejected"
        elif followups:
            thread_workflow_status = "manual_required"

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

        crm_context_html = "<p class='muted'>CRM контекст недоступен.</p>"
        if isinstance(crm_context, dict):
            if bool(crm_context.get("enabled")):
                if bool(crm_context.get("found")):
                    tags = ", ".join(str(item) for item in (crm_context.get("tags") or [])) or "-"
                    interests = ", ".join(str(item) for item in (crm_context.get("interests") or [])) or "-"
                    crm_context_html = (
                        "<div class='card'>"
                        f"<b>Match:</b> yes<br/>"
                        f"<b>Lookup field:</b> {html.escape(str(crm_context.get('lookup_field') or '-'))}<br/>"
                        f"<b>Tags:</b> {html.escape(tags)}<br/>"
                        f"<b>Interests:</b> {html.escape(interests)}<br/>"
                        f"<b>Last touch (days):</b> {html.escape(str(crm_context.get('last_touch_days') or '-'))}"
                        "</div>"
                    )
                else:
                    crm_error = str(crm_context.get("error") or "").strip().lower()
                    if crm_error == "tallanto_read_only_disabled":
                        crm_context_html = (
                            "<div class='card'>"
                            "<b>CRM error:</b> Tallanto read-only mode отключен.<br/>"
                            "<span class='muted'>Включите TALLANTO_READ_ONLY=1 для безопасного enrichment.</span>"
                            "</div>"
                        )
                    elif crm_error == "tallanto_not_configured":
                        crm_context_html = (
                            "<div class='card'>"
                            "<b>CRM error:</b> Tallanto не настроен.<br/>"
                            "<span class='muted'>Заполните TALLANTO_API_URL и TALLANTO_API_TOKEN.</span>"
                            "</div>"
                        )
                    elif crm_error == "lookup_candidates_empty":
                        crm_context_html = (
                            "<div class='card'>"
                            "<b>CRM note:</b> Недостаточно данных для поиска контакта.<br/>"
                            "<span class='muted'>Нужен telegram_id/username или другой идентификатор.</span>"
                            "</div>"
                        )
                    else:
                        crm_context_html = (
                            "<div class='card'>"
                            "<b>Match:</b> no<br/>"
                            "<span class='muted'>Контакт не найден в CRM по доступным safe-полям.</span>"
                            "</div>"
                        )
            else:
                crm_context_html = (
                    "<div class='card'>"
                    "<span class='muted'>CRM enrichment выключен feature-flag или провайдером.</span>"
                    "</div>"
                )

        body = (
            f"<h1>Inbox Thread #{user_id}</h1>"
            f"<p class='muted'>Клиент: {customer_name}</p>"
            f"<p>{inbox_workflow_badge(thread_workflow_status)}</p>"
            "<p><a href='/admin/ui/inbox'>← Назад к списку</a></p>"
            "<h2>CRM Context (read-only)</h2>"
            f"{crm_context_html}"
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

    @router.post("/admin/ui/inbox/{user_id}/drafts")
    async def admin_inbox_create_draft_ui(
        request: Request,
        user_id: int,
        draft_text: str = Form(...),
        draft_model: str = Form("", alias="model_name"),
        admin_username: str = Depends(require_admin_dependency),
    ):
        enforce_ui_csrf(request)
        conn = get_connection(db_path)
        try:
            require_user_exists(conn, user_id)
            thread_id = thread_id_from_user_id(user_id)
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

    @router.post("/admin/ui/inbox/drafts/{draft_id}/edit")
    async def admin_inbox_edit_draft_ui(
        request: Request,
        draft_id: int,
        draft_text: str = Form(...),
        draft_model: str = Form("", alias="model_name"),
        admin_username: str = Depends(require_admin_dependency),
    ):
        enforce_ui_csrf(request)
        conn = get_connection(db_path)
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

    @router.post("/admin/ui/inbox/drafts/{draft_id}/approve")
    async def admin_inbox_approve_draft_ui(
        request: Request,
        draft_id: int,
        admin_username: str = Depends(require_admin_dependency),
    ):
        enforce_ui_csrf(request)
        conn = get_connection(db_path)
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

    @router.post("/admin/ui/inbox/drafts/{draft_id}/send")
    async def admin_inbox_send_draft_ui(
        request: Request,
        draft_id: int,
        sent_message_id: str = Form(""),
        admin_username: str = Depends(require_admin_dependency),
    ):
        enforce_ui_csrf(request)
        conn = get_connection(db_path)
        try:
            result = await send_approved_draft(
                conn,
                draft_id=draft_id,
                actor=admin_username,
                manual_sent_message_id=sent_message_id.strip(),
                is_business_thread=lambda thread_id: parse_business_thread_key(thread_id) is not None,
                business_sender=lambda draft_item, actor_name: send_business_draft_and_log(
                    conn,
                    draft_item,
                    actor_name,
                ),
            )
            user_id = int(result.draft["user_id"])
        finally:
            conn.close()
        return RedirectResponse(url=f"/admin/ui/inbox/{user_id}", status_code=status.HTTP_303_SEE_OTHER)

    @router.post("/admin/ui/inbox/drafts/{draft_id}/reject")
    async def admin_inbox_reject_draft_ui(
        request: Request,
        draft_id: int,
        admin_username: str = Depends(require_admin_dependency),
    ):
        enforce_ui_csrf(request)
        conn = get_connection(db_path)
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

    @router.post("/admin/ui/inbox/{user_id}/outcome")
    async def admin_inbox_set_outcome_ui(
        request: Request,
        user_id: int,
        outcome: str = Form(...),
        note: str = Form(""),
        admin_username: str = Depends(require_admin_dependency),
    ):
        enforce_ui_csrf(request)
        conn = get_connection(db_path)
        try:
            require_user_exists(conn, user_id)
            thread_id = thread_id_from_user_id(user_id)
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

    @router.post("/admin/ui/inbox/{user_id}/followups")
    async def admin_inbox_followup_ui(
        request: Request,
        user_id: int,
        priority: str = Form("warm"),
        reason: str = Form(...),
        due_at: str = Form(""),
        assigned_to: str = Form(""),
        admin_username: str = Depends(require_admin_dependency),
    ):
        enforce_ui_csrf(request)
        normalized_priority = priority.strip().lower()
        if normalized_priority not in {"hot", "warm", "cold"}:
            normalized_priority = "warm"
        conn = get_connection(db_path)
        try:
            require_user_exists(conn, user_id)
            thread_id = thread_id_from_user_id(user_id)
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

    @router.post("/admin/ui/inbox/{user_id}/lead-score")
    async def admin_inbox_lead_score_ui(
        request: Request,
        user_id: int,
        score: float = Form(...),
        temperature: str = Form("warm"),
        confidence: str = Form(""),
        admin_username: str = Depends(require_admin_dependency),
    ):
        enforce_ui_csrf(request)
        normalized_temperature = temperature.strip().lower()
        if normalized_temperature not in {"hot", "warm", "cold"}:
            normalized_temperature = "warm"
        confidence_value: Optional[float] = None
        if confidence.strip():
            try:
                confidence_value = float(confidence.strip())
            except ValueError:
                confidence_value = None
        conn = get_connection(db_path)
        try:
            require_user_exists(conn, user_id)
            thread_id = thread_id_from_user_id(user_id)
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

    return router

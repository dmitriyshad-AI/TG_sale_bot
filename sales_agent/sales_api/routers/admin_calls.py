from __future__ import annotations

import html
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, Request, UploadFile, status
from fastapi.responses import HTMLResponse, RedirectResponse

from sales_agent.sales_core.db import get_call_record, get_connection, list_call_records, list_mango_events


def build_admin_calls_router(
    *,
    db_path: Path,
    settings: Any,
    require_admin_dependency: Callable[..., str],
    enforce_ui_csrf: Callable[[Request], None],
    render_page: Callable[[str, str], HTMLResponse],
    process_manual_call_upload: Callable[..., Awaitable[Dict[str, Any]]],
    mango_ingest_enabled: Callable[[], bool],
    run_mango_poll_once: Callable[..., Awaitable[Dict[str, Any]]],
    run_mango_retry_failed_once: Callable[..., Awaitable[Dict[str, Any]]],
    run_call_retry_failed_once: Callable[..., Awaitable[Dict[str, Any]]],
    cleanup_old_call_files: Callable[[], Dict[str, Any]],
) -> APIRouter:
    router = APIRouter()

    @router.get("/admin/calls")
    async def admin_calls(
        _: str = Depends(require_admin_dependency),
        status_filter: Optional[str] = Query(default=None, alias="status"),
        limit: int = 100,
    ):
        if not settings.enable_call_copilot:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Call Copilot is disabled. Set ENABLE_CALL_COPILOT=true.",
            )
        conn = get_connection(db_path)
        try:
            items = list_call_records(
                conn,
                status=(status_filter or "").strip() or None,
                limit=max(1, min(limit, 500)),
            )
        finally:
            conn.close()
        return {"ok": True, "items": items}

    @router.get("/admin/calls/{call_id}")
    async def admin_call_detail(call_id: int, _: str = Depends(require_admin_dependency)):
        if not settings.enable_call_copilot:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Call Copilot is disabled. Set ENABLE_CALL_COPILOT=true.",
            )
        conn = get_connection(db_path)
        try:
            item = get_call_record(conn, call_id=call_id)
        finally:
            conn.close()
        if item is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Call {call_id} not found.")
        return {"ok": True, "item": item}

    @router.post("/admin/calls/upload")
    async def admin_calls_upload(
        _: str = Depends(require_admin_dependency),
        user_id: Optional[int] = Form(default=None),
        thread_id: Optional[str] = Form(default=None),
        recording_url: Optional[str] = Form(default=None),
        transcript_hint: Optional[str] = Form(default=None),
        audio_file: Optional[UploadFile] = File(default=None),
    ):
        if not settings.enable_call_copilot:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Call Copilot is disabled. Set ENABLE_CALL_COPILOT=true.",
            )
        return await process_manual_call_upload(
            user_id=user_id,
            thread_id=thread_id,
            recording_url=recording_url,
            transcript_hint=transcript_hint,
            audio_file=audio_file,
        )

    @router.post("/admin/calls/mango/poll")
    async def admin_calls_mango_poll(
        _: str = Depends(require_admin_dependency),
        limit: Optional[int] = Query(default=None, ge=1, le=500),
    ):
        if not mango_ingest_enabled():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Mango auto-ingest is disabled. Set ENABLE_MANGO_AUTO_INGEST=true and ENABLE_CALL_COPILOT=true.",
            )
        return await run_mango_poll_once(trigger="manual", limit_override=limit)

    @router.post("/admin/calls/mango/retry-failed")
    async def admin_calls_mango_retry_failed(
        _: str = Depends(require_admin_dependency),
        limit: Optional[int] = Query(default=None, ge=1, le=500),
    ):
        if not mango_ingest_enabled():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Mango auto-ingest is disabled. Set ENABLE_MANGO_AUTO_INGEST=true and ENABLE_CALL_COPILOT=true.",
        )
        return await run_mango_retry_failed_once(trigger="manual", limit_override=limit)

    @router.post("/admin/calls/retry-failed")
    async def admin_calls_retry_failed(
        _: str = Depends(require_admin_dependency),
        limit: Optional[int] = Query(default=None, ge=1, le=500),
    ):
        if not settings.enable_call_copilot:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Call Copilot is disabled. Set ENABLE_CALL_COPILOT=true.",
            )
        return await run_call_retry_failed_once(trigger="manual", limit_override=limit)

    @router.get("/admin/calls/mango/events")
    async def admin_calls_mango_events(
        _: str = Depends(require_admin_dependency),
        status_filter: Optional[str] = Query(default=None, alias="status"),
        limit: int = 100,
    ):
        conn = get_connection(db_path)
        try:
            items = list_mango_events(
                conn,
                status=(status_filter or "").strip() or None,
                limit=max(1, min(limit, 500)),
            )
        finally:
            conn.close()
        return {"ok": True, "items": items}

    @router.post("/admin/calls/cleanup")
    async def admin_calls_cleanup(
        _: str = Depends(require_admin_dependency),
    ):
        if not settings.enable_call_copilot:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Call Copilot is disabled. Set ENABLE_CALL_COPILOT=true.",
            )
        return cleanup_old_call_files()

    @router.get("/admin/ui/calls", response_class=HTMLResponse)
    async def admin_calls_ui(
        _: str = Depends(require_admin_dependency),
        status_filter: Optional[str] = Query(default=None, alias="status"),
        limit: int = 100,
    ):
        if not settings.enable_call_copilot:
            body = (
                "<h1>Calls</h1>"
                "<div class='card'>Call Copilot disabled. Set <code>ENABLE_CALL_COPILOT=true</code>.</div>"
            )
            return render_page("Calls", body)

        conn = get_connection(db_path)
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
            f"<b>Mango Auto-Ingest:</b> enabled={mango_ingest_enabled()} | "
            f"polling={settings.mango_polling_enabled} | interval={settings.mango_poll_interval_seconds}s | "
            f"ttl={settings.mango_call_recording_ttl_hours}h | "
            f"retries={settings.mango_poll_retry_attempts} (backoff={settings.mango_poll_retry_backoff_seconds}s)<br/>"
            "<form method='post' action='/admin/ui/calls/mango/poll'>"
            f"<p><label>Manual poll limit: <input type='number' name='limit' min='1' max='500' value='{settings.mango_poll_limit_per_run}' /></label></p>"
            "<p><button type='submit'>Запустить Mango poll сейчас</button></p>"
            "</form>"
            "<form method='post' action='/admin/ui/calls/mango/retry-failed'>"
            f"<p><label>Retry failed limit: <input type='number' name='limit' min='1' max='500' value='{settings.mango_retry_failed_limit_per_run}' /></label></p>"
            "<p><button type='submit'>Повторно обработать failed events</button></p>"
            "</form>"
            "<form method='post' action='/admin/ui/calls/retry-failed'>"
            "<p><label>Retry failed calls limit: <input type='number' name='limit' min='1' max='500' value='50' /></label></p>"
            "<p><button type='submit'>Повторно обработать failed call_records</button></p>"
            "</form>"
            "<form method='post' action='/admin/ui/calls/cleanup'>"
            "<p><button type='submit'>Очистить старые call-файлы сейчас</button></p>"
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

    @router.post("/admin/ui/calls/mango/poll")
    async def admin_calls_ui_mango_poll(
        request: Request,
        _: str = Depends(require_admin_dependency),
        limit: int = Form(50),
    ):
        enforce_ui_csrf(request)
        if not mango_ingest_enabled():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Mango auto-ingest is disabled. Set ENABLE_MANGO_AUTO_INGEST=true and ENABLE_CALL_COPILOT=true.",
            )
        await run_mango_poll_once(trigger="manual_ui", limit_override=limit)
        return RedirectResponse(url="/admin/ui/calls", status_code=status.HTTP_303_SEE_OTHER)

    @router.post("/admin/ui/calls/mango/retry-failed")
    async def admin_calls_ui_mango_retry_failed(
        request: Request,
        _: str = Depends(require_admin_dependency),
        limit: int = Form(25),
    ):
        enforce_ui_csrf(request)
        if not mango_ingest_enabled():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Mango auto-ingest is disabled. Set ENABLE_MANGO_AUTO_INGEST=true and ENABLE_CALL_COPILOT=true.",
            )
        await run_mango_retry_failed_once(trigger="manual_ui", limit_override=limit)
        return RedirectResponse(url="/admin/ui/calls", status_code=status.HTTP_303_SEE_OTHER)

    @router.post("/admin/ui/calls/cleanup")
    async def admin_calls_ui_cleanup(
        request: Request,
        _: str = Depends(require_admin_dependency),
    ):
        enforce_ui_csrf(request)
        if not settings.enable_call_copilot:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Call Copilot is disabled. Set ENABLE_CALL_COPILOT=true.",
            )
        cleanup_old_call_files()
        return RedirectResponse(url="/admin/ui/calls", status_code=status.HTTP_303_SEE_OTHER)

    @router.post("/admin/ui/calls/retry-failed")
    async def admin_calls_ui_retry_failed(
        request: Request,
        _: str = Depends(require_admin_dependency),
        limit: int = Form(50),
    ):
        enforce_ui_csrf(request)
        if not settings.enable_call_copilot:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Call Copilot is disabled. Set ENABLE_CALL_COPILOT=true.",
            )
        await run_call_retry_failed_once(trigger="manual_ui", limit_override=limit)
        return RedirectResponse(url="/admin/ui/calls", status_code=status.HTTP_303_SEE_OTHER)

    @router.post("/admin/ui/calls/upload")
    async def admin_calls_ui_upload(
        request: Request,
        _: str = Depends(require_admin_dependency),
        user_id: Optional[int] = Form(default=None),
        thread_id: Optional[str] = Form(default=None),
        recording_url: Optional[str] = Form(default=None),
        transcript_hint: Optional[str] = Form(default=None),
        audio_file: Optional[UploadFile] = File(default=None),
    ):
        enforce_ui_csrf(request)
        if not settings.enable_call_copilot:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Call Copilot is disabled. Set ENABLE_CALL_COPILOT=true.",
            )
        result = await process_manual_call_upload(
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

    @router.get("/admin/ui/calls/{call_id}", response_class=HTMLResponse)
    async def admin_call_detail_ui(call_id: int, _: str = Depends(require_admin_dependency)):
        if not settings.enable_call_copilot:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Call Copilot is disabled. Set ENABLE_CALL_COPILOT=true.",
            )
        conn = get_connection(db_path)
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

    return router

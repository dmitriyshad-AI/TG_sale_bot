from __future__ import annotations

import html
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field

from sales_agent.sales_core import faq_lab
from sales_agent.sales_core.db import (
    get_connection,
    list_answer_performance,
    list_canonical_answers,
    list_faq_candidates,
    list_faq_lab_events,
    list_faq_lab_runs,
    list_rejected_reply_drafts,
)


class FaqLabPromotePayload(BaseModel):
    answer_text: Optional[str] = Field(default=None, max_length=8000)


class FaqLabRunPayload(BaseModel):
    limit: Optional[int] = Field(default=None, ge=1, le=1000)


def build_faq_lab_router(
    *,
    db_path: Path,
    require_admin_dependency: Callable[..., str],
    enforce_ui_csrf: Callable[[Request], None],
    render_page: Callable[[str, str], HTMLResponse],
    enabled: bool,
    scheduler_enabled: bool,
    interval_seconds: int,
    window_days: int,
    min_question_count: int,
    default_limit: int,
) -> APIRouter:
    router = APIRouter()

    def _normalize_answer_text(value: Optional[str]) -> Optional[str]:
        normalized = " ".join((value or "").split()).strip()
        if not normalized:
            return None
        if len(normalized) < 20:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Canonical answer is too short. Use at least 20 characters.",
            )
        return normalized[:4000]

    def _candidate_priority_score(item: dict[str, Any]) -> float:
        question_count = float(item.get("question_count") or 0.0)
        next_step_rate = float(item.get("next_step_rate") or 0.0)
        approved_rate = float(item.get("reply_approved_rate") or 0.0)
        recency_bonus = 0.0
        last_seen_raw = str(item.get("last_seen_at") or "").strip()
        if last_seen_raw:
            try:
                parsed = datetime.fromisoformat(last_seen_raw.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                age_days = max(0.0, (now - parsed).total_seconds() / 86400.0)
                recency_bonus = max(0.0, 8.0 - min(age_days, 8.0))
            except Exception:
                recency_bonus = 0.0
        return round(question_count * 1.6 + next_step_rate * 120.0 + approved_rate * 70.0 + recency_bonus, 3)

    def _ensure_enabled() -> None:
        if not enabled:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="FAQ Lab is disabled. Set ENABLE_FAQ_LAB=true.",
            )

    def _snapshot(*, refresh: bool, limit: int, trigger: str) -> dict[str, Any]:
        conn = get_connection(db_path)
        try:
            latest_run = None
            current_candidates = list_faq_candidates(conn, limit=1)
            if refresh or not current_candidates:
                latest_run = faq_lab.refresh_faq_lab(
                    conn,
                    window_days=window_days,
                    min_question_count=min_question_count,
                    limit=limit,
                    trigger=trigger,
                )
            candidates = list_faq_candidates(conn, limit=limit)
            ranked_candidates: list[dict[str, Any]] = []
            for item in candidates:
                scored = dict(item)
                scored["priority_score"] = _candidate_priority_score(scored)
                ranked_candidates.append(scored)
            ranked_candidates.sort(
                key=lambda item: (
                    float(item.get("priority_score") or 0.0),
                    int(item.get("question_count") or 0),
                    float(item.get("next_step_rate") or 0.0),
                    str(item.get("last_seen_at") or ""),
                ),
                reverse=True,
            )
            canonical = list_canonical_answers(conn, limit=limit)
            top_performance = list_answer_performance(conn, status="active", limit=limit)
            rejected = list_rejected_reply_drafts(conn, limit=min(limit, 100))
            recent_runs = list_faq_lab_runs(conn, limit=min(100, max(10, limit)))
            recent_events = list_faq_lab_events(conn, limit=min(200, max(20, limit * 2)))
            promoted_events = [item for item in recent_events if str(item.get("event_type") or "") == "candidate_promoted"]

            metrics = {
                "question_count": sum(int(item.get("question_count") or 0) for item in candidates),
                "candidate_count": len(candidates),
                "canonical_count": len(canonical),
                "rejected_replies_count": len(rejected),
                "reply_approved_rate": round(
                    (
                        sum(float(item.get("reply_approved_rate") or 0.0) for item in candidates)
                        / max(1, len(candidates))
                    ),
                    4,
                ),
                "next_step_rate": round(
                    (
                        sum(float(item.get("next_step_rate") or 0.0) for item in candidates)
                        / max(1, len(candidates))
                    ),
                    4,
                ),
                "runs_total": len(recent_runs),
                "runs_failed": sum(1 for item in recent_runs if str(item.get("status") or "") == "failed"),
                "events_total": len(recent_events),
                "promoted_events": len(promoted_events),
            }
            return {
                "ok": True,
                "metrics": metrics,
                "settings": {
                    "enabled": enabled,
                    "scheduler_enabled": scheduler_enabled,
                    "interval_seconds": interval_seconds,
                    "window_days": window_days,
                    "min_question_count": min_question_count,
                    "default_limit": default_limit,
                },
                "latest_run": latest_run,
                "candidates": ranked_candidates,
                "canonical_answers": canonical,
                "top_performing_replies": top_performance,
                "rejected_replies": rejected,
                "runs": recent_runs,
                "events": recent_events,
            }
        finally:
            conn.close()

    @router.get("/admin/faq-lab")
    async def admin_faq_lab(
        _: str = Depends(require_admin_dependency),
        refresh: bool = False,
        limit: int = Query(default=50, ge=1, le=500),
    ):
        _ensure_enabled()
        return _snapshot(refresh=refresh, limit=limit, trigger="admin_api")

    @router.post("/admin/faq-lab/run")
    async def admin_faq_lab_run(
        payload: FaqLabRunPayload,
        _: str = Depends(require_admin_dependency),
    ):
        _ensure_enabled()
        effective_limit = int(payload.limit or default_limit)
        conn = get_connection(db_path)
        try:
            summary = faq_lab.refresh_faq_lab(
                conn,
                window_days=window_days,
                min_question_count=min_question_count,
                limit=effective_limit,
                trigger="manual_api",
            )
            return {"ok": True, "summary": summary}
        finally:
            conn.close()

    @router.post("/admin/faq-lab/candidates/{candidate_id}/promote")
    async def admin_faq_lab_promote_candidate(
        candidate_id: int,
        payload: FaqLabPromotePayload,
        actor: str = Depends(require_admin_dependency),
    ):
        _ensure_enabled()
        conn = get_connection(db_path)
        try:
            try:
                promoted = faq_lab.promote_candidate_to_canonical_safe(
                    conn,
                    candidate_id=candidate_id,
                    answer_text=_normalize_answer_text(payload.answer_text),
                    created_by=actor,
                )
            except faq_lab.FaqLabPromotionError as exc:
                code = str(getattr(exc, "code", "") or "").strip().lower()
                if code == "not_found":
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
                if code == "archived":
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
                if code == "answer_too_short":
                    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
            return {"ok": True, **promoted}
        finally:
            conn.close()

    @router.get("/admin/ui/faq-lab", response_class=HTMLResponse)
    async def admin_faq_lab_ui(
        _: str = Depends(require_admin_dependency),
        refresh: bool = False,
        limit: int = Query(default=25, ge=1, le=100),
    ):
        _ensure_enabled()
        payload = _snapshot(refresh=refresh, limit=limit, trigger="admin_ui")

        settings = payload.get("settings") if isinstance(payload.get("settings"), dict) else {}
        metrics = payload.get("metrics") if isinstance(payload.get("metrics"), dict) else {}
        run_info = payload.get("latest_run") if isinstance(payload.get("latest_run"), dict) else None

        candidate_rows: list[str] = []
        for item in payload.get("candidates") or []:
            candidate_id = int(item.get("id") or 0)
            if candidate_id <= 0:
                continue
            candidate_rows.append(
                "<tr>"
                f"<td>{candidate_id}</td>"
                f"<td>{html.escape(str(item.get('question_text') or ''))}</td>"
                f"<td>{int(item.get('question_count') or 0)}</td>"
                f"<td>{int(item.get('thread_count') or 0)}</td>"
                f"<td>{float(item.get('reply_approved_rate') or 0.0):.2f}</td>"
                f"<td>{float(item.get('next_step_rate') or 0.0):.2f}</td>"
                f"<td>{float(item.get('priority_score') or 0.0):.1f}</td>"
                f"<td>{html.escape(str(item.get('status') or 'new'))}</td>"
                "<td>"
                f"<form method='post' action='/admin/ui/faq-lab/candidates/{candidate_id}/promote'>"
                "<input type='text' name='answer_text' placeholder='Краткий canonical answer' style='width:280px'>"
                "<button type='submit'>Promote</button>"
                "</form>"
                "</td>"
                "</tr>"
            )

        top_rows: list[str] = []
        for item in payload.get("top_performing_replies") or []:
            top_rows.append(
                "<tr>"
                f"<td>{html.escape(str(item.get('answer_kind') or ''))}</td>"
                f"<td>{html.escape(str(item.get('question_text') or ''))}</td>"
                f"<td>{int(item.get('question_count') or 0)}</td>"
                f"<td>{float(item.get('reply_approved_rate') or 0.0):.2f}</td>"
                f"<td>{float(item.get('next_step_rate') or 0.0):.2f}</td>"
                "</tr>"
            )

        rejected_rows: list[str] = []
        for item in payload.get("rejected_replies") or []:
            rejected_rows.append(
                "<tr>"
                f"<td>{int(item.get('id') or 0)}</td>"
                f"<td>{html.escape(str(item.get('thread_id') or ''))}</td>"
                f"<td>{html.escape(str(item.get('draft_text') or '')[:220])}</td>"
                f"<td>{html.escape(str(item.get('updated_at') or ''))}</td>"
                "</tr>"
            )

        run_rows: list[str] = []
        for item in payload.get("runs") or []:
            run_rows.append(
                "<tr>"
                f"<td>{int(item.get('id') or 0)}</td>"
                f"<td>{html.escape(str(item.get('trigger') or ''))}</td>"
                f"<td>{html.escape(str(item.get('status') or ''))}</td>"
                f"<td>{html.escape(str(item.get('started_at') or ''))}</td>"
                f"<td>{html.escape(str(item.get('finished_at') or ''))}</td>"
                f"<td>{html.escape(str(item.get('error_text') or '-'))}</td>"
                "</tr>"
            )

        event_rows: list[str] = []
        for item in payload.get("events") or []:
            event_rows.append(
                "<tr>"
                f"<td>{int(item.get('id') or 0)}</td>"
                f"<td>{html.escape(str(item.get('event_type') or ''))}</td>"
                f"<td>{html.escape(str(item.get('actor') or '-'))}</td>"
                f"<td>{html.escape(str(item.get('question_key') or '-'))}</td>"
                f"<td>{html.escape(str(item.get('created_at') or ''))}</td>"
                "</tr>"
            )

        run_text = "" if run_info is None else html.escape(str(run_info))
        body = (
            "<h1>FAQ Lab</h1>"
            "<p class='muted'>Кластеризация вопросов, кандидаты canonical answers и качество ответов.</p>"
            "<div class='card'>"
            f"<b>ENABLE_FAQ_LAB:</b> {settings.get('enabled')}<br/>"
            f"<b>FAQ_LAB_SCHEDULER_ENABLED:</b> {settings.get('scheduler_enabled')}<br/>"
            f"<b>FAQ_LAB_INTERVAL_SECONDS:</b> {settings.get('interval_seconds')}<br/>"
            f"<b>FAQ_LAB_WINDOW_DAYS:</b> {settings.get('window_days')}<br/>"
            f"<b>FAQ_LAB_MIN_QUESTION_COUNT:</b> {settings.get('min_question_count')}<br/>"
            f"<b>Question Count:</b> {int(metrics.get('question_count') or 0)}<br/>"
            f"<b>Candidates:</b> {int(metrics.get('candidate_count') or 0)}<br/>"
            f"<b>Canonical Answers:</b> {int(metrics.get('canonical_count') or 0)}<br/>"
            f"<b>Reply Approved Rate:</b> {float(metrics.get('reply_approved_rate') or 0.0):.2f}<br/>"
            f"<b>Next Step Rate:</b> {float(metrics.get('next_step_rate') or 0.0):.2f}<br/>"
            f"<b>Runs Failed:</b> {int(metrics.get('runs_failed') or 0)}<br/>"
            f"<b>Promoted Events:</b> {int(metrics.get('promoted_events') or 0)}"
            "</div>"
            "<div class='card'>"
            "<form method='get' action='/admin/ui/faq-lab'>"
            "<label><input type='checkbox' name='refresh' value='true'> Refresh now</label>"
            f"<input type='number' name='limit' min='1' max='100' value='{int(limit)}'>"
            "<button type='submit'>Reload</button>"
            "</form>"
            "<form method='post' action='/admin/ui/faq-lab/run'>"
            f"<input type='number' name='limit' min='1' max='1000' value='{int(settings.get('default_limit') or 100)}'>"
            "<button type='submit'>Run FAQ refresh</button>"
            "</form>"
            f"<pre>{run_text or 'No manual run in this request.'}</pre>"
            "</div>"
            "<h2>Top New Questions</h2>"
            "<table><thead><tr><th>ID</th><th>Question</th><th>Count</th><th>Threads</th><th>Approved Rate</th><th>Next Step Rate</th><th>Priority</th><th>Status</th><th>Promote</th></tr></thead>"
            f"<tbody>{''.join(candidate_rows) if candidate_rows else '<tr><td colspan=9>Нет кандидатов</td></tr>'}</tbody></table>"
            "<h2>Top Performing Replies</h2>"
            "<table><thead><tr><th>Kind</th><th>Question</th><th>Count</th><th>Approved Rate</th><th>Next Step Rate</th></tr></thead>"
            f"<tbody>{''.join(top_rows) if top_rows else '<tr><td colspan=5>Нет данных</td></tr>'}</tbody></table>"
            "<h2>Rejected Replies</h2>"
            "<table><thead><tr><th>ID</th><th>Thread</th><th>Draft</th><th>Updated</th></tr></thead>"
            f"<tbody>{''.join(rejected_rows) if rejected_rows else '<tr><td colspan=4>Нет отклоненных ответов</td></tr>'}</tbody></table>"
            "<h2>Pipeline Runs</h2>"
            "<table><thead><tr><th>ID</th><th>Trigger</th><th>Status</th><th>Started</th><th>Finished</th><th>Error</th></tr></thead>"
            f"<tbody>{''.join(run_rows) if run_rows else '<tr><td colspan=6>Нет запусков</td></tr>'}</tbody></table>"
            "<h2>Audit Events</h2>"
            "<table><thead><tr><th>ID</th><th>Type</th><th>Actor</th><th>Question Key</th><th>Created</th></tr></thead>"
            f"<tbody>{''.join(event_rows) if event_rows else '<tr><td colspan=5>Нет событий</td></tr>'}</tbody></table>"
        )
        return render_page("FAQ Lab", body)

    @router.post("/admin/ui/faq-lab/run")
    async def admin_faq_lab_ui_run(
        request: Request,
        _: str = Depends(require_admin_dependency),
        limit: int = Form(default=100),
    ):
        _ensure_enabled()
        enforce_ui_csrf(request)
        conn = get_connection(db_path)
        try:
            faq_lab.refresh_faq_lab(
                conn,
                window_days=window_days,
                min_question_count=min_question_count,
                limit=max(1, min(limit, 1000)),
                trigger="manual_ui",
            )
        finally:
            conn.close()
        return RedirectResponse(url="/admin/ui/faq-lab?refresh=true", status_code=status.HTTP_303_SEE_OTHER)

    @router.post("/admin/ui/faq-lab/candidates/{candidate_id}/promote")
    async def admin_faq_lab_ui_promote(
        request: Request,
        candidate_id: int,
        _: str = Depends(require_admin_dependency),
        answer_text: str = Form(default=""),
    ):
        _ensure_enabled()
        enforce_ui_csrf(request)
        conn = get_connection(db_path)
        try:
            try:
                faq_lab.promote_candidate_to_canonical_safe(
                    conn,
                    candidate_id=candidate_id,
                    answer_text=_normalize_answer_text(answer_text),
                    created_by="admin_ui",
                )
            except faq_lab.FaqLabPromotionError as exc:
                code = str(getattr(exc, "code", "") or "").strip().lower()
                if code == "not_found":
                    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
                if code == "archived":
                    raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
                if code == "answer_too_short":
                    raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
        finally:
            conn.close()
        return RedirectResponse(url="/admin/ui/faq-lab?refresh=true", status_code=status.HTTP_303_SEE_OTHER)

    return router

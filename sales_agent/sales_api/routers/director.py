from __future__ import annotations

import html
from pathlib import Path
from typing import Any, Callable, Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field

from sales_agent.sales_core import director_agent
from sales_agent.sales_core.db import (
    create_campaign_goal,
    create_campaign_plan,
    get_campaign_goal,
    get_campaign_plan,
    get_connection,
    list_campaign_actions,
    list_campaign_goals,
    list_campaign_plans,
    list_campaign_reports,
    update_campaign_goal_status,
    update_campaign_plan_status,
)


class DirectorPlanPayload(BaseModel):
    goal_text: str = Field(min_length=8, max_length=4000)
    max_actions: int = Field(default=20, ge=1, le=200)


class DirectorApplyPayload(BaseModel):
    max_actions: Optional[int] = Field(default=None, ge=1, le=200)


DIRECTOR_TEMPLATES: list[dict[str, Any]] = [
    {
        "id": "reactivate_oge_informatics",
        "title": "Реактивация: ОГЭ информатика",
        "goal_text": "Верни 10 теплых лидов по ОГЭ информатика и подготовь очередь follow-up на неделю.",
        "max_actions": 20,
    },
    {
        "id": "reactivate_ege_math",
        "title": "Реактивация: ЕГЭ математика",
        "goal_text": "Верни 10 теплых лидов по ЕГЭ математика и подготовь персональные follow-up.",
        "max_actions": 20,
    },
    {
        "id": "calls_no_next_step",
        "title": "После звонков: нет next step",
        "goal_text": "Собери клиентов после звонков без next step и подготовь приоритетные follow-up.",
        "max_actions": 25,
    },
]


def build_director_router(
    *,
    db_path: Path,
    require_admin_dependency: Callable[..., str],
    enforce_ui_csrf: Callable[[Request], None],
    render_page: Callable[[str, str], HTMLResponse],
    enabled: bool,
) -> APIRouter:
    router = APIRouter()

    def _ensure_enabled() -> None:
        if not enabled:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Director Agent is disabled. Set ENABLE_DIRECTOR_AGENT=true.",
            )

    def _build_plan(conn: Any, *, goal_text: str, max_actions: int, actor: str) -> dict[str, Any]:
        candidates = director_agent.discover_thread_candidates(
            conn,
            goal_text=goal_text,
            max_candidates=max(50, int(max_actions) * 5),
            lookback_days=120,
        )
        plan = director_agent.build_campaign_plan(
            goal_text=goal_text,
            candidates=candidates,
            max_actions=max_actions,
        )

        goal_id = create_campaign_goal(
            conn,
            goal_text=goal_text,
            created_by=actor,
            status="draft",
        )
        plan_id = create_campaign_plan(
            conn,
            goal_id=goal_id,
            objective=str(plan.get("objective") or goal_text),
            assumptions=plan.get("assumptions") if isinstance(plan.get("assumptions"), list) else [],
            target_segment=plan.get("target_segment") if isinstance(plan.get("target_segment"), dict) else {},
            success_metric=str(plan.get("success_metric") or ""),
            actions=plan.get("actions") if isinstance(plan.get("actions"), list) else [],
            status="draft",
            approvals_required=1 if bool(plan.get("approvals_required")) else 0,
            created_by=actor,
        )

        return {
            "goal_id": goal_id,
            "plan_id": plan_id,
            "plan": get_campaign_plan(conn, plan_id=plan_id),
            "goal": get_campaign_goal(conn, goal_id=goal_id),
            "candidates_found": len(candidates),
        }

    def _resolve_template(template_id: str) -> Optional[dict[str, Any]]:
        normalized = (template_id or "").strip()
        if not normalized:
            return None
        for item in DIRECTOR_TEMPLATES:
            if str(item.get("id") or "").strip() == normalized:
                return item
        return None

    @router.get("/admin/director")
    async def admin_director_overview(
        _: str = Depends(require_admin_dependency),
        limit: int = Query(default=50, ge=1, le=500),
    ):
        _ensure_enabled()
        conn = get_connection(db_path)
        try:
            goals = list_campaign_goals(conn, limit=limit)
            plans = list_campaign_plans(conn, limit=limit)
            actions = list_campaign_actions(conn, limit=limit)
            reports = list_campaign_reports(conn, limit=limit)
        finally:
            conn.close()

        return {
            "ok": True,
            "goals": goals,
            "plans": plans,
            "actions": actions,
            "reports": reports,
        }

    @router.post("/admin/director/plan")
    async def admin_director_create_plan(
        payload: DirectorPlanPayload,
        actor: str = Depends(require_admin_dependency),
    ):
        _ensure_enabled()
        conn = get_connection(db_path)
        try:
            created = _build_plan(
                conn,
                goal_text=payload.goal_text,
                max_actions=payload.max_actions,
                actor=actor,
            )
        finally:
            conn.close()
        return {"ok": True, **created}

    @router.get("/admin/director/templates")
    async def admin_director_templates(_: str = Depends(require_admin_dependency)):
        _ensure_enabled()
        return {"ok": True, "items": DIRECTOR_TEMPLATES}

    @router.post("/admin/director/plans/{plan_id}/approve")
    async def admin_director_approve_plan(
        plan_id: int,
        actor: str = Depends(require_admin_dependency),
    ):
        _ensure_enabled()
        conn = get_connection(db_path)
        try:
            plan = get_campaign_plan(conn, plan_id=plan_id)
            if not plan:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Campaign plan not found.")
            updated_plan = update_campaign_plan_status(conn, plan_id=plan_id, status="approved", actor=actor)
            updated_goal = update_campaign_goal_status(conn, goal_id=int(plan["goal_id"]), status="approved", actor=actor)
            if not updated_plan or not updated_goal:
                raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unable to approve plan.")
            return {
                "ok": True,
                "goal": get_campaign_goal(conn, goal_id=int(plan["goal_id"])),
                "plan": get_campaign_plan(conn, plan_id=plan_id),
            }
        finally:
            conn.close()

    @router.post("/admin/director/plans/{plan_id}/apply")
    async def admin_director_apply_plan(
        plan_id: int,
        payload: DirectorApplyPayload,
        actor: str = Depends(require_admin_dependency),
    ):
        _ensure_enabled()
        conn = get_connection(db_path)
        try:
            plan = get_campaign_plan(conn, plan_id=plan_id)
            if not plan:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Campaign plan not found.")
            if str(plan.get("status") or "").lower() != "approved":
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Campaign plan must be approved before apply.",
                )

            actions = plan.get("actions") if isinstance(plan.get("actions"), list) else []
            max_actions = int(payload.max_actions or len(actions) or 1)
            try:
                validated = director_agent.validate_plan_for_apply(
                    {
                        **plan,
                        "actions": actions[:max_actions],
                    },
                    max_actions=max_actions,
                )
            except director_agent.DirectorPlanValidationError as exc:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail={
                        "message": "Campaign plan validation failed.",
                        "errors": exc.errors,
                        "warnings": exc.warnings,
                    },
                ) from exc
            plan_for_apply = validated["plan"]

            report = director_agent.apply_campaign_plan(
                conn,
                goal_id=int(plan["goal_id"]),
                plan_id=plan_id,
                plan=plan_for_apply,
                actor=actor,
            )
            update_campaign_plan_status(conn, plan_id=plan_id, status="applied", actor=actor)
            update_campaign_goal_status(conn, goal_id=int(plan["goal_id"]), status="applied", actor=actor)

            return {
                "ok": True,
                "report": report,
                "validation_warnings": validated["warnings"],
                "goal": get_campaign_goal(conn, goal_id=int(plan["goal_id"])),
                "plan": get_campaign_plan(conn, plan_id=plan_id),
                "actions": list_campaign_actions(conn, plan_id=plan_id, limit=500),
            }
        finally:
            conn.close()

    @router.get("/admin/director/goals/{goal_id}")
    async def admin_director_goal_detail(
        goal_id: int,
        _: str = Depends(require_admin_dependency),
    ):
        _ensure_enabled()
        conn = get_connection(db_path)
        try:
            goal = get_campaign_goal(conn, goal_id=goal_id)
            if not goal:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Campaign goal not found.")
            return {
                "ok": True,
                "goal": goal,
                "plans": list_campaign_plans(conn, goal_id=goal_id, limit=200),
                "actions": list_campaign_actions(conn, goal_id=goal_id, limit=500),
                "reports": list_campaign_reports(conn, goal_id=goal_id, limit=200),
            }
        finally:
            conn.close()

    @router.get("/admin/director/plans/{plan_id}")
    async def admin_director_plan_detail(
        plan_id: int,
        _: str = Depends(require_admin_dependency),
    ):
        _ensure_enabled()
        conn = get_connection(db_path)
        try:
            plan = get_campaign_plan(conn, plan_id=plan_id)
            if not plan:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Campaign plan not found.")
            return {
                "ok": True,
                "plan": plan,
                "goal": get_campaign_goal(conn, goal_id=int(plan["goal_id"])),
                "actions": list_campaign_actions(conn, plan_id=plan_id, limit=500),
                "reports": list_campaign_reports(conn, plan_id=plan_id, limit=200),
            }
        finally:
            conn.close()

    @router.get("/admin/ui/director", response_class=HTMLResponse)
    async def admin_director_ui(
        _: str = Depends(require_admin_dependency),
        limit: int = Query(default=50, ge=1, le=200),
    ):
        _ensure_enabled()
        conn = get_connection(db_path)
        try:
            goals = list_campaign_goals(conn, limit=limit)
            plans = list_campaign_plans(conn, limit=limit)
            actions = list_campaign_actions(conn, limit=limit)
            reports = list_campaign_reports(conn, limit=limit)
        finally:
            conn.close()

        goal_rows = []
        for item in goals:
            goal_rows.append(
                "<tr>"
                f"<td>{int(item.get('id') or 0)}</td>"
                f"<td>{html.escape(str(item.get('goal_text') or ''))}</td>"
                f"<td>{html.escape(str(item.get('status') or 'draft'))}</td>"
                f"<td>{html.escape(str(item.get('created_at') or ''))}</td>"
                f"<td><a href='/admin/director/goals/{int(item.get('id') or 0)}'>JSON</a></td>"
                "</tr>"
            )

        plan_rows = []
        for item in plans:
            plan_id = int(item.get("id") or 0)
            plan_rows.append(
                "<tr>"
                f"<td>{plan_id}</td>"
                f"<td>{int(item.get('goal_id') or 0)}</td>"
                f"<td>{html.escape(str(item.get('objective') or ''))}</td>"
                f"<td>{html.escape(str(item.get('status') or 'draft'))}</td>"
                f"<td>{len(item.get('actions') or [])}</td>"
                "<td>"
                f"<form method='post' action='/admin/ui/director/plans/{plan_id}/approve' style='display:inline-block'><button type='submit'>Approve</button></form> "
                f"<form method='post' action='/admin/ui/director/plans/{plan_id}/apply' style='display:inline-block'><button type='submit'>Apply</button></form> "
                f"<a href='/admin/director/plans/{plan_id}'>JSON</a>"
                "</td>"
                "</tr>"
            )

        action_rows = []
        for item in actions:
            action_rows.append(
                "<tr>"
                f"<td>{int(item.get('id') or 0)}</td>"
                f"<td>{int(item.get('plan_id') or 0)}</td>"
                f"<td>{html.escape(str(item.get('action_type') or ''))}</td>"
                f"<td>{html.escape(str(item.get('status') or ''))}</td>"
                f"<td>{html.escape(str(item.get('thread_id') or '-'))}</td>"
                f"<td>{html.escape(str(item.get('reason') or '-'))}</td>"
                "</tr>"
            )

        body = (
            "<h1>Director Agent</h1>"
            "<p class='muted'>Goal -> Campaign Plan -> Approve -> Apply (без автоотправки клиенту)</p>"
            "<div class='card'>"
            "<h3>Быстрые шаблоны кампаний</h3>"
            + "".join(
                (
                    "<form method='post' action='/admin/ui/director/plan' style='margin:0 0 8px 0;'>"
                    f"<input type='hidden' name='template_id' value='{html.escape(str(item.get('id') or ''))}'>"
                    "<input type='hidden' name='goal_text' value=''>"
                    "<input type='hidden' name='max_actions' value='0'>"
                    f"<button type='submit'>{html.escape(str(item.get('title') or 'Template'))}</button>"
                    "</form>"
                )
                for item in DIRECTOR_TEMPLATES
            )
            + "</div>"
            "<div class='card'>"
            "<form method='post' action='/admin/ui/director/plan'>"
            "<input type='hidden' name='template_id' value=''>"
            "<p><label>Goal</label><br/><textarea name='goal_text' rows='4' style='width:100%' placeholder='Верни 10 теплых лидов по ОГЭ информатика'></textarea></p>"
            "<p><label>Max actions <input type='number' name='max_actions' min='1' max='200' value='20'></label></p>"
            "<p><button type='submit'>Build Plan</button></p>"
            "</form>"
            "</div>"
            "<h2>Campaign Goals</h2>"
            "<table><thead><tr><th>ID</th><th>Goal</th><th>Status</th><th>Created</th><th></th></tr></thead>"
            f"<tbody>{''.join(goal_rows) if goal_rows else '<tr><td colspan=5>Нет goals</td></tr>'}</tbody></table>"
            "<h2>Campaign Plans</h2>"
            "<table><thead><tr><th>ID</th><th>Goal ID</th><th>Objective</th><th>Status</th><th>Actions</th><th></th></tr></thead>"
            f"<tbody>{''.join(plan_rows) if plan_rows else '<tr><td colspan=6>Нет plans</td></tr>'}</tbody></table>"
            "<h2>Campaign Actions</h2>"
            "<table><thead><tr><th>ID</th><th>Plan ID</th><th>Type</th><th>Status</th><th>Thread</th><th>Reason</th></tr></thead>"
            f"<tbody>{''.join(action_rows) if action_rows else '<tr><td colspan=6>Нет actions</td></tr>'}</tbody></table>"
            f"<h2>Reports</h2><pre>{html.escape(str(reports))}</pre>"
        )
        return render_page("Director Agent", body)

    @router.post("/admin/ui/director/plan")
    async def admin_director_ui_create_plan(
        request: Request,
        actor: str = Depends(require_admin_dependency),
        goal_text: str = Form(...),
        max_actions: int = Form(default=20),
        template_id: str = Form(default=""),
    ):
        _ensure_enabled()
        enforce_ui_csrf(request)
        template = _resolve_template(template_id)
        resolved_goal_text = goal_text
        resolved_max_actions = max_actions
        if template is not None:
            resolved_goal_text = str(template.get("goal_text") or resolved_goal_text)
            resolved_max_actions = int(template.get("max_actions") or resolved_max_actions)
        if not str(resolved_goal_text or "").strip():
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="goal_text is required.",
            )
        conn = get_connection(db_path)
        try:
            _build_plan(
                conn,
                goal_text=resolved_goal_text,
                max_actions=max(1, min(resolved_max_actions, 200)),
                actor=actor,
            )
        finally:
            conn.close()
        return RedirectResponse(url="/admin/ui/director", status_code=status.HTTP_303_SEE_OTHER)

    @router.post("/admin/ui/director/plans/{plan_id}/approve")
    async def admin_director_ui_approve_plan(
        request: Request,
        plan_id: int,
        actor: str = Depends(require_admin_dependency),
    ):
        _ensure_enabled()
        enforce_ui_csrf(request)
        conn = get_connection(db_path)
        try:
            plan = get_campaign_plan(conn, plan_id=plan_id)
            if not plan:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Campaign plan not found.")
            update_campaign_plan_status(conn, plan_id=plan_id, status="approved", actor=actor)
            update_campaign_goal_status(conn, goal_id=int(plan["goal_id"]), status="approved", actor=actor)
        finally:
            conn.close()
        return RedirectResponse(url="/admin/ui/director", status_code=status.HTTP_303_SEE_OTHER)

    @router.post("/admin/ui/director/plans/{plan_id}/apply")
    async def admin_director_ui_apply_plan(
        request: Request,
        plan_id: int,
        actor: str = Depends(require_admin_dependency),
    ):
        _ensure_enabled()
        enforce_ui_csrf(request)
        conn = get_connection(db_path)
        try:
            plan = get_campaign_plan(conn, plan_id=plan_id)
            if not plan:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Campaign plan not found.")
            if str(plan.get("status") or "").lower() != "approved":
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Plan must be approved before apply.")
            try:
                validated = director_agent.validate_plan_for_apply(plan, max_actions=200)
            except director_agent.DirectorPlanValidationError as exc:
                raise HTTPException(
                    status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                    detail=f"Campaign plan validation failed: {'; '.join(exc.errors)}",
                ) from exc
            director_agent.apply_campaign_plan(
                conn,
                goal_id=int(plan["goal_id"]),
                plan_id=plan_id,
                plan=validated["plan"],
                actor=actor,
            )
            update_campaign_plan_status(conn, plan_id=plan_id, status="applied", actor=actor)
            update_campaign_goal_status(conn, goal_id=int(plan["goal_id"]), status="applied", actor=actor)
        finally:
            conn.close()
        return RedirectResponse(url="/admin/ui/director", status_code=status.HTTP_303_SEE_OTHER)

    return router

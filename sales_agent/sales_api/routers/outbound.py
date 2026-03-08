from __future__ import annotations

import html
from pathlib import Path
from typing import Any, Callable, Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field

from sales_agent.sales_core import db
from sales_agent.sales_core.outbound_copilot import (
    build_outbound_proposal,
    evaluate_outbound_proposal_guard,
    parse_outbound_companies_csv,
    score_company_fit,
)


class OutboundCompanyCreatePayload(BaseModel):
    company_name: str = Field(min_length=1, max_length=180)
    website: str = Field(default="", max_length=180)
    city: str = Field(default="", max_length=120)
    segment: str = Field(default="", max_length=140)
    source: str = Field(default="manual", max_length=64)
    owner: str = Field(default="", max_length=120)
    note: str = Field(default="", max_length=1000)


class OutboundStatusPayload(BaseModel):
    status: str = Field(min_length=1, max_length=32)
    actor: str = Field(default="admin:ui", max_length=120)


class OutboundScorePayload(BaseModel):
    campaign_tags: list[str] = Field(default_factory=list)


class OutboundProposalPayload(BaseModel):
    offer_focus: str = Field(default="подготовка к ОГЭ/ЕГЭ, олимпиадам и профильным сменам", max_length=220)
    created_by: str = Field(default="admin:ui", max_length=120)


class OutboundProposalApprovePayload(BaseModel):
    actor: str = Field(default="admin:ui", max_length=120)


class OutboundImportResult(BaseModel):
    imported: int
    skipped: int
    company_ids: list[int]


_OUTBOUND_HELP_TEXT = (
    "B2B Outbound Copilot: импорт компаний, оценка fit, драфты сообщений и КП. "
    "Авторассылки не выполняются: только подготовка и ручные действия менеджера."
)
OUTBOUND_MAX_OPEN_PROPOSALS = 1
OUTBOUND_MAX_RECENT_TOUCHES = 2
OUTBOUND_TOUCH_WINDOW_HOURS = 24


def build_outbound_router(
    *,
    db_path: Path,
    require_admin_dependency: Callable[..., str],
    enforce_ui_csrf: Callable[[Request], None],
    render_page: Callable[[str, str], HTMLResponse],
    enabled: bool,
) -> APIRouter:
    router = APIRouter()

    def _require_enabled() -> None:
        if not enabled:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Outbound Copilot is disabled. Set ENABLE_OUTBOUND_COPILOT=true.",
            )

    def _load_outbound_snapshot(
        *,
        status_filter: Optional[str] = None,
        search: Optional[str] = None,
        min_fit_score: Optional[float] = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        conn = db.get_connection(db_path)
        try:
            items = db.list_outbound_companies(
                conn,
                status=status_filter,
                search=search,
                min_fit_score=min_fit_score,
                limit=max(1, min(limit, 500)),
            )
            proposals = db.list_outbound_proposals(conn, limit=max(1, min(limit, 500)))
            return {
                "items": items,
                "proposals": proposals,
                "stats": {
                    "total": len(items),
                    "new": sum(1 for row in items if str(row.get("status") or "") == "new"),
                    "qualified": sum(1 for row in items if str(row.get("status") or "") == "qualified"),
                    "proposal_ready": sum(1 for row in items if str(row.get("status") or "") == "proposal_ready"),
                    "in_progress": sum(1 for row in items if str(row.get("status") or "") == "in_progress"),
                    "won": sum(1 for row in items if str(row.get("status") or "") == "won"),
                },
            }
        finally:
            conn.close()

    @router.get("/admin/outbound")
    async def admin_outbound(
        _: str = Depends(require_admin_dependency),
        status: str = "",
        search: str = "",
        min_fit_score: float = 0.0,
        limit: int = 100,
    ):
        _require_enabled()
        snapshot = _load_outbound_snapshot(
            status_filter=status or None,
            search=search or None,
            min_fit_score=min_fit_score if min_fit_score > 0 else None,
            limit=limit,
        )
        return {
            "ok": True,
            "help": _OUTBOUND_HELP_TEXT,
            **snapshot,
        }

    @router.post("/admin/outbound/companies")
    async def admin_outbound_create_company(
        payload: OutboundCompanyCreatePayload,
        _: str = Depends(require_admin_dependency),
    ):
        _require_enabled()
        company = payload.model_dump()
        fit = score_company_fit(company)
        conn = db.get_connection(db_path)
        try:
            duplicate = db.find_outbound_company_duplicate(
                conn,
                company_name=company["company_name"],
                website=company.get("website"),
                city=company.get("city"),
            )
            deduplicated = False
            if isinstance(duplicate, dict):
                company_id = int(duplicate["id"])
                db.update_outbound_company(
                    conn,
                    company_id=company_id,
                    website=company.get("website"),
                    city=company.get("city"),
                    segment=company.get("segment"),
                    owner=company.get("owner"),
                    note=company.get("note"),
                    fit_score=max(float(fit["score"]), float(duplicate.get("fit_score") or 0.0)),
                    fit_tags=[str(item) for item in (fit.get("tags") or [])],
                    fit_reason=str(fit.get("reason") or ""),
                )
                db.log_outbound_event(
                    conn,
                    company_id=company_id,
                    event_type="company_deduplicated",
                    actor="admin:api",
                    payload={"source": company.get("source") or "manual"},
                )
                deduplicated = True
            else:
                company_id = db.create_outbound_company(
                    conn,
                    company_name=company["company_name"],
                    website=company.get("website"),
                    city=company.get("city"),
                    segment=company.get("segment"),
                    source=company.get("source") or "manual",
                    owner=company.get("owner"),
                    note=company.get("note"),
                    fit_score=float(fit["score"]),
                    fit_tags=[str(item) for item in (fit.get("tags") or [])],
                    fit_reason=str(fit.get("reason") or ""),
                    created_by="admin:api",
                )
                db.log_outbound_event(
                    conn,
                    company_id=company_id,
                    event_type="company_created",
                    actor="admin:api",
                    payload={"source": company.get("source") or "manual"},
                )
            item = db.get_outbound_company(conn, company_id=company_id)
        finally:
            conn.close()
        return {"ok": True, "company": item, "deduplicated": deduplicated}

    @router.post("/admin/outbound/import-csv")
    async def admin_outbound_import_csv(
        _: str = Depends(require_admin_dependency),
        file: UploadFile = File(...),
        source: str = Form(default="csv_import"),
    ) -> OutboundImportResult:
        _require_enabled()
        raw_bytes = await file.read()
        if not raw_bytes:
            raise HTTPException(status_code=400, detail="CSV file is empty")
        try:
            content = raw_bytes.decode("utf-8")
        except UnicodeDecodeError:
            content = raw_bytes.decode("utf-8-sig", errors="ignore")
        rows = parse_outbound_companies_csv(content, source=(source or "csv_import"))
        conn = db.get_connection(db_path)
        imported_ids: list[int] = []
        skipped = 0
        try:
            for item in rows:
                duplicate = db.find_outbound_company_duplicate(
                    conn,
                    company_name=item.get("company_name") or "",
                    website=item.get("website"),
                    city=item.get("city"),
                )
                if isinstance(duplicate, dict):
                    skipped += 1
                    db.log_outbound_event(
                        conn,
                        company_id=int(duplicate["id"]),
                        event_type="company_deduplicated",
                        actor="admin:csv",
                        payload={"source": source or "csv_import"},
                    )
                    continue
                fit = score_company_fit(item)
                company_id = db.create_outbound_company(
                    conn,
                    company_name=item["company_name"],
                    website=item.get("website"),
                    city=item.get("city"),
                    segment=item.get("segment"),
                    source=item.get("source") or "csv_import",
                    owner=item.get("owner"),
                    note=item.get("note"),
                    fit_score=float(fit["score"]),
                    fit_tags=[str(tag) for tag in (fit.get("tags") or [])],
                    fit_reason=str(fit.get("reason") or ""),
                    created_by="admin:csv",
                )
                imported_ids.append(company_id)
            db.log_outbound_event(
                conn,
                event_type="csv_import",
                actor="admin:api",
                payload={
                    "filename": file.filename,
                    "imported": len(imported_ids),
                    "parsed": len(rows),
                    "skipped": skipped,
                    "source": source or "csv_import",
                },
            )
        finally:
            conn.close()
        return OutboundImportResult(imported=len(imported_ids), skipped=skipped, company_ids=imported_ids)

    @router.get("/admin/outbound/companies/{company_id}")
    async def admin_outbound_company_detail(company_id: int, _: str = Depends(require_admin_dependency)):
        _require_enabled()
        conn = db.get_connection(db_path)
        try:
            company = db.get_outbound_company(conn, company_id=int(company_id))
            if not isinstance(company, dict):
                raise HTTPException(status_code=404, detail="Outbound company not found")
            proposals = db.list_outbound_proposals(conn, company_id=int(company_id), limit=100)
            events = db.list_outbound_events(conn, company_id=int(company_id), limit=200)
        finally:
            conn.close()
        return {"ok": True, "company": company, "proposals": proposals, "events": events}

    @router.patch("/admin/outbound/companies/{company_id}/status")
    async def admin_outbound_company_status(
        company_id: int,
        payload: OutboundStatusPayload,
        _: str = Depends(require_admin_dependency),
    ):
        _require_enabled()
        conn = db.get_connection(db_path)
        try:
            company = db.get_outbound_company(conn, company_id=int(company_id))
            if not isinstance(company, dict):
                raise HTTPException(status_code=404, detail="Outbound company not found")
            updated_ok = db.update_outbound_company(conn, company_id=int(company_id), status=payload.status)
            if not updated_ok:
                db.log_outbound_event(
                    conn,
                    company_id=int(company_id),
                    event_type="company_status_rejected",
                    actor=payload.actor,
                    payload={
                        "current_status": str(company.get("status") or "new"),
                        "target_status": payload.status,
                    },
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "invalid_company_status_transition",
                        "message": "Invalid outbound company status transition.",
                        "current_status": str(company.get("status") or "new"),
                        "target_status": payload.status,
                    },
                )
            db.log_outbound_event(
                conn,
                company_id=int(company_id),
                event_type="company_status_changed",
                actor=payload.actor,
                payload={"status": payload.status},
            )
            updated = db.get_outbound_company(conn, company_id=int(company_id))
        finally:
            conn.close()
        return {"ok": True, "company": updated}

    @router.post("/admin/outbound/companies/{company_id}/score")
    async def admin_outbound_company_score(
        company_id: int,
        payload: OutboundScorePayload,
        _: str = Depends(require_admin_dependency),
    ):
        _require_enabled()
        conn = db.get_connection(db_path)
        try:
            company = db.get_outbound_company(conn, company_id=int(company_id))
            if not isinstance(company, dict):
                raise HTTPException(status_code=404, detail="Outbound company not found")
            fit = score_company_fit(company, campaign_tags=payload.campaign_tags)
            db.update_outbound_company(
                conn,
                company_id=int(company_id),
                fit_score=float(fit["score"]),
                fit_tags=[str(tag) for tag in (fit.get("tags") or [])],
                fit_reason=str(fit.get("reason") or ""),
                status="qualified" if float(fit.get("score") or 0) >= 45 else company.get("status"),
            )
            db.log_outbound_event(
                conn,
                company_id=int(company_id),
                event_type="company_scored",
                actor="admin:api",
                payload=fit,
            )
            updated = db.get_outbound_company(conn, company_id=int(company_id))
        finally:
            conn.close()
        return {"ok": True, "fit": fit, "company": updated}

    @router.post("/admin/outbound/companies/{company_id}/proposal")
    async def admin_outbound_company_proposal(
        company_id: int,
        payload: OutboundProposalPayload,
        _: str = Depends(require_admin_dependency),
    ):
        _require_enabled()
        conn = db.get_connection(db_path)
        try:
            company = db.get_outbound_company(conn, company_id=int(company_id))
            if not isinstance(company, dict):
                raise HTTPException(status_code=404, detail="Outbound company not found")

            open_proposals = db.count_outbound_company_open_proposals(conn, company_id=int(company_id))
            recent_touches = db.count_outbound_company_recent_touches(
                conn,
                company_id=int(company_id),
                within_hours=OUTBOUND_TOUCH_WINDOW_HOURS,
            )
            guard = evaluate_outbound_proposal_guard(
                company_status=str(company.get("status") or "new"),
                open_proposals=open_proposals,
                recent_touches=recent_touches,
                max_open_proposals=OUTBOUND_MAX_OPEN_PROPOSALS,
                max_recent_touches=OUTBOUND_MAX_RECENT_TOUCHES,
            )
            if not guard.allowed:
                db.log_outbound_event(
                    conn,
                    company_id=int(company_id),
                    event_type="proposal_rejected",
                    actor=payload.created_by,
                    payload={
                        "reason_code": guard.reason_code,
                        "open_proposals": open_proposals,
                        "recent_touches": recent_touches,
                    },
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": guard.reason_code,
                        "message": guard.reason_text,
                        "open_proposals": open_proposals,
                        "recent_touches": recent_touches,
                        "touch_window_hours": OUTBOUND_TOUCH_WINDOW_HOURS,
                    },
                )

            fit = {
                "score": float(company.get("fit_score") or 0),
                "tags": [str(item) for item in (company.get("fit_tags") or [])],
                "reason": str(company.get("fit_reason") or ""),
            }
            if fit["score"] <= 0:
                fit = score_company_fit(company)
                db.update_outbound_company(
                    conn,
                    company_id=int(company_id),
                    fit_score=float(fit["score"]),
                    fit_tags=[str(tag) for tag in (fit.get("tags") or [])],
                    fit_reason=str(fit.get("reason") or ""),
                )
            draft = build_outbound_proposal(company, fit=fit, offer_focus=payload.offer_focus)
            proposal_id = db.create_outbound_proposal(
                conn,
                company_id=int(company_id),
                short_message=draft["short_message"],
                proposal_text=draft["proposal_text"],
                offer_type="education_program",
                status="draft",
                model_name=draft.get("model_name") or "outbound_copilot_v1",
                created_by=payload.created_by,
            )
            db.update_outbound_company(conn, company_id=int(company_id), status="proposal_ready")
            db.log_outbound_event(
                conn,
                company_id=int(company_id),
                proposal_id=proposal_id,
                event_type="proposal_created",
                actor=payload.created_by,
                payload={"offer_focus": payload.offer_focus},
            )
            proposal = db.get_outbound_proposal(conn, proposal_id=proposal_id)
            company_updated = db.get_outbound_company(conn, company_id=int(company_id))
        finally:
            conn.close()
        return {"ok": True, "proposal": proposal, "company": company_updated}

    @router.post("/admin/outbound/proposals/{proposal_id}/approve")
    async def admin_outbound_proposal_approve(
        proposal_id: int,
        payload: OutboundProposalApprovePayload,
        _: str = Depends(require_admin_dependency),
    ):
        _require_enabled()
        conn = db.get_connection(db_path)
        try:
            proposal = db.get_outbound_proposal(conn, proposal_id=int(proposal_id))
            if not isinstance(proposal, dict):
                raise HTTPException(status_code=404, detail="Outbound proposal not found")
            updated_ok = db.update_outbound_proposal_status(
                conn,
                proposal_id=int(proposal_id),
                status="approved",
                actor=payload.actor,
            )
            if not updated_ok:
                db.log_outbound_event(
                    conn,
                    company_id=int(proposal["company_id"]),
                    proposal_id=int(proposal_id),
                    event_type="proposal_approve_rejected",
                    actor=payload.actor,
                    payload={"current_status": str(proposal.get("status") or "draft")},
                )
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail={
                        "code": "invalid_proposal_status_transition",
                        "message": "Proposal cannot be approved from current status.",
                        "current_status": str(proposal.get("status") or "draft"),
                        "target_status": "approved",
                    },
                )
            db.log_outbound_event(
                conn,
                company_id=int(proposal["company_id"]),
                proposal_id=int(proposal_id),
                event_type="proposal_approved",
                actor=payload.actor,
                payload={},
            )
            updated = db.get_outbound_proposal(conn, proposal_id=int(proposal_id))
        finally:
            conn.close()
        return {"ok": True, "proposal": updated}

    @router.get("/admin/ui/outbound", response_class=HTMLResponse)
    async def admin_outbound_ui(
        _: str = Depends(require_admin_dependency),
        status: str = "",
        search: str = "",
        min_fit_score: float = 0.0,
        limit: int = 100,
    ):
        _require_enabled()
        snapshot = _load_outbound_snapshot(
            status_filter=status or None,
            search=search or None,
            min_fit_score=min_fit_score if min_fit_score > 0 else None,
            limit=limit,
        )
        rows: list[str] = []
        for item in snapshot["items"]:
            company_id = int(item["id"])
            fit_score = float(item.get("fit_score") or 0)
            status_value = html.escape(str(item.get("status") or "new"))
            tags = ", ".join(str(tag) for tag in (item.get("fit_tags") or []) if str(tag))
            rows.append(
                "<tr>"
                f"<td>{company_id}</td>"
                f"<td><a href='/admin/outbound/companies/{company_id}'>{html.escape(str(item.get('company_name') or ''))}</a><br/><span class='muted'>{html.escape(str(item.get('website') or '-'))}</span></td>"
                f"<td>{html.escape(str(item.get('city') or '-'))}</td>"
                f"<td>{html.escape(str(item.get('segment') or '-'))}</td>"
                f"<td>{fit_score:.1f}<br/><span class='muted'>{html.escape(tags or '-')}</span></td>"
                f"<td>{status_value}</td>"
                "<td>"
                f"<form method='post' action='/admin/ui/outbound/companies/{company_id}/score' style='display:inline-block; margin:0 4px 4px 0;'>"
                "<button type='submit'>Score</button>"
                "</form>"
                f"<form method='post' action='/admin/ui/outbound/companies/{company_id}/proposal' style='display:inline-block; margin:0 4px 4px 0;'>"
                "<button type='submit'>Draft КП</button>"
                "</form>"
                f"<form method='post' action='/admin/ui/outbound/companies/{company_id}/status' style='display:inline-block;'>"
                "<select name='new_status'>"
                "<option value='new'>new</option>"
                "<option value='qualified'>qualified</option>"
                "<option value='proposal_ready'>proposal_ready</option>"
                "<option value='in_progress'>in_progress</option>"
                "<option value='won'>won</option>"
                "<option value='lost'>lost</option>"
                "</select>"
                "<button type='submit'>Set</button>"
                "</form>"
                "</td>"
                "</tr>"
            )

        proposals_rows: list[str] = []
        for item in snapshot["proposals"][:25]:
            proposal_id = int(item["id"])
            proposal_status = html.escape(str(item.get("status") or "draft"))
            proposals_rows.append(
                "<tr>"
                f"<td>{proposal_id}</td>"
                f"<td>{html.escape(str(item.get('company_name') or '-'))}</td>"
                f"<td>{proposal_status}</td>"
                f"<td>{html.escape(str(item.get('created_at') or '-'))}</td>"
                "<td>"
                f"<form method='post' action='/admin/ui/outbound/proposals/{proposal_id}/approve'>"
                "<button type='submit'>Approve</button>"
                "</form>"
                "</td>"
                "</tr>"
            )

        body = (
            "<h1>Outbound Copilot</h1>"
            f"<p class='muted'>{html.escape(_OUTBOUND_HELP_TEXT)}</p>"
            "<div class='card'>"
            f"<b>Компаний:</b> {int(snapshot['stats']['total'])} &nbsp;"
            f"<b>new:</b> {int(snapshot['stats']['new'])} &nbsp;"
            f"<b>qualified:</b> {int(snapshot['stats']['qualified'])} &nbsp;"
            f"<b>proposal_ready:</b> {int(snapshot['stats']['proposal_ready'])} &nbsp;"
            f"<b>in_progress:</b> {int(snapshot['stats']['in_progress'])} &nbsp;"
            f"<b>won:</b> {int(snapshot['stats']['won'])}"
            "</div>"
            "<div class='card'>"
            "<h2>Добавить компанию</h2>"
            "<form method='post' action='/admin/ui/outbound/companies/create'>"
            "<label>Название</label><br/><input name='company_name' required style='min-width: 320px;'/><br/><br/>"
            "<label>Сайт</label><br/><input name='website' style='min-width: 320px;'/><br/><br/>"
            "<label>Город</label><br/><input name='city'/><br/><br/>"
            "<label>Сегмент</label><br/><input name='segment'/><br/><br/>"
            "<label>Owner</label><br/><input name='owner'/><br/><br/>"
            "<label>Note</label><br/><textarea name='note' rows='2' style='min-width: 320px;'></textarea><br/><br/>"
            "<button type='submit'>Создать и оценить fit</button>"
            "</form>"
            "</div>"
            "<div class='card'>"
            "<h2>Импорт CSV</h2>"
            "<p class='muted'>Колонки: <code>company_name</code> (или <code>name</code>), <code>website</code>, <code>city</code>, <code>segment</code>, <code>note</code>, <code>owner</code>.</p>"
            "<form method='post' action='/admin/ui/outbound/import-csv' enctype='multipart/form-data'>"
            "<input type='file' name='file' accept='.csv,text/csv' required/>"
            "<button type='submit'>Импортировать</button>"
            "</form>"
            "</div>"
            "<h2>Компании</h2>"
            "<table><thead><tr><th>ID</th><th>Компания</th><th>Город</th><th>Сегмент</th><th>Fit</th><th>Status</th><th>Действия</th></tr></thead>"
            f"<tbody>{''.join(rows) if rows else '<tr><td colspan=7>Нет компаний</td></tr>'}</tbody></table>"
            "<h2>Последние драфты КП</h2>"
            "<table><thead><tr><th>ID</th><th>Компания</th><th>Status</th><th>Создан</th><th>Действие</th></tr></thead>"
            f"<tbody>{''.join(proposals_rows) if proposals_rows else '<tr><td colspan=5>Нет драфтов</td></tr>'}</tbody></table>"
        )
        return render_page("Outbound Copilot", body)

    @router.post("/admin/ui/outbound/companies/create")
    async def admin_outbound_ui_create_company(
        request: Request,
        company_name: str = Form(default=""),
        website: str = Form(default=""),
        city: str = Form(default=""),
        segment: str = Form(default=""),
        owner: str = Form(default=""),
        note: str = Form(default=""),
        _: str = Depends(require_admin_dependency),
    ):
        _require_enabled()
        enforce_ui_csrf(request)
        payload = OutboundCompanyCreatePayload(
            company_name=company_name,
            website=website,
            city=city,
            segment=segment,
            owner=owner,
            note=note,
            source="manual_ui",
        )
        await admin_outbound_create_company(payload=payload, _="admin")
        return RedirectResponse(url="/admin/ui/outbound", status_code=303)

    @router.post("/admin/ui/outbound/import-csv")
    async def admin_outbound_ui_import_csv(
        request: Request,
        file: UploadFile = File(...),
        _: str = Depends(require_admin_dependency),
    ):
        _require_enabled()
        enforce_ui_csrf(request)
        await admin_outbound_import_csv(_="admin", file=file, source="csv_ui")
        return RedirectResponse(url="/admin/ui/outbound", status_code=303)

    @router.post("/admin/ui/outbound/companies/{company_id}/score")
    async def admin_outbound_ui_score(
        request: Request,
        company_id: int,
        _: str = Depends(require_admin_dependency),
    ):
        _require_enabled()
        enforce_ui_csrf(request)
        await admin_outbound_company_score(company_id=company_id, payload=OutboundScorePayload(campaign_tags=[]), _="admin")
        return RedirectResponse(url="/admin/ui/outbound", status_code=303)

    @router.post("/admin/ui/outbound/companies/{company_id}/proposal")
    async def admin_outbound_ui_proposal(
        request: Request,
        company_id: int,
        _: str = Depends(require_admin_dependency),
    ):
        _require_enabled()
        enforce_ui_csrf(request)
        await admin_outbound_company_proposal(
            company_id=company_id,
            payload=OutboundProposalPayload(),
            _="admin",
        )
        return RedirectResponse(url="/admin/ui/outbound", status_code=303)

    @router.post("/admin/ui/outbound/companies/{company_id}/status")
    async def admin_outbound_ui_status(
        request: Request,
        company_id: int,
        new_status: str = Form(default="new"),
        _: str = Depends(require_admin_dependency),
    ):
        _require_enabled()
        enforce_ui_csrf(request)
        await admin_outbound_company_status(
            company_id=company_id,
            payload=OutboundStatusPayload(status=new_status, actor="admin:ui"),
            _="admin",
        )
        return RedirectResponse(url="/admin/ui/outbound", status_code=303)

    @router.post("/admin/ui/outbound/proposals/{proposal_id}/approve")
    async def admin_outbound_ui_approve_proposal(
        request: Request,
        proposal_id: int,
        _: str = Depends(require_admin_dependency),
    ):
        _require_enabled()
        enforce_ui_csrf(request)
        await admin_outbound_proposal_approve(
            proposal_id=proposal_id,
            payload=OutboundProposalApprovePayload(actor="admin:ui"),
            _="admin",
        )
        return RedirectResponse(url="/admin/ui/outbound", status_code=303)

    return router

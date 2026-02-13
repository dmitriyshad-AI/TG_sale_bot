from __future__ import annotations

import secrets

from fastapi import Depends, FastAPI, File, HTTPException, UploadFile, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from sales_agent.sales_core.config import Settings, get_settings
from sales_agent.sales_core.copilot import create_tallanto_copilot_task, run_copilot_from_file
from sales_agent.sales_core.db import (
    get_connection,
    init_db,
    list_conversation_messages,
    list_recent_conversations,
    list_recent_leads,
)
from sales_agent.sales_core.tallanto_client import TallantoClient


security = HTTPBasic()


def create_app(settings: Settings | None = None) -> FastAPI:
    cfg = settings or get_settings()
    init_db(cfg.database_path)

    app = FastAPI(title="sales-agent")

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

    @app.get("/api/health")
    async def health():
        return {"status": "ok", "service": "sales-agent"}

    @app.get("/admin/leads")
    async def admin_leads(_: str = Depends(require_admin), limit: int = 100):
        conn = get_connection(cfg.database_path)
        try:
            return {"items": list_recent_leads(conn, limit=max(1, min(limit, 500)))}
        finally:
            conn.close()

    @app.get("/admin/conversations")
    async def admin_conversations(_: str = Depends(require_admin), limit: int = 100):
        conn = get_connection(cfg.database_path)
        try:
            return {"items": list_recent_conversations(conn, limit=max(1, min(limit, 500)))}
        finally:
            conn.close()

    @app.get("/admin/conversations/{user_id}")
    async def admin_conversation_history(user_id: int, _: str = Depends(require_admin), limit: int = 500):
        conn = get_connection(cfg.database_path)
        try:
            messages = list_conversation_messages(conn, user_id=user_id, limit=max(1, min(limit, 2000)))
            return {"user_id": user_id, "messages": messages}
        finally:
            conn.close()

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
            tallanto = TallantoClient.from_settings(cfg)
            task_result = create_tallanto_copilot_task(
                tallanto=tallanto,
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

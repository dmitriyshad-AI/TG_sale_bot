from __future__ import annotations

from pathlib import Path
from typing import Any, Callable

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse

from sales_agent.sales_core.db import (
    get_connection,
    list_conversation_messages,
    list_recent_conversations,
    list_recent_leads,
)


def build_admin_miniapp_router(
    *,
    settings: Any,
    miniapp_dir: Path,
    db_path: Path,
    require_miniapp_user: Callable[..., dict],
) -> APIRouter:
    router = APIRouter()

    @router.get("/admin/miniapp")
    async def admin_miniapp_page():
        if not settings.admin_miniapp_enabled:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Admin miniapp is disabled.")
        index_path = miniapp_dir / "index.html"
        if not index_path.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Miniapp UI is not available.")
        return FileResponse(index_path)

    @router.get("/admin/miniapp/api/me")
    async def admin_miniapp_me(auth_user: dict = Depends(require_miniapp_user)):
        return {"ok": True, "user": auth_user.get("user", {}), "user_id": auth_user["user_id"]}

    @router.get("/admin/miniapp/api/leads")
    async def admin_miniapp_leads(
        limit: int = 100,
        auth_user: dict = Depends(require_miniapp_user),
    ):
        conn = get_connection(db_path)
        try:
            items = list_recent_leads(conn, limit=max(1, min(limit, 500)))
        finally:
            conn.close()
        return {"ok": True, "requested_by": auth_user["user_id"], "items": items}

    @router.get("/admin/miniapp/api/conversations")
    async def admin_miniapp_conversations(
        limit: int = 100,
        auth_user: dict = Depends(require_miniapp_user),
    ):
        conn = get_connection(db_path)
        try:
            items = list_recent_conversations(conn, limit=max(1, min(limit, 500)))
        finally:
            conn.close()
        return {"ok": True, "requested_by": auth_user["user_id"], "items": items}

    @router.get("/admin/miniapp/api/conversations/{user_id}")
    async def admin_miniapp_conversation_history(
        user_id: int,
        limit: int = 500,
        auth_user: dict = Depends(require_miniapp_user),
    ):
        conn = get_connection(db_path)
        try:
            messages = list_conversation_messages(conn, user_id=user_id, limit=max(1, min(limit, 2000)))
        finally:
            conn.close()
        return {
            "ok": True,
            "requested_by": auth_user["user_id"],
            "user_id": user_id,
            "messages": messages,
        }

    return router

from __future__ import annotations

from contextlib import asynccontextmanager
import html
import json
import logging
import secrets

from fastapi import Depends, FastAPI, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from telegram import Update

from sales_agent.sales_bot import bot as bot_runtime
from sales_agent.sales_core.config import Settings, get_settings, project_root
from sales_agent.sales_core.copilot import run_copilot_from_file
from sales_agent.sales_core.crm import build_crm_client
from sales_agent.sales_core.db import (
    get_connection,
    init_db,
    list_conversation_messages,
    list_recent_conversations,
    list_recent_leads,
)
from sales_agent.sales_core.runtime_diagnostics import build_runtime_diagnostics
from sales_agent.sales_core.telegram_webapp import verify_telegram_webapp_init_data


security = HTTPBasic()
logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None) -> FastAPI:
    cfg = settings or get_settings()
    init_db(cfg.database_path)
    webhook_path = cfg.telegram_webhook_path if cfg.telegram_webhook_path.startswith("/") else f"/{cfg.telegram_webhook_path}"
    telegram_application = None

    if cfg.telegram_mode == "webhook":
        if not cfg.telegram_bot_token:
            logger.warning("TELEGRAM_MODE=webhook but TELEGRAM_BOT_TOKEN is empty. Webhook endpoint will return 503.")
        else:
            telegram_application = bot_runtime.build_application(cfg.telegram_bot_token)

    @asynccontextmanager
    async def lifespan(app_instance: FastAPI):
        if telegram_application is not None:
            app_instance.state.telegram_application = telegram_application
            await telegram_application.initialize()
            await telegram_application.start()
            logger.info("Telegram webhook application initialized at path: %s", webhook_path)
        yield
        if telegram_application is not None:
            await telegram_application.stop()
            await telegram_application.shutdown()
            logger.info("Telegram webhook application stopped")

    app = FastAPI(title="sales-agent", lifespan=lifespan)
    miniapp_dir = project_root() / "sales_agent" / "sales_api" / "static" / "admin_miniapp"
    if miniapp_dir.exists():
        app.mount(
            "/admin/miniapp/static",
            StaticFiles(directory=str(miniapp_dir)),
            name="admin-miniapp-static",
        )

    def require_miniapp_user(request: Request) -> dict:
        if not cfg.admin_miniapp_enabled:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Admin miniapp is disabled.")
        if not cfg.telegram_bot_token:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="TELEGRAM_BOT_TOKEN is not configured.",
            )
        if not cfg.admin_telegram_ids:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="ADMIN_TELEGRAM_IDS is not configured.",
            )

        init_data = request.headers.get("X-Telegram-Init-Data", "").strip()

        auth = verify_telegram_webapp_init_data(
            init_data=init_data,
            bot_token=cfg.telegram_bot_token,
        )
        if not auth.ok:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid Telegram miniapp auth: {auth.reason}",
            )
        if auth.user_id is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Telegram user id is missing in init data.",
            )
        if auth.user_id not in cfg.admin_telegram_ids:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="This Telegram account is not allowed.",
            )
        return {
            "user_id": auth.user_id,
            "user": auth.user or {},
        }

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

    def render_page(title: str, body_html: str) -> HTMLResponse:
        page = f"""
<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>
    body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 24px; color: #1f2937; }}
    h1, h2 {{ margin: 0 0 12px; }}
    .muted {{ color: #6b7280; }}
    nav {{ margin-bottom: 16px; }}
    nav a {{ margin-right: 12px; }}
    table {{ border-collapse: collapse; width: 100%; margin-top: 12px; }}
    th, td {{ border: 1px solid #d1d5db; padding: 8px; text-align: left; vertical-align: top; }}
    th {{ background: #f3f4f6; }}
    .card {{ border: 1px solid #d1d5db; border-radius: 8px; padding: 12px; margin-bottom: 12px; }}
    .badge {{ display: inline-block; padding: 2px 8px; border-radius: 999px; background: #eef2ff; }}
    pre {{ white-space: pre-wrap; word-break: break-word; background: #f9fafb; padding: 10px; border-radius: 6px; }}
    input, button {{ font-size: 14px; }}
    button {{ padding: 8px 12px; cursor: pointer; }}
  </style>
</head>
<body>
  <nav>
    <a href="/admin">Dashboard</a>
    <a href="/admin/ui/leads">Leads</a>
    <a href="/admin/ui/conversations">Conversations</a>
    <a href="/admin/ui/copilot">Copilot</a>
  </nav>
  {body_html}
</body>
</html>
"""
        return HTMLResponse(page)

    @app.get("/admin", response_class=HTMLResponse)
    async def admin_home(_: str = Depends(require_admin)):
        conn = get_connection(cfg.database_path)
        try:
            leads_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM leads").fetchone()["cnt"])
            users_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM users").fetchone()["cnt"])
            messages_count = int(conn.execute("SELECT COUNT(*) AS cnt FROM messages").fetchone()["cnt"])
            recent_conversations = list_recent_conversations(conn, limit=5)
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

    @app.get("/api/health")
    async def health():
        return {"status": "ok", "service": "sales-agent"}

    @app.get("/api/runtime/diagnostics")
    async def runtime_diagnostics():
        return build_runtime_diagnostics(cfg)

    @app.get("/admin/miniapp")
    async def admin_miniapp_page():
        if not cfg.admin_miniapp_enabled:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Admin miniapp is disabled.")
        index_path = miniapp_dir / "index.html"
        if not index_path.exists():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Miniapp UI is not available.")
        return FileResponse(index_path)

    @app.get("/admin/miniapp/api/me")
    async def admin_miniapp_me(auth_user: dict = Depends(require_miniapp_user)):
        return {"ok": True, "user": auth_user.get("user", {}), "user_id": auth_user["user_id"]}

    @app.get("/admin/miniapp/api/leads")
    async def admin_miniapp_leads(
        limit: int = 100,
        auth_user: dict = Depends(require_miniapp_user),
    ):
        conn = get_connection(cfg.database_path)
        try:
            items = list_recent_leads(conn, limit=max(1, min(limit, 500)))
        finally:
            conn.close()
        return {"ok": True, "requested_by": auth_user["user_id"], "items": items}

    @app.get("/admin/miniapp/api/conversations")
    async def admin_miniapp_conversations(
        limit: int = 100,
        auth_user: dict = Depends(require_miniapp_user),
    ):
        conn = get_connection(cfg.database_path)
        try:
            items = list_recent_conversations(conn, limit=max(1, min(limit, 500)))
        finally:
            conn.close()
        return {"ok": True, "requested_by": auth_user["user_id"], "items": items}

    @app.get("/admin/miniapp/api/conversations/{user_id}")
    async def admin_miniapp_conversation_history(
        user_id: int,
        limit: int = 500,
        auth_user: dict = Depends(require_miniapp_user),
    ):
        conn = get_connection(cfg.database_path)
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

    @app.post(webhook_path)
    async def telegram_webhook(request: Request):
        if cfg.telegram_mode != "webhook":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Webhook endpoint is disabled. Set TELEGRAM_MODE=webhook.",
            )
        if not cfg.telegram_bot_token:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="TELEGRAM_BOT_TOKEN is not configured.",
            )
        if cfg.telegram_webhook_secret:
            header_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
            if not secrets.compare_digest(header_secret, cfg.telegram_webhook_secret):
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid webhook secret token.")

        payload = await request.json()
        if not isinstance(payload, dict):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Telegram payload.")

        telegram_application = getattr(app.state, "telegram_application", None)
        if telegram_application is None:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Telegram webhook application is not initialized.",
            )

        update = Update.de_json(payload, telegram_application.bot)
        if update is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Could not parse Telegram update payload.",
            )

        await telegram_application.process_update(update)
        return {"ok": True}

    @app.get("/admin/leads")
    async def admin_leads(_: str = Depends(require_admin), limit: int = 100):
        conn = get_connection(cfg.database_path)
        try:
            return {"items": list_recent_leads(conn, limit=max(1, min(limit, 500)))}
        finally:
            conn.close()

    @app.get("/admin/ui/leads", response_class=HTMLResponse)
    async def admin_leads_ui(_: str = Depends(require_admin), limit: int = 100):
        conn = get_connection(cfg.database_path)
        try:
            items = list_recent_leads(conn, limit=max(1, min(limit, 500)))
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

    @app.get("/admin/conversations")
    async def admin_conversations(_: str = Depends(require_admin), limit: int = 100):
        conn = get_connection(cfg.database_path)
        try:
            return {"items": list_recent_conversations(conn, limit=max(1, min(limit, 500)))}
        finally:
            conn.close()

    @app.get("/admin/ui/conversations", response_class=HTMLResponse)
    async def admin_conversations_ui(_: str = Depends(require_admin), limit: int = 100):
        conn = get_connection(cfg.database_path)
        try:
            items = list_recent_conversations(conn, limit=max(1, min(limit, 500)))
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

    @app.get("/admin/conversations/{user_id}")
    async def admin_conversation_history(user_id: int, _: str = Depends(require_admin), limit: int = 500):
        conn = get_connection(cfg.database_path)
        try:
            messages = list_conversation_messages(conn, user_id=user_id, limit=max(1, min(limit, 2000)))
            return {"user_id": user_id, "messages": messages}
        finally:
            conn.close()

    @app.get("/admin/ui/conversations/{user_id}", response_class=HTMLResponse)
    async def admin_conversation_history_ui(user_id: int, _: str = Depends(require_admin), limit: int = 500):
        conn = get_connection(cfg.database_path)
        try:
            messages = list_conversation_messages(conn, user_id=user_id, limit=max(1, min(limit, 2000)))
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

    @app.get("/admin/ui/copilot", response_class=HTMLResponse)
    async def admin_copilot_ui(_: str = Depends(require_admin)):
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

    @app.post("/admin/ui/copilot/import", response_class=HTMLResponse)
    async def admin_copilot_import_ui(
        _: str = Depends(require_admin),
        file: UploadFile = File(...),
        create_task: bool = Form(False),
    ):
        content = await file.read()
        if not content:
            return render_page("Copilot Error", "<h1>Ошибка</h1><p>Файл пустой.</p>")

        try:
            result = run_copilot_from_file(filename=file.filename or "dialog.txt", content=content)
        except ValueError as exc:
            return render_page("Copilot Error", f"<h1>Ошибка</h1><p>{html.escape(str(exc))}</p>")

        task_html = ""
        if create_task:
            crm = build_crm_client(cfg)
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
            crm = build_crm_client(cfg)
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

    return app


app = create_app()

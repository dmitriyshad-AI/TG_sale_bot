from __future__ import annotations

import html
from pathlib import Path
from typing import Any, Callable, Optional

from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse

from sales_agent.sales_api.services.runtime_metrics import build_runtime_enrichment
from sales_agent.sales_core.catalog import (
    CatalogValidationError,
    SearchCriteria,
    explain_match,
    select_top_products,
)
from sales_agent.sales_core.db import get_connection
from sales_agent.sales_core.runtime_diagnostics import build_runtime_diagnostics
from sales_agent.sales_core.telegram_webapp import verify_telegram_webapp_init_data


def build_public_api_router(
    *,
    settings: Any,
    extract_tg_init_data: Callable[[Request], str],
    safe_user_payload: Callable[[dict | None], dict],
    evaluate_match_quality: Callable[[SearchCriteria, list[Any]], str],
    build_manager_offer: Callable[[str, bool], dict[str, object]],
    format_price_text: Callable[[object], str],
    format_next_start_text: Callable[[object], str],
    render_page: Callable[[str, str], HTMLResponse],
    user_webapp_ready: bool,
    user_webapp_dist: Path,
    mango_webhook_path: str,
    mango_ingest_enabled: Callable[[], bool],
) -> APIRouter:
    router = APIRouter()

    @router.get("/api/health")
    async def health():
        return {"status": "ok", "service": "sales-agent"}

    @router.get("/api/auth/whoami")
    async def auth_whoami(request: Request):
        init_data = extract_tg_init_data(request)
        if not init_data:
            return {"ok": False, "reason": "not_in_telegram", "user": None}
        if not settings.telegram_bot_token:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="TELEGRAM_BOT_TOKEN is not configured.",
            )

        auth = verify_telegram_webapp_init_data(
            init_data=init_data,
            bot_token=settings.telegram_bot_token,
        )
        if not auth.ok:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid Telegram miniapp auth: {auth.reason}",
            )

        user = safe_user_payload(auth.user)
        if user.get("id") is None and auth.user_id is not None:
            user["id"] = auth.user_id
        return {"ok": True, "user": user}

    @router.get("/api/catalog/search")
    async def catalog_search(
        brand: Optional[str] = None,
        grade: Optional[int] = Query(default=None, ge=1, le=11),
        goal: Optional[str] = None,
        subject: Optional[str] = None,
        format: Optional[str] = None,
    ):
        criteria = SearchCriteria(
            brand=brand,
            grade=grade,
            goal=goal,
            subject=subject,
            format=format,
        )
        try:
            products = select_top_products(
                criteria,
                path=settings.catalog_path,
                top_k=3,
                brand_default=settings.brand_default,
            )
        except CatalogValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Catalog validation error: {exc}",
            ) from exc
        except FileNotFoundError as exc:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Catalog file not found: {settings.catalog_path}",
            ) from exc

        match_quality = evaluate_match_quality(criteria, products)
        manager_offer = build_manager_offer(match_quality, has_results=bool(products))
        items = []
        for product in products:
            items.append(
                {
                    "id": product.id,
                    "title": product.title,
                    "url": str(product.url),
                    "usp": list(product.usp[:3]),
                    "price_text": format_price_text(product),
                    "next_start_text": format_next_start_text(product),
                    "why_match": explain_match(product, criteria),
                }
            )

        return {
            "ok": True,
            "criteria": {
                "brand": brand or settings.brand_default,
                "grade": grade,
                "goal": goal,
                "subject": subject,
                "format": format,
            },
            "count": len(items),
            "items": items,
            "match_quality": match_quality,
            "manager_recommended": bool(manager_offer.get("recommended")),
            "manager_message": str(manager_offer.get("message") or ""),
            "manager_call_to_action": str(manager_offer.get("call_to_action") or ""),
        }

    @router.get("/api/runtime/diagnostics")
    async def runtime_diagnostics():
        payload = build_runtime_diagnostics(settings)
        conn = get_connection(settings.database_path)
        try:
            payload.setdefault("runtime", {})
            payload["runtime"].update(
                build_runtime_enrichment(
                    conn=conn,
                    settings=settings,
                    mango_webhook_path=mango_webhook_path,
                    mango_ingest_enabled=mango_ingest_enabled,
                )
            )
        finally:
            conn.close()
        return payload

    @router.get("/api/miniapp/meta")
    async def miniapp_meta():
        manager_chat_url = settings.sales_manager_chat_url.strip()
        user_miniapp_url = settings.user_webapp_url.strip() or "/app"
        if user_miniapp_url and not (
            user_miniapp_url.startswith("http://")
            or user_miniapp_url.startswith("https://")
            or user_miniapp_url.startswith("/")
        ):
            user_miniapp_url = f"/{user_miniapp_url}"

        return {
            "ok": True,
            "brand_name": settings.miniapp_brand_name,
            "advisor_name": settings.miniapp_advisor_name,
            "manager_label": settings.sales_manager_label,
            "manager_chat_url": manager_chat_url,
            "user_miniapp_url": user_miniapp_url,
        }

    @router.get("/")
    async def root():
        app_status = "ready" if user_webapp_ready else "build-required"
        return {
            "status": "ok",
            "service": "sales-agent",
            "user_miniapp": {
                "status": app_status,
                "entrypoint": "/app",
            },
        }

    if not user_webapp_ready:

        @router.get("/app", response_class=HTMLResponse)
        async def user_miniapp_placeholder():
            body = (
                "<h1>User Mini App is not built yet</h1>"
                "<p>Run:</p>"
                "<pre>cd webapp\nnpm install\nnpm run build</pre>"
                f"<p>Expected dist path: <code>{html.escape(str(user_webapp_dist))}</code></p>"
            )
            return render_page("Mini App Build Required", body)

    return router

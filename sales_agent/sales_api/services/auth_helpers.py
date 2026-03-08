from __future__ import annotations

import secrets
from typing import Any, Callable, Dict
from urllib.parse import urlparse

from fastapi import HTTPException, Request, status


def require_admin_credentials(
    *,
    username: str,
    password: str,
    expected_username: str,
    expected_password: str,
) -> str:
    if not expected_username or not expected_password:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Admin auth is not configured. Set ADMIN_USER and ADMIN_PASS.",
        )

    user_ok = secrets.compare_digest(username, expected_username)
    pass_ok = secrets.compare_digest(password, expected_password)
    if not (user_ok and pass_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid admin credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return username


def enforce_admin_ui_csrf(
    request: Request,
    *,
    csrf_enabled: bool,
    admin_webapp_url: str,
) -> None:
    if not csrf_enabled:
        return
    origin = request.headers.get("Origin", "").strip()
    referer = request.headers.get("Referer", "").strip()
    source = origin or referer
    if not source:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Missing Origin/Referer for admin UI POST request.",
        )

    parsed = urlparse(source)
    source_host = (parsed.hostname or "").strip().lower()
    if not source_host:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid Origin/Referer host for admin UI POST request.",
        )

    allowed_hosts: set[str] = set()
    request_host = (request.url.hostname or "").strip().lower()
    if request_host:
        allowed_hosts.add(request_host)
    app_host = (urlparse(str(request.base_url)).hostname or "").strip().lower()
    if app_host:
        allowed_hosts.add(app_host)
    if admin_webapp_url:
        allowed_hosts.add((urlparse(admin_webapp_url).hostname or "").strip().lower())

    if source_host not in {host for host in allowed_hosts if host}:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="CSRF check failed for admin UI POST request.",
        )


def require_assistant_access(
    request: Request,
    *,
    telegram_bot_token: str,
    assistant_api_token: str,
    assistant_api_token_header: str,
    extract_tg_init_data: Callable[[Request], str],
    extract_bearer_token: Callable[[Request], str],
    verify_telegram_auth: Callable[[str, str], Any],
) -> Dict[str, Any]:
    init_data = extract_tg_init_data(request)
    if init_data:
        if not telegram_bot_token:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Assistant auth via Telegram is unavailable: TELEGRAM_BOT_TOKEN is not configured.",
            )
        auth = verify_telegram_auth(init_data, telegram_bot_token)
        if not auth.ok:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=f"Invalid Telegram miniapp auth: {auth.reason}",
            )
        return {
            "kind": "telegram",
            "user_id": auth.user_id,
        }

    provided_token = request.headers.get(assistant_api_token_header, "").strip() or extract_bearer_token(request)
    expected_token = assistant_api_token.strip()
    if expected_token and secrets.compare_digest(provided_token, expected_token):
        return {"kind": "service_token", "user_id": None}

    if expected_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=(
                "Assistant auth is required. Provide Telegram initData or "
                f"{assistant_api_token_header}."
            ),
        )
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Assistant endpoint is available from Telegram Mini App only.",
    )


from __future__ import annotations

from typing import Any, Callable, Optional

from fastapi import HTTPException, Request, status

FORWARDED_FOR_HEADER = "X-Forwarded-For"


def extract_tg_init_data(request: Request) -> str:
    direct = request.headers.get("X-Tg-Init-Data", "").strip()
    if direct:
        return direct

    legacy = request.headers.get("X-Telegram-Init-Data", "").strip()
    if legacy:
        return legacy

    auth = request.headers.get("Authorization", "").strip()
    if auth.lower().startswith("tma "):
        token = auth[4:].strip()
        if token:
            return token
    return ""


def safe_user_payload(payload: dict | None) -> dict:
    if not isinstance(payload, dict):
        return {}
    user_id_raw = payload.get("id")
    user_id = int(user_id_raw) if isinstance(user_id_raw, int) else user_id_raw
    return {
        "id": user_id,
        "first_name": payload.get("first_name"),
        "last_name": payload.get("last_name"),
        "username": payload.get("username"),
        "language_code": payload.get("language_code"),
    }


def request_id_from_request(request: Request) -> str:
    request_id = getattr(request.state, "request_id", "")
    if isinstance(request_id, str) and request_id.strip():
        return request_id.strip()
    return "unknown"


def extract_bearer_token(request: Request) -> str:
    auth = request.headers.get("Authorization", "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return ""


def request_client_ip(request: Request, *, forwarded_for_header: str = FORWARDED_FOR_HEADER) -> str:
    forwarded = request.headers.get(forwarded_for_header, "").strip()
    if forwarded:
        first = forwarded.split(",", 1)[0].strip()
        if first:
            return first
    if request.client and request.client.host:
        return str(request.client.host)
    return "unknown"


def build_rate_limiter(
    *,
    backend: str,
    window_seconds: int,
    redis_url: str,
    key_prefix: str,
    redis_rate_limiter_cls: Callable[..., Any],
    in_memory_rate_limiter_cls: Callable[..., Any],
    logger: Optional[Any] = None,
) -> Any:
    normalized_backend = (backend or "").strip().lower()
    if normalized_backend == "redis":
        try:
            return redis_rate_limiter_cls(
                redis_url=redis_url,
                window_seconds=window_seconds,
                key_prefix=key_prefix,
            )
        except Exception as exc:
            if logger is not None:
                logger.warning("Redis rate limiter fallback to in-memory (%s)", exc)
    return in_memory_rate_limiter_cls(window_seconds=window_seconds)


def enforce_rate_limit(
    *,
    request: Request,
    limiter: Any,
    key: str,
    limit: int,
    scope: str,
    request_id_getter: Callable[[Request], str],
) -> None:
    decision = limiter.check(key, limit=max(1, int(limit)))
    if decision.allowed:
        return

    request_id = request_id_getter(request)
    raise HTTPException(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        detail={
            "code": "rate_limited",
            "scope": scope,
            "message": "Rate limit exceeded.",
            "user_message": "Слишком много запросов подряд. Подождите немного и повторите.",
            "retry_after_seconds": decision.retry_after_seconds,
            "request_id": request_id,
        },
        headers={"Retry-After": str(decision.retry_after_seconds)},
    )

